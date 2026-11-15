#!/usr/bin/env python3
"""Offline dbt Cloud fixture server for the SQL tests (stdlib only).

Serves the JSON fixtures in ``fixtures/`` (shaped like the documented dbt
Cloud Admin API v2/v3 and Discovery API responses; no live account was
contacted to build them) with the documented query semantics, so the
worker's pushdown parameters change what comes back:

Admin API (``Authorization: Token <token>``)
    GET /api/v2/accounts/
    GET /api/v3/accounts/<id>/projects/
    GET /api/v3/accounts/<id>/environments/       ?project_id=
    GET /api/v2/accounts/<id>/jobs/               ?project_id= &environment_id=
    GET /api/v2/accounts/<id>/runs/               ?job_definition_id= &environment_id= &project_id=
                                                  &status= &status__in=[..] &created_at__range=[lo,hi]
                                                  &include_related=[..] &order_by=-id
    GET /api/v2/accounts/<id>/runs/<run_id>/      ?include_related=["run_steps"]
    GET /api/v2/accounts/<id>/runs/<run_id>/artifacts/            (list)
    GET /api/v2/accounts/<id>/runs/<run_id>/artifacts/<path>      (file)
    GET /api/v2/accounts/<id>/jobs/<job_id>/artifacts/<path>      (latest run's file)
    Lists honour ``limit``/``offset`` but return at most MOCK_PAGE_MAX
    (default 3) items per page, so pagination is always exercised.

Discovery API (``Authorization: Bearer <token>``)
    POST /graphql   environment(id).applied.{models,tests,sources}(first, after, filter{uniqueIds})
    Pages hold at most MOCK_GQL_PAGE (default 2) nodes.

Test helpers
    GET /__requests  every request seen (method, path, query, GraphQL variables) as JSON
    GET /files/<v12/manifest.json>  a checked-in artifact over plain HTTP (no auth)
    GET /health

Switches: MOCK_API_TOKEN (required token, default "test-token");
MOCK_API_FLAKY=1 answers the first request of every distinct URL with 429 +
Retry-After: 0. Prints ``PORT:<n>`` once listening.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import sys
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

HERE = Path(__file__).resolve().parent
FIX = HERE / "fixtures"
SAMPLES = HERE.parent.parent / "internal" / "dbt" / "samples" / "jaffle_shop"
ARTIFACTS = {9001: SAMPLES / "v12"}
TOKEN = os.environ.get("MOCK_API_TOKEN", "test-token")
PAGE_MAX = int(os.environ.get("MOCK_PAGE_MAX", "3"))
GQL_PAGE = int(os.environ.get("MOCK_GQL_PAGE", "2"))


def load(name: str) -> list[dict]:
    return json.loads((FIX / f"{name}.json").read_text())


ACCOUNTS, PROJECTS, ENVS = load("accounts"), load("projects"), load("environments")
JOBS, RUNS = load("jobs"), load("runs")
DISCOVERY = {k: load(f"discovery_{k}") for k in ("models", "tests", "sources")}

_seen: set[str] = set()
_requests: list[dict] = []
_lock = threading.Lock()


def parse_ts(s: str) -> datetime:
    s = s.replace("Z", "+00:00")
    if "T" not in s and " " in s:
        s = s.replace(" ", "T", 1)
    t = datetime.fromisoformat(s)
    return t if t.tzinfo else t.replace(tzinfo=timezone.utc)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        sys.stderr.write("mock_dbt_cloud: " + (format % args) + "\n")

    def _send(self, status: int, body: object, headers: dict[str, str] | None = None) -> None:
        payload = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(payload)

    def _envelope(self, data: object, total: int | None = None) -> None:
        extra = {"pagination": {"count": len(data) if isinstance(data, list) else 1, "total_count": total or 0}}
        self._send(200, {"status": {"code": 200, "is_success": True}, "data": data, "extra": extra})

    def _gate(self, scheme: str, variables: str = "") -> bool:
        url = urlparse(self.path)
        with _lock:
            _requests.append({"method": self.command, "path": url.path, "query": url.query, "variables": variables})
        if self.headers.get("Authorization") != f"{scheme} {TOKEN}":
            self._send(401, {"status": {"code": 401, "is_success": False, "user_message": "Invalid token."}})
            return False
        if os.environ.get("MOCK_API_FLAKY") == "1":
            key = self.command + " " + self.path
            with _lock:
                if key not in _seen:
                    _seen.add(key)
                    self._send(429, {"status": {"code": 429, "user_message": "Too many requests"}}, {"Retry-After": "0"})
                    return False
        return True

    def _page(self, items: list[dict], q: dict[str, list[str]]) -> None:
        limit = min(int(q.get("limit", ["100"])[0]), 100, PAGE_MAX)
        offset = int(q.get("offset", ["0"])[0])
        self._envelope(items[offset : offset + limit], len(items))

    def do_GET(self) -> None:  # noqa: C901 - one router
        url = urlparse(self.path)
        if url.path == "/health":
            self._send(200, {"status": "ok"})
            return
        if url.path == "/__requests":
            with _lock:
                self._send(200, list(_requests))
            return
        if url.path.startswith("/files/"):
            root = SAMPLES.resolve()
            f = (root / url.path[len("/files/"):]).resolve()
            if root in f.parents and f.is_file():
                self._send(200, f.read_bytes())
            else:
                self._send(404, {"error": "not found"})
            return
        if not self._gate("Token"):
            return
        q = parse_qs(url.query)
        p = url.path
        if p == "/api/v2/accounts/":
            self._page(ACCOUNTS, q)
            return
        m = re.fullmatch(r"/api/(v2|v3)/accounts/(\d+)/(.*)", p)
        if not m or int(m.group(2)) != ACCOUNTS[0]["id"]:
            self._send(404, {"status": {"code": 404, "user_message": f"no route {p}"}})
            return
        ver, rest = m.group(1), m.group(3)

        def eq(items: list[dict], param: str, field: str) -> list[dict]:
            if param in q:
                return [i for i in items if str(i.get(field)) == q[param][0]]
            return items

        if ver == "v3" and rest == "projects/":
            self._page(PROJECTS, q)
        elif ver == "v3" and rest == "environments/":
            self._page(eq(ENVS, "project_id", "project_id"), q)
        elif ver == "v2" and rest == "jobs/":
            self._page(eq(eq(JOBS, "project_id", "project_id"), "environment_id", "environment_id"), q)
        elif ver == "v2" and rest == "runs/":
            self._page(self._runs(q), q)
        elif mm := re.fullmatch(r"runs/(\d+)/", rest):
            run = next((r for r in RUNS if r["id"] == int(mm.group(1))), None)
            if run is None:
                self._send(404, {"status": {"code": 404, "user_message": "run not found"}})
                return
            self._envelope(self._shape(run, json.loads(q.get("include_related", ["[]"])[0])))
        elif mm := re.fullmatch(r"(runs|jobs)/(\d+)/artifacts/(.*)", rest):
            self._artifact(mm.group(1), int(mm.group(2)), mm.group(3))
        else:
            self._send(404, {"status": {"code": 404, "user_message": f"no route {p}"}})

    def _shape(self, run: dict, related: list[str]) -> dict:
        return {k: v for k, v in run.items() if k not in ("job", "trigger", "run_steps") or k in related}

    def _runs(self, q: dict[str, list[str]]) -> list[dict]:
        runs = list(RUNS)
        for param, field in (("job_definition_id", "job_definition_id"), ("environment_id", "environment_id"),
                             ("project_id", "project_id"), ("status", "status")):
            if param in q:
                runs = [r for r in runs if str(r[field]) == q[param][0]]
        if "status__in" in q:
            wanted = set(json.loads(q["status__in"][0]))
            runs = [r for r in runs if r["status"] in wanted]
        if "created_at__range" in q:
            lo, hi = (parse_ts(x) for x in json.loads(q["created_at__range"][0]))
            runs = [r for r in runs if lo <= parse_ts(r["created_at"]) <= hi]
        order = q.get("order_by", ["id"])[0]
        runs.sort(key=lambda r: r["id"], reverse=order.startswith("-"))
        related = json.loads(q.get("include_related", ["[]"])[0])
        return [self._shape(r, related) for r in runs]

    def _artifact(self, kind: str, ident: int, path: str) -> None:
        if kind == "jobs":
            done = [r for r in RUNS if r["job_definition_id"] == ident and r["artifacts_saved"]]
            ident = max((r["id"] for r in done), default=0)
        root = ARTIFACTS.get(ident)
        if root is None:
            self._send(404, {"status": {"code": 404, "user_message": "No artifacts for this run."}})
            return
        if path == "":
            self._envelope(sorted(f.name for f in root.iterdir() if f.suffix == ".json"))
            return
        f = root / path
        if not f.is_file() or f.parent != root:
            self._send(404, {"status": {"code": 404, "user_message": f"artifact {path} not found"}})
            return
        self._send(200, f.read_bytes())

    def do_POST(self) -> None:
        url = urlparse(self.path)
        if url.path != "/graphql":
            self._send(404, {"errors": [{"message": "not found"}]})
            return
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length) or b"{}")
        query, variables = body.get("query", ""), body.get("variables", {})
        if not self._gate("Bearer", json.dumps(variables, sort_keys=True)):
            return
        m = re.search(r"applied\s*{\s*(models|tests|sources)\s*\(", query)
        if not m or variables.get("environmentId") != 202:
            self._send(200, {"data": {"environment": None}, "errors": [{"message": "unknown environment or connection"}]})
            return
        conn = m.group(1)
        nodes = DISCOVERY[conn]
        ids = ((variables.get("filter") or {}).get("uniqueIds"))
        if ids:
            nodes = [n for n in nodes if n["uniqueId"] in ids]
        start = int(variables.get("after") or 0)
        size = min(int(variables.get("first", 100)), GQL_PAGE)
        chunk = nodes[start : start + size]
        more = start + size < len(nodes)
        page = {"pageInfo": {"hasNextPage": more, "endCursor": str(start + size) if more else None},
                "edges": [{"node": n} for n in chunk], "totalCount": len(nodes)}
        self._send(200, {"data": {"environment": {"applied": {conn: page}}}})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"PORT:{server.server_address[1]}", flush=True)
    with contextlib.suppress(KeyboardInterrupt):
        server.serve_forever()


if __name__ == "__main__":
    main()
