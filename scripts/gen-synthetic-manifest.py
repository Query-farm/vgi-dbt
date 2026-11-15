#!/usr/bin/env python3
"""Write a large synthetic dbt manifest (v12 shape) for the benchmark.

    python3 scripts/gen-synthetic-manifest.py /tmp/big_manifest.json --nodes 5000 --code-kb 30

Each model node is a copy of the jaffle_shop `orders` node with a fresh
unique_id, a chained depends_on, and raw_code/compiled_code padded to
--code-kb kilobytes each (5,000 nodes x 2 x 30 KB is about 300 MB). The
file is not checked in: generate it, or download it from a release.
"""

import argparse
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--nodes", type=int, default=5000)
    ap.add_argument("--code-kb", type=int, default=30)
    args = ap.parse_args()
    src = json.loads((HERE.parent / "internal/dbt/samples/jaffle_shop/v12/manifest.json").read_text())
    template = src["nodes"]["model.jaffle_shop.orders"]
    pad = ("select order_id, customer_id, amount from upstream where amount > 0 union all\n" * 1024)[: args.code_kb * 1024]
    with open(args.out, "w") as f:
        f.write('{"metadata": ' + json.dumps(src["metadata"]) + ', "nodes": {')
        parents, children = {}, {}
        for i in range(args.nodes):
            uid = f"model.big.m{i:05d}"
            node = dict(template, unique_id=uid, name=f"m{i:05d}", package_name="big",
                        fqn=["big", f"layer{i % 10}", f"m{i:05d}"], raw_code=pad, compiled_code=pad,
                        depends_on={"macros": [], "nodes": [f"model.big.m{i - 1:05d}"] if i else []})
            parents[uid] = node["depends_on"]["nodes"]
            children.setdefault(uid, [])
            if i:
                children[f"model.big.m{i - 1:05d}"].append(uid)
            f.write(("," if i else "") + json.dumps(uid) + ": " + json.dumps(node))
        f.write('}, "sources": {}, "macros": {}, "exposures": {}, "metrics": {}, "groups": {}, "selectors": {}, "disabled": {},')
        f.write(' "parent_map": ' + json.dumps(parents) + ', "child_map": ' + json.dumps(children) + "}")


if __name__ == "__main__":
    main()
