# Changelog

## 0.1.3 (2026-11-16)

- CI tooling (haybarn-unittest, lint helpers) runs on Python 3.14.

## 0.1.2 (2026-11-16)

- Bump vgi-go to 0.42.2.

## 0.1.1

- Catalog and schema docs are overviews instead of object lists, and schemas have their own one-line comments (vgi-lint 2.8: VGI173, VGI102).

## 0.1.0

Initial release.

- `main`: streaming, version-tolerant tables over manifest.json (v7-v12), run_results.json,
  catalog.json and sources.json; `artifact_info` triage and the `schema_versions` matrix;
  blended `descendants` / `ancestors`; `selector_match` / `parse_selector` (strict subset);
  `parse_unique_id`, `normalize_relation`, `manifest_version`; bundled `sample://` artifacts.
- `cloud`: dbt Cloud Admin API (accounts, projects, environments, jobs, runs, run_steps,
  run_artifacts) and Discovery API (models, tests, sources) via the `dbt_cloud` secret;
  `dbt-cloud://` artifact paths.
- `unified`: `runs` / `task_runs` in the shared orchestrator shape.

Targets: vgi-go 0.42 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · vgi-lint-check 2.x (unpinned)
