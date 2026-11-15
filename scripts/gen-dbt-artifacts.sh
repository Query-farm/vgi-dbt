#!/usr/bin/env bash
# Regenerate the checked-in dbt artifact fixtures by running real dbt-core +
# dbt-duckdb on test/dbt_project in a throwaway uv environment.
#
#   scripts/gen-dbt-artifacts.sh <label> <dbt-core version> <dbt-duckdb version> [python]
#   scripts/gen-dbt-artifacts.sh v12 1.12.7 1.11.1 3.12
#   EXTRA_WITH="--with duckdb==0.6.1" scripts/gen-dbt-artifacts.sh v7 1.3.7 1.3.4 3.10
#
# Writes manifest.json, run_results.json, catalog.json and sources.json to
# internal/dbt/samples/jaffle_shop/<label>/ (embedded in the binary as
# sample://jaffle_shop/<label>/<file>.json). Nothing touches the network except package downloads.
set -euo pipefail
label="$1"; core="$2"; adapter="$3"; py="${4:-3.12}"
repo="$(cd "$(dirname "$0")/.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
cp -R "$repo/test/dbt_project/." "$work/"
rm -rf "$work/extras"
# Groups, semantic models and metrics need dbt >= 1.6 (MetricFlow).
minor="$(echo "$core" | cut -d. -f2)"
if [ "$minor" -ge 6 ]; then
  cp "$repo/test/dbt_project/extras/v16/semantic.yml" "$work/models/marts/"
  if [ "$minor" -eq 6 ]; then
    # dbt 1.6 accepts no config block on semantic models or metrics.
    sed -i.bak '/^    config:$/{N;d;}' "$work/models/marts/semantic.yml" && rm -f "$work/models/marts/semantic.yml.bak"
  fi
  cp "$repo/test/dbt_project/extras/v16/metricflow_time_spine.sql" "$work/models/marts/"
  if [ "$minor" -ge 9 ]; then cp "$repo/test/dbt_project/extras/v16/time_spine.yml" "$work/models/marts/"; fi
fi
if [ "$minor" -lt 6 ]; then
  cp "$repo/test/dbt_project/extras/legacy/metrics.yml" "$work/models/marts/"
  # Groups arrived in dbt 1.5.
  if [ "$minor" -eq 5 ]; then cp "$repo/test/dbt_project/extras/legacy/groups.yml" "$work/models/marts/"; fi
fi
cd "$work"
export DBT_PROFILES_DIR="$work" DBT_DUCKDB_PATH="$work/jaffle_shop.duckdb" DBT_SEND_ANONYMOUS_USAGE_STATS=false DO_NOT_TRACK=1 DBT_PARTIAL_PARSE=false
run() { uv run --isolated --no-project --python "$py" --with "dbt-core==$core" --with "dbt-duckdb==$adapter" ${EXTRA_WITH:-} dbt "$@"; }
run seed
run build || true   # a failing relationships test is intentional
cp target/run_results.json "$work/build_run_results.json"
run source freshness || true           # raw_orders is stale on purpose
cp target/sources.json "$work/sources.json"
run docs generate
out="$repo/internal/dbt/samples/jaffle_shop/$label"
mkdir -p "$out"
cp target/manifest.json target/catalog.json "$out/"
cp "$work/build_run_results.json" "$out/run_results.json"
cp "$work/sources.json" "$out/sources.json"
echo "wrote $out"
