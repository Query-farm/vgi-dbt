-- dbt project audit pack for the vgi-dbt worker (https://github.com/Query-farm/vgi-dbt).
--
-- Edit the five paths in the preamble to point at your project's target/ (or an
-- archive of run_results.json files), then run the whole file:
--   duckdb -c "ATTACH 'dbt' (TYPE vgi, LOCATION '...')" -f audit/audit.sql
-- As shipped it reads the jaffle_shop samples bundled in the worker.

-- 0. Preamble: bind each artifact once.
CREATE OR REPLACE TEMP VIEW nodes       AS SELECT * FROM dbt.main.nodes('sample://jaffle_shop/v12/manifest.json');
CREATE OR REPLACE TEMP VIEW depends_on  AS SELECT * FROM dbt.main.depends_on('sample://jaffle_shop/v12/manifest.json');
CREATE OR REPLACE TEMP VIEW decl_cols   AS SELECT * FROM dbt.main.node_columns('sample://jaffle_shop/v12/manifest.json');
CREATE OR REPLACE TEMP VIEW cat_cols    AS SELECT * FROM dbt.main.catalog_columns('sample://jaffle_shop/v12/catalog.json');
CREATE OR REPLACE TEMP VIEW results     AS SELECT * FROM dbt.main.run_results('sample://jaffle_shop/*/run_results.json');
CREATE OR REPLACE TEMP VIEW freshness   AS SELECT * FROM dbt.main.source_freshness('sample://jaffle_shop/v12/sources.json');
CREATE OR REPLACE TEMP VIEW src         AS SELECT * FROM dbt.main.sources('sample://jaffle_shop/v12/manifest.json');
CREATE OR REPLACE TEMP VIEW macro_edges AS SELECT * FROM dbt.main.depends_on('sample://jaffle_shop/v12/manifest.json', kind := 'macros');

-- 1. Test coverage by layer (directory under models/).
SELECT fqn[2] AS layer, count(*) AS models,
       count(*) FILTER (WHERE unique_id IN (
         SELECT d.depends_on_id FROM depends_on d JOIN nodes t ON t.unique_id = d.unique_id WHERE t.resource_type = 'test'
       )) AS tested
FROM nodes WHERE resource_type = 'model' GROUP BY layer ORDER BY layer;

-- 2. Models with no tests.
SELECT m.unique_id FROM nodes m
WHERE m.resource_type = 'model'
  AND m.unique_id NOT IN (SELECT d.depends_on_id FROM depends_on d JOIN nodes t ON t.unique_id = d.unique_id WHERE t.resource_type = 'test')
ORDER BY 1;

-- 3. Models with neither a description nor any documented column.
SELECT m.unique_id FROM nodes m
WHERE m.resource_type = 'model' AND m.description = ''
  AND NOT EXISTS (SELECT 1 FROM decl_cols c WHERE c.unique_id = m.unique_id AND c.description <> '')
ORDER BY 1;

-- 4. Orphan models: nothing (model, test, exposure, metric) depends on them.
SELECT m.unique_id FROM nodes m
WHERE m.resource_type = 'model' AND m.unique_id NOT IN (SELECT depends_on_id FROM depends_on)
ORDER BY 1;

-- 5. Slowest models across archived runs, with spread.
SELECT unique_id, count(*) AS runs, round(median(execution_time), 3) AS p50,
       round(quantile_cont(execution_time, 0.95), 3) AS p95
FROM results WHERE unique_id LIKE 'model.%' AND status = 'success'
GROUP BY unique_id ORDER BY p95 DESC LIMIT 10;

-- 6. Tests that fail or warn most often.
SELECT unique_id, count(*) FILTER (WHERE status IN ('fail', 'error')) AS failures,
       count(*) FILTER (WHERE status = 'warn') AS warnings, count(*) AS runs
FROM results WHERE unique_id LIKE 'test.%'
GROUP BY unique_id HAVING failures + warnings > 0 ORDER BY failures DESC, warnings DESC;

-- 7. Contract drift: declared data_type vs. what the warehouse has.
SELECT d.unique_id, d.name, d.data_type AS declared, c.column_type AS actual
FROM decl_cols d JOIN cat_cols c ON c.unique_id = d.unique_id AND lower(c.column_name) = lower(d.name)
WHERE d.data_type IS NOT NULL AND lower(d.data_type) <> lower(c.column_type)
ORDER BY 1, 2;

-- 8. Declared columns the warehouse does not have (stale YAML).
SELECT d.unique_id, d.name FROM decl_cols d
WHERE d.unique_id IN (SELECT unique_id FROM cat_cols)
  AND NOT EXISTS (SELECT 1 FROM cat_cols c WHERE c.unique_id = d.unique_id AND lower(c.column_name) = lower(d.name))
ORDER BY 1, 2;

-- 9. Stale sources, and sources with no freshness check at all.
SELECT s.unique_id, coalesce(f.status, 'not checked') AS freshness, round(f.age_seconds / 3600, 1) AS hours_old
FROM src s LEFT JOIN freshness f USING (unique_id)
WHERE f.status IS DISTINCT FROM 'pass' ORDER BY 1;

-- 10. Macro fan-in: the project macros most nodes depend on.
SELECT depends_on_id AS macro, count(DISTINCT unique_id) AS used_by
FROM macro_edges WHERE depends_on_id NOT LIKE 'macro.dbt.%' AND depends_on_id NOT LIKE 'macro.dbt_%'
GROUP BY macro ORDER BY used_by DESC, macro;

-- 11. DAG depth outliers: models with the longest upstream chain.
SELECT n.unique_id, max(a.depth) AS upstream_depth
FROM nodes n, dbt.main.ancestors(n.unique_id, manifest := 'sample://jaffle_shop/v12/manifest.json') a
WHERE n.resource_type = 'model' GROUP BY n.unique_id ORDER BY upstream_depth DESC, n.unique_id LIMIT 10;

-- 12. Blast radius: how many models sit downstream of each staging model.
SELECT n.unique_id, count(*) FILTER (WHERE d.unique_id LIKE 'model.%') AS downstream_models
FROM nodes n, dbt.main.descendants(n.unique_id, manifest := 'sample://jaffle_shop/v12/manifest.json') d
WHERE n.resource_type = 'model' AND dbt.main.selector_match(n.unique_id, n.fqn, n.tags, n.materialized, 'tag:staging')
GROUP BY n.unique_id ORDER BY downstream_models DESC;

-- 13. Materialization mix per layer.
SELECT fqn[2] AS layer, materialized, count(*) AS models
FROM nodes WHERE resource_type = 'model' GROUP BY ALL ORDER BY ALL;

-- 14. Public models without an enforced contract (governance).
SELECT unique_id, access, contract_enforced FROM nodes
WHERE resource_type = 'model' AND access = 'public' AND contract_enforced IS NOT TRUE ORDER BY 1;

-- 15. Run duration trend: one row per archived invocation.
SELECT run_id, pipeline, status, round(duration_seconds, 2) AS seconds, started_at
FROM dbt.unified.runs(path := 'sample://jaffle_shop/*/run_results.json') ORDER BY started_at;
