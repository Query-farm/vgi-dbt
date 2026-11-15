// Tables over run_results.json, catalog.json and sources.json.

package dbt

import (
	"github.com/Query-farm/vgi-go/vgi"
)

const runResultsArgDoc = "run_results.json to read: a local path or glob (e.g. 'archive/**/run_results.json', one partition per file), an https:// URL, or dbt-cloud://runs/<run_id>/run_results.json."

// timing returns the started_at / completed_at of the named timing phase.
func timing(c *rowCtx, name, field string) any {
	for _, t := range rawList(c.F["timing"]) {
		m := rawObject(t)
		if asText(m["name"]) == name {
			return rawTime(m[field])
		}
	}
	return nil
}

var runResultsTable = &artifactTable{
	Name: "run_results", Kind: kindRunResults, ArgName: "path", ArgDoc: runResultsArgDoc, Sections: []string{"results"},
	Cols: []colSpec{
		{Name: "invocation_id", Type: tString, Doc: "dbt invocation id of the run (metadata.invocation_id)."},
		{Name: "unique_id", Type: tString, Doc: "Node the result is for.", Keys: []string{"unique_id"}},
		{Name: "status", Type: tString, Doc: "success, error, skipped (models); pass, fail, warn, error (tests); no-op (operations).", Keys: []string{"status"}},
		{Name: "execution_time", Type: tFloat64, Doc: "Seconds spent on the node.", Keys: []string{"execution_time"}},
		{Name: "rows_affected", Type: tInt64, Doc: "adapter_response.rows_affected, when the adapter reports it.", Keys: []string{"adapter_response"}},
		{Name: "failures", Type: tInt64, Doc: "Failing rows for tests, else NULL.", Keys: []string{"failures"}},
		{Name: "message", Type: tString, Doc: "Status message or error text.", Keys: []string{"message"}},
		{Name: "thread_id", Type: tString, Doc: "dbt thread that ran the node.", Keys: []string{"thread_id"}},
		{Name: "adapter_response", Type: tJSON, Doc: "Raw adapter response, as JSON.", Keys: []string{"adapter_response"}},
		{Name: "compile_started", Type: tTS, Doc: "Start of the compile phase.", Keys: []string{"timing"}},
		{Name: "execute_completed", Type: tTS, Doc: "End of the execute phase.", Keys: []string{"timing"}},
		{Name: "generated_at", Type: tTS, Doc: "When dbt wrote the file (metadata.generated_at)."},
		{Name: "artifact_path", Type: tString, Doc: "File the row came from (useful with globs).", NotNull: true},
	},
	Rows: func(c *rowCtx) [][]any {
		return one(nilIfEmpty(c.Meta.InvocationID), c.s("unique_id"), c.s("status"), c.f64("execution_time"),
			c.i64("adapter_response", "rows_affected"), c.i64("failures"), c.s("message"), c.s("thread_id"),
			c.js("adapter_response"), timing(c, "compile", "started_at"), timing(c, "execute", "completed_at"),
			c.generatedAt(), c.File.Display)
	},
	FilterCols: []string{"status", "unique_id"},
	Docs: fnDocs{
		Title:       "dbt Run Results",
		Category:    "runs",
		Keywords:    "dbt, run_results, test failures, execution time, slow models, archive, invocations",
		Description: "Per-node results from one or many run_results.json files (a glob scans an archive, one file per partition)",
		DocLLM: "run_results(path) returns one row per node result: invocation_id, unique_id, status, execution_time, " +
			"rows_affected, failures, message, thread_id, adapter_response (JSON), compile_started, execute_completed, " +
			"generated_at, artifact_path. path may be a glob such as 'archive/**/run_results.json' to trend runs over time, " +
			"or dbt-cloud://runs/<id>/run_results.json. Status values: success/error/skipped for models, pass/fail/warn/error " +
			"for tests.",
		DocMD: "## run_results(path)\n\nNode results. A glob such as `archive/**/run_results.json` scans an archive of runs, one file per partition, so p50/p95 execution time per model over months of runs is one `GROUP BY`.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT status, count(*) AS nodes FROM dbt.main.run_results('sample://jaffle_shop/v12/run_results.json') GROUP BY status ORDER BY status",
				Description: "Results of a run by status"},
			{SQL: "SELECT unique_id, round(execution_time, 3) AS seconds FROM dbt.main.run_results('sample://jaffle_shop/*/run_results.json') WHERE unique_id LIKE 'model.%' ORDER BY execution_time DESC LIMIT 5",
				Description: "Slowest model executions across an archive of runs"},
		},
	},
}

var runTimingsTable = &artifactTable{
	Name: "run_timings", Kind: kindRunResults, ArgName: "path", ArgDoc: runResultsArgDoc, Sections: []string{"results"},
	AlwaysKeys: []string{"timing", "unique_id"},
	Cols: []colSpec{
		{Name: "invocation_id", Type: tString, Doc: "dbt invocation id."},
		{Name: "unique_id", Type: tString, Doc: "Node the timing is for."},
		{Name: "name", Type: tString, Doc: "Phase: compile or execute."},
		{Name: "started_at", Type: tTS, Doc: "Phase start."},
		{Name: "completed_at", Type: tTS, Doc: "Phase end."},
		{Name: "duration_seconds", Type: tFloat64, Doc: "completed_at - started_at in seconds."},
	},
	Rows: func(c *rowCtx) [][]any {
		var rows [][]any
		for _, t := range rawList(c.F["timing"]) {
			m := rawObject(t)
			s, e := rawTime(m["started_at"]), rawTime(m["completed_at"])
			rows = append(rows, []any{nilIfEmpty(c.Meta.InvocationID), c.s("unique_id"), rawString(m["name"]), s, e, durationSeconds(s, e)})
		}
		return rows
	},
	Docs: fnDocs{
		Title:       "dbt Run Phase Timings",
		Category:    "runs",
		Keywords:    "dbt, timing, compile, execute, critical path, gantt",
		Description: "Compile and execute phase timings for every node in one or many run_results.json files",
		DocLLM: "run_timings(path) returns one row per node phase (compile, execute): invocation_id, unique_id, name, " +
			"started_at, completed_at, duration_seconds. Use it for Gantt charts and critical-path analysis; path accepts " +
			"the same globs and URLs as run_results.",
		DocMD: "## run_timings(path)\n\nPer-phase timings of each node.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT name, count(*) AS phases FROM dbt.main.run_timings('sample://jaffle_shop/v12/run_results.json') GROUP BY name ORDER BY name",
				Description: "Count compile and execute phases in a run"},
		},
	},
}

var catalogColumnsTable = &artifactTable{
	Name: "catalog_columns", Kind: kindCatalog, ArgName: "path", Sections: []string{"nodes", "sources"},
	ArgDoc:     "catalog.json to read (written by dbt docs generate): a local path, https:// URL or dbt-cloud://runs/<run_id>/catalog.json.",
	AlwaysKeys: []string{"columns", "metadata", "stats", "unique_id"},
	Cols: []colSpec{
		{Name: "unique_id", Type: tString, Doc: "Node or source the relation belongs to.", NotNull: true},
		{Name: "table_type", Type: tString, Doc: "Warehouse relation type, e.g. BASE TABLE or VIEW."},
		{Name: "column_name", Type: tString, Doc: "Column name as the warehouse reports it."},
		{Name: "column_index", Type: tInt32, Doc: "Ordinal position in the relation."},
		{Name: "column_type", Type: tString, Doc: "Warehouse data type."},
		{Name: "comment", Type: tString, Doc: "Warehouse column comment."},
		{Name: "stats", Type: tJSON, Doc: "Relation-level statistics the adapter reported, as JSON."},
		{Name: "owner", Type: tString, Doc: "Relation owner, when reported."},
		{Name: "table_database", Type: tString, Doc: "Relation database."},
		{Name: "table_schema", Type: tString, Doc: "Relation schema."},
		{Name: "table_name", Type: tString, Doc: "Relation name."},
	},
	Rows: func(c *rowCtx) [][]any {
		md := rawObject(c.F["metadata"])
		var rows [][]any
		for _, col := range orderedObject(c.F["columns"]) {
			m := rawObject(col.Value)
			rows = append(rows, []any{c.id(), rawString(md["type"]), firstNonNull(rawString(m["name"]), col.Key),
				rawInt(m["index"]), rawString(m["type"]), rawString(m["comment"]), c.js("stats"), rawString(md["owner"]),
				rawString(md["database"]), rawString(md["schema"]), rawString(md["name"])})
		}
		return rows
	},
	Docs: fnDocs{
		Title:       "dbt Catalog Columns",
		Category:    "catalog",
		Keywords:    "dbt, catalog, columns, data types, warehouse schema, contract drift, docs generate",
		Description: "Warehouse-observed columns of every node and source relation in a catalog.json",
		DocLLM: "catalog_columns(path) reads catalog.json (dbt docs generate) and returns one row per warehouse column: " +
			"unique_id, table_type, column_name, column_index, column_type, comment, stats (JSON), owner, table_database, " +
			"table_schema, table_name. Join node_columns(manifest) on unique_id and lower(column name) to find contract drift.",
		DocMD: "## catalog_columns(path)\n\nWhat the warehouse actually has, from `catalog.json`.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT table_name, count(*) AS columns FROM dbt.main.catalog_columns('sample://jaffle_shop/v12/catalog.json') GROUP BY table_name ORDER BY table_name",
				Description: "Column count per relation"},
		},
	},
}

var sourceFreshnessTable = &artifactTable{
	Name: "source_freshness", Kind: kindSources, ArgName: "path", Sections: []string{"results"},
	ArgDoc: "sources.json to read (written by dbt source freshness): a local path or glob, an https:// URL, or dbt-cloud://runs/<run_id>/sources.json.",
	Cols: []colSpec{
		{Name: "unique_id", Type: tString, Doc: "Source checked.", Keys: []string{"unique_id"}},
		{Name: "status", Type: tString, Doc: "pass, warn, error or runtime error.", Keys: []string{"status"}},
		{Name: "max_loaded_at", Type: tTS, Doc: "Latest loaded_at_field value found.", Keys: []string{"max_loaded_at"}},
		{Name: "snapshotted_at", Type: tTS, Doc: "When the check ran.", Keys: []string{"snapshotted_at"}},
		{Name: "age_seconds", Type: tFloat64, Doc: "snapshotted_at - max_loaded_at in seconds.", Keys: []string{"max_loaded_at_time_ago_in_s"}},
		{Name: "criteria", Type: tJSON, Doc: "warn_after / error_after criteria applied, as JSON.", Keys: []string{"criteria"}},
		{Name: "error", Type: tString, Doc: "Error message for runtime errors, else NULL.", Keys: []string{"error"}},
		{Name: "artifact_path", Type: tString, Doc: "File the row came from.", NotNull: true},
	},
	Rows: func(c *rowCtx) [][]any {
		return one(c.s("unique_id"), c.s("status"), c.ts("max_loaded_at"), c.ts("snapshotted_at"),
			c.f64("max_loaded_at_time_ago_in_s"), c.js("criteria"), c.s("error"), c.File.Display)
	},
	FilterCols: []string{"status"},
	Docs: fnDocs{
		Title:       "dbt Source Freshness",
		Category:    "runs",
		Keywords:    "dbt, source freshness, stale sources, sources.json, sla",
		Description: "Source freshness results from sources.json (dbt source freshness)",
		DocLLM: "source_freshness(path) returns one row per checked source: unique_id, status (pass/warn/error/runtime error), " +
			"max_loaded_at, snapshotted_at, age_seconds, criteria (JSON), error. Join sources(manifest) on unique_id for " +
			"names and owners.",
		DocMD: "## source_freshness(path)\n\nFreshness results; `status <> 'pass'` lists stale sources.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT unique_id, status, round(age_seconds / 86400) AS days_old FROM dbt.main.source_freshness('sample://jaffle_shop/v12/sources.json') ORDER BY unique_id",
				Description: "How stale each checked source is"},
		},
	},
}
