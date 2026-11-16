// Discovery metadata: the one file to edit when you rename or extend the worker.
//
// DuckDB, coding agents and the vgi-lint metadata checker all read these tags.
// Keep every object documented and `make lint-explain` stays at 100.
//
//   - Catalog-level tags (title, license, author, links, long description)
//     live in CatalogTags below.
//   - Every function carries objectTags(...): a title, a category from
//     SchemaCategories, search keywords, and LLM- and human-facing docs.

package dbt

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// CatalogName is the name DuckDB attaches the worker under.
const CatalogName = "dbt"

// SourceURL is where this worker's code lives.
const SourceURL = "https://github.com/Query-farm/vgi-dbt"

// category is one entry of the schema's vgi.categories list.
type category struct {
	Name        string `json:"name"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

// SchemaCategories group functions in docs and agent tools. Every function's
// vgi.category must name one of these.
var SchemaCategories = []category{
	{Name: "manifest", Title: "Manifest", Description: "Tables over manifest.json: nodes, sources, exposures, macros, groups and declared columns."},
	{Name: "semantic", Title: "Semantic Layer Definitions", Description: "Metrics and semantic models declared in manifest.json."},
	{Name: "lineage", Title: "DAG and Lineage", Description: "Dependency edges and blended descendant/ancestor walks over the DAG."},
	{Name: "runs", Title: "Run Results", Description: "Node results, phase timings and source freshness from run_results.json and sources.json."},
	{Name: "catalog", Title: "Warehouse Catalog", Description: "Warehouse-observed columns from catalog.json."},
	{Name: "selectors", Title: "Selectors", Description: "A strict, enumerated subset of dbt node selection."},
	{Name: "reference", Title: "Reference and Triage", Description: "Artifact triage, schema versions and unique_id / relation helpers."},
	{Name: "cloud", Title: "dbt Cloud Admin API", Description: "Accounts, projects, environments, jobs, runs, run steps and run artifacts."},
	{Name: "discovery", Title: "dbt Cloud Discovery API", Description: "Applied-state models, tests and sources with latest execution and freshness."},
	{Name: "unified", Title: "Unified Orchestrator Shape", Description: "runs / task_runs with the columns shared by every orchestrator worker."},
}

// mustJSON marshals v, panicking on failure (only static data is passed).
func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// keywordsArray turns "a, b, c" into the JSON array string vgi.keywords requires.
func keywordsArray(keywords string) string {
	out := []string{}
	for _, k := range strings.Split(keywords, ",") {
		if k = strings.TrimSpace(k); k != "" {
			out = append(out, k)
		}
	}
	return mustJSON(out)
}

// docs is the set of standard per-object discovery fields.
type docs struct {
	Title    string
	Category string
	Keywords string
	DocLLM   string
	DocMD    string
}

// objectTags builds the standard per-object discovery tags.
func objectTags(d docs) map[string]string {
	known := false
	for _, c := range SchemaCategories {
		known = known || c.Name == d.Category
	}
	if !known {
		panic(fmt.Sprintf("unknown category %q; add it to SchemaCategories", d.Category))
	}
	return map[string]string{
		"vgi.title":    d.Title,
		"vgi.category": d.Category,
		"vgi.keywords": keywordsArray(d.Keywords),
		"vgi.doc_llm":  d.DocLLM,
		"vgi.doc_md":   d.DocMD,
	}
}

// resultColumn describes one output column for vgi.result_columns_schema.
type resultColumn struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
}

// column builds an Arrow field whose comment shows up in DuckDB's DESCRIBE and agent tools.
func column(name string, typ arrow.DataType, comment string, nullable bool) arrow.Field {
	return arrow.Field{Name: name, Type: typ, Nullable: nullable,
		Metadata: arrow.NewMetadata([]string{"comment"}, []string{comment})}
}

// exampleQuery is one entry of vgi.example_queries.
type exampleQuery struct {
	Description string `json:"description"`
	SQL         string `json:"sql"`
}

// exampleQueries converts catalog examples to vgi.example_queries entries.
func exampleQueries(lists ...[]vgi.CatalogExample) []exampleQuery {
	out := []exampleQuery{}
	for _, examples := range lists {
		for _, ex := range examples {
			out = append(out, exampleQuery{Description: ex.Description, SQL: ex.SQL})
		}
	}
	return out
}

// withExamples publishes examples (with descriptions) as vgi.example_queries.
// Overloads share one SQL name, so pass every overload's examples.
func withExamples(tags map[string]string, lists ...[]vgi.CatalogExample) map[string]string {
	tags["vgi.example_queries"] = mustJSON(exampleQueries(lists...))
	return tags
}

const catalogDocLLM = "dbt artifacts and dbt Cloud as SQL. Schema main streams dbt artifact files - local paths " +
	"or globs, https:// URLs, the bundled sample:// jaffle_shop artifacts, or dbt-cloud://runs/<id>/<file>.json - " +
	"into flat, version-tolerant tables over manifest.json (schema v7-v12: the DAG's nodes and their columns, " +
	"dependency edges, the semantic layer), run_results.json (globs scan whole archives), catalog.json and " +
	"sources.json; it needs no credentials. Every artifact function takes the path as its first argument; bind each " +
	"file once with CREATE VIEW. Triage an unfamiliar file first (an unknown schema version becomes a row, not a " +
	"crash). DAG closure is a per-row (LATERAL) walk, and node selection supports a strict, enumerated subset of " +
	"dbt's selector grammar where unsupported terms are errors. Schema cloud reads the dbt Cloud Admin API (account " +
	"structure, job runs and their saved artifacts) and Discovery API (applied state and freshness) with a dbt_cloud " +
	"secret (TOKEN, ACCOUNT_ID, BASE_URL). Schema unified exposes run history in the cross-orchestrator shape."

// CatalogComment is the catalog's one-line description in directories.
const CatalogComment = "Query dbt artifacts (manifest, run results, catalog, source freshness) and dbt Cloud runs, jobs and Discovery metadata as SQL tables."

// CatalogDocMD is the catalog's long human-facing description.
const CatalogDocMD = "# dbt\n\n" +
	"**dbt artifacts and dbt Cloud as SQL.** DuckDB's `read_json` turns a `manifest.json` into one row holding a " +
	"project-specific `STRUCT` with one field per node, which is slow to infer, different for every project, and " +
	"fully materialized before the first row. This worker streams the same file into flat tables whose columns do " +
	"not change between projects or dbt releases (manifest schema v7 to v12), and skips unselected compiled SQL " +
	"without decoding it, so the first rows of a 300 MB manifest arrive in well under a second.\n\n" +
	"The `main` schema reads artifact files you already have (local paths and globs, https URLs, the bundled " +
	"`sample://` jaffle_shop artifacts, or a dbt Cloud run's artifacts) and needs no credentials. Run artifact triage " +
	"first on an unfamiliar file: an unknown schema version becomes a row, not a crash. The `cloud` schema reads the " +
	"dbt Cloud Admin and Discovery APIs with a `dbt_cloud` secret, and the `unified` schema exposes runs and task runs " +
	"in the shape every orchestrator worker shares, so dbt runs union with Airflow, Dagster or Prefect runs.\n\n" +
	"See dbt's [artifact reference](https://docs.getdbt.com/reference/artifacts/dbt-artifacts) and the " +
	"[dbt Cloud API docs](https://docs.getdbt.com/docs/dbt-cloud-apis/overview)."

// schemaInfo is the per-schema discovery text.
type schemaInfo struct {
	Title, Domain, Keywords, DocLLM, DocMD string
	Categories                             []string
}

var mainSchema = schemaInfo{
	Title: "dbt Artifacts", Domain: "analytics engineering",
	Keywords: "dbt, manifest.json, run_results.json, catalog.json, sources.json, dag, lineage, selectors, test coverage",
	DocLLM: "Schema main reads dbt artifact files. Every table function takes the artifact path first (a local path or " +
		"glob, an https:// URL, or dbt-cloud://runs/<run_id>/<file>.json), so two manifests (main and a pull request) " +
		"can be compared in one query. Tables cover manifest.json (nodes, declared columns, dependency edges to nodes or " +
		"macros, sources, exposures, the semantic layer, groups), run_results.json (node results and phase timings), " +
		"catalog.json (warehouse-observed columns) and sources.json (freshness), normalized across manifest schema v7 to " +
		"v12: fields a version lacks are NULL and adapter blobs such as config and meta stay JSON. Start with artifact " +
		"triage on an unfamiliar file. Descendant/ancestor walks take a unique_id and the manifest path and run per row " +
		"(LATERAL). Selector matching supports tag:, path:, fqn:, package:, resource_type:, config.materialized: and " +
		"names; graph operators are errors. Scalar helpers split unique_ids, normalize relation names and read a " +
		"manifest's schema version, and the supported artifact versions are themselves a table.",
	DocMD: "## main\n\nTables over dbt's four artifact files, normalized across manifest schema v7 to v12. Every " +
		"function takes the artifact path as its first argument, so two manifests (main and a pull request) can be " +
		"compared in one query; bind each file once as a view and join the views.\n\n" +
		"Fields a version lacks are `NULL`, adapter-specific blobs such as `config` and `meta` stay `JSON`, and the " +
		"support matrix is itself a table. Graph closure is a per-row (LATERAL) walk, and node selection supports a " +
		"strict, enumerated subset of dbt's selector grammar: unsupported terms are errors, never a silent false.",
	Categories: []string{"manifest", "semantic", "lineage", "runs", "catalog", "selectors", "reference"},
}

var cloudSchema = schemaInfo{
	Title: "dbt Cloud", Domain: "analytics engineering",
	Keywords: "dbt cloud, admin api, discovery api, jobs, runs, environments, freshness, graphql",
	DocLLM: "Schema cloud reads a dbt Cloud account through its Admin API (v2 and v3) and its Discovery (metadata " +
		"GraphQL) API, authenticated by a dbt_cloud secret holding TOKEN and ACCOUNT_ID, with BASE_URL for EMEA, APAC, " +
		"cell-based or single-tenant hosts, an optional DISCOVERY_URL, and an optional default ENVIRONMENT_ID. The " +
		"account's structure (projects, environments, job definitions) is exposed as tables; run history is a function " +
		"whose job, environment, project, status and created_at filters become API query parameters and which takes an " +
		"incremental cursor; per-run steps and saved artifact files are functions of a run id, and every saved artifact " +
		"can be passed to the main schema's artifact functions as dbt-cloud://runs/<id>/<file>.json. Discovery functions " +
		"return the applied state of an environment with the latest execution and freshness of each resource.",
	DocMD: "## cloud\n\nRead-only dbt Cloud access with a `dbt_cloud` DuckDB secret (`TOKEN`, `ACCOUNT_ID`, and " +
		"`BASE_URL` for non-US or single-tenant accounts).\n\n" +
		"Account structure is plain tables; run history filters on job, environment, project, status and creation " +
		"date are sent to the Admin API, and an incremental cursor returns only runs newer than one you already have. " +
		"A run's saved artifacts open directly in the `main` tables through `dbt-cloud://runs/<id>/manifest.json`.\n\n" +
		"The Discovery functions answer \"what is the state of production\": the latest status of every model and " +
		"test, and the freshness of every source, for one environment.",
	Categories: []string{"cloud", "discovery"},
}

var unifiedSchema = schemaInfo{
	Title: "Unified Orchestrator Runs", Domain: "orchestration",
	Keywords: "unified, runs, task runs, orchestrators, airflow, dagster, prefect, union all",
	DocLLM: "Schema unified exposes runs and task runs with exactly the columns the airflow, dagster, prefect, temporal, " +
		"databricks and github workers use, so run histories UNION ALL across tools. status is one of queued, running, " +
		"success, failed, cancelled, skipped; raw_status keeps the tool's own value. Given a run_results.json path or " +
		"glob each file is one run (orchestrator 'dbt') and each node result a task; without a path the dbt Cloud runs " +
		"of the dbt_cloud secret's account are read (orchestrator 'dbt_cloud').",
	DocMD: "## unified\n\nThe cross-orchestrator run shape: `orchestrator`, `run_id`, `pipeline`, `status`, " +
		"`raw_status`, `trigger`, timing columns and a `url`.\n\n" +
		"Point it at an archive of `run_results.json` files to treat every dbt invocation as a run and every node " +
		"result as a task, or leave the path out to read dbt Cloud runs; either way the rows union with the other " +
		"orchestrator workers' `unified` tables.",
	Categories: []string{"unified"},
}

// agentTask is one public analyst task for `vgi-lint simulate`.
type agentTask struct {
	Name   string `json:"name"`
	Prompt string `json:"prompt"`
}

// agentTestTasks are graded privately by vgi-agent-tests.yaml.
var agentTestTasks = []agentTask{
	{Name: "models_by_materialization", Prompt: "In the bundled sample manifest sample://jaffle_shop/v12/manifest.json, how many models are materialized as table and how many as view?"},
	{Name: "failed_tests", Prompt: "Which tests failed (status fail or error) in sample://jaffle_shop/v12/run_results.json? Give their unique_ids."},
	{Name: "downstream_of_stg_orders", Prompt: "Using sample://jaffle_shop/v12/manifest.json, which models are downstream of model.jaffle_shop.stg_orders?"},
	{Name: "manifest_versions", Prompt: "What manifest schema versions are in the bundled samples sample://jaffle_shop/*/manifest.json, and which dbt version wrote each?"},
	{Name: "sources_behind_customers", Prompt: "Which declared sources does model.jaffle_shop.customers ultimately read in sample://jaffle_shop/v12/manifest.json, and what loaded_at_field does each use?"},
	{Name: "contract_drift", Prompt: "Compare declared column data types in sample://jaffle_shop/v12/manifest.json with the warehouse types in sample://jaffle_shop/v12/catalog.json. List each declared column of model.jaffle_shop.orders with both types."},
	{Name: "macro_usage", Prompt: "Which macros defined by the jaffle_shop project itself are used by which models, in sample://jaffle_shop/v12/manifest.json?"},
	{Name: "exposure_inputs", Prompt: "Which models feed the weekly_revenue exposure in sample://jaffle_shop/v12/manifest.json?"},
	{Name: "finance_group", Prompt: "In sample://jaffle_shop/v12/manifest.json, who owns the finance group (email), how many metrics are defined, and how many semantic models?"},
	{Name: "longest_execute", Prompt: "Which node had the longest execute phase in sample://jaffle_shop/v12/run_results.json?"},
	{Name: "stale_sources", Prompt: "Which sources failed their freshness check in sample://jaffle_shop/v12/sources.json?"},
	{Name: "staging_views", Prompt: "Which nodes in sample://jaffle_shop/v12/manifest.json match the dbt selector 'tag:staging,config.materialized:view'?"},
	{Name: "explain_selector", Prompt: "Is every part of the dbt selector 'tag:finance +orders' supported by the dbt worker? Show each term and whether it is supported."},
	{Name: "seed_relations", Prompt: "For every seed in sample://jaffle_shop/v12/manifest.json, give the name parsed from its unique_id and its canonical db.schema.table relation key."},
	{Name: "v9_version", Prompt: "What is the manifest schema version of sample://jaffle_shop/v9/manifest.json, according to the worker's header probe?"},
	{Name: "supported_run_results", Prompt: "Which run_results.json schema versions does the dbt worker support?"},
	{Name: "unified_failures", Prompt: "Treating each bundled sample run_results.json (sample://jaffle_shop/*/run_results.json) as a run in the unified orchestrator shape, how many tasks failed in each run?"},
	{Name: "cloud_failed_runs", Prompt: "With a dbt_cloud secret configured, which dbt Cloud jobs had runs end in Error, and which step of each failed run failed?"},
	{Name: "cloud_inventory", Prompt: "With a dbt_cloud secret configured, list each dbt Cloud project with its deployment environments, and the account name."},
	{Name: "cloud_production_health", Prompt: "With a dbt_cloud secret whose ENVIRONMENT_ID is the production environment, how many models last errored, how many tests last failed, and how many sources are not fresh? Also list the artifact files saved by run 9001."},
}

// CatalogTags are the catalog-level discovery tags.
var CatalogTags = map[string]string{
	"vgi.title":              "dbt Artifacts",
	"vgi.subtitle":           "Stream dbt manifest, run results, catalog and freshness artifacts, and dbt Cloud runs, into stable SQL tables.",
	"vgi.doc_llm":            catalogDocLLM,
	"vgi.doc_md":             CatalogDocMD,
	"vgi.source_url":         SourceURL,
	"vgi.icon_url":           "https://query.farm/media-kit/products/vgi/vgi-logo.png",
	"vgi.license":            "0BSD",
	"vgi.author":             "Query.Farm",
	"vgi.copyright":          "Copyright 2026 Query Farm LLC - https://query.farm",
	"vgi.keywords":           keywordsArray("dbt, dbt cloud, manifest.json, run_results.json, analytics engineering, dag, lineage, data quality"),
	"vgi.support_contact":    SourceURL + "/issues",
	"vgi.support_policy_url": SourceURL + "#support",
	// Public analyst tasks for `vgi-lint simulate`; private graders live in vgi-agent-tests.yaml.
	"vgi.agent_test_tasks": mustJSON(agentTestTasks),
}

// schemaTags builds a schema's tags.
func schemaTags(si schemaInfo, examples []exampleQuery) map[string]string {
	var cats []category
	for _, name := range si.Categories {
		for _, c := range SchemaCategories {
			if c.Name == name {
				cats = append(cats, c)
			}
		}
	}
	return map[string]string{
		"vgi.title":           si.Title,
		"domain":              si.Domain,
		"vgi.keywords":        keywordsArray(si.Keywords),
		"vgi.doc_llm":         si.DocLLM,
		"vgi.doc_md":          si.DocMD,
		"vgi.categories":      mustJSON(cats),
		"vgi.example_queries": mustJSON(examples),
	}
}

// MainSchemaComment, CloudSchemaComment and UnifiedSchemaComment are the schema comments.
const (
	MainSchemaComment    = "dbt artifact tables (manifest, run_results, catalog, sources), selectors and DAG walks."
	CloudSchemaComment   = "dbt Cloud Admin API (jobs, runs, environments) and Discovery API (models, tests, sources)."
	UnifiedSchemaComment = "runs and task_runs in the shape shared by every orchestrator worker."
)
