// Table definitions over manifest.json, normalized across schema versions
// v7 (dbt 1.3) to v12 (dbt 1.8+). Fields a version lacks are NULL; renamed
// fields are read from every spelling (raw_sql -> raw_code, sql ->
// expression, calculation_method -> metric_type, top-level group ->
// config.group).

package dbt

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
)

func (c *rowCtx) s(keys ...string) any      { return rawString(path(c.F, keys...)) }
func (c *rowCtx) tags(keys ...string) any   { return rawStrings(path(c.F, keys...)) }
func (c *rowCtx) js(keys ...string) any     { return rawJSON(path(c.F, keys...)) }
func (c *rowCtx) b(keys ...string) any      { return rawBool(path(c.F, keys...)) }
func (c *rowCtx) ts(keys ...string) any     { return rawTime(path(c.F, keys...)) }
func (c *rowCtx) f64(keys ...string) any    { return rawFloat(path(c.F, keys...)) }
func (c *rowCtx) i64(keys ...string) any    { return rawInt(path(c.F, keys...)) }
func (c *rowCtx) id() any                   { return firstNonNull(c.s("unique_id"), nilIfEmpty(c.Key)) }
func (c *rowCtx) version() any              { return nilIfEmpty(c.Meta.VersionLabel()) }
func (c *rowCtx) generatedAt() any          { return timeOrNil(c.Meta.GeneratedAt) }
func (c *rowCtx) dependsOn(kind string) any { return rawStrings(path(c.F, "depends_on", kind)) }

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func timeOrNil(s string) any {
	if t, ok := parseTime(s); ok {
		return t
	}
	return nil
}

// kv is one member of a JSON object, in document order.
type kv struct {
	Key   string
	Value json.RawMessage
}

// orderedObject decodes an object keeping member order (columns are ordered).
func orderedObject(raw json.RawMessage) []kv {
	if isNullRaw(raw) || raw[0] != '{' {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	if _, err := dec.Token(); err != nil {
		return nil
	}
	var out []kv
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return out
		}
		k, _ := tok.(string)
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return out
		}
		out = append(out, kv{k, v})
	}
	return out
}

// uniqueIDPrefix splits "model.pkg.rest" into ("model", "pkg").
func uniqueIDPrefix(id string) (string, string) {
	parts := strings.SplitN(id, ".", 3)
	if len(parts) < 2 {
		return id, ""
	}
	return parts[0], parts[1]
}

// keyFilterOn prunes entries whose unique_id prefix cannot match = / IN
// filters on resource_type and package_name, without reading the entry.
func keyFilterOn(pf *vgi.PushdownFilters, key string) bool {
	rt, pkg := uniqueIDPrefix(key)
	if vals, ok := vgi.EqualOrInValues[string](pf, "resource_type"); ok && !contains(vals, rt) {
		return false
	}
	if vals, ok := vgi.EqualOrInValues[string](pf, "package_name"); ok && !contains(vals, pkg) {
		return false
	}
	return true
}

func contains(vals []string, v string) bool {
	for _, x := range vals {
		if x == v {
			return true
		}
	}
	return false
}

const manifestArgDoc = "Path to a dbt manifest.json: a local path or glob, an https:// URL, or dbt-cloud://runs/<run_id>/manifest.json (needs a dbt_cloud secret)."

func manifestTable(name string, sections []string, cols []colSpec, rows func(c *rowCtx) [][]any, d fnDocs) *artifactTable {
	return &artifactTable{Name: name, Kind: kindManifest, ArgName: "manifest", ArgDoc: manifestArgDoc,
		Sections: sections, Cols: cols, Rows: rows, Docs: d}
}

// one wraps a single row.
func one(vals ...any) [][]any { return [][]any{vals} }

var nodesTable = func() *artifactTable {
	t := manifestTable("nodes", []string{"nodes"}, []colSpec{
		{Name: "unique_id", Type: tString, Doc: "dbt unique id, e.g. model.jaffle_shop.orders.", NotNull: true},
		{Name: "resource_type", Type: tString, Doc: "model, test, seed, snapshot, analysis, operation, sql_operation or unit_test.", Keys: []string{"resource_type"}},
		{Name: "package_name", Type: tString, Doc: "dbt package (project) that defines the node.", Keys: []string{"package_name"}},
		{Name: "name", Type: tString, Doc: "Node name.", Keys: []string{"name"}},
		{Name: "relation_name", Type: tString, Doc: "Fully quoted warehouse relation, e.g. \"db\".\"main\".\"orders\"; NULL for ephemeral models and tests without a relation.", Keys: []string{"relation_name"}},
		{Name: "db_name", Type: tString, Doc: "Warehouse database the node builds into.", Keys: []string{"database"}},
		{Name: "db_schema", Type: tString, Doc: "Warehouse schema the node builds into.", Keys: []string{"schema"}},
		{Name: "alias", Type: tString, Doc: "Relation identifier (alias) in the warehouse.", Keys: []string{"alias"}},
		{Name: "materialized", Type: tString, Doc: "config.materialized: table, view, incremental, ephemeral, seed, snapshot, test, ...", Keys: []string{"config"}},
		{Name: "incremental_strategy", Type: tString, Doc: "config.incremental_strategy for incremental models, else NULL.", Keys: []string{"config"}},
		{Name: "tags", Type: tStrList, Doc: "Node tags.", Keys: []string{"tags"}},
		{Name: "meta", Type: tJSON, Doc: "The node's meta mapping, as JSON.", Keys: []string{"meta"}},
		{Name: "config", Type: tJSON, Doc: "The node's full resolved config, as JSON (adapter-specific keys included).", Keys: []string{"config"}},
		{Name: "raw_code", Type: tString, Doc: "Uncompiled SQL/Python source (raw_sql before dbt 1.3). Skipped without decoding when not selected.", Keys: []string{"raw_code", "raw_sql"}},
		{Name: "compiled_code", Type: tString, Doc: "Compiled SQL, present when the manifest was written after a compile/run. Skipped without decoding when not selected.", Keys: []string{"compiled_code", "compiled_sql"}},
		{Name: "language", Type: tString, Doc: "sql or python.", Keys: []string{"language"}},
		{Name: "path", Type: tString, Doc: "Path relative to the resource directory (models/, seeds/, ...).", Keys: []string{"path"}},
		{Name: "original_file_path", Type: tString, Doc: "Path relative to the project root, e.g. models/marts/orders.sql.", Keys: []string{"original_file_path"}},
		{Name: "fqn", Type: tStrList, Doc: "Fully qualified name: package, then directories, then the node name.", Keys: []string{"fqn"}},
		{Name: "checksum", Type: tString, Doc: "checksum.checksum of the source file; compare two manifests with IS DISTINCT FROM.", Keys: []string{"checksum"}},
		{Name: "description", Type: tString, Doc: "Documented description ('' when undocumented).", Keys: []string{"description"}},
		{Name: "patch_path", Type: tString, Doc: "YAML file that documents the node, e.g. jaffle_shop://models/marts/schema.yml.", Keys: []string{"patch_path"}},
		{Name: "group_name", Type: tString, Doc: "Model group (manifest v9+), else NULL.", Keys: []string{"group", "config"}},
		{Name: "access", Type: tString, Doc: "Model access: private, protected or public (manifest v9+), else NULL.", Keys: []string{"access", "config"}},
		{Name: "contract_enforced", Type: tBool, Doc: "Whether the model contract is enforced (manifest v9+), else NULL.", Keys: []string{"contract", "config"}},
		{Name: "deprecation_date", Type: tTS, Doc: "Model deprecation date (manifest v10+), else NULL.", Keys: []string{"deprecation_date"}},
		{Name: "manifest_version", Type: tString, Doc: "Manifest schema version the row came from, e.g. v12."},
		{Name: "generated_at", Type: tTS, Doc: "When dbt wrote the manifest (metadata.generated_at)."},
	}, func(c *rowCtx) [][]any {
		return one(c.id(), c.s("resource_type"), c.s("package_name"), c.s("name"), c.s("relation_name"),
			c.s("database"), c.s("schema"), c.s("alias"), c.s("config", "materialized"),
			c.s("config", "incremental_strategy"), c.tags("tags"), c.js("meta"), c.js("config"),
			firstNonNull(c.s("raw_code"), c.s("raw_sql")), firstNonNull(c.s("compiled_code"), c.s("compiled_sql")),
			c.s("language"), c.s("path"), c.s("original_file_path"), c.tags("fqn"), c.s("checksum", "checksum"),
			c.s("description"), c.s("patch_path"), firstNonNull(c.s("group"), c.s("config", "group")),
			firstNonNull(c.s("access"), c.s("config", "access")),
			firstNonNull(c.b("contract", "enforced"), c.b("config", "contract", "enforced")),
			c.ts("deprecation_date"), c.version(), c.generatedAt())
	}, fnDocs{
		Title:       "dbt Nodes",
		Category:    "manifest",
		Keywords:    "dbt, manifest, nodes, models, tests, seeds, snapshots, compiled sql, materialization",
		Description: "Every node (model, test, seed, snapshot, analysis) in a dbt manifest.json, one row per node",
		DocLLM: "nodes(manifest) streams manifest.json's `nodes` map as one row per node with stable columns across manifest " +
			"schema versions v7-v12: unique_id, resource_type, package_name, name, relation_name, db_name, db_schema, alias, " +
			"materialized, tags, meta/config (JSON), raw_code, compiled_code, fqn, checksum, description, group_name, access, " +
			"contract_enforced and more. Filters on resource_type and package_name prune nodes by unique_id before they are " +
			"read; unselected raw_code/compiled_code are skipped without decoding. Bind it once with CREATE VIEW and join. " +
			"Sources, exposures, metrics and macros are separate functions.",
		DocMD: "## nodes(manifest)\n\nOne row per entry of `manifest.json`'s `nodes` map, with the same columns for every manifest version. Bind the file once as a view and join the view; unselected `raw_code` and `compiled_code` are never decoded.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT materialized, count(*) AS models FROM dbt.main.nodes('sample://jaffle_shop/v12/manifest.json') WHERE resource_type = 'model' GROUP BY materialized ORDER BY materialized",
				Description: "Count models by materialization"},
			{SQL: "SELECT name, description FROM dbt.main.nodes('sample://jaffle_shop/v7/manifest.json') WHERE resource_type = 'model' AND description = '' ORDER BY name",
				Description: "Models with no description, in a dbt 1.3 (v7) manifest"},
		},
	})
	t.KeyFilter = func(key string, pf *vgi.PushdownFilters) bool { return keyFilterOn(pf, key) }
	t.FilterCols = []string{"resource_type", "package_name", "materialized"}
	return t
}()

var nodeColumnsTable = func() *artifactTable {
	t := manifestTable("node_columns", []string{"nodes", "sources"}, []colSpec{
		{Name: "unique_id", Type: tString, Doc: "Node or source that declares the column.", NotNull: true},
		{Name: "name", Type: tString, Doc: "Declared column name."},
		{Name: "ordinal", Type: tInt32, Doc: "1-based position in the YAML declaration."},
		{Name: "data_type", Type: tString, Doc: "Declared data_type (contract), else NULL."},
		{Name: "description", Type: tString, Doc: "Column description."},
		{Name: "tags", Type: tStrList, Doc: "Column tags."},
		{Name: "meta", Type: tJSON, Doc: "Column meta, as JSON."},
		{Name: "constraints", Type: tJSON, Doc: "Declared column constraints (manifest v9+), as JSON."},
	}, func(c *rowCtx) [][]any {
		var rows [][]any
		for i, col := range orderedObject(c.F["columns"]) {
			m := rawObject(col.Value)
			name := firstNonNull(rawString(m["name"]), col.Key)
			rows = append(rows, []any{c.id(), name, int64(i + 1), rawString(m["data_type"]), rawString(m["description"]),
				rawStrings(m["tags"]), rawJSON(m["meta"]), rawJSON(m["constraints"])})
		}
		return rows
	}, fnDocs{
		Title:       "dbt Declared Columns",
		Category:    "manifest",
		Keywords:    "dbt, columns, contract, data_type, schema.yml, documentation",
		Description: "Columns declared in YAML for each node and source in a manifest.json (the contract side, not the warehouse)",
		DocLLM: "node_columns(manifest) returns one row per column declared in schema YAML for models, seeds, snapshots, tests and " +
			"sources: unique_id, name, ordinal, data_type (contract type), description, tags, meta, constraints. These are " +
			"declared columns; compare with catalog_columns(catalog.json) for what the warehouse actually has.",
		DocMD: "## node_columns(manifest)\n\nDeclared (YAML/contract) columns, one row per column.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT unique_id, count(*) AS documented FROM dbt.main.node_columns('sample://jaffle_shop/v12/manifest.json') WHERE description <> '' GROUP BY unique_id ORDER BY unique_id",
				Description: "Documented columns per node"},
		},
	})
	t.AlwaysKeys = []string{"columns", "unique_id"}
	return t
}()

var dependsOnTable = func() *artifactTable {
	t := manifestTable("depends_on", []string{"nodes", "exposures", "metrics", "semantic_models", "saved_queries", "unit_tests"}, []colSpec{
		{Name: "unique_id", Type: tString, Doc: "The dependent node.", NotNull: true},
		{Name: "depends_on_id", Type: tString, Doc: "What it depends on (a node, source, metric or macro unique_id).", NotNull: true},
		{Name: "depth", Type: tInt32, Doc: "Always 1: this is the direct edge list; use descendants/ancestors for closure.", NotNull: true},
	}, func(c *rowCtx) [][]any {
		deps, _ := c.dependsOn(c.Opt).([]string)
		id := c.id()
		rows := make([][]any, 0, len(deps))
		for _, d := range deps {
			rows = append(rows, []any{id, d, int64(1)})
		}
		return rows
	}, fnDocs{
		Title:       "dbt Dependency Edges",
		Category:    "lineage",
		Keywords:    "dbt, dag, depends_on, lineage, edges, ref, source, macros",
		Description: "The direct dependency edges (depends_on) of every node, exposure, metric and semantic model in a manifest.json",
		DocLLM: "depends_on(manifest, kind := 'nodes') returns the raw DAG edge list: one row (unique_id, depends_on_id, depth=1) " +
			"per entry of each resource's depends_on.nodes (kind := 'macros' gives depends_on.macros instead). Covers nodes, " +
			"exposures, metrics, semantic models, saved queries and unit tests. For transitive closure use " +
			"descendants(...)/ancestors(...).",
		DocMD: "## depends_on(manifest, kind := 'nodes')\n\nDirect DAG edges. `kind := 'macros'` lists macro dependencies.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT depends_on_id, count(*) AS dependents FROM dbt.main.depends_on('sample://jaffle_shop/v12/manifest.json') GROUP BY depends_on_id ORDER BY dependents DESC, depends_on_id LIMIT 5",
				Description: "Most depended-upon resources"},
			{SQL: "SELECT unique_id, depends_on_id FROM dbt.main.depends_on('sample://jaffle_shop/v12/manifest.json', kind := 'macros') WHERE unique_id LIKE 'model.%' ORDER BY ALL",
				Description: "Macros used by each model"},
		},
	})
	t.AlwaysKeys = []string{"depends_on", "unique_id"}
	t.Option = &vgi.ArgSpec{Name: "kind", Position: -1, ArrowType: "varchar", IsConst: true, HasDefault: true, DefaultValue: "nodes",
		Choices: []any{"nodes", "macros"}, Doc: "Which edges: 'nodes' (refs, sources, metrics) or 'macros'."}
	return t
}()

var sourcesTable = func() *artifactTable {
	t := manifestTable("sources", []string{"sources"}, []colSpec{
		{Name: "unique_id", Type: tString, Doc: "Source unique id, e.g. source.jaffle_shop.raw.orders.", NotNull: true},
		{Name: "package_name", Type: tString, Doc: "Package that declares the source.", Keys: []string{"package_name"}},
		{Name: "source_name", Type: tString, Doc: "The source group name (first argument to source()).", Keys: []string{"source_name"}},
		{Name: "name", Type: tString, Doc: "Table name within the source (second argument to source()).", Keys: []string{"name"}},
		{Name: "relation_name", Type: tString, Doc: "Fully quoted warehouse relation.", Keys: []string{"relation_name"}},
		{Name: "loaded_at_field", Type: tString, Doc: "Column used for freshness checks, else NULL.", Keys: []string{"loaded_at_field", "config"}},
		{Name: "freshness", Type: tJSON, Doc: "Freshness criteria (warn_after / error_after / filter), as JSON; NULL when not configured.", Keys: []string{"freshness", "config"}},
		{Name: "db_name", Type: tString, Doc: "Warehouse database.", Keys: []string{"database"}},
		{Name: "db_schema", Type: tString, Doc: "Warehouse schema.", Keys: []string{"schema"}},
		{Name: "identifier", Type: tString, Doc: "Warehouse table identifier.", Keys: []string{"identifier"}},
		{Name: "loader", Type: tString, Doc: "Declared loader (e.g. fivetran), '' when unset.", Keys: []string{"loader"}},
		{Name: "description", Type: tString, Doc: "Source table description.", Keys: []string{"description"}},
		{Name: "tags", Type: tStrList, Doc: "Source tags.", Keys: []string{"tags"}},
		{Name: "meta", Type: tJSON, Doc: "Source meta, as JSON.", Keys: []string{"meta"}},
	}, func(c *rowCtx) [][]any {
		return one(c.id(), c.s("package_name"), c.s("source_name"), c.s("name"), c.s("relation_name"),
			firstNonNull(c.s("loaded_at_field"), c.s("config", "loaded_at_field")),
			firstNonNull(c.js("freshness"), c.js("config", "freshness")),
			c.s("database"), c.s("schema"), c.s("identifier"), c.s("loader"), c.s("description"), c.tags("tags"), c.js("meta"))
	}, fnDocs{
		Title:       "dbt Sources",
		Category:    "manifest",
		Keywords:    "dbt, sources, raw tables, freshness, loaded_at_field, source()",
		Description: "Every source table declared in a manifest.json",
		DocLLM: "sources(manifest) returns one row per declared source table: unique_id, source_name, name, relation_name, " +
			"loaded_at_field, freshness (JSON criteria), db_name, db_schema, identifier, loader, description, tags, meta. " +
			"Join to source_freshness(sources.json) on unique_id for the observed freshness.",
		DocMD: "## sources(manifest)\n\nDeclared sources. Freshness *results* come from `source_freshness('target/sources.json')`.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT unique_id, loaded_at_field FROM dbt.main.sources('sample://jaffle_shop/v12/manifest.json') WHERE freshness IS NOT NULL ORDER BY unique_id",
				Description: "Sources with freshness checks configured"},
		},
	})
	t.KeyFilter = func(key string, pf *vgi.PushdownFilters) bool { return keyFilterOn(pf, key) }
	t.FilterCols = []string{"package_name", "source_name"}
	return t
}()

var exposuresTable = manifestTable("exposures", []string{"exposures"}, []colSpec{
	{Name: "unique_id", Type: tString, Doc: "Exposure unique id.", NotNull: true},
	{Name: "package_name", Type: tString, Doc: "Package that declares it.", Keys: []string{"package_name"}},
	{Name: "name", Type: tString, Doc: "Exposure name.", Keys: []string{"name"}},
	{Name: "exposure_type", Type: tString, Doc: "dashboard, notebook, analysis, ml or application.", Keys: []string{"type"}},
	{Name: "maturity", Type: tString, Doc: "high, medium or low; NULL when unset.", Keys: []string{"maturity"}},
	{Name: "url", Type: tString, Doc: "Link to the exposure.", Keys: []string{"url"}},
	{Name: "owner_name", Type: tString, Doc: "Owner name.", Keys: []string{"owner"}},
	{Name: "owner_email", Type: tString, Doc: "Owner email.", Keys: []string{"owner"}},
	{Name: "description", Type: tString, Doc: "Description.", Keys: []string{"description"}},
	{Name: "label", Type: tString, Doc: "Display label.", Keys: []string{"label"}},
	{Name: "tags", Type: tStrList, Doc: "Tags.", Keys: []string{"tags"}},
	{Name: "meta", Type: tJSON, Doc: "Meta, as JSON.", Keys: []string{"meta"}},
	{Name: "depends_on_ids", Type: tStrList, Doc: "Nodes and sources the exposure depends on.", Keys: []string{"depends_on"}},
	{Name: "path", Type: tString, Doc: "YAML file path relative to its resource directory.", Keys: []string{"path"}},
	{Name: "original_file_path", Type: tString, Doc: "YAML file path relative to the project root.", Keys: []string{"original_file_path"}},
}, func(c *rowCtx) [][]any {
	return one(c.id(), c.s("package_name"), c.s("name"), c.s("type"), c.s("maturity"), c.s("url"),
		c.s("owner", "name"), c.s("owner", "email"), c.s("description"), c.s("label"), c.tags("tags"), c.js("meta"),
		c.dependsOn("nodes"), c.s("path"), c.s("original_file_path"))
}, fnDocs{
	Title:       "dbt Exposures",
	Category:    "manifest",
	Keywords:    "dbt, exposures, dashboards, downstream, owners, bi",
	Description: "Every exposure (dashboard, notebook, application) declared in a manifest.json",
	DocLLM: "exposures(manifest) returns one row per exposure: unique_id, name, exposure_type, maturity, url, owner_name, " +
		"owner_email, description, label, tags, meta, depends_on_ids (list), path. Unnest depends_on_ids to find which " +
		"dashboards a model feeds.",
	DocMD: "## exposures(manifest)\n\nDeclared exposures and what they depend on.",
	Examples: []vgi.CatalogExample{
		{SQL: "SELECT name, unnest(depends_on_ids) AS feeds_from FROM dbt.main.exposures('sample://jaffle_shop/v12/manifest.json') ORDER BY ALL",
			Description: "Which models feed each exposure"},
	},
})

var metricsTable = manifestTable("metrics", []string{"metrics"}, []colSpec{
	{Name: "unique_id", Type: tString, Doc: "Metric unique id.", NotNull: true},
	{Name: "package_name", Type: tString, Doc: "Package that declares it.", Keys: []string{"package_name"}},
	{Name: "name", Type: tString, Doc: "Metric name.", Keys: []string{"name"}},
	{Name: "label", Type: tString, Doc: "Display label.", Keys: []string{"label"}},
	{Name: "metric_type", Type: tString, Doc: "MetricFlow type (simple, ratio, derived, cumulative, conversion); calculation_method on pre-MetricFlow manifests (v7-v9).", Keys: []string{"type", "calculation_method"}},
	{Name: "description", Type: tString, Doc: "Description.", Keys: []string{"description"}},
	{Name: "expression", Type: tString, Doc: "type_params.expr or the input measure (MetricFlow); expression/sql on older manifests.", Keys: []string{"expression", "sql", "type_params"}},
	{Name: "filter_expression", Type: tString, Doc: "Metric filter: where_filters joined with AND (MetricFlow), or 'field operator value' filters on older manifests.", Keys: []string{"filter", "filters"}},
	{Name: "agg", Type: tString, Doc: "Aggregation for pre-MetricFlow metrics (calculation_method); NULL on MetricFlow metrics, whose aggregation lives on the measure.", Keys: []string{"calculation_method", "type"}},
	{Name: "agg_time_dimension", Type: tString, Doc: "Time dimension: timestamp (pre-MetricFlow) or the measure's agg_time_dimension when set on the metric.", Keys: []string{"timestamp", "type_params"}},
	{Name: "tags", Type: tStrList, Doc: "Tags.", Keys: []string{"tags"}},
	{Name: "meta", Type: tJSON, Doc: "Meta, as JSON.", Keys: []string{"meta"}},
	{Name: "depends_on_ids", Type: tStrList, Doc: "Nodes, semantic models and metrics it depends on.", Keys: []string{"depends_on"}},
	{Name: "group_name", Type: tString, Doc: "Group, else NULL.", Keys: []string{"group", "config"}},
	{Name: "path", Type: tString, Doc: "YAML path relative to its resource directory.", Keys: []string{"path"}},
	{Name: "original_file_path", Type: tString, Doc: "YAML path relative to the project root.", Keys: []string{"original_file_path"}},
}, func(c *rowCtx) [][]any {
	preMF := c.s("calculation_method")
	metricType := firstNonNull(preMF, c.s("type"))
	expr := firstNonNull(c.s("type_params", "expr"), c.s("type_params", "measure", "name"), c.s("expression"), c.s("sql"))
	if s, ok := expr.(string); ok && strings.HasPrefix(s, "{") {
		expr = firstNonNull(c.s("type_params", "measure", "name"), nil)
	}
	var filter any
	if wt, ok := rawString(path(c.F, "filter", "where_sql_template")).(string); ok {
		filter = wt
	} else if wf, ok := rawObject(path(c.F, "filter"))["where_filters"]; ok {
		var parts []string
		var arr []map[string]json.RawMessage
		if json.Unmarshal(wf, &arr) == nil {
			for _, w := range arr {
				if s, ok := rawString(w["where_sql_template"]).(string); ok {
					parts = append(parts, s)
				}
			}
		}
		if len(parts) > 0 {
			filter = strings.Join(parts, " AND ")
		}
	} else {
		var arr []map[string]json.RawMessage
		if json.Unmarshal(path(c.F, "filters"), &arr) == nil {
			var parts []string
			for _, f := range arr {
				parts = append(parts, strings.TrimSpace(asText(f["field"])+" "+asText(f["operator"])+" "+asText(f["value"])))
			}
			if len(parts) > 0 {
				filter = strings.Join(parts, " AND ")
			}
		}
	}
	return one(c.id(), c.s("package_name"), c.s("name"), c.s("label"), metricType, c.s("description"), expr, filter,
		preMF, firstNonNull(c.s("timestamp"), c.s("type_params", "measure", "agg_time_dimension")),
		c.tags("tags"), c.js("meta"), c.dependsOn("nodes"), firstNonNull(c.s("group"), c.s("config", "group")),
		c.s("path"), c.s("original_file_path"))
}, fnDocs{
	Title:       "dbt Metrics",
	Category:    "semantic",
	Keywords:    "dbt, metrics, metricflow, semantic layer, kpi, measures",
	Description: "Every metric in a manifest.json, normalized across pre-MetricFlow (v7-v9) and MetricFlow (v10+) shapes",
	DocLLM: "metrics(manifest) returns one row per metric: unique_id, name, label, metric_type, description, expression, " +
		"filter_expression, agg, agg_time_dimension, tags, meta, depends_on_ids, group_name, path. Pre-MetricFlow manifests " +
		"map calculation_method to metric_type/agg and sql to expression. A manifest without metrics returns no rows.",
	DocMD: "## metrics(manifest)\n\nMetric definitions, both legacy and MetricFlow shapes.",
	Examples: []vgi.CatalogExample{
		{SQL: "SELECT name, metric_type, expression FROM dbt.main.metrics('sample://jaffle_shop/v12/manifest.json') ORDER BY name",
			Description: "List metrics and their type"},
	},
})

func asText(raw json.RawMessage) string {
	if v, ok := rawString(raw).(string); ok {
		return v
	}
	return ""
}

var semanticModelsTable = manifestTable("semantic_models", []string{"semantic_models"}, []colSpec{
	{Name: "unique_id", Type: tString, Doc: "Semantic model unique id.", NotNull: true},
	{Name: "package_name", Type: tString, Doc: "Package that declares it.", Keys: []string{"package_name"}},
	{Name: "name", Type: tString, Doc: "Semantic model name.", Keys: []string{"name"}},
	{Name: "description", Type: tString, Doc: "Description.", Keys: []string{"description"}},
	{Name: "model_ref", Type: tString, Doc: "The model it is built on, e.g. ref('orders').", Keys: []string{"model"}},
	{Name: "db_name", Type: tString, Doc: "Database of the underlying relation.", Keys: []string{"node_relation"}},
	{Name: "db_schema", Type: tString, Doc: "Schema of the underlying relation.", Keys: []string{"node_relation"}},
	{Name: "identifier", Type: tString, Doc: "Identifier (alias) of the underlying relation.", Keys: []string{"node_relation"}},
	{Name: "primary_entity", Type: tString, Doc: "Declared primary_entity, else NULL.", Keys: []string{"primary_entity"}},
	{Name: "entities", Type: tJSON, Doc: "Entities, as JSON.", Keys: []string{"entities"}},
	{Name: "dimensions", Type: tJSON, Doc: "Dimensions, as JSON.", Keys: []string{"dimensions"}},
	{Name: "measures", Type: tJSON, Doc: "Measures, as JSON.", Keys: []string{"measures"}},
	{Name: "defaults", Type: tJSON, Doc: "Defaults such as agg_time_dimension, as JSON.", Keys: []string{"defaults"}},
	{Name: "group_name", Type: tString, Doc: "Group, else NULL.", Keys: []string{"group", "config"}},
	{Name: "path", Type: tString, Doc: "YAML path relative to its resource directory.", Keys: []string{"path"}},
	{Name: "original_file_path", Type: tString, Doc: "YAML path relative to the project root.", Keys: []string{"original_file_path"}},
}, func(c *rowCtx) [][]any {
	return one(c.id(), c.s("package_name"), c.s("name"), c.s("description"), c.s("model"),
		c.s("node_relation", "database"), c.s("node_relation", "schema_name"), c.s("node_relation", "alias"),
		c.s("primary_entity"), c.js("entities"), c.js("dimensions"), c.js("measures"), c.js("defaults"),
		firstNonNull(c.s("group"), c.s("config", "group")), c.s("path"), c.s("original_file_path"))
}, fnDocs{
	Title:       "dbt Semantic Models",
	Category:    "semantic",
	Keywords:    "dbt, semantic models, metricflow, entities, dimensions, measures",
	Description: "Every MetricFlow semantic model in a manifest.json (v10+; older manifests return no rows)",
	DocLLM: "semantic_models(manifest) returns one row per semantic model: unique_id, name, description, model_ref, db_name, " +
		"db_schema, identifier, primary_entity, entities/dimensions/measures/defaults (JSON), group_name, path. Use " +
		"json_extract / unnest on the JSON columns to list measures or dimensions.",
	DocMD: "## semantic_models(manifest)\n\nMetricFlow semantic models (manifest v10+).",
	Examples: []vgi.CatalogExample{
		{SQL: "SELECT name, model_ref, json_array_length(measures) AS measures FROM dbt.main.semantic_models('sample://jaffle_shop/v12/manifest.json') ORDER BY name",
			Description: "Semantic models and how many measures each defines"},
	},
})

var groupsTable = manifestTable("groups", []string{"groups"}, []colSpec{
	{Name: "unique_id", Type: tString, Doc: "Group unique id.", NotNull: true},
	{Name: "package_name", Type: tString, Doc: "Package that declares it.", Keys: []string{"package_name"}},
	{Name: "name", Type: tString, Doc: "Group name.", Keys: []string{"name"}},
	{Name: "owner_name", Type: tString, Doc: "Owner name.", Keys: []string{"owner"}},
	{Name: "owner_email", Type: tString, Doc: "Owner email.", Keys: []string{"owner"}},
	{Name: "path", Type: tString, Doc: "YAML path relative to its resource directory.", Keys: []string{"path"}},
	{Name: "original_file_path", Type: tString, Doc: "YAML path relative to the project root.", Keys: []string{"original_file_path"}},
}, func(c *rowCtx) [][]any {
	return one(c.id(), c.s("package_name"), c.s("name"), c.s("owner", "name"), c.s("owner", "email"), c.s("path"), c.s("original_file_path"))
}, fnDocs{
	Title:       "dbt Groups",
	Category:    "manifest",
	Keywords:    "dbt, groups, ownership, access, model governance",
	Description: "Every group (model ownership unit) declared in a manifest.json (v9+; older manifests return no rows)",
	DocLLM: "groups(manifest) returns one row per group: unique_id, package_name, name, owner_name, owner_email, path. Join " +
		"nodes.group_name = groups.name to see which models each team owns.",
	DocMD: "## groups(manifest)\n\nGroups and their owners (manifest v9+).",
	Examples: []vgi.CatalogExample{
		{SQL: "SELECT name, owner_email FROM dbt.main.groups('sample://jaffle_shop/v12/manifest.json') ORDER BY name",
			Description: "Groups and their owners"},
	},
})

var macrosTable = func() *artifactTable {
	t := manifestTable("macros", []string{"macros"}, []colSpec{
		{Name: "unique_id", Type: tString, Doc: "Macro unique id, e.g. macro.dbt.ref.", NotNull: true},
		{Name: "package_name", Type: tString, Doc: "Package that defines the macro (dbt, dbt_duckdb, your project, ...).", Keys: []string{"package_name"}},
		{Name: "name", Type: tString, Doc: "Macro name.", Keys: []string{"name"}},
		{Name: "macro_sql", Type: tString, Doc: "Jinja source of the macro. Skipped without decoding when not selected.", Keys: []string{"macro_sql"}},
		{Name: "path", Type: tString, Doc: "File path relative to its macro directory.", Keys: []string{"path", "original_file_path"}},
		{Name: "arguments", Type: tJSON, Doc: "Documented arguments, as JSON.", Keys: []string{"arguments"}},
		{Name: "depends_on_macros", Type: tStrList, Doc: "Macros this macro calls.", Keys: []string{"depends_on"}},
	}, func(c *rowCtx) [][]any {
		return one(c.id(), c.s("package_name"), c.s("name"), c.s("macro_sql"), firstNonNull(c.s("original_file_path"), c.s("path")),
			c.js("arguments"), c.dependsOn("macros"))
	}, fnDocs{
		Title:       "dbt Macros",
		Category:    "manifest",
		Keywords:    "dbt, macros, jinja, packages, adapter macros",
		Description: "Every macro in a manifest.json, including those from dbt and installed packages",
		DocLLM: "macros(manifest) returns one row per macro: unique_id, package_name, name, macro_sql (Jinja source, skipped " +
			"when not selected), path, arguments (JSON), depends_on_macros. Filter package_name to your project to " +
			"exclude dbt's built-in macros.",
		DocMD: "## macros(manifest)\n\nAll macros; filter `package_name` to skip dbt's own.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT name, path FROM dbt.main.macros('sample://jaffle_shop/v12/manifest.json') WHERE package_name = 'jaffle_shop' ORDER BY name",
				Description: "Macros defined by the project itself"},
		},
	})
	t.KeyFilter = func(key string, pf *vgi.PushdownFilters) bool {
		_, pkg := uniqueIDPrefix(key)
		if vals, ok := vgi.EqualOrInValues[string](pf, "package_name"); ok && !contains(vals, pkg) {
			return false
		}
		return true
	}
	t.FilterCols = []string{"package_name"}
	return t
}()
