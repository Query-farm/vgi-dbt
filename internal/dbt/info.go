// artifact_info (compatibility triage), manifest_version, and the static
// schema_versions support matrix.

package dbt

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// supportRange is the support window for one artifact type.
type supportRange struct{ Min, Max int }

// supportWindow: versions inside are supported, newer are best_effort, older unsupported.
var supportWindow = map[string]supportRange{
	"manifest":    {7, 12},
	"run_results": {4, 6},
	"catalog":     {1, 1},
	"sources":     {3, 3},
}

// supportStatus classifies an artifact version under the published policy.
func supportStatus(kind string, v int) (string, string) {
	w, ok := supportWindow[kind]
	if !ok || v == 0 {
		return "unsupported", "not a recognized dbt artifact schema"
	}
	switch {
	case v > w.Max:
		return "best_effort", fmt.Sprintf("%s v%d is newer than the newest verified version (v%d); parsed best-effort, columns the worker does not know are ignored", kind, v, w.Max)
	case v < w.Min:
		return "unsupported", fmt.Sprintf("%s v%d predates the support window (v%d-v%d); parsed best-effort, many columns will be NULL", kind, v, w.Min, w.Max)
	}
	return "supported", ""
}

var countSections = map[string][]string{
	"manifest":    {"nodes"},
	"run_results": {"results"},
	"catalog":     {"nodes", "sources"},
	"sources":     {"results"},
}

// countEntries counts the members of the given top-level sections, skipping values.
func countEntries(p string, sections []string) (int64, error) {
	var n int64
	for _, sec := range sections {
		s, err := openScanner(p, 0)
		if err != nil {
			return 0, err
		}
		found, err := s.seekTopLevelKey(sec)
		if err != nil || !found {
			_ = s.Close()
			if err != nil {
				return 0, err
			}
			continue
		}
		b, err := s.peek()
		if err != nil || (b != '{' && b != '[') {
			_ = s.Close()
			continue
		}
		_, _ = s.readByte()
		isObj := b == '{'
		first := true
		for {
			c, err := s.peek()
			if err != nil {
				_ = s.Close()
				return 0, err
			}
			if c == '}' || c == ']' {
				break
			}
			if !first {
				if err := s.expect(','); err != nil {
					_ = s.Close()
					return 0, err
				}
			}
			first = false
			if isObj {
				if err := s.skipValue(); err != nil {
					_ = s.Close()
					return 0, err
				}
				if err := s.expect(':'); err != nil {
					_ = s.Close()
					return 0, err
				}
			}
			if err := s.skipValue(); err != nil {
				_ = s.Close()
				return 0, err
			}
			n++
		}
		_ = s.Close()
	}
	return n, nil
}

var infoSchema = arrow.NewSchema([]arrow.Field{
	column("path", tString, "The artifact file (as named, or the URL).", false),
	column("artifact_type", tString, "manifest, run_results, catalog or sources (from metadata.dbt_schema_version).", true),
	column("schema_version", tString, "Artifact schema version, e.g. v12.", true),
	column("dbt_version", tString, "dbt version that wrote the file.", true),
	column("generated_at", tTS, "When dbt wrote the file.", true),
	column("invocation_id", tString, "dbt invocation id.", true),
	column("env", tJSON, "metadata.env (DBT_ENV_CUSTOM_ENV_* values), as JSON.", true),
	column("node_count", tInt64, "Entries in the main section: nodes (manifest), results (run_results, sources), nodes+sources (catalog).", true),
	column("bytes", tInt64, "File size in bytes.", true),
	column("support_status", tString, "supported, best_effort or unsupported under the published support policy.", false),
	column("warning", tString, "Why the version is best_effort/unsupported; NULL when supported.", true),
}, nil)

// InfoState is artifact_info's cursor: the next file to describe.
type InfoState struct {
	Files []ArtifactFile
	Next  int
}

type artifactInfo struct{}

var _ vgi.TypedTableFunc[InfoState] = (*artifactInfo)(nil)

func (*artifactInfo) Name() string { return "artifact_info" }

var infoDocs = fnDocs{
	Title:       "dbt Artifact Info",
	Category:    "reference",
	Keywords:    "dbt, artifact, schema version, compatibility, triage, manifest version, metadata",
	Description: "Compatibility triage for dbt artifacts: type, schema version, dbt version, size and support status, one row per file",
	DocLLM: "artifact_info(path) describes each artifact file the path (or glob/URL) names: artifact_type, schema_version " +
		"(e.g. v12), dbt_version, generated_at, invocation_id, env, node_count, bytes, support_status " +
		"(supported/best_effort/unsupported) and a warning. Run it first on an unfamiliar artifact: an unknown version " +
		"is a row here, not a crash later.",
	DocMD: "## artifact_info(path)\n\nThe triage step: what is this file, which dbt wrote it, and is its schema version supported? Point it at `target/*.json` before querying an unfamiliar project.",
	Examples: []vgi.CatalogExample{
		{SQL: "SELECT artifact_type, schema_version, dbt_version, support_status FROM dbt.main.artifact_info('sample://jaffle_shop/v12/*.json') ORDER BY artifact_type",
			Description: "Triage every artifact in a target directory"},
		{SQL: "SELECT schema_version, dbt_version, node_count FROM dbt.main.artifact_info('sample://jaffle_shop/*/manifest.json') ORDER BY dbt_version",
			Description: "Manifest versions across several dbt releases"},
	},
}

func (*artifactInfo) Metadata() vgi.FunctionMetadata {
	return functionMetadata(infoDocs, schemaColumns(infoSchema), vgi.FunctionMetadata{
		Stability:       vgi.StabilityConsistentWithinQuery,
		RequiredSecrets: []vgi.SecretRequirement{{SecretType: CloudSecretType}},
	})
}

func (*artifactInfo) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{{Name: "path", Position: 0, ArrowType: "varchar", IsConst: true,
		Doc: "Artifact file, glob (e.g. 'target/*.json'), https:// URL or dbt-cloud://runs/<run_id>/<artifact>.json."}}
}

func (*artifactInfo) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(infoSchema)
}

func (*artifactInfo) NewState(params *vgi.ProcessParams) (*InfoState, error) {
	arg, err := params.Args.GetScalarString(0)
	if err != nil {
		return nil, argErr("path must be a non-NULL string")
	}
	files, err := resolveArtifacts(context.Background(), arg, params.Secrets)
	if err != nil {
		return nil, err
	}
	return &InfoState{Files: files}, nil
}

func (*artifactInfo) Process(_ context.Context, params *vgi.ProcessParams, st *InfoState, out *vgirpc.OutputCollector) error {
	if st.Next >= len(st.Files) {
		return out.Finish()
	}
	var rows [][]any
	for ; st.Next < len(st.Files) && len(rows) < 64; st.Next++ {
		f := st.Files[st.Next]
		var size any
		if info, err := os.Stat(f.Local); err == nil {
			size = info.Size()
		}
		m, err := readMeta(f.Local)
		if err != nil {
			rows = append(rows, []any{f.Display, nil, nil, nil, nil, nil, nil, nil, size, "unsupported", err.Error()})
			continue
		}
		status, warn := supportStatus(m.Type, m.SchemaVersion)
		var count any
		if secs, ok := countSections[m.Type]; ok {
			n, err := countEntries(f.Local, secs)
			if err != nil {
				return fmt.Errorf("%s: %w", f.Display, err)
			}
			count = n
		}
		rows = append(rows, []any{f.Display, nilIfEmpty(m.Type), nilIfEmpty(m.VersionLabel()), nilIfEmpty(m.DbtVersion),
			timeOrNil(m.GeneratedAt), nilIfEmpty(m.InvocationID), jsonOrNil(m.Env), count, size, status, nilIfEmpty(warn)})
	}
	batch, err := buildBatch(params.OutputSchema, infoSchema, rows)
	if err != nil {
		return err
	}
	return out.Emit(batch)
}

func jsonOrNil(s string) any {
	if s == "" {
		return nil
	}
	return jsonText(s)
}

// schemaColumns describes a schema for vgi.result_columns_schema.
func schemaColumns(s *arrow.Schema) []resultColumn {
	out := make([]resultColumn, s.NumFields())
	for i, f := range s.Fields() {
		c, _ := f.Metadata.GetValue("comment")
		out[i] = resultColumn{Name: f.Name, Type: duckType(f.Type), Description: c}
	}
	return out
}

// ---------------------------------------------------------------- manifest_version

var versionMemo sync.Map

type manifestVersionArgs struct {
	Path *array.String `vgi:"pos=0,const=false,doc=Path to a local dbt artifact file (manifest.json or any other artifact)."`
}

// manifestVersionFn is the manifest_version(path) scalar.
type manifestVersionFn struct{}

var manifestVersionDocs = fnDocs{
	Title:       "Artifact Schema Version",
	Category:    "reference",
	Keywords:    "dbt, manifest version, schema version, dbt_schema_version, header",
	Description: "Schema version of a local dbt artifact (e.g. 'v12'), read from its metadata block only",
	DocLLM: "manifest_version(path) returns the artifact's schema version such as 'v12' by reading only the metadata block " +
		"at the top of the file (cheap even on a 500 MB manifest; memoized on path, size and mtime). NULL path gives NULL; " +
		"a missing or non-dbt file is an error. Local files only.",
	DocMD: "## manifest_version(path)\n\nCheap header probe: `'v12'` for a dbt 1.8+ manifest.",
	Examples: []vgi.CatalogExample{
		{SQL: "SELECT dbt.main.manifest_version('sample://jaffle_shop/v12/manifest.json') AS version", Description: "Schema version of a manifest"},
	},
	Executable: []execExample{
		{Description: "Schema version of the v9 fixture", SQL: "SELECT dbt.main.manifest_version('sample://jaffle_shop/v9/manifest.json') AS version", Expected: [][]any{{"v9"}}},
	},
}

func (*manifestVersionFn) Name() string { return "manifest_version" }

func (*manifestVersionFn) Metadata() vgi.FunctionMetadata {
	return functionMetadata(manifestVersionDocs, nil, vgi.FunctionMetadata{
		Stability: vgi.StabilityConsistentWithinQuery, ReturnType: tString})
}

func (*manifestVersionFn) OnBindTyped(*manifestVersionArgs, *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(tString)
}

func (*manifestVersionFn) ProcessTyped(_ context.Context, _ *manifestVersionArgs, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	col := batch.Column(0)
	var firstErr error
	res, err := vgi.MapColumn(params, batch, 0, array.NewStringBuilder, func(c arrow.Array, i int) string {
		p, err := localPath(vgi.GetStringValue(c, i))
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			return ""
		}
		id, err := fileIdentity(p)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("artifact not found: %s", p)
			}
			return ""
		}
		if v, ok := versionMemo.Load(id); ok {
			return v.(string)
		}
		m, err := readMeta(p)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			return ""
		}
		versionMemo.Store(id, m.VersionLabel())
		return m.VersionLabel()
	})
	_ = col
	if firstErr != nil {
		if res != nil {
			res.Release()
		}
		return nil, firstErr
	}
	return res, err
}

// ---------------------------------------------------------------- schema_versions

type versionRow struct {
	Type, Version, DbtRange, Notes string
}

var versionRows = []versionRow{
	{"manifest", "v5", "1.1", "Pre-1.3 field names (raw_sql/compiled_sql)."},
	{"manifest", "v6", "1.2", "Pre-1.3 field names (raw_sql/compiled_sql)."},
	{"manifest", "v7", "1.3", "raw_code/compiled_code introduced; metrics use calculation_method/expression."},
	{"manifest", "v8", "1.4", "Legacy metrics; no groups or contracts."},
	{"manifest", "v9", "1.5", "Groups, access, model contracts and model versions."},
	{"manifest", "v10", "1.6", "MetricFlow semantic models and metrics; deprecation_date."},
	{"manifest", "v11", "1.7", "Saved queries; semantic-model refinements."},
	{"manifest", "v12", "1.8 - 1.12", "Unit tests; current format."},
	{"run_results", "v4", "1.0 - 1.5", "Results with timing, adapter_response and args."},
	{"run_results", "v5", "1.6 - 1.7", "Adds compiled_code and relation_name per result."},
	{"run_results", "v6", "1.8 - 1.12", "Current format."},
	{"catalog", "v1", "1.0 - 1.12", "Unchanged since dbt 1.0."},
	{"sources", "v3", "1.0 - 1.12", "Freshness results with criteria and timing."},
}

var schemaVersionsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "artifact_type", Type: tString},
	{Name: "schema_version", Type: tString},
	{Name: "dbt_version_range", Type: tString, Nullable: true},
	{Name: "supported", Type: tBool, Nullable: true},
	{Name: "support_status", Type: tString, Nullable: true},
	{Name: "notes", Type: tString, Nullable: true},
}, nil)

// SchemaVersionsState is the (single-batch) cursor of the schema_versions table.
type SchemaVersionsState struct{ Done bool }

type schemaVersionsScan struct{}

func (*schemaVersionsScan) Name() string { return "schema_versions_scan" }

func (*schemaVersionsScan) Metadata() vgi.FunctionMetadata {
	return vgi.FunctionMetadata{Description: "Rows of the schema_versions support matrix", Stability: vgi.StabilityConsistent}
}

func (*schemaVersionsScan) ArgumentSpecs() []vgi.ArgSpec { return nil }

func (*schemaVersionsScan) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(schemaVersionsSchema)
}

func (*schemaVersionsScan) NewState(*vgi.ProcessParams) (*SchemaVersionsState, error) {
	return &SchemaVersionsState{}, nil
}

func (*schemaVersionsScan) Process(_ context.Context, params *vgi.ProcessParams, st *SchemaVersionsState, out *vgirpc.OutputCollector) error {
	if st.Done {
		return out.Finish()
	}
	st.Done = true
	rows := make([][]any, len(versionRows))
	for i, r := range versionRows {
		var n int
		_, _ = fmt.Sscanf(r.Version, "v%d", &n)
		status, _ := supportStatus(r.Type, n)
		rows[i] = []any{r.Type, r.Version, r.DbtRange, status == "supported", status, r.Notes}
	}
	batch, err := buildBatch(params.OutputSchema, schemaVersionsSchema, rows)
	if err != nil {
		return err
	}
	return out.Emit(batch)
}

var schemaVersionsExamples = []vgi.CatalogExample{
	{SQL: "SELECT schema_version, dbt_version_range FROM dbt.main.schema_versions WHERE artifact_type = 'manifest' AND supported ORDER BY schema_version",
		Description: "Supported manifest versions and the dbt releases that write them"},
	{SQL: "SELECT i.dbt_version, v.support_status FROM dbt.main.artifact_info('sample://jaffle_shop/v12/manifest.json') i JOIN dbt.main.schema_versions v USING (artifact_type, schema_version)",
		Description: "Check an artifact against the support matrix"},
}

func schemaVersionsTable() vgi.CatalogTable {
	cons := func(id string, kind vgi.ConstraintKind, cols ...string) []byte {
		cc := make([][]string, len(cols))
		for i, c := range cols {
			cc[i] = []string{c}
		}
		rec, err := vgi.ConstraintRecord(vgi.ConstraintRecordSpec{SchemaPath: []string{"main"}, Table: "schema_versions", ID: id,
			Kind: kind, Columns: cc, Enforcement: vgi.ConstraintEnforced, Validation: vgi.ConstraintAllRows})
		if err != nil {
			panic(err)
		}
		return rec
	}
	return vgi.CatalogTable{
		Name:     "schema_versions",
		Comment:  "Static compatibility matrix: which dbt artifact schema versions the worker supports (supported / best_effort / unsupported).",
		Columns:  schemaVersionsSchema,
		Function: vgi.AsTableFunction[SchemaVersionsState](&schemaVersionsScan{}),
		Constraints: [][]byte{
			cons("type_not_null", vgi.ConstraintNotNull, "artifact_type"),
			cons("version_not_null", vgi.ConstraintNotNull, "schema_version"),
			cons("pk", vgi.ConstraintPrimaryKey, "artifact_type", "schema_version"),
		},
		Tags: withDomain(withExamples(objectTags(docs{
			Title:    "Artifact Schema Versions",
			Category: "reference",
			Keywords: "dbt, schema versions, support policy, compatibility, manifest v12, run_results v6",
			DocLLM: "schema_versions is the worker's static support matrix: one row per (artifact_type, schema_version) with " +
				"the dbt-core releases that write it, supported (a `BOOLEAN`), support_status (supported, best_effort or " +
				"unsupported) and notes. manifest v7-v12, run_results v4-v6, catalog v1 and sources v3 are supported; a newer " +
				"version is best_effort. Join it to artifact_info(path) on artifact_type and schema_version.",
			DocMD: "## schema_versions\n\nThe published support policy as a table, so \"is my version supported\" is a query.",
		}), schemaVersionsExamples)),
		ColumnComments: map[string]string{
			"artifact_type":     "manifest, run_results, catalog or sources.",
			"schema_version":    "Schema version, e.g. v12.",
			"dbt_version_range": "dbt-core releases that write this version.",
			"supported":         "True when inside the verified support window.",
			"support_status":    "supported, best_effort (newer than verified) or unsupported (older than the window).",
			"notes":             "What changed in this version.",
		},
	}
}

// withDomain adds the classifying domain tag catalog tables carry.
func withDomain(tags map[string]string) map[string]string {
	tags["domain"] = "analytics engineering"
	return tags
}
