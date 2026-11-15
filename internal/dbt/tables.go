// The generic artifact table function.
//
// Every artifact table (nodes, sources, run_results, catalog_columns, ...) is
// an artifactTable definition run by one engine:
//
//   - the argument resolves to one or more files (a glob is one partition per
//     file, streamed in order);
//   - each file's metadata block is read first (version normalization keys
//     off metadata.dbt_schema_version);
//   - the engine walks the named top-level sections (an object keyed by
//     unique_id, or an array), reading only the JSON keys the projected
//     columns need: unselected raw_code / compiled_code / macro_sql are
//     skipped without being decoded;
//   - after each batch the cursor is (file index, section index, byte offset),
//     so the scan resumes by seeking, on any transport.

package dbt

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

const (
	// batchRows caps rows per emitted batch.
	batchRows = 4096
	// batchEntries caps artifact entries read per Process call.
	batchEntries = 2048
	// firstBatchEntries caps the first call, so the first rows arrive fast.
	firstBatchEntries = 64
	// batchBytes caps the artifact bytes scanned per Process call.
	batchBytes = 64 << 20
)

// colSpec is one output column and the top-level JSON keys it reads.
type colSpec struct {
	Name string
	Type arrow.DataType
	Doc  string
	Keys []string
	// NotNull marks a column that is never NULL.
	NotNull bool
}

// rowCtx is one artifact entry handed to a table's row builder.
type rowCtx struct {
	Key     string // object key (unique_id); "" for array entries
	F       map[string]json.RawMessage
	Meta    FileMeta
	File    ArtifactFile
	Section string
	Ordinal int64 // index of the entry within its section
	Opt     string
}

// fnDocs is the discovery metadata of one function.
type fnDocs struct {
	Title       string
	Category    string
	Keywords    string
	Description string
	DocLLM      string
	DocMD       string
	Examples    []vgi.CatalogExample
	Executable  []execExample
}

// execExample is one vgi.executable_examples entry.
type execExample struct {
	Description string  `json:"description"`
	SQL         string  `json:"sql"`
	Expected    [][]any `json:"expected_result"`
}

// artifactTable defines one artifact table function.
type artifactTable struct {
	Name     string
	Kind     artifactKind
	ArgName  string // "manifest" or "path"
	ArgDoc   string
	Sections []string
	Cols     []colSpec
	// AlwaysKeys are read whatever the projection (multi-row expansions).
	AlwaysKeys []string
	// Rows returns full-width rows (values in Cols order) for one entry.
	Rows func(c *rowCtx) [][]any
	// KeyFilter can reject an entry from its unique_id alone (nil = keep all).
	KeyFilter func(key string, pf *vgi.PushdownFilters) bool
	// FilterCols are string columns whose = / IN filters prune rows early.
	FilterCols []string
	// Option is an optional extra named VARCHAR argument (depends_on kind).
	Option *vgi.ArgSpec
	Docs   fnDocs
}

func (t *artifactTable) schema() *arrow.Schema {
	fields := make([]arrow.Field, len(t.Cols))
	for i, c := range t.Cols {
		fields[i] = column(c.Name, c.Type, c.Doc, !c.NotNull)
	}
	return arrow.NewSchema(fields, nil)
}

// ArtifactScanState is the resumable cursor (exported: gob-encoded over HTTP).
type ArtifactScanState struct {
	Files      []ArtifactFile
	FileIdx    int
	SecIdx     int
	Offset     int64
	InSection  bool
	IsArray    bool
	First      bool
	Ordinal    int64
	Meta       FileMeta
	MetaLoaded bool
	Opt        string
	Calls      int
}

type artifactFunc struct{ t *artifactTable }

var _ vgi.TypedTableFunc[ArtifactScanState] = (*artifactFunc)(nil)

func (f *artifactFunc) Name() string { return f.t.Name }

func (f *artifactFunc) Metadata() vgi.FunctionMetadata {
	return functionMetadata(f.t.Docs, f.t.resultColumns(), vgi.FunctionMetadata{
		Stability:          vgi.StabilityConsistentWithinQuery,
		ProjectionPushdown: true,
		FilterPushdown:     len(f.t.FilterCols) > 0,
		RequiredSecrets:    []vgi.SecretRequirement{{SecretType: CloudSecretType}},
	})
}

func (t *artifactTable) resultColumns() []resultColumn {
	out := make([]resultColumn, len(t.Cols))
	for i, c := range t.Cols {
		out[i] = resultColumn{Name: c.Name, Type: duckType(c.Type), Description: c.Doc}
	}
	return out
}

func (f *artifactFunc) ArgumentSpecs() []vgi.ArgSpec {
	specs := []vgi.ArgSpec{{Name: f.t.ArgName, Position: 0, ArrowType: "varchar", IsConst: true, Doc: f.t.ArgDoc}}
	if f.t.Option != nil {
		specs = append(specs, *f.t.Option)
	}
	return specs
}

func (f *artifactFunc) OnBind(_ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(f.t.schema())
}

func (f *artifactFunc) NewState(params *vgi.ProcessParams) (*ArtifactScanState, error) {
	arg, err := params.Args.GetScalarString(0)
	if err != nil {
		return nil, argErr(f.t.ArgName + " must be a non-NULL path")
	}
	files, err := resolveArtifacts(context.Background(), arg, params.Secrets)
	if err != nil {
		return nil, err
	}
	st := &ArtifactScanState{Files: files}
	if f.t.Option != nil {
		st.Opt = vgi.OptionalString(params.Args, f.t.Option.Name, f.t.Option.DefaultValue)
	}
	return st, nil
}

// neededKeys is the set of top-level keys the projection (and filters) read.
func (t *artifactTable) neededKeys(out *arrow.Schema, pf *vgi.PushdownFilters) map[string]bool {
	want := map[string]bool{}
	for _, k := range t.AlwaysKeys {
		want[k] = true
	}
	names := map[string]bool{}
	for _, fld := range out.Fields() {
		names[fld.Name] = true
	}
	if pf != nil {
		for c := range pf.FilteredColumns() {
			names[c] = true
		}
	}
	for _, c := range t.Cols {
		if names[c.Name] {
			for _, k := range c.Keys {
				want[k] = true
			}
		}
	}
	return want
}

// rowFilter builds the early = / IN pruning for FilterCols.
func (t *artifactTable) rowFilter(pf *vgi.PushdownFilters) func(row []any) bool {
	type cond struct {
		idx  int
		vals map[string]bool
	}
	var conds []cond
	for _, name := range t.FilterCols {
		vals, ok := vgi.EqualOrInValues[string](pf, name)
		if !ok {
			continue
		}
		set := map[string]bool{}
		for _, v := range vals {
			set[v] = true
		}
		for i, c := range t.Cols {
			if c.Name == name {
				conds = append(conds, cond{i, set})
			}
		}
	}
	return func(row []any) bool {
		for _, c := range conds {
			s, ok := row[c.idx].(string)
			if !ok || !c.vals[s] {
				return false
			}
		}
		return true
	}
}

func (f *artifactFunc) Process(ctx context.Context, params *vgi.ProcessParams, st *ArtifactScanState, out *vgirpc.OutputCollector) error {
	t := f.t
	pf := params.CurrentPushdownFilters
	rows, err := t.step(st, t.neededKeys(params.OutputSchema, pf), pf)
	if err != nil {
		return err
	}
	if rows, err = applyFilters(ctx, pf, t.schema(), rows); err != nil {
		return err
	}
	if len(rows) == 0 && st.FileIdx >= len(st.Files) {
		return out.Finish()
	}
	batch, err := buildBatch(params.OutputSchema, t.schema(), rows)
	if err != nil {
		return err
	}
	return out.Emit(batch)
}

// step reads up to one batch of entries, advancing the cursor. want is the
// set of JSON keys to decode (nil = AlwaysKeys only).
func (t *artifactTable) step(st *ArtifactScanState, want map[string]bool, pf *vgi.PushdownFilters) ([][]any, error) {
	st.Calls++
	if st.Calls == 1 {
		// A small first batch keeps first-row latency low on huge manifests.
		return t.stepN(st, want, pf, firstBatchEntries)
	}
	return t.stepN(st, want, pf, batchEntries)
}

// stepN is step reading at most maxEntries entries.
func (t *artifactTable) stepN(st *ArtifactScanState, want map[string]bool, pf *vgi.PushdownFilters, maxEntries int) ([][]any, error) {
	if want == nil {
		want = map[string]bool{}
		for _, k := range t.AlwaysKeys {
			want[k] = true
		}
	}
	keep := t.rowFilter(pf)
	var rows [][]any
	entries := 0
	var sc *scanner
	scFile := -1
	defer func() {
		if sc != nil {
			_ = sc.Close()
		}
	}()
	closeSc := func() {
		if sc != nil {
			_ = sc.Close()
			sc = nil
		}
	}
	scanned := int64(0)
	for len(rows) < batchRows && entries < maxEntries && scanned < batchBytes {
		if st.FileIdx >= len(st.Files) {
			break
		}
		file := st.Files[st.FileIdx]
		if !st.MetaLoaded {
			m, err := readMeta(file.Local)
			if err != nil {
				return nil, err
			}
			if err := checkKind(file, m, t.Kind); err != nil {
				return nil, err
			}
			st.Meta, st.MetaLoaded = m, true
		}
		if st.SecIdx >= len(t.Sections) {
			st.FileIdx++
			st.SecIdx, st.Offset, st.InSection, st.MetaLoaded = 0, 0, false, false
			closeSc()
			continue
		}
		if sc == nil || scFile != st.FileIdx || sc.off != st.Offset {
			closeSc()
			var err error
			if sc, err = openScanner(file.Local, st.Offset); err != nil {
				return nil, err
			}
			scFile = st.FileIdx
		}
		section := t.Sections[st.SecIdx]
		wrap := func(err error) error { return fmt.Errorf("%s: %w", file.Display, err) }
		if !st.InSection {
			found, err := sc.seekTopLevelKey(section)
			if err != nil {
				return nil, wrap(err)
			}
			b := byte('n')
			if found {
				if b, err = sc.peek(); err != nil {
					return nil, wrap(err)
				}
			}
			if b != '{' && b != '[' {
				st.SecIdx++
				st.Offset = 0
				closeSc()
				continue
			}
			_, _ = sc.readByte()
			st.InSection, st.IsArray, st.First, st.Ordinal = true, b == '[', true, 0
			st.Offset = sc.off
		}
		b, err := sc.peek()
		if err != nil {
			return nil, wrap(err)
		}
		if b == '}' || b == ']' {
			st.SecIdx++
			st.InSection, st.Offset = false, 0
			closeSc()
			continue
		}
		if !st.First {
			if err := sc.expect(','); err != nil {
				return nil, wrap(err)
			}
		}
		st.First = false
		key := ""
		if !st.IsArray {
			if key, err = sc.readString(); err != nil {
				return nil, wrap(err)
			}
			if err := sc.expect(':'); err != nil {
				return nil, wrap(err)
			}
		}
		entries++
		ordinal := st.Ordinal
		st.Ordinal++
		vb, err := sc.peek()
		if err != nil {
			return nil, wrap(err)
		}
		if vb != '{' || (t.KeyFilter != nil && key != "" && !t.KeyFilter(key, pf)) {
			before := sc.off
			if err := sc.skipValue(); err != nil {
				return nil, wrap(err)
			}
			scanned += sc.off - before
			st.Offset = sc.off
			continue
		}
		before := sc.off
		fields, err := sc.readObjectFields(want)
		if err != nil {
			return nil, wrap(err)
		}
		scanned += sc.off - before
		st.Offset = sc.off
		ctx := &rowCtx{Key: key, F: fields, Meta: st.Meta, File: file, Section: section, Ordinal: ordinal, Opt: st.Opt}
		for _, r := range t.Rows(ctx) {
			if keep(r) {
				rows = append(rows, r)
			}
		}
	}
	return rows, nil
}

// duckType renders an Arrow type the way DuckDB names it.
func duckType(t arrow.DataType) string {
	switch t.ID() {
	case arrow.STRING:
		return "VARCHAR"
	case arrow.INT64:
		return "BIGINT"
	case arrow.INT32:
		return "INTEGER"
	case arrow.FLOAT64:
		return "DOUBLE"
	case arrow.BOOL:
		return "BOOLEAN"
	case arrow.TIMESTAMP:
		return "TIMESTAMP WITH TIME ZONE"
	case arrow.LIST:
		return duckType(t.(*arrow.ListType).Elem()) + "[]"
	case arrow.EXTENSION:
		return "JSON"
	case arrow.STRUCT:
		st := t.(*arrow.StructType)
		parts := make([]string, st.NumFields())
		for i, f := range st.Fields() {
			parts[i] = f.Name + " " + duckType(f.Type)
		}
		return "STRUCT(" + strings.Join(parts, ", ") + ")"
	}
	return strings.ToUpper(t.String())
}

// functionMetadata assembles discovery tags around base metadata.
func functionMetadata(d fnDocs, cols []resultColumn, base vgi.FunctionMetadata) vgi.FunctionMetadata {
	tags := objectTags(docs{Title: d.Title, Category: d.Category, Keywords: d.Keywords, DocLLM: d.DocLLM, DocMD: d.DocMD})
	if cols != nil {
		tags["vgi.result_columns_schema"] = mustJSON(cols)
	}
	if len(d.Executable) > 0 {
		tags["vgi.executable_examples"] = mustJSON(d.Executable)
	}
	base.Description = d.Description
	base.Tags = withExamples(tags, d.Examples)
	base.Examples = d.Examples
	return base
}
