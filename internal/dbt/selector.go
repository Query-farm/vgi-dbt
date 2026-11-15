// dbt node selectors (an enumerated subset) and the unique_id / relation
// helpers.
//
// Supported grammar, exactly: tag:, path:, fqn:, package:, resource_type:,
// config.materialized: and bare name / unique_id globs; set union (space)
// and intersection (comma). Everything else -- graph operators (+, n+, +n,
// @), state:, result:, source_status:, other methods and --exclude -- is
// rejected with a reason naming the term. selector_match raises that
// reason as a bind error; parse_selector returns it as a row. Never a
// silent false.

package dbt

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// selAtom is one selector atom, e.g. tag:finance.
type selAtom struct {
	TermIndex int
	Text      string
	Kind      string
	Value     string
	Negated   bool
	Supported bool
	Reason    string
}

var supportedMethods = map[string]string{
	"tag": "tag", "path": "path", "fqn": "fqn", "package": "package",
	"resource_type": "resource_type", "config.materialized": "config.materialized",
}

var (
	graphPrefixRe = regexp.MustCompile(`^(\d*)\+`)
	graphSuffixRe = regexp.MustCompile(`\+(\d*)$`)
)

// parseSelector splits a selector into atoms and classifies each one.
func parseSelector(sel string) []selAtom {
	var atoms []selAtom
	negated := false
	term := 0
	for _, word := range strings.Fields(sel) {
		if word == "--exclude" {
			negated = true
			continue
		}
		term++
		for _, part := range strings.Split(word, ",") {
			if part == "" {
				continue
			}
			atoms = append(atoms, classify(term, part, negated))
		}
	}
	return atoms
}

func classify(term int, text string, negated bool) selAtom {
	a := selAtom{TermIndex: term, Text: text, Negated: negated}
	if negated {
		a.Kind, a.Value = "exclude", text
		a.Reason = fmt.Sprintf("--exclude %q unsupported in a selector string; write AND NOT dbt.main.selector_match(..., '%s') in SQL", text, text)
		return a
	}
	if strings.HasPrefix(text, "@") {
		a.Kind, a.Value = "graph_op", strings.TrimPrefix(text, "@")
		a.Reason = `graph operator "@" unsupported; use dbt.main.descendants(...) and dbt.main.ancestors(...)`
		return a
	}
	if m := graphPrefixRe.FindString(text); m != "" {
		a.Kind, a.Value = "graph_op", strings.TrimPrefix(text, m)
		a.Reason = fmt.Sprintf("graph operator %q unsupported; use dbt.main.ancestors(...)", m)
		if m != "+" {
			a.Reason = fmt.Sprintf("graph operator %q unsupported; use dbt.main.ancestors(..., max_depth := %s)", m, strings.TrimSuffix(m, "+"))
		}
		return a
	}
	if m := graphSuffixRe.FindString(text); m != "" {
		a.Kind, a.Value = "graph_op", strings.TrimSuffix(text, m)
		a.Reason = fmt.Sprintf("graph operator %q unsupported; use dbt.main.descendants(...)", m)
		if m != "+" {
			a.Reason = fmt.Sprintf("graph operator %q unsupported; use dbt.main.descendants(..., max_depth := %s)", m, strings.TrimPrefix(m, "+"))
		}
		return a
	}
	if method, value, ok := strings.Cut(text, ":"); ok {
		a.Value = value
		if k, ok := supportedMethods[method]; ok {
			a.Kind, a.Supported = k, true
			return a
		}
		a.Kind = method
		switch method {
		case "state":
			a.Reason = `selector method "state:" unsupported (needs a comparison manifest); diff two dbt.main.nodes(...) views on checksum with IS DISTINCT FROM`
		case "result":
			a.Reason = `selector method "result:" unsupported (needs run results); join dbt.main.run_results(...) on unique_id`
		case "source_status":
			a.Reason = `selector method "source_status:" unsupported (needs freshness results); join dbt.main.source_freshness(...) on unique_id`
		default:
			a.Reason = fmt.Sprintf("selector method %q unsupported; supported methods are tag:, path:, fqn:, package:, resource_type:, config.materialized:", method+":")
		}
		return a
	}
	a.Kind, a.Value, a.Supported = "name", text, true
	return a
}

// firstUnsupported returns the reason of the first unsupported atom, or "".
func firstUnsupported(atoms []selAtom) string {
	for _, a := range atoms {
		if !a.Supported {
			return fmt.Sprintf("selector term %q: %s", a.Text, a.Reason)
		}
	}
	return ""
}

func globEq(pattern, s string) bool {
	if pattern == s {
		return true
	}
	ok, err := filepath.Match(pattern, s)
	return err == nil && ok
}

// selNode is what a selector is evaluated against.
type selNode struct {
	UniqueID     string
	FQN          []string
	Tags         []string
	Materialized string
}

func (n selNode) resourceType() string { rt, _ := uniqueIDPrefix(n.UniqueID); return rt }

// matchAtom evaluates one supported atom.
func matchAtom(a selAtom, n selNode) bool {
	switch a.Kind {
	case "tag":
		for _, t := range n.Tags {
			if globEq(a.Value, t) {
				return true
			}
		}
		return false
	case "package":
		_, pkg := uniqueIDPrefix(n.UniqueID)
		if len(n.FQN) > 0 {
			pkg = n.FQN[0]
		}
		return globEq(a.Value, pkg)
	case "resource_type":
		return globEq(a.Value, n.resourceType())
	case "config.materialized":
		return globEq(a.Value, n.Materialized)
	case "fqn":
		return fqnMatch(strings.Split(a.Value, "."), n.FQN)
	case "path":
		return pathMatch(a.Value, n.FQN)
	case "name":
		if strings.Contains(a.Value, "/") {
			return pathMatch(a.Value, n.FQN)
		}
		if globEq(a.Value, n.UniqueID) {
			return true
		}
		return fqnMatch(strings.Split(a.Value, "."), n.FQN)
	}
	return false
}

// fqnMatch: the selector parts match a prefix of the fqn, with or without
// the leading package; a single part also matches the node name.
func fqnMatch(parts, fqn []string) bool {
	if len(fqn) == 0 {
		return false
	}
	if len(parts) == 1 && globEq(parts[0], fqn[len(fqn)-1]) {
		return true
	}
	prefix := func(f []string) bool {
		if len(parts) > len(f) {
			return false
		}
		for i, p := range parts {
			if !globEq(p, f[i]) {
				return false
			}
		}
		return true
	}
	return prefix(fqn) || prefix(fqn[1:])
}

var resourceDirs = map[string]bool{"models": true, "seeds": true, "snapshots": true, "tests": true, "analyses": true, "macros": true, "data": true}

// pathMatch compares a path: selector with the fqn's directory structure
// (models/marts -> fqn [pkg, marts, ...]). It assumes the conventional
// resource directories (models/, seeds/, snapshots/, ...).
func pathMatch(p string, fqn []string) bool {
	if len(fqn) < 2 {
		return false
	}
	p = strings.Trim(filepath.ToSlash(p), "/")
	segs := strings.Split(p, "/")
	if len(segs) > 0 && resourceDirs[segs[0]] {
		segs = segs[1:]
	}
	if len(segs) > 0 {
		last := segs[len(segs)-1]
		if ext := filepath.Ext(last); ext == ".sql" || ext == ".py" || ext == ".csv" || ext == ".yml" {
			segs[len(segs)-1] = strings.TrimSuffix(last, ext)
		}
	}
	rel := fqn[1:]
	if len(segs) > len(rel) {
		return false
	}
	for i, s := range segs {
		if !globEq(s, rel[i]) {
			return false
		}
	}
	return true
}

// matchSelector: union over terms, intersection within a term.
func matchSelector(atoms []selAtom, n selNode) bool {
	byTerm := map[int][]selAtom{}
	order := []int{}
	for _, a := range atoms {
		if _, ok := byTerm[a.TermIndex]; !ok {
			order = append(order, a.TermIndex)
		}
		byTerm[a.TermIndex] = append(byTerm[a.TermIndex], a)
	}
	for _, t := range order {
		all := true
		for _, a := range byTerm[t] {
			if !matchAtom(a, n) {
				all = false
				break
			}
		}
		if all {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------- selector_match

// selectorMatchFn is selector_match(unique_id, fqn, tags, materialized, selector).
type selectorMatchFn struct{}

var selectorMatchDocs = fnDocs{
	Title:       "Match dbt Selector",
	Category:    "selectors",
	Keywords:    "dbt, selector, select, tag:, path:, fqn:, node selection, dbt ls",
	Description: "True when a node matches a dbt selector (an enumerated subset of dbt's grammar); unsupported terms fail at bind",
	DocLLM: "selector_match(unique_id, fqn, tags, materialized, selector) evaluates a constant dbt selector against one node " +
		"per row, using the columns of dbt.main.nodes(...). Supported: tag:x, path:x, fqn:x, package:x, resource_type:x, " +
		"config.materialized:x and bare names/globs; space-separated terms are a union, comma-joined atoms an intersection. " +
		"Graph operators (+, n+, @), state:, result:, source_status: and --exclude raise a bind error naming the term; " +
		"use descendants/ancestors, joins, or NOT selector_match(...) instead. Check a selector first with parse_selector.",
	DocMD: "## selector_match(unique_id, fqn, tags, materialized, selector)\n\nA strict subset of dbt node selection. " +
		"Unsupported terms are **errors**, never a silent `false`.",
	Examples: []vgi.CatalogExample{
		{SQL: "SELECT name FROM dbt.main.nodes('sample://jaffle_shop/v12/manifest.json') WHERE dbt.main.selector_match(unique_id, fqn, tags, materialized, 'tag:marts,resource_type:model') ORDER BY name",
			Description: "Models tagged marts"},
	},
	Executable: []execExample{
		{Description: "A path selector against literal node columns",
			SQL:      "SELECT dbt.main.selector_match('model.shop.orders', ['shop', 'marts', 'orders'], ['marts'], 'table', 'path:models/marts') AS hit",
			Expected: [][]any{{true}}},
	},
}

func (*selectorMatchFn) Name() string { return "selector_match" }

func (*selectorMatchFn) Metadata() vgi.FunctionMetadata {
	return functionMetadata(selectorMatchDocs, nil, vgi.FunctionMetadata{
		Stability: vgi.StabilityConsistent, ReturnType: tBool, NullHandling: vgi.NullHandlingSpecial})
}

func (*selectorMatchFn) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{
		{Name: "unique_id", Position: 0, ArrowType: "varchar", Doc: "Node unique_id (nodes.unique_id)."},
		{Name: "fqn", Position: 1, ArrowType: "list", ArrowDataType: tStrList, Doc: "Node fqn list (nodes.fqn)."},
		{Name: "tags", Position: 2, ArrowType: "list", ArrowDataType: tStrList, Doc: "Node tags list (nodes.tags)."},
		{Name: "materialized", Position: 3, ArrowType: "varchar", Doc: "Node materialization (nodes.materialized); may be NULL."},
		{Name: "selector", Position: 4, ArrowType: "varchar", IsConst: true,
			Doc: "Constant dbt selector using only tag:/path:/fqn:/package:/resource_type:/config.materialized: and bare names; space = union, comma = intersection."},
	}
}

func selectorArg(args *vgi.Arguments) (string, error) {
	s, err := args.GetScalarString("selector")
	if err != nil {
		s, err = args.GetScalarString(4)
	}
	if err != nil {
		return "", argErr("selector must be a constant, non-NULL string")
	}
	return s, nil
}

func (*selectorMatchFn) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	sel, err := selectorArg(params.Args)
	if err != nil {
		return nil, err
	}
	atoms := parseSelector(sel)
	if len(atoms) == 0 {
		return nil, argErr("selector is empty")
	}
	if reason := firstUnsupported(atoms); reason != "" {
		return nil, argErr(reason)
	}
	return vgi.BindResult(tBool)
}

func listStrings(a arrow.Array, i int) []string {
	l, ok := a.(*array.List)
	if !ok || l.IsNull(i) {
		return nil
	}
	start, end := l.ValueOffsets(i)
	vals, ok := l.ListValues().(*array.String)
	if !ok {
		return nil
	}
	out := make([]string, 0, end-start)
	for j := start; j < end; j++ {
		if !vals.IsNull(int(j)) {
			out = append(out, vals.Value(int(j)))
		}
	}
	return out
}

func (*selectorMatchFn) Process(_ context.Context, params *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	sel, err := selectorArg(params.Args)
	if err != nil {
		return nil, err
	}
	atoms := parseSelector(sel)
	if reason := firstUnsupported(atoms); reason != "" {
		return nil, argErr(reason)
	}
	ids, fqn, tags, mat := batch.Column(0), batch.Column(1), batch.Column(2), batch.Column(3)
	b := array.NewBooleanBuilder(memory.NewGoAllocator())
	defer b.Release()
	for i := 0; i < int(batch.NumRows()); i++ {
		if ids.IsNull(i) {
			b.AppendNull()
			continue
		}
		n := selNode{UniqueID: vgi.GetStringValue(ids, i), FQN: listStrings(fqn, i), Tags: listStrings(tags, i)}
		if !mat.IsNull(i) {
			n.Materialized = vgi.GetStringValue(mat, i)
		}
		b.Append(matchSelector(atoms, n))
	}
	arr := b.NewArray()
	defer arr.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "result", Type: tBool, Nullable: true}}, nil)
	return array.NewRecordBatch(schema, []arrow.Array{arr}, batch.NumRows()), nil
}

// ---------------------------------------------------------------- parse_selector

var parseSelectorSchema = arrow.NewSchema([]arrow.Field{
	column("term_index", tInt32, "1-based index of the space-separated term the atom belongs to.", false),
	column("term", tString, "The atom's text, e.g. tag:finance.", false),
	column("kind", tString, "Method (tag, path, fqn, package, resource_type, config.materialized, name), graph_op, exclude, or the unsupported method name.", false),
	column("value", tString, "The value after the method.", true),
	column("negated", tBool, "True for atoms after --exclude.", false),
	column("supported", tBool, "Whether selector_match accepts the atom.", false),
	column("reason", tString, "Why the atom is unsupported and what to use instead; NULL when supported.", true),
}, nil)

// ParseSelectorState is the single-batch cursor.
type ParseSelectorState struct {
	Selector string
	Done     bool
}

type parseSelectorFn struct{}

var parseSelectorDocs = fnDocs{
	Title:       "Parse dbt Selector",
	Category:    "selectors",
	Keywords:    "dbt, selector, validate, explain, graph operator, unsupported",
	Description: "Explain a dbt selector atom by atom and say which atoms selector_match supports and why not",
	DocLLM: "parse_selector(selector) splits a dbt selector into atoms (space = union terms, comma = intersection) and returns " +
		"term_index, term, kind, value, negated, supported, reason. Unsupported atoms carry the same reason selector_match " +
		"raises, with the alternative (descendants/ancestors for graph operators, joins for state:/result:). Needs no artifact.",
	DocMD: "## parse_selector(selector)\n\nThe discoverable failure path for `selector_match`.",
	Examples: []vgi.CatalogExample{
		{SQL: "SELECT term, kind, supported, reason FROM dbt.main.parse_selector('tag:finance,+fct_orders') ORDER BY term",
			Description: "See which atom of a selector is unsupported"},
	},
	Executable: []execExample{
		{Description: "A graph operator is rejected with a reason",
			SQL:      "SELECT term, kind, supported FROM dbt.main.parse_selector('tag:finance +fct_orders') ORDER BY term_index",
			Expected: [][]any{{"tag:finance", "tag", true}, {"+fct_orders", "graph_op", false}}},
	},
}

func (*parseSelectorFn) Name() string { return "parse_selector" }

func (*parseSelectorFn) Metadata() vgi.FunctionMetadata {
	return functionMetadata(parseSelectorDocs, schemaColumns(parseSelectorSchema), vgi.FunctionMetadata{Stability: vgi.StabilityConsistent})
}

func (*parseSelectorFn) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{{Name: "selector", Position: 0, ArrowType: "varchar", IsConst: true, Doc: "A dbt selector string, e.g. 'tag:finance,config.materialized:table'."}}
}

func (*parseSelectorFn) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(parseSelectorSchema)
}

func (*parseSelectorFn) NewState(params *vgi.ProcessParams) (*ParseSelectorState, error) {
	s, err := params.Args.GetScalarString(0)
	if err != nil {
		return nil, argErr("selector must be a non-NULL string")
	}
	return &ParseSelectorState{Selector: s}, nil
}

func (*parseSelectorFn) Process(_ context.Context, params *vgi.ProcessParams, st *ParseSelectorState, out *vgirpc.OutputCollector) error {
	if st.Done {
		return out.Finish()
	}
	st.Done = true
	var rows [][]any
	for _, a := range parseSelector(st.Selector) {
		rows = append(rows, []any{int64(a.TermIndex), a.Text, a.Kind, nilIfEmpty(a.Value), a.Negated, a.Supported, nilIfEmpty(a.Reason)})
	}
	batch, err := buildBatch(params.OutputSchema, parseSelectorSchema, rows)
	if err != nil {
		return err
	}
	return out.Emit(batch)
}

// ---------------------------------------------------------------- parse_unique_id

var uniqueIDType = arrow.StructOf(
	arrow.Field{Name: "resource_type", Type: tString, Nullable: true},
	arrow.Field{Name: "package", Type: tString, Nullable: true},
	arrow.Field{Name: "name", Type: tString, Nullable: true},
	arrow.Field{Name: "version", Type: tString, Nullable: true},
)

var versionSuffixRe = regexp.MustCompile(`^v\d+$`)
var testHashRe = regexp.MustCompile(`^[0-9a-f]{10}$`)

// splitUniqueID parses a dbt unique_id into its parts.
func splitUniqueID(id string) (rt, pkg, name, version string) {
	parts := strings.Split(id, ".")
	if len(parts) < 3 {
		if len(parts) == 2 {
			return parts[0], parts[1], "", ""
		}
		return id, "", "", ""
	}
	rt, pkg = parts[0], parts[1]
	rest := parts[2:]
	switch rt {
	case "model", "snapshot", "seed", "analysis":
		if len(rest) > 1 && versionSuffixRe.MatchString(rest[len(rest)-1]) {
			version = strings.TrimPrefix(rest[len(rest)-1], "v")
			rest = rest[:len(rest)-1]
		}
	case "test":
		if len(rest) > 1 && testHashRe.MatchString(rest[len(rest)-1]) {
			rest = rest[:len(rest)-1]
		}
	}
	return rt, pkg, strings.Join(rest, "."), version
}

type parseUniqueIDArgs struct {
	UniqueID *array.String `vgi:"pos=0,const=false,doc=A dbt unique_id such as model.jaffle_shop.orders.v2 or source.jaffle_shop.raw.orders."`
}

// parseUniqueIDFn is parse_unique_id(unique_id).
type parseUniqueIDFn struct{}

var parseUniqueIDDocs = fnDocs{
	Title:       "Parse dbt Unique ID",
	Category:    "reference",
	Keywords:    "dbt, unique_id, parse, resource type, package, model version",
	Description: "Split a dbt unique_id into resource_type, package, name and model version",
	DocLLM: "parse_unique_id(unique_id) returns a `STRUCT` of resource_type, package, name and version. Model versions (model.p.m.v2) " +
		"set version '2'; a test's hash suffix is dropped from name; source ids keep 'source_name.table' as name. NULL in, " +
		"NULL out.",
	DocMD: "## parse_unique_id(unique_id)\n\n`parse_unique_id('model.shop.orders.v2')` gives `{model, shop, orders, 2}`.",
	Examples: []vgi.CatalogExample{
		{SQL: "SELECT dbt.main.parse_unique_id('model.jaffle_shop.orders.v2').version AS version", Description: "Model version from a unique_id"},
	},
	Executable: []execExample{
		{Description: "Name and package of a source id",
			SQL:      "SELECT p.resource_type, p.package, p.name FROM (SELECT dbt.main.parse_unique_id('source.jaffle_shop.raw.raw_orders') AS p)",
			Expected: [][]any{{"source", "jaffle_shop", "raw.raw_orders"}}},
	},
}

func (*parseUniqueIDFn) Name() string { return "parse_unique_id" }

func (*parseUniqueIDFn) Metadata() vgi.FunctionMetadata {
	return functionMetadata(parseUniqueIDDocs, nil, vgi.FunctionMetadata{Stability: vgi.StabilityConsistent, ReturnType: uniqueIDType})
}

func (*parseUniqueIDFn) OnBindTyped(*parseUniqueIDArgs, *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(uniqueIDType)
}

func (*parseUniqueIDFn) ProcessTyped(_ context.Context, _ *parseUniqueIDArgs, _ *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	col := batch.Column(0)
	b := array.NewStructBuilder(memory.NewGoAllocator(), uniqueIDType)
	defer b.Release()
	fbs := []*array.StringBuilder{
		b.FieldBuilder(0).(*array.StringBuilder), b.FieldBuilder(1).(*array.StringBuilder),
		b.FieldBuilder(2).(*array.StringBuilder), b.FieldBuilder(3).(*array.StringBuilder),
	}
	for i := 0; i < int(batch.NumRows()); i++ {
		if col.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(true)
		rt, pkg, name, ver := splitUniqueID(vgi.GetStringValue(col, i))
		for j, v := range []string{rt, pkg, name, ver} {
			if v == "" {
				fbs[j].AppendNull()
			} else {
				fbs[j].Append(v)
			}
		}
	}
	arr := b.NewArray()
	defer arr.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "result", Type: uniqueIDType, Nullable: true}}, nil)
	return array.NewRecordBatch(schema, []arrow.Array{arr}, batch.NumRows()), nil
}

// ---------------------------------------------------------------- normalize_relation

type normalizeRelationArgs struct {
	Database   *array.String `vgi:"pos=0,const=false,doc=Database (nodes.db_name); NULL or '' is omitted."`
	DbSchema   *array.String `vgi:"pos=1,const=false,doc=Schema (nodes.db_schema)."`
	Identifier *array.String `vgi:"pos=2,const=false,doc=Relation identifier (nodes.alias or sources.identifier)."`
	Quoting    *array.String `vgi:"pos=3,const=false,doc=Quoting policy as a JSON object of database/schema/identifier booleans (dbt quoting config); NULL means no part is quoted."`
}

// normalizeRelationFn is normalize_relation(database, db_schema, identifier, quoting).
type normalizeRelationFn struct{}

var normalizeRelationDocs = fnDocs{
	Title:       "Normalize Relation Name",
	Category:    "reference",
	Keywords:    "dbt, relation name, join key, database, schema, identifier, quoting",
	Description: "Canonical db.schema.table join key for a warehouse relation, honouring dbt quoting",
	DocLLM: "normalize_relation(database, db_schema, identifier, quoting) strips quotes and backticks from each part, " +
		"lowercases the parts the quoting JSON does not mark as quoted (NULL quoting = none quoted), skips NULL/empty " +
		"parts and joins with '.'. A part containing '.' is double-quoted. Use it to join dbt relations to warehouse " +
		"catalogs on an identical key.",
	DocMD: "## normalize_relation(database, db_schema, identifier, quoting)\n\n`normalize_relation('Analytics', 'Main', 'Orders', NULL)` " +
		"is `analytics.main.orders`.",
	Examples: []vgi.CatalogExample{
		{SQL: "SELECT dbt.main.normalize_relation(db_name, db_schema, alias, NULL) AS relation FROM dbt.main.nodes('sample://jaffle_shop/v12/manifest.json') WHERE resource_type = 'model' ORDER BY relation",
			Description: "Canonical relation keys for every model"},
	},
	Executable: []execExample{
		{Description: "Quoted parts keep their case",
			SQL:      `SELECT dbt.main.normalize_relation('"Analytics"', 'Main', 'Orders', '{"database": true}') AS relation`,
			Expected: [][]any{{"Analytics.main.orders"}}},
	},
}

func (*normalizeRelationFn) Name() string { return "normalize_relation" }

func (*normalizeRelationFn) Metadata() vgi.FunctionMetadata {
	return functionMetadata(normalizeRelationDocs, nil, vgi.FunctionMetadata{Stability: vgi.StabilityConsistent,
		ReturnType: tString, NullHandling: vgi.NullHandlingSpecial})
}

func (*normalizeRelationFn) OnBindTyped(*normalizeRelationArgs, *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(tString)
}

// normalizeRelation implements the scalar for one row.
func normalizeRelation(parts [3]*string, quoting string) string {
	q := map[string]bool{}
	if quoting != "" {
		if m := rawObject([]byte(quoting)); m != nil {
			for _, k := range []string{"database", "schema", "identifier"} {
				if v, ok := rawBool(m[k]).(bool); ok {
					q[k] = v
				}
			}
		}
	}
	names := []string{"database", "schema", "identifier"}
	var out []string
	for i, p := range parts {
		if p == nil {
			continue
		}
		s := strings.TrimSpace(*p)
		s = strings.Trim(s, "\"`[]")
		if s == "" {
			continue
		}
		if !q[names[i]] {
			s = strings.ToLower(s)
		}
		if strings.Contains(s, ".") {
			s = `"` + s + `"`
		}
		out = append(out, s)
	}
	return strings.Join(out, ".")
}

func (*normalizeRelationFn) ProcessTyped(_ context.Context, _ *normalizeRelationArgs, _ *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	b := array.NewStringBuilder(memory.NewGoAllocator())
	defer b.Release()
	get := func(c int, i int) *string {
		col := batch.Column(c)
		if col.IsNull(i) {
			return nil
		}
		s := vgi.GetStringValue(col, i)
		return &s
	}
	for i := 0; i < int(batch.NumRows()); i++ {
		parts := [3]*string{get(0, i), get(1, i), get(2, i)}
		if parts[2] == nil {
			b.AppendNull()
			continue
		}
		quoting := ""
		if qp := get(3, i); qp != nil {
			quoting = *qp
		}
		b.Append(normalizeRelation(parts, quoting))
	}
	arr := b.NewArray()
	defer arr.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "result", Type: tString, Nullable: true}}, nil)
	return array.NewRecordBatch(schema, []arrow.Array{arr}, batch.NumRows()), nil
}
