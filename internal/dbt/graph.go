// descendants / ancestors: blended (per-row, LATERAL-friendly) DAG walks.
//
// The adjacency index is built once per manifest from its parent_map and
// child_map (falling back to every resource's depends_on when a manifest
// lacks them) and memoized on (path, size, mtime); each walk is memoized on
// (root, direction, max_depth). One input chunk is one exchange: every
// output row carries the index of the input row that produced it.

package dbt

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// dagIndex is the adjacency of one manifest.
type dagIndex struct {
	parents  map[string][]string
	children map[string][]string
	mu       sync.Mutex
	walks    map[string][]walkHit
}

type walkHit struct {
	ID    string
	Depth int
	Path  []string
}

var (
	dagMu    sync.Mutex
	dagCache = map[string]*dagIndex{} // identity -> index (small: last few manifests)
	dagOrder []string
)

const dagCacheSize = 4

// loadDAG returns the memoized adjacency index for a local manifest.
func loadDAG(p string) (*dagIndex, error) {
	p, err := localPath(p)
	if err != nil {
		return nil, err
	}
	id, err := fileIdentity(p)
	if err != nil {
		return nil, fmt.Errorf("artifact not found: %s", p)
	}
	dagMu.Lock()
	if d, ok := dagCache[id]; ok {
		dagMu.Unlock()
		return d, nil
	}
	dagMu.Unlock()
	m, err := readMeta(p)
	if err != nil {
		return nil, err
	}
	if err := checkKind(ArtifactFile{Local: p, Display: p}, m, kindManifest); err != nil {
		return nil, err
	}
	d := &dagIndex{parents: map[string][]string{}, children: map[string][]string{}, walks: map[string][]walkHit{}}
	pm, err := readTopMap(p, "parent_map")
	if err != nil {
		return nil, err
	}
	cm, err := readTopMap(p, "child_map")
	if err != nil {
		return nil, err
	}
	if pm == nil || cm == nil {
		if err := d.fromDependsOn(p); err != nil {
			return nil, err
		}
	} else {
		d.parents, d.children = pm, cm
	}
	dagMu.Lock()
	dagCache[id] = d
	dagOrder = append(dagOrder, id)
	if len(dagOrder) > dagCacheSize {
		delete(dagCache, dagOrder[0])
		dagOrder = dagOrder[1:]
	}
	dagMu.Unlock()
	return d, nil
}

// readTopMap decodes a top-level map of id -> []id; nil when absent or null.
func readTopMap(p, key string) (map[string][]string, error) {
	s, err := openScanner(p, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = s.Close() }()
	found, err := s.seekTopLevelKey(key)
	if err != nil || !found {
		return nil, err
	}
	raw, err := s.readRaw()
	if err != nil {
		return nil, err
	}
	if isNullRaw(raw) {
		return nil, nil
	}
	out := map[string][]string{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("%s: %w", key, err)
	}
	return out, nil
}

func (d *dagIndex) fromDependsOn(p string) error {
	for _, sec := range dependsOnTable.Sections {
		s, err := openScanner(p, 0)
		if err != nil {
			return err
		}
		found, err := s.seekTopLevelKey(sec)
		if err != nil || !found {
			_ = s.Close()
			if err != nil {
				return err
			}
			continue
		}
		raw, err := s.readRaw()
		_ = s.Close()
		if err != nil {
			return err
		}
		var entries map[string]struct {
			DependsOn struct {
				Nodes []string `json:"nodes"`
			} `json:"depends_on"`
		}
		if isNullRaw(raw) || json.Unmarshal(raw, &entries) != nil {
			continue
		}
		for id, e := range entries {
			for _, dep := range e.DependsOn.Nodes {
				d.parents[id] = append(d.parents[id], dep)
				d.children[dep] = append(d.children[dep], id)
			}
		}
	}
	return nil
}

// walk returns the BFS closure from root (shortest paths, ordered by depth then id).
func (d *dagIndex) walk(root string, down bool, maxDepth int) []walkHit {
	key := fmt.Sprintf("%t|%d|%s", down, maxDepth, root)
	d.mu.Lock()
	if w, ok := d.walks[key]; ok {
		d.mu.Unlock()
		return w
	}
	d.mu.Unlock()
	adj := d.parents
	if down {
		adj = d.children
	}
	seen := map[string]bool{root: true}
	frontier := []walkHit{{ID: root, Depth: 0, Path: []string{root}}}
	var hits []walkHit
	for depth := 1; depth <= maxDepth && len(frontier) > 0; depth++ {
		var next []walkHit
		for _, h := range frontier {
			nbrs := append([]string(nil), adj[h.ID]...)
			sort.Strings(nbrs)
			for _, n := range nbrs {
				if seen[n] {
					continue
				}
				seen[n] = true
				p := append(append([]string(nil), h.Path...), n)
				next = append(next, walkHit{ID: n, Depth: depth, Path: p})
			}
		}
		sort.Slice(next, func(i, j int) bool { return next[i].ID < next[j].ID })
		hits = append(hits, next...)
		frontier = next
	}
	d.mu.Lock()
	if len(d.walks) > 100_000 {
		d.walks = map[string][]walkHit{}
	}
	d.walks[key] = hits
	d.mu.Unlock()
	return hits
}

var walkSchema = arrow.NewSchema([]arrow.Field{
	column("root_id", tString, "The unique_id the walk started from (the input row).", false),
	column("unique_id", tString, "A node reached from root_id.", false),
	column("depth", tInt32, "Edges between root_id and unique_id (0 = the root itself when include_self).", false),
	column("path", tStrList, "Shortest path from root_id to unique_id, both ends included.", false),
}, nil)

// walkFn is descendants (down = true) or ancestors.
type walkFn struct{ down bool }

var _ vgi.TypedTableInOutFunc[struct{}] = (*walkFn)(nil)

func (f *walkFn) Name() string {
	if f.down {
		return "descendants"
	}
	return "ancestors"
}

func (f *walkFn) docs() fnDocs {
	if f.down {
		return fnDocs{
			Title:       "dbt Descendants",
			Category:    "lineage",
			Keywords:    "dbt, descendants, downstream, blast radius, impact analysis, dag, children, +model",
			Description: "Every node downstream of a unique_id in a manifest's DAG (the + suffix operator), per input row",
			DocLLM: "descendants(unique_id, manifest := ..., max_depth := 64, include_self := false) walks the manifest's child " +
				"map from each input unique_id and returns root_id, unique_id, depth and the shortest path. It is a blended " +
				"function: call it with a literal or LATERAL over a column of ids (FROM changed c, dbt.main.descendants(c.id, " +
				"manifest := 'target/manifest.json')). It is the replacement for dbt's 'model+' / 'n+' selectors.",
			DocMD: "## descendants(unique_id, manifest := ..., max_depth := 64, include_self := false)\n\nDownstream closure (blast radius).",
			Examples: []vgi.CatalogExample{
				{SQL: "SELECT unique_id, depth FROM dbt.main.descendants('model.jaffle_shop.stg_orders', manifest := 'sample://jaffle_shop/v12/manifest.json') WHERE unique_id LIKE 'model.%' ORDER BY depth, unique_id",
					Description: "Models downstream of stg_orders"},
			},
			Executable: []execExample{
				{Description: "Models one hop downstream of stg_payments",
					SQL:      "SELECT unique_id FROM dbt.main.descendants('model.jaffle_shop.stg_payments', manifest := 'sample://jaffle_shop/v12/manifest.json', max_depth := 1) WHERE unique_id LIKE 'model.%' ORDER BY unique_id",
					Expected: [][]any{{"model.jaffle_shop.customers"}, {"model.jaffle_shop.orders"}}},
			},
		}
	}
	return fnDocs{
		Title:       "dbt Ancestors",
		Category:    "lineage",
		Keywords:    "dbt, ancestors, upstream, parents, sources, dag, lineage, +model",
		Description: "Every node upstream of a unique_id in a manifest's DAG (the + prefix operator), per input row",
		DocLLM: "ancestors(unique_id, manifest := ..., max_depth := 64, include_self := false) walks the manifest's parent " +
			"map from each input unique_id and returns root_id, unique_id, depth and the shortest path. Blended: works with " +
			"a literal id or LATERAL over a column. Replacement for dbt's '+model' / '+n' selectors.",
		DocMD: "## ancestors(unique_id, manifest := ..., max_depth := 64, include_self := false)\n\nUpstream closure.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT unique_id, depth FROM dbt.main.ancestors('model.jaffle_shop.customers', manifest := 'sample://jaffle_shop/v12/manifest.json') WHERE unique_id LIKE 'source.%' ORDER BY unique_id",
				Description: "Sources a model ultimately reads"},
		},
		Executable: []execExample{
			{Description: "The source behind stg_orders, with its path",
				SQL:      "SELECT unique_id, depth FROM dbt.main.ancestors('model.jaffle_shop.stg_orders', manifest := 'sample://jaffle_shop/v12/manifest.json') ORDER BY unique_id",
				Expected: [][]any{{"source.jaffle_shop.raw.raw_orders", 1}}},
		},
	}
}

func (f *walkFn) Metadata() vgi.FunctionMetadata {
	return functionMetadata(f.docs(), schemaColumns(walkSchema), vgi.FunctionMetadata{
		Stability:     vgi.StabilityConsistentWithinQuery,
		InputFromArgs: true,
	})
}

func (f *walkFn) ArgumentSpecs() []vgi.ArgSpec {
	ge0, le := 0.0, 10000.0
	return []vgi.ArgSpec{
		{Name: "unique_id", Position: 0, ArrowType: "varchar", Doc: "Root node unique_id (a literal or a column)."},
		{Name: "manifest", Position: -1, ArrowType: "varchar", IsConst: true, HasDefault: true, DefaultValue: "",
			Doc: "Local path of the manifest.json whose DAG to walk (required)."},
		{Name: "max_depth", Position: -1, ArrowType: "int64", IsConst: true, HasDefault: true, DefaultValue: "64", Ge: &ge0, Le: &le,
			Doc: "Stop after this many edges (1 = direct children/parents only)."},
		{Name: "include_self", Position: -1, ArrowType: "bool", IsConst: true, HasDefault: true, DefaultValue: "false",
			Doc: "Also emit the root itself at depth 0."},
	}
}

func (f *walkFn) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	if vgi.OptionalString(params.Args, "manifest", "") == "" {
		return nil, argErr(f.Name() + " needs manifest := 'path/to/manifest.json'")
	}
	return vgi.BindSchema(walkSchema)
}

func (f *walkFn) NewState(*vgi.ProcessParams) (*struct{}, error) { return &struct{}{}, nil }

func (f *walkFn) Process(_ context.Context, params *vgi.ProcessParams, _ *struct{}, batch arrow.RecordBatch, out *vgirpc.OutputCollector) error {
	manifest := vgi.OptionalString(params.Args, "manifest", "")
	maxDepth := int(vgi.OptionalInt64(params.Args, "max_depth", 64))
	includeSelf := vgi.OptionalBool(params.Args, "include_self", false)
	d, err := loadDAG(manifest)
	if err != nil {
		return err
	}
	col := batch.Column(0)
	mem := memory.NewGoAllocator()
	roots, ids := array.NewStringBuilder(mem), array.NewStringBuilder(mem)
	depths := array.NewInt32Builder(mem)
	paths := array.NewListBuilder(mem, tString)
	defer roots.Release()
	defer ids.Release()
	defer depths.Release()
	defer paths.Release()
	pv := paths.ValueBuilder().(*array.StringBuilder)
	var parents []int32
	add := func(row int, root string, h walkHit) {
		roots.Append(root)
		ids.Append(h.ID)
		depths.Append(int32(h.Depth))
		paths.Append(true)
		for _, p := range h.Path {
			pv.Append(p)
		}
		parents = append(parents, int32(row))
	}
	for row := 0; row < int(batch.NumRows()); row++ {
		if col.IsNull(row) {
			continue
		}
		root := vgi.GetStringValue(col, row)
		if includeSelf {
			add(row, root, walkHit{ID: root, Depth: 0, Path: []string{root}})
		}
		for _, h := range d.walk(root, f.down, maxDepth) {
			add(row, root, h)
		}
	}
	cols := []arrow.Array{roots.NewArray(), ids.NewArray(), depths.NewArray(), paths.NewArray()}
	for _, c := range cols {
		defer c.Release()
	}
	rb := array.NewRecordBatch(walkSchema, cols, int64(len(parents)))
	defer rb.Release()
	return vgi.EmitParentRows(out, rb, parents)
}

func (f *walkFn) Finalize(context.Context, *vgi.ProcessParams, *struct{}) ([]arrow.RecordBatch, error) {
	return nil, nil
}
