package dbt

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

const fixtures = "samples/jaffle_shop"

func writeTemp(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "a.json")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestScannerSkipAndRaw(t *testing.T) {
	p := writeTemp(t, `{"a": {"x": [1, "}]\"", {"y": null}]}, "metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json"}, "b": -1.5e3}`)
	s, err := openScanner(p, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	found, err := s.seekTopLevelKey("b")
	if err != nil || !found {
		t.Fatalf("seek b: %v %v", found, err)
	}
	raw, err := s.readRaw()
	if err != nil || string(raw) != "-1.5e3" {
		t.Fatalf("raw = %q, %v", raw, err)
	}
	m, err := readMeta(p)
	if err != nil || m.Type != "manifest" || m.SchemaVersion != 12 {
		t.Fatalf("meta = %+v, %v", m, err)
	}
}

func TestScannerDepthLimit(t *testing.T) {
	p := writeTemp(t, `{"metadata": {}, "nodes": `+strings.Repeat("[", maxDepth+5)+strings.Repeat("]", maxDepth+5)+`}`)
	s, _ := openScanner(p, 0)
	defer func() { _ = s.Close() }()
	if _, err := s.seekTopLevelKey("nodes"); err != nil {
		t.Fatal(err)
	}
	if err := s.skipValue(); err == nil || !strings.Contains(err.Error(), "nesting deeper") {
		t.Fatalf("want depth error, got %v", err)
	}
}

func TestSkipDoesNotAllocateCode(t *testing.T) {
	// One node with a 4 MB compiled_code: reading it with compiled_code
	// unselected must not allocate anything near the code's size.
	code := strings.Repeat("select 1 union all ", 4<<20/19)
	body, _ := json.Marshal(map[string]any{
		"metadata": map[string]any{"dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json"},
		"nodes":    map[string]any{"model.p.a": map[string]any{"unique_id": "model.p.a", "name": "a", "compiled_code": code, "raw_code": code}},
	})
	p := writeTemp(t, string(body))
	want := map[string]bool{"name": true, "unique_id": true}
	run := func() {
		s, _ := openScanner(p, 0)
		defer func() { _ = s.Close() }()
		_, _ = s.seekTopLevelKey("nodes")
		_ = s.expect('{')
		_, _ = s.readString()
		_ = s.expect(':')
		f, err := s.readObjectFields(want)
		if err != nil || f["compiled_code"] != nil || string(f["name"]) != `"a"` {
			t.Fatalf("fields %v err %v", f, err)
		}
	}
	run()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	run()
	runtime.ReadMemStats(&after)
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 1<<20 {
		t.Fatalf("skipping 8 MB of code allocated %d bytes", alloc)
	}
}

// TestResumeAcrossSteps checks the cursor: a scan split into many steps
// returns the same rows as one pass.
func TestResumeAcrossSteps(t *testing.T) {
	files := []ArtifactFile{{Local: fixtures + "/v12/manifest.json", Display: "m"}}
	all := func(batch int) []string {
		st := &ArtifactScanState{Files: files}
		var ids []string
		tbl := *nodesTable
		for st.FileIdx < len(st.Files) {
			rows, err := tbl.stepN(st, map[string]bool{"unique_id": true}, nil, batch)
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range rows {
				ids = append(ids, r[0].(string))
			}
		}
		return ids
	}
	one, many := all(100000), all(1)
	if len(one) != 23 || strings.Join(one, ",") != strings.Join(many, ",") {
		t.Fatalf("one pass %d rows, stepwise %d rows", len(one), len(many))
	}
}

func TestSupportStatus(t *testing.T) {
	for _, c := range []struct {
		kind string
		v    int
		want string
	}{{"manifest", 7, "supported"}, {"manifest", 12, "supported"}, {"manifest", 13, "best_effort"}, {"manifest", 6, "unsupported"}, {"run_results", 6, "supported"}, {"bogus", 1, "unsupported"}} {
		if got, _ := supportStatus(c.kind, c.v); got != c.want {
			t.Errorf("%s v%d: got %s want %s", c.kind, c.v, got, c.want)
		}
	}
}

func TestParseSelector(t *testing.T) {
	atoms := parseSelector("tag:a,config.materialized:view +b 3+c d+ e+2 @f state:modified result:error source_status:fresher exposure:x --exclude g")
	kinds := []string{}
	for _, a := range atoms {
		kinds = append(kinds, a.Kind+"/"+map[bool]string{true: "ok", false: "no"}[a.Supported])
	}
	want := "tag/ok config.materialized/ok graph_op/no graph_op/no graph_op/no graph_op/no graph_op/no state/no result/no source_status/no exposure/no exclude/no"
	if strings.Join(kinds, " ") != want {
		t.Fatalf("got %s", strings.Join(kinds, " "))
	}
	n := selNode{UniqueID: "model.shop.orders", FQN: []string{"shop", "marts", "orders"}, Tags: []string{"finance"}, Materialized: "table"}
	for sel, want := range map[string]bool{
		"tag:finance": true, "tag:fin*": true, "tag:x": false, "path:models/marts": true, "path:models/marts/orders.sql": true,
		"path:models/staging": false, "fqn:shop.marts": true, "fqn:marts": true, "package:shop": true, "resource_type:model": true,
		"config.materialized:view": false, "orders": true, "ord*": true, "model.shop.orders": true, "tag:x tag:finance": true,
		"tag:finance,resource_type:seed": false,
	} {
		if got := matchSelector(parseSelector(sel), n); got != want {
			t.Errorf("%q: got %v want %v", sel, got, want)
		}
	}
}

func TestSplitUniqueID(t *testing.T) {
	for id, want := range map[string]string{
		"model.p.orders.v2":                      "model|p|orders|2",
		"model.p.orders":                         "model|p|orders|",
		"test.p.unique_orders_id.fed79b3a6e":     "test|p|unique_orders_id|",
		"source.p.raw.orders":                    "source|p|raw.orders|",
		"macro.dbt.ref":                          "macro|dbt|ref|",
		"operation.p.p-on-run-end-0":             "operation|p|p-on-run-end-0|",
		"semantic_model.jaffle_shop.orders_sm":   "semantic_model|jaffle_shop|orders_sm|",
		"unit_test.p.orders.test_orders_v2_okay": "unit_test|p|orders.test_orders_v2_okay|",
	} {
		rt, pkg, name, ver := splitUniqueID(id)
		if got := strings.Join([]string{rt, pkg, name, ver}, "|"); got != want {
			t.Errorf("%s: got %s want %s", id, got, want)
		}
	}
}

func TestNormalizeRelation(t *testing.T) {
	s := func(v string) *string { return &v }
	if got := normalizeRelation([3]*string{s(`"DB"`), s("Main"), s("Orders")}, `{"database": true}`); got != "DB.main.orders" {
		t.Fatal(got)
	}
	if got := normalizeRelation([3]*string{nil, s(""), s("a.b")}, ""); got != `"a.b"` {
		t.Fatal(got)
	}
}

func TestDeriveDiscoveryURL(t *testing.T) {
	for base, want := range map[string]string{
		"https://cloud.getdbt.com":  "https://metadata.cloud.getdbt.com/graphql",
		"https://emea.dbt.com":      "https://metadata.emea.dbt.com/graphql",
		"https://ab123.us1.dbt.com": "https://ab123.metadata.us1.dbt.com/graphql",
		"http://127.0.0.1:8080":     "http://127.0.0.1:8080/graphql",
	} {
		if got := deriveDiscoveryURL(base); got != want {
			t.Errorf("%s: got %s want %s", base, got, want)
		}
	}
}

func TestCloudConfigRequiresToken(t *testing.T) {
	if _, err := cloudConfigFrom(vgi.Secrets{}); err == nil {
		t.Fatal("want missing secret error")
	}
	sec := vgi.Secrets{"__default_dbt_cloud": {"type": "dbt_cloud", "token": "t", "account_id": "12"}}
	c, err := cloudConfigFrom(sec)
	if err != nil || c.BaseURL != DefaultBaseURL || c.adminURL("v2", "runs/", nil) != "https://cloud.getdbt.com/api/v2/accounts/12/runs/" {
		t.Fatalf("%+v %v", c, err)
	}
}

func TestStatusMappings(t *testing.T) {
	for s, want := range map[string]string{"success": "success", "pass": "success", "warn": "success", "fail": "failed", "error": "failed", "runtime error": "failed", "skipped": "skipped", "no-op": "skipped"} {
		if got := nodeStatus(s); got != want {
			t.Errorf("%s: %s", s, got)
		}
	}
	for cause, want := range map[string]any{"Scheduled": "schedule", "Triggered via API": "api", "Kicked off from UI by x": "manual", "Triggered via GitHub pull request #4": "event", "": nil, "something": "other"} {
		if got := triggerKind(cause); got != want {
			t.Errorf("%q: %v", cause, got)
		}
	}
	if !pageDone(3, 100, 9, 8) || pageDone(3, 100, 3, 8) || !pageDone(0, 100, 0, 0) || !pageDone(5, 100, 5, 0) {
		t.Fatal("pageDone")
	}
}

func TestGlob(t *testing.T) {
	if !globMatch("a/**/run_results.json", "a/x/y/run_results.json") || !globMatch("a/**/r.json", "a/r.json") || globMatch("a/*/r.json", "a/x/y/r.json") {
		t.Fatal("globMatch")
	}
	files, err := expandLocal(fixtures + "/*/manifest.json")
	if err != nil || len(files) != 6 {
		t.Fatalf("%v %v", files, err)
	}
}

func TestApplyFiltersNative(t *testing.T) {
	full := arrow.NewSchema([]arrow.Field{{Name: "s", Type: tString, Nullable: true}, {Name: "t", Type: tTS, Nullable: true}}, nil)
	t0 := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	rows := [][]any{{"a", t0}, {"b", t0.Add(time.Hour)}, {nil, nil}}
	if v, ok := scalarGo(scalar.NewTimestampScalar(arrow.Timestamp(t0.UnixMicro()), tTS)); !ok || !v.(time.Time).Equal(t0) {
		t.Fatal("scalarGo timestamp")
	}
	got, err := applyFilters(context.Background(), nil, full, rows)
	if err != nil || len(got) != 3 {
		t.Fatal("nil filters must keep all rows")
	}
	if c, ok := compareGo(int64(2), 2.5); !ok || c != -1 {
		t.Fatal("int/float compare")
	}
}

func TestMetricsAcrossVersions(t *testing.T) {
	for _, v := range []string{"v7", "v12"} {
		st := &ArtifactScanState{Files: []ArtifactFile{{Local: fixtures + "/" + v + "/manifest.json", Display: v}}}
		rows, err := metricsTable.stepN(st, metricsTable.neededKeys(metricsTable.schema(), nil), nil, 1000)
		if err != nil || len(rows) != 2 {
			t.Fatalf("%s: %d rows, %v", v, len(rows), err)
		}
		want := map[string]string{"v7": "sum", "v12": "simple"}[v]
		found := false
		for _, r := range rows {
			found = found || r[4] == want
		}
		if !found {
			t.Fatalf("%s: no metric_type %s in %v", v, want, rows)
		}
	}
}

func TestBuildBatchProjection(t *testing.T) {
	full := nodesTable.schema()
	out := arrow.NewSchema([]arrow.Field{full.Field(3), full.Field(0)}, nil)
	row := make([]any, full.NumFields())
	row[0], row[3] = "model.p.a", "a"
	b, err := buildBatch(out, full, [][]any{row})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Release()
	if b.Column(0).(*array.String).Value(0) != "a" || b.Column(1).(*array.String).Value(0) != "model.p.a" {
		t.Fatal("projection order")
	}
}
