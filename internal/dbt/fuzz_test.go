package dbt

import (
	"os"
	"path/filepath"
	"testing"
)

// FuzzArtifactParsers feeds arbitrary bytes to every artifact parser: errors
// are fine, panics and hangs are not.
func FuzzArtifactParsers(f *testing.F) {
	for _, s := range []string{
		`{"metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json"}, "nodes": {"model.a.b": {"name": "x", "columns": {"c": {"name": "c"}}, "depends_on": {"nodes": ["y"]}}}}`,
		`{"metadata": {}, "results": [{"unique_id": "a", "timing": [{"name": "compile"}]}]}`,
		`{"metadata": {"dbt_schema_version": "x"}, "nodes": {"a": 1, "b": [}`,
		`{"nodes": {"a": {"b": "\u12"}}}`, `[`, ``, `{"metadata": null}`,
	} {
		f.Add([]byte(s))
	}
	tables := append(append([]*artifactTable{}, artifactTables...), runResultsTable)
	f.Fuzz(func(t *testing.T, data []byte) {
		p := filepath.Join(t.TempDir(), "f.json")
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		for _, tbl := range tables {
			probe := *tbl
			probe.Kind = kindAny
			st := &ArtifactScanState{Files: []ArtifactFile{{Local: p, Display: p}}}
			for i := 0; i < 100 && st.FileIdx < len(st.Files); i++ {
				if _, err := probe.stepN(st, probe.neededKeys(probe.schema(), nil), nil, 50); err != nil {
					break
				}
			}
		}
		_, _ = countEntries(p, []string{"nodes", "results"})
		_, _ = readTopMap(p, "parent_map")
	})
}

// FuzzSelector: the selector parser and matcher never panic.
func FuzzSelector(f *testing.F) {
	for _, s := range []string{"tag:a,b +c 2+d @e state:x --exclude f", "", ",,,", "path:", "+", "fqn:a.*.c"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, sel string) {
		atoms := parseSelector(sel)
		_ = firstUnsupported(atoms)
		_ = matchSelector(atoms, selNode{UniqueID: "model.p.a", FQN: []string{"p", "a"}, Tags: []string{"x"}})
	})
}
