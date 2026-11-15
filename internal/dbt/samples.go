// Bundled sample artifacts: the jaffle_shop project's real dbt-core
// artifacts for manifest schema v7-v12, addressable as
// sample://jaffle_shop/<v>/<file>.json (globs allowed), so every example
// runs offline with nothing but the worker binary.

package dbt

import (
	"embed"
	"fmt"
	"io/fs"
	"os"
	gopath "path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

//go:embed samples
var samplesFS embed.FS

const samplePrefix = "sample://"

var sampleMu sync.Mutex

// localPath maps a sample:// path to its materialized file; other paths are
// returned unchanged (used by functions that read one local file).
func localPath(p string) (string, error) {
	if !strings.HasPrefix(p, samplePrefix) {
		return p, nil
	}
	files, err := resolveSample(p)
	if err != nil {
		return "", err
	}
	if len(files) != 1 {
		return "", argErr(fmt.Sprintf("%s must name exactly one sample file", p))
	}
	return files[0].Local, nil
}

// resolveSample materializes bundled sample files matching a sample:// path
// (or glob) into the cache directory.
func resolveSample(arg string) ([]ArtifactFile, error) {
	pattern := gopath.Clean(strings.TrimPrefix(arg, samplePrefix))
	if strings.HasPrefix(pattern, "..") || strings.HasPrefix(pattern, "/") {
		return nil, argErr("sample paths look like sample://jaffle_shop/v12/manifest.json")
	}
	names, err := fs.Glob(samplesFS, "samples/"+pattern)
	if err != nil {
		return nil, argErr(fmt.Sprintf("bad sample glob %q: %v", arg, err))
	}
	sort.Strings(names)
	if len(names) == 0 && !hasGlobMeta(pattern) {
		return nil, fmt.Errorf("artifact not found: %s (bundled samples: sample://jaffle_shop/{v7,v8,v9,v10,v11,v12}/{manifest,run_results,catalog,sources}.json)", arg)
	}
	sampleMu.Lock()
	defer sampleMu.Unlock()
	var out []ArtifactFile
	for _, n := range names {
		data, err := samplesFS.ReadFile(n)
		if err != nil {
			continue // a directory
		}
		dst := filepath.Join(cacheDir(), "samples-"+Version, filepath.FromSlash(strings.TrimPrefix(n, "samples/")))
		if st, err := os.Stat(dst); err != nil || st.Size() != int64(len(data)) {
			if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
				return nil, err
			}
			tmp := dst + ".tmp"
			if err := os.WriteFile(tmp, data, 0o600); err != nil {
				return nil, err
			}
			if err := os.Rename(tmp, dst); err != nil {
				return nil, err
			}
		}
		out = append(out, ArtifactFile{Local: dst, Display: samplePrefix + strings.TrimPrefix(n, "samples/")})
	}
	return out, nil
}
