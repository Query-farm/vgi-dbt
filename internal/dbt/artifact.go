// Locating artifacts and reading their metadata header.
//
// An artifact argument is one of:
//
//   - a local path, or a glob with *, ?, [..] and ** (one partition per file);
//   - sample://jaffle_shop/<v>/<file>.json, the bundled samples (samples.go);
//   - an http:// or https:// URL (downloaded once per scan to a local cache);
//   - dbt-cloud://runs/<run_id>/<artifact>.json or
//     dbt-cloud://jobs/<job_id>/<artifact>.json (the job's latest run), read
//     through the dbt Cloud Admin API with the dbt_cloud secret. Add
//     ?step=<n> to pick a run step's artifacts.

package dbt

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/Query-farm/vgi-go/vgi"
)

// artifactKind names the four dbt artifact files.
type artifactKind string

const (
	kindManifest   artifactKind = "manifest"
	kindRunResults artifactKind = "run_results"
	kindCatalog    artifactKind = "catalog"
	kindSources    artifactKind = "sources"
	kindAny        artifactKind = ""
)

// ArtifactFile is one resolved artifact: Local is readable on disk, Display is
// what the user named (reported in artifact_path columns).
type ArtifactFile struct {
	Local   string
	Display string
}

// FileMeta is an artifact's metadata block (exported: it rides in scan state).
type FileMeta struct {
	Type          string
	SchemaURL     string
	SchemaVersion int
	DbtVersion    string
	GeneratedAt   string
	InvocationID  string
	Env           string
}

// VersionLabel renders the schema version as "v12" ("" when unknown).
func (m FileMeta) VersionLabel() string {
	if m.SchemaVersion == 0 {
		return ""
	}
	return "v" + strconv.Itoa(m.SchemaVersion)
}

var schemaURLRe = regexp.MustCompile(`/dbt/([a-z-]+)/v(\d+)\.json`)

// parseSchemaURL maps https://schemas.getdbt.com/dbt/manifest/v12.json to
// (manifest, 12).
func parseSchemaURL(u string) (string, int) {
	m := schemaURLRe.FindStringSubmatch(u)
	if m == nil {
		return "", 0
	}
	n, _ := strconv.Atoi(m[2])
	t := strings.ReplaceAll(m[1], "-", "_")
	return t, n
}

// metaCache memoizes metadata headers on (path, size, mtime).
var metaCache sync.Map

func fileIdentity(p string) (string, error) {
	st, err := os.Stat(p)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s|%d|%d", p, st.Size(), st.ModTime().UnixNano()), nil
}

// readMeta reads only the metadata block of an artifact.
func readMeta(p string) (FileMeta, error) {
	id, err := fileIdentity(p)
	if err != nil {
		return FileMeta{}, err
	}
	if v, ok := metaCache.Load(id); ok {
		return v.(FileMeta), nil
	}
	s, err := openScanner(p, 0)
	if err != nil {
		return FileMeta{}, err
	}
	defer func() { _ = s.Close() }()
	found, err := s.seekTopLevelKey("metadata")
	if err != nil {
		return FileMeta{}, fmt.Errorf("%s: %w", p, err)
	}
	if !found {
		return FileMeta{}, fmt.Errorf("%s: not a dbt artifact (no metadata block)", p)
	}
	f, err := s.readObjectFields(nil)
	if err != nil {
		return FileMeta{}, fmt.Errorf("%s: %w", p, err)
	}
	m := FileMeta{}
	if v, ok := rawString(f["dbt_schema_version"]).(string); ok {
		m.SchemaURL = v
		m.Type, m.SchemaVersion = parseSchemaURL(v)
	}
	if v, ok := rawString(f["dbt_version"]).(string); ok {
		m.DbtVersion = v
	}
	if v, ok := rawString(f["generated_at"]).(string); ok {
		m.GeneratedAt = v
	}
	if v, ok := rawString(f["invocation_id"]).(string); ok {
		m.InvocationID = v
	}
	if v, ok := rawJSON(f["env"]).(jsonText); ok {
		m.Env = string(v)
	}
	metaCache.Store(id, m)
	return m, nil
}

// checkKind fails when an artifact is not the kind a table reads.
func checkKind(f ArtifactFile, m FileMeta, want artifactKind) error {
	if want == kindAny || m.Type == "" || m.Type == string(want) {
		return nil
	}
	return fmt.Errorf("%s is a %s artifact, but this function reads %s.json", f.Display, m.Type, want)
}

func hasGlobMeta(s string) bool { return strings.ContainsAny(s, "*?[") }

// globMatch matches a slash-separated path against a pattern where ** spans
// directories and other segments use filepath.Match.
func globMatch(pattern, name string) bool {
	return matchSegs(strings.Split(pattern, "/"), strings.Split(name, "/"))
}

func matchSegs(p, n []string) bool {
	for len(p) > 0 {
		if p[0] == "**" {
			for i := 0; i <= len(n); i++ {
				if matchSegs(p[1:], n[i:]) {
					return true
				}
			}
			return false
		}
		if len(n) == 0 {
			return false
		}
		if ok, err := filepath.Match(p[0], n[0]); err != nil || !ok {
			return false
		}
		p, n = p[1:], n[1:]
	}
	return len(n) == 0
}

// expandLocal resolves a local path or glob to sorted files.
func expandLocal(pattern string) ([]string, error) {
	if !hasGlobMeta(pattern) {
		st, err := os.Stat(pattern)
		if err != nil {
			return nil, fmt.Errorf("artifact not found: %s", pattern)
		}
		if st.IsDir() {
			return nil, fmt.Errorf("%s is a directory; name the artifact file (e.g. %s/manifest.json)", pattern, strings.TrimRight(pattern, "/"))
		}
		return []string{pattern}, nil
	}
	slashed := filepath.ToSlash(pattern)
	segs := strings.Split(slashed, "/")
	rootSegs := []string{}
	for _, s := range segs {
		if hasGlobMeta(s) {
			break
		}
		rootSegs = append(rootSegs, s)
	}
	root := strings.Join(rootSegs, "/")
	if root == "" {
		if strings.HasPrefix(slashed, "/") {
			root = "/"
		} else {
			root = "."
		}
	}
	var out []string
	err := filepath.WalkDir(filepath.FromSlash(root), func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		rel := filepath.ToSlash(p)
		if root == "." && !strings.HasPrefix(slashed, "./") {
			rel = strings.TrimPrefix(rel, "./")
		}
		if globMatch(slashed, rel) {
			out = append(out, p)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

// cacheDir holds downloaded artifacts.
func cacheDir() string {
	d := filepath.Join(os.TempDir(), "vgi-dbt-cache")
	_ = os.MkdirAll(d, 0o700)
	return d
}

// download fetches url (with optional headers) into the cache and returns the path.
func download(ctx context.Context, rawURL string, header http.Header) (string, error) {
	sum := sha256.Sum256([]byte(rawURL))
	dst := filepath.Join(cacheDir(), hex.EncodeToString(sum[:12])+".json")
	body, status, err := httpGet(ctx, rawURL, header)
	if err != nil {
		return "", err
	}
	defer func() { _ = body.Close() }()
	if status != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(body, 500))
		return "", fmt.Errorf("GET %s failed with HTTP %d %s: %s", redactURL(rawURL), status, http.StatusText(status), strings.TrimSpace(string(snippet)))
	}
	tmp, err := os.CreateTemp(cacheDir(), "dl-*")
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(tmp, io.LimitReader(body, 4<<30)); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmp.Name(), dst); err != nil {
		return "", err
	}
	return dst, nil
}

func redactURL(u string) string {
	p, err := url.Parse(u)
	if err != nil {
		return u
	}
	p.RawQuery = ""
	p.User = nil
	return p.String()
}

var cloudArtifactRe = regexp.MustCompile(`^dbt-cloud://(runs|jobs)/(\d+)/(.+)$`)

// resolveArtifacts turns the user's argument into readable local files.
func resolveArtifacts(ctx context.Context, arg string, secrets vgi.Secrets) ([]ArtifactFile, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, argErr("artifact path is empty; pass a path such as 'target/manifest.json'")
	}
	switch {
	case strings.HasPrefix(arg, samplePrefix):
		return resolveSample(arg)
	case strings.HasPrefix(arg, "dbt-cloud://"):
		m := cloudArtifactRe.FindStringSubmatch(arg)
		if m == nil {
			return nil, argErr("dbt Cloud artifact paths look like dbt-cloud://runs/<run_id>/manifest.json or dbt-cloud://jobs/<job_id>/run_results.json")
		}
		cfg, err := cloudConfigFrom(secrets)
		if err != nil {
			return nil, err
		}
		file, query, _ := strings.Cut(m[3], "?")
		q := url.Values{}
		if query != "" {
			if vals, err := url.ParseQuery(query); err == nil && vals.Get("step") != "" {
				q.Set("step", vals.Get("step"))
			}
		}
		u := cfg.adminURL("v2", m[1]+"/"+m[2]+"/artifacts/"+file, q)
		local, err := download(ctx, u, cfg.headers())
		if err != nil {
			return nil, err
		}
		return []ArtifactFile{{Local: local, Display: arg}}, nil
	case strings.HasPrefix(arg, "http://") || strings.HasPrefix(arg, "https://"):
		local, err := download(ctx, arg, nil)
		if err != nil {
			return nil, err
		}
		return []ArtifactFile{{Local: local, Display: arg}}, nil
	case strings.HasPrefix(arg, "s3://") || strings.HasPrefix(arg, "gs://"):
		return nil, argErr("object-store URLs are not read directly; use an https:// presigned URL or a local copy")
	}
	files, err := expandLocal(arg)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, nil
	}
	out := make([]ArtifactFile, len(files))
	for i, f := range files {
		out[i] = ArtifactFile{Local: f, Display: f}
	}
	return out, nil
}
