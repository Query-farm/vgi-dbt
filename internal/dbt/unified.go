// unified.runs / unified.task_runs: the cross-orchestrator shape shared by
// the airflow, dagster, prefect, temporal, databricks, dbt and github
// workers, so runs can be UNION ALLed across tools.
//
//   - runs(path := NULL): with path, one row per run_results.json invocation
//     (orchestrator 'dbt'); without, dbt Cloud runs (orchestrator 'dbt_cloud').
//   - task_runs(path := NULL, run_id := NULL): with path, node results of
//     local run_results files; with run_id, node results of that dbt Cloud
//     run's run_results.json; with neither, the steps of recent dbt Cloud runs.

package dbt

import (
	"context"
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

var unifiedRunsSchema = arrow.NewSchema([]arrow.Field{
	column("orchestrator", tString, "'dbt' for a run_results.json invocation, 'dbt_cloud' for a dbt Cloud run.", false),
	column("run_id", tString, "dbt invocation_id, or the dbt Cloud run id.", false),
	column("pipeline", tString, "'dbt <command>' for an invocation (e.g. dbt build); the job name for a dbt Cloud run.", true),
	column("status", tString, "queued, running, success, failed, cancelled or skipped.", false),
	column("raw_status", tString, "The tool's own status: worst node status of an invocation, or the dbt Cloud status name.", true),
	column("trigger", tString, "schedule, manual, api, event or other (dbt Cloud); NULL for local invocations.", true),
	column("queued_at", tTS, "When the run was queued (dbt Cloud created_at); NULL for local invocations.", true),
	column("started_at", tTS, "When the run started.", true),
	column("ended_at", tTS, "When the run ended.", true),
	column("duration_seconds", tFloat64, "Wall-clock seconds.", true),
	column("url", tString, "Link to the run (dbt Cloud) or the run_results.json path.", true),
}, nil)

var unifiedTaskRunsSchema = arrow.NewSchema([]arrow.Field{
	column("orchestrator", tString, "'dbt' or 'dbt_cloud'.", false),
	column("run_id", tString, "The run (invocation_id or dbt Cloud run id) the task belongs to.", false),
	column("task_run_id", tString, "Node unique_id for node results; '<run_id>:<step index>' for dbt Cloud steps.", false),
	column("task_name", tString, "Node unique_id, or the dbt Cloud step name.", true),
	column("status", tString, "queued, running, success, failed, cancelled or skipped.", false),
	column("raw_status", tString, "dbt node status (success, error, pass, fail, warn, skipped, ...) or dbt Cloud step status.", true),
	column("attempt", tInt32, "Always 1 (dbt does not retry nodes inside one invocation).", true),
	column("started_at", tTS, "Task start (compile start for nodes).", true),
	column("ended_at", tTS, "Task end (execute end for nodes).", true),
	column("duration_seconds", tFloat64, "Seconds the task took (execution_time for nodes).", true),
	column("url", tString, "Link to the dbt Cloud run, or the run_results.json path.", true),
}, nil)

// nodeStatus maps a dbt node result status onto the unified value set.
func nodeStatus(s string) string {
	switch strings.ToLower(s) {
	case "success", "pass", "warn":
		return "success"
	case "error", "fail", "runtime error":
		return "failed"
	case "skipped", "no-op", "reused":
		return "skipped"
	}
	return "failed"
}

// cloudUnifiedStatus maps a dbt Cloud status code.
func cloudUnifiedStatus(code any) string {
	switch c, _ := code.(int64); c {
	case 1, 2:
		return "queued"
	case 3:
		return "running"
	case 10:
		return "success"
	case 30:
		return "cancelled"
	}
	return "failed"
}

// triggerKind classifies a dbt Cloud trigger.cause.
func triggerKind(cause any) any {
	s, ok := cause.(string)
	if !ok || s == "" {
		return nil
	}
	l := strings.ToLower(s)
	switch {
	case strings.Contains(l, "schedul"):
		return "schedule"
	case strings.Contains(l, "api"):
		return "api"
	case strings.Contains(l, "pull request") || strings.Contains(l, "webhook") || strings.Contains(l, "merge") ||
		strings.Contains(l, "github") || strings.Contains(l, "gitlab") || strings.Contains(l, "azure"):
		return "event"
	case strings.Contains(l, "ui") || strings.Contains(l, "manual") || strings.Contains(l, "kicked off"):
		return "manual"
	}
	return "other"
}

// statusRank orders node statuses by severity for an invocation's raw_status.
var statusRank = map[string]int{"error": 6, "runtime error": 6, "fail": 5, "warn": 3, "skipped": 2, "success": 1, "pass": 1}

// summarizeInvocation reads one run_results.json for unified.runs.
func summarizeInvocation(f ArtifactFile) ([]any, error) {
	m, err := readMeta(f.Local)
	if err != nil {
		return nil, err
	}
	if err := checkKind(f, m, kindRunResults); err != nil {
		return nil, err
	}
	pipeline := any("dbt")
	if args, err := readTopRaw(f.Local, "args"); err == nil {
		if w, ok := rawString(rawObject(args)["which"]).(string); ok && w != "" {
			pipeline = "dbt " + w
		}
	}
	var elapsed any
	if raw, err := readTopRaw(f.Local, "elapsed_time"); err == nil {
		elapsed = rawFloat(raw)
	}
	st := &ArtifactScanState{Files: []ArtifactFile{f}}
	worst, failed := "", false
	var start, end time.Time
	err = scanEntries(st, runTimingsTable, map[string]bool{"status": true, "timing": true}, func(c *rowCtx) {
		s := asText(c.F["status"])
		if statusRank[s] > statusRank[worst] {
			worst = s
		}
		if nodeStatus(s) == "failed" {
			failed = true
		}
		for _, t := range rawList(c.F["timing"]) {
			tm := rawObject(t)
			if v, ok := rawTime(tm["started_at"]).(time.Time); ok && (start.IsZero() || v.Before(start)) {
				start = v
			}
			if v, ok := rawTime(tm["completed_at"]).(time.Time); ok && v.After(end) {
				end = v
			}
		}
	})
	if err != nil {
		return nil, err
	}
	gen, _ := parseTime(m.GeneratedAt)
	if end.IsZero() && !gen.IsZero() {
		end = gen
	}
	if e, ok := elapsed.(float64); ok && !gen.IsZero() && start.IsZero() {
		start = gen.Add(-time.Duration(e * float64(time.Second)))
	}
	status := "success"
	if failed {
		status = "failed"
	}
	var startV, endV any
	if !start.IsZero() {
		startV = start
	}
	if !end.IsZero() {
		endV = end
	}
	dur := firstNonNull(elapsed, durationSeconds(startV, endV))
	runID := m.InvocationID
	if runID == "" {
		runID = f.Display
	}
	return []any{"dbt", runID, pipeline, status, nilIfEmpty(worst), nil, nil, startV, endV, dur, f.Display}, nil
}

// readTopRaw returns the raw value of a top-level key.
func readTopRaw(p, key string) (json.RawMessage, error) {
	s, err := openScanner(p, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = s.Close() }()
	found, err := s.seekTopLevelKey(key)
	if err != nil || !found {
		return nil, err
	}
	return s.readRaw()
}

// scanEntries runs a table's section walk to completion, calling fn per entry.
func scanEntries(st *ArtifactScanState, t *artifactTable, want map[string]bool, fn func(c *rowCtx)) error {
	probe := &artifactTable{Kind: t.Kind, Sections: t.Sections, Rows: func(c *rowCtx) [][]any { fn(c); return nil },
		Cols: []colSpec{{Name: "_", Type: tString}}, AlwaysKeys: keysOf(want)}
	for st.FileIdx < len(st.Files) {
		if _, err := probe.step(st, nil, nil); err != nil {
			return err
		}
	}
	return nil
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// UnifiedState covers both the artifact and the dbt Cloud variants.
type UnifiedState struct {
	Mode     string // "artifacts", "cloud", "cloud_run"
	Files    []ArtifactFile
	Next     int
	Scan     ArtifactScanState
	Cloud    CloudScanState
	RunID    int64
	CloudURL string
}

type unifiedFunc struct{ tasks bool }

var _ vgi.TypedTableFunc[UnifiedState] = (*unifiedFunc)(nil)

func (f *unifiedFunc) Name() string {
	if f.tasks {
		return "task_runs"
	}
	return "runs"
}

func (f *unifiedFunc) schema() *arrow.Schema {
	if f.tasks {
		return unifiedTaskRunsSchema
	}
	return unifiedRunsSchema
}

func (f *unifiedFunc) docs() fnDocs {
	if f.tasks {
		return fnDocs{
			Title: "Unified Task Runs", Category: "unified",
			Keywords:    "unified, task runs, node results, orchestrator, cross-tool, union all, dbt",
			Description: "dbt node results (or dbt Cloud run steps) in the cross-orchestrator task_runs shape",
			DocLLM: "unified.task_runs(path := NULL, run_id := NULL) returns orchestrator, run_id, task_run_id, task_name, " +
				"status (queued/running/success/failed/cancelled/skipped), raw_status, attempt, started_at, ended_at, " +
				"duration_seconds, url - the same columns as airflow/dagster/prefect workers' unified.task_runs. With path " +
				"(a run_results.json glob) each node result is a task; with run_id the node results of that dbt Cloud run; " +
				"with neither, the steps of recent dbt Cloud runs (needs the dbt_cloud secret).",
			DocMD: "## unified.task_runs(path := NULL, run_id := NULL)\n\nNode results as tasks, for UNION ALL across orchestrators.",
			Examples: []vgi.CatalogExample{
				{SQL: "SELECT status, count(*) AS tasks FROM dbt.unified.task_runs(path := 'sample://jaffle_shop/v12/run_results.json') GROUP BY status ORDER BY status",
					Description: "Node outcomes of one invocation in the unified shape"},
			},
		}
	}
	return fnDocs{
		Title: "Unified Runs", Category: "unified",
		Keywords:    "unified, runs, orchestrator, cross-tool, union all, dbt cloud, invocations",
		Description: "dbt invocations (run_results.json) or dbt Cloud runs in the cross-orchestrator runs shape",
		DocLLM: "unified.runs(path := NULL) returns orchestrator, run_id, pipeline, status (queued/running/success/failed/" +
			"cancelled/skipped), raw_status, trigger, queued_at, started_at, ended_at, duration_seconds, url - the same " +
			"columns as the other orchestrator workers' unified.runs, for UNION ALL. With path (a run_results.json file or " +
			"glob) each file is one run (orchestrator 'dbt', run_id = invocation_id). Without path it lists dbt Cloud runs " +
			"(orchestrator 'dbt_cloud'); status and queued_at filters push down to the Admin API.",
		DocMD: "## unified.runs(path := NULL)\n\nRuns in the shared orchestrator shape.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT run_id, pipeline, status, round(duration_seconds, 1) AS seconds FROM dbt.unified.runs(path := 'sample://jaffle_shop/*/run_results.json') ORDER BY started_at",
				Description: "An archive of local dbt invocations as runs"},
			{SQL: "SELECT pipeline, count(*) FILTER (WHERE status = 'failed') AS failures FROM dbt.unified.runs() WHERE queued_at >= TIMESTAMPTZ '2026-01-01' GROUP BY pipeline ORDER BY failures DESC",
				Description: "dbt Cloud job failures in the unified shape"},
		},
	}
}

func (f *unifiedFunc) Metadata() vgi.FunctionMetadata {
	return functionMetadata(f.docs(), schemaColumns(f.schema()), vgi.FunctionMetadata{
		Stability:          vgi.StabilityVolatile,
		ProjectionPushdown: true,
		FilterPushdown:     !f.tasks,
		RequiredSecrets:    []vgi.SecretRequirement{{SecretType: CloudSecretType}},
	})
}

func (f *unifiedFunc) ArgumentSpecs() []vgi.ArgSpec {
	specs := []vgi.ArgSpec{{Name: "path", Position: -1, ArrowType: "varchar", IsConst: true, HasDefault: true, DefaultValue: "",
		Doc: "run_results.json file or glob; '' (default) reads dbt Cloud through the dbt_cloud secret."}}
	if f.tasks {
		specs = append(specs, vgi.ArgSpec{Name: "run_id", Position: -1, ArrowType: "int64", IsConst: true, HasDefault: true, DefaultValue: "0",
			Doc: "dbt Cloud run id whose run_results.json to read as node results; 0 = not set."})
	}
	return specs
}

func (f *unifiedFunc) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(f.schema())
}

func (f *unifiedFunc) NewState(params *vgi.ProcessParams) (*UnifiedState, error) {
	p := vgi.OptionalString(params.Args, "path", "")
	runID := int64(0)
	if f.tasks {
		runID = vgi.OptionalInt64(params.Args, "run_id", 0)
	}
	st := &UnifiedState{}
	switch {
	case p != "":
		files, err := resolveArtifacts(context.Background(), p, params.Secrets)
		if err != nil {
			return nil, err
		}
		st.Mode, st.Files = "artifacts", files
		st.Scan = ArtifactScanState{Files: files}
	case runID != 0:
		u := "dbt-cloud://runs/" + strconv.FormatInt(runID, 10) + "/run_results.json"
		files, err := resolveArtifacts(context.Background(), u, params.Secrets)
		if err != nil {
			return nil, err
		}
		st.Mode, st.RunID, st.Files = "artifacts", runID, files
		st.Scan = ArtifactScanState{Files: files}
		cfg, err := cloudConfigFrom(params.Secrets)
		if err == nil {
			st.CloudURL = cfg.BaseURL
		}
	default:
		st.Mode = "cloud"
	}
	return st, nil
}

func (f *unifiedFunc) Process(ctx context.Context, params *vgi.ProcessParams, st *UnifiedState, out *vgirpc.OutputCollector) error {
	switch {
	case st.Mode == "artifacts" && !f.tasks:
		if st.Next >= len(st.Files) {
			return out.Finish()
		}
		var rows [][]any
		for ; st.Next < len(st.Files) && len(rows) < 64; st.Next++ {
			r, err := summarizeInvocation(st.Files[st.Next])
			if err != nil {
				return err
			}
			rows = append(rows, r)
		}
		return emitRows(ctx, out, params, f.schema(), rows)
	case st.Mode == "artifacts":
		if st.Scan.FileIdx >= len(st.Scan.Files) {
			return out.Finish()
		}
		var rows [][]any
		want := map[string]bool{"unique_id": true, "status": true, "timing": true, "execution_time": true}
		probe := &artifactTable{Kind: kindRunResults, Sections: []string{"results"}, Cols: []colSpec{{Name: "_", Type: tString}},
			AlwaysKeys: keysOf(want), Rows: func(c *rowCtx) [][]any {
				raw := asText(c.F["status"])
				runID := c.Meta.InvocationID
				orch, link := "dbt", any(c.File.Display)
				if st.RunID != 0 {
					orch = "dbt_cloud"
					runID = strconv.FormatInt(st.RunID, 10)
					link = nil
				}
				id := asText(c.F["unique_id"])
				rows = append(rows, []any{orch, runID, id, id, nodeStatus(raw), nilIfEmpty(raw), int64(1),
					timing(c, "compile", "started_at"), timing(c, "execute", "completed_at"), c.f64("execution_time"), link})
				return nil
			}}
		for len(rows) < batchRows && st.Scan.FileIdx < len(st.Scan.Files) {
			if _, err := probe.step(&st.Scan, nil, nil); err != nil {
				return err
			}
		}
		return emitRows(ctx, out, params, f.schema(), rows)
	}
	// dbt Cloud runs (and their steps).
	if st.Cloud.Done {
		return out.Finish()
	}
	cfg, err := cloudConfigFrom(params.Secrets)
	if err != nil {
		return err
	}
	if err := cfg.requireAccount(); err != nil {
		return err
	}
	if !st.Cloud.Started {
		q := url.Values{}
		runsQuery(f.tasks)(&st.Cloud, nil, q)
		if !f.tasks {
			pf := params.CurrentPushdownFilters
			if vals, ok := vgi.EqualOrInValues[string](pf, "status"); ok {
				var codes []int64
				for _, v := range vals {
					switch v {
					case "queued":
						codes = append(codes, 1, 2)
					case "running":
						codes = append(codes, 3)
					case "success":
						codes = append(codes, 10)
					case "failed":
						codes = append(codes, 20)
					case "cancelled":
						codes = append(codes, 30)
					}
				}
				pushStatus(codes, q)
			}
			pushRange(pf, "queued_at", q)
		}
		st.Cloud.Query, st.Cloud.Started = q.Encode(), true
	}
	q, _ := url.ParseQuery(st.Cloud.Query)
	q.Set("limit", "100")
	q.Set("offset", strconv.FormatInt(st.Cloud.Offset, 10))
	items, total, err := cfg.adminGet(ctx, cfg.adminURL("v2", "runs/", q))
	if err != nil {
		return err
	}
	var rows [][]any
	for _, raw := range items {
		c := &rowCtx{F: rawObject(raw)}
		runID := strconv.FormatInt(atoiAny(c.i64("id")), 10)
		if f.tasks {
			for _, s := range rawList(c.F["run_steps"]) {
				m := rawObject(s)
				sr := stepRow(nil, m)
				code := rawInt(m["status"])
				idx := strconv.FormatInt(atoiAny(rawInt(m["index"])), 10)
				rows = append(rows, []any{"dbt_cloud", runID, runID + ":" + idx, sr[3], cloudUnifiedStatus(code), sr[4], int64(1),
					sr[6], sr[7], sr[8], c.s("href")})
			}
			continue
		}
		r := runRow(c)
		rows = append(rows, []any{"dbt_cloud", runID, r[5], cloudUnifiedStatus(c.i64("status")), r[6], triggerKind(r[9]),
			r[13], r[14], r[15], r[16], r[22]})
	}
	st.Cloud.Offset += int64(len(items))
	if pageDone(len(items), 100, st.Cloud.Offset, total) {
		st.Cloud.Done = true
	}
	if len(rows) == 0 && st.Cloud.Done {
		return out.Finish()
	}
	return emitRows(ctx, out, params, f.schema(), rows)
}

func atoiAny(v any) int64 {
	n, _ := v.(int64)
	return n
}

func emitRows(ctx context.Context, out *vgirpc.OutputCollector, params *vgi.ProcessParams, full *arrow.Schema, rows [][]any) error {
	rows, err := applyFilters(ctx, params.CurrentPushdownFilters, full, rows)
	if err != nil {
		return err
	}
	batch, err := buildBatch(params.OutputSchema, full, rows)
	if err != nil {
		return err
	}
	return out.Emit(batch)
}
