// dbt Cloud Admin API (v2/v3) tables in schema `cloud`.
//
// Pagination is offset/limit; the offset lives in exported scan state, so a
// long scan resumes on any transport. Rate limits (429) and 5xx are retried
// with backoff honouring Retry-After. Filters on the columns listed per table
// are translated into the API's own query parameters; DuckDB re-applies every
// filter, so pushdown only narrows what is fetched.

package dbt

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// runStatus maps dbt Cloud run/step status codes to their names.
var runStatus = map[int64]string{1: "Queued", 2: "Starting", 3: "Running", 10: "Success", 20: "Error", 30: "Cancelled"}

func statusCode(name string) (int64, bool) {
	for k, v := range runStatus {
		if strings.EqualFold(v, name) {
			return k, true
		}
	}
	return 0, false
}

// CloudScanState is a dbt Cloud scan cursor. It never holds credentials.
type CloudScanState struct {
	Started bool
	Done    bool
	Offset  int64
	Query   string
	Path    string
	Cursor  int64
	After   string
	Arg     int64
}

// cloudTable is one Admin API list endpoint.
type cloudTable struct {
	Name    string
	Version string
	Cols    []colSpec
	Docs    fnDocs
	Args    []vgi.ArgSpec
	// Path is the endpoint below /accounts/<id>/ ("" = the accounts list).
	Path func(st *CloudScanState) string
	// Query adds pushdown parameters (called once per scan).
	Query func(st *CloudScanState, pf *vgi.PushdownFilters, q url.Values)
	// Rows maps one API object to rows.
	Rows     func(c *rowCtx) [][]any
	PageSize int64
	// Single marks an endpoint returning one object (no paging).
	Single bool
	// NewState fills Arg / Cursor from arguments.
	Init func(params *vgi.ProcessParams, st *CloudScanState) error
	// Stop ends paging early (cursor reached).
	Stop func(c *rowCtx, st *CloudScanState) bool
	// Table exposes it as a catalog table (no arguments) instead of a function.
	Table bool
}

func (t *cloudTable) schema() *arrow.Schema {
	return (&artifactTable{Cols: t.Cols}).schema()
}

type cloudFunc struct{ t *cloudTable }

var _ vgi.TypedTableFunc[CloudScanState] = (*cloudFunc)(nil)

func (f *cloudFunc) Name() string {
	if f.t.Table {
		return f.t.Name + "_scan"
	}
	return f.t.Name
}

// catalogTable wraps a parameterless endpoint as a catalog table.
func (t *cloudTable) catalogTable() vgi.CatalogTable {
	comments := map[string]string{}
	for _, c := range t.Cols {
		comments[c.Name] = c.Doc
	}
	tags := objectTags(docs{Title: t.Docs.Title, Category: t.Docs.Category, Keywords: t.Docs.Keywords, DocLLM: t.Docs.DocLLM, DocMD: t.Docs.DocMD})
	tags["domain"] = "analytics engineering"
	cons := func(id string, kind vgi.ConstraintKind) []byte {
		rec, err := vgi.ConstraintRecord(vgi.ConstraintRecordSpec{SchemaPath: []string{"cloud"}, Table: t.Name, ID: id,
			Kind: kind, Columns: [][]string{{"id"}}, Enforcement: vgi.ConstraintEnforced, Validation: vgi.ConstraintAllRows})
		if err != nil {
			panic(err)
		}
		return rec
	}
	return vgi.CatalogTable{
		// dbt Cloud ids are unique per object type.
		Constraints:    [][]byte{cons("id_not_null", vgi.ConstraintNotNull), cons("pk", vgi.ConstraintPrimaryKey)},
		Name:           t.Name,
		Comment:        t.Docs.Description,
		Columns:        t.schema(),
		Function:       vgi.AsTableFunction[CloudScanState](&cloudFunc{t: t}),
		ColumnComments: comments,
		Tags:           withExamples(tags, t.Docs.Examples),
	}
}

func (f *cloudFunc) Metadata() vgi.FunctionMetadata {
	return functionMetadata(f.t.Docs, (&artifactTable{Cols: f.t.Cols}).resultColumns(), vgi.FunctionMetadata{
		Stability:          vgi.StabilityVolatile,
		ProjectionPushdown: true,
		FilterPushdown:     f.t.Query != nil,
		RequiredSecrets:    []vgi.SecretRequirement{{SecretType: CloudSecretType}},
	})
}

func (f *cloudFunc) ArgumentSpecs() []vgi.ArgSpec { return f.t.Args }

func (f *cloudFunc) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(f.t.schema())
}

func (f *cloudFunc) NewState(params *vgi.ProcessParams) (*CloudScanState, error) {
	st := &CloudScanState{}
	if f.t.Init != nil {
		if err := f.t.Init(params, st); err != nil {
			return nil, err
		}
	}
	return st, nil
}

func (f *cloudFunc) Process(ctx context.Context, params *vgi.ProcessParams, st *CloudScanState, out *vgirpc.OutputCollector) error {
	if st.Done {
		return out.Finish()
	}
	cfg, err := cloudConfigFrom(params.Secrets)
	if err != nil {
		return err
	}
	if f.t.Path != nil {
		if err := cfg.requireAccount(); err != nil {
			return err
		}
	}
	if !st.Started {
		q := url.Values{}
		if f.t.Query != nil {
			f.t.Query(st, params.CurrentPushdownFilters, q)
		}
		st.Query, st.Started = q.Encode(), true
	}
	q, _ := url.ParseQuery(st.Query)
	pageSize := f.t.PageSize
	if pageSize == 0 {
		pageSize = 100
	}
	if !f.t.Single {
		q.Set("limit", strconv.FormatInt(pageSize, 10))
		q.Set("offset", strconv.FormatInt(st.Offset, 10))
	}
	rest := ""
	if f.t.Path != nil {
		rest = f.t.Path(st)
	}
	items, total, err := cfg.adminGet(ctx, cfg.adminURL(f.t.Version, rest, q))
	if err != nil {
		return err
	}
	var rows [][]any
	for _, raw := range items {
		it := rawObject(raw)
		if it == nil {
			it = map[string]json.RawMessage{"_value": raw}
		}
		c := &rowCtx{F: it, Opt: strconv.FormatInt(st.Arg, 10)}
		if f.t.Stop != nil && f.t.Stop(c, st) {
			st.Done = true
			break
		}
		rows = append(rows, f.t.Rows(c)...)
	}
	st.Offset += int64(len(items))
	if f.t.Single || pageDone(len(items), pageSize, st.Offset, total) {
		st.Done = true
	}
	if rows, err = applyFilters(ctx, params.CurrentPushdownFilters, f.t.schema(), rows); err != nil {
		return err
	}
	if len(rows) == 0 && st.Done {
		return out.Finish()
	}
	batch, err := buildBatch(params.OutputSchema, f.t.schema(), rows)
	if err != nil {
		return err
	}
	return out.Emit(batch)
}

// ---------------------------------------------------------------- pushdown helpers

func scalarTime(s scalar.Scalar) (time.Time, bool) {
	ts, ok := s.(*scalar.Timestamp)
	if !ok || !ts.IsValid() {
		return time.Time{}, false
	}
	unit := ts.Type.(*arrow.TimestampType).Unit
	return ts.Value.ToTime(unit).UTC(), true
}

// pushInt sets param from an = filter on an integer column.
func pushInt(pf *vgi.PushdownFilters, col, param string, q url.Values) {
	if v, ok := vgi.EqualValue[int64](pf, col); ok {
		q.Set(param, strconv.FormatInt(v, 10))
	}
}

// pushRange sets created_at__range from bounds on a timestamp column.
func pushRange(pf *vgi.PushdownFilters, col string, q url.Values) {
	if pf == nil {
		return
	}
	b := pf.GetColumnBounds(col)
	if b == nil {
		return
	}
	lo, hi := "1970-01-01T00:00:00Z", time.Now().UTC().Add(48*time.Hour).Format(time.RFC3339)
	if t, ok := scalarTime(b.MinValue); ok && b.MinValue != nil {
		lo = t.Format(time.RFC3339)
	}
	if t, ok := scalarTime(b.MaxValue); ok && b.MaxValue != nil {
		hi = t.Add(time.Second).Format(time.RFC3339)
	}
	enc, _ := json.Marshal([]string{lo, hi})
	q.Set("created_at__range", string(enc))
}

// pushStatus maps = / IN on a status-name column to status / status__in.
func pushStatus(codes []int64, q url.Values) {
	switch len(codes) {
	case 0:
	case 1:
		q.Set("status", strconv.FormatInt(codes[0], 10))
	default:
		enc, _ := json.Marshal(codes)
		q.Set("status__in", string(enc))
	}
}

func cloudStatusCodes(pf *vgi.PushdownFilters) []int64 {
	vals, ok := vgi.EqualOrInValues[string](pf, "status")
	if !ok {
		return nil
	}
	var codes []int64
	for _, v := range vals {
		c, ok := statusCode(v)
		if !ok {
			return nil // unknown name: let DuckDB filter (returns nothing)
		}
		codes = append(codes, c)
	}
	return codes
}

// ---------------------------------------------------------------- shared columns

func statusName(raw json.RawMessage) any {
	if c, ok := rawInt(raw).(int64); ok {
		if n, ok := runStatus[c]; ok {
			return n
		}
	}
	return nil
}

func int32Of(v any) any {
	if i, ok := v.(int64); ok {
		return i
	}
	return nil
}

var noArgs []vgi.ArgSpec

func accountPath(p string) func(*CloudScanState) string {
	return func(*CloudScanState) string { return p }
}

// ---------------------------------------------------------------- accounts

var cloudAccounts = &cloudTable{
	Name: "accounts", Table: true, Version: "v2", Args: noArgs, Single: false,
	Cols: []colSpec{
		{Name: "id", Type: tInt64, Doc: "Account id (numeric; the secret's ACCOUNT_ID).", NotNull: true},
		{Name: "name", Type: tString, Doc: "Account name."},
		{Name: "plan", Type: tString, Doc: "Billing plan, e.g. enterprise, team, developer."},
		{Name: "state", Type: tInt32, Doc: "Lifecycle state code (1 = active, 2 = deleted)."},
		{Name: "run_slots", Type: tInt64, Doc: "Number of jobs the account can run concurrently."},
		{Name: "developer_seats", Type: tInt64, Doc: "Number of licensed IDE (developer) users."},
		{Name: "read_only_seats", Type: tInt64, Doc: "Number of licensed read-only users."},
		{Name: "locked", Type: tBool, Doc: "Whether the account is locked."},
		{Name: "created_at", Type: tTS, Doc: "Creation time."},
		{Name: "updated_at", Type: tTS, Doc: "Last update."},
	},
	Rows: func(c *rowCtx) [][]any {
		return one(c.i64("id"), c.s("name"), c.s("plan"), int32Of(c.i64("state")), c.i64("run_slots"), c.i64("developer_seats"),
			c.i64("read_only_seats"), c.b("locked"), c.ts("created_at"), c.ts("updated_at"))
	},
	Docs: fnDocs{
		Title: "dbt Cloud Accounts", Category: "cloud",
		Keywords:    "dbt cloud, accounts, admin api, plan, seats",
		Description: "dbt Cloud accounts the dbt_cloud secret's token can see",
		DocLLM: "cloud.accounts lists the dbt Cloud accounts visible to the token in the dbt_cloud secret: id, name, plan, " +
			"state, run_slots, seats, locked, created_at, updated_at. Use it to find the ACCOUNT_ID for the secret. Needs " +
			"CREATE SECRET (TYPE dbt_cloud, TOKEN '...').",
		DocMD: "## cloud.accounts\n\nAccounts the token can access (Admin API v2).",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT id, name, plan FROM dbt.cloud.accounts ORDER BY id", Description: "Find your account id"},
		},
	},
}

// ---------------------------------------------------------------- projects

var cloudProjects = &cloudTable{
	Name: "projects", Table: true, Version: "v3", Args: noArgs, Path: accountPath("projects/"),
	Cols: []colSpec{
		{Name: "id", Type: tInt64, Doc: "Project id (numeric dbt Cloud identifier).", NotNull: true},
		{Name: "account_id", Type: tInt64, Doc: "Owning account (id in cloud.accounts)."},
		{Name: "name", Type: tString, Doc: "Project name."},
		{Name: "description", Type: tString, Doc: "Free-text project summary written by its owners."},
		{Name: "dbt_project_subdirectory", Type: tString, Doc: "Subdirectory of the repository holding dbt_project.yml."},
		{Name: "connection_id", Type: tInt64, Doc: "Warehouse connection (numeric dbt Cloud id)."},
		{Name: "repository_id", Type: tInt64, Doc: "Git repository (numeric dbt Cloud id)."},
		{Name: "state", Type: tInt32, Doc: "Lifecycle state code (1 = active, 2 = deleted)."},
		{Name: "created_at", Type: tTS, Doc: "Creation time."},
		{Name: "updated_at", Type: tTS, Doc: "Last update."},
	},
	Rows: func(c *rowCtx) [][]any {
		return one(c.i64("id"), c.i64("account_id"), c.s("name"), c.s("description"), c.s("dbt_project_subdirectory"),
			c.i64("connection_id"), c.i64("repository_id"), int32Of(c.i64("state")), c.ts("created_at"), c.ts("updated_at"))
	},
	Docs: fnDocs{
		Title: "dbt Cloud Projects", Category: "cloud",
		Keywords:    "dbt cloud, projects, admin api, repository, connection",
		Description: "Projects in the dbt Cloud account",
		DocLLM: "cloud.projects lists the account's projects (Admin API v3): id, account_id, name, description, " +
			"dbt_project_subdirectory, connection_id, repository_id, state, created_at, updated_at.",
		DocMD: "## cloud.projects\n\nProjects in the account.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT id, name FROM dbt.cloud.projects WHERE state = 1 ORDER BY name", Description: "Active projects"},
		},
	},
}

// ---------------------------------------------------------------- environments

var cloudEnvironments = &cloudTable{
	Name: "environments", Table: true, Version: "v3", Args: noArgs, Path: accountPath("environments/"),
	Query: func(_ *CloudScanState, pf *vgi.PushdownFilters, q url.Values) {
		pushInt(pf, "project_id", "project_id", q)
	},
	Cols: []colSpec{
		{Name: "id", Type: tInt64, Doc: "Environment id (numeric; pass it to the Discovery functions).", NotNull: true},
		{Name: "account_id", Type: tInt64, Doc: "Owning account (id in cloud.accounts)."},
		{Name: "project_id", Type: tInt64, Doc: "Project the environment belongs to (id in cloud.projects). = filters push down."},
		{Name: "name", Type: tString, Doc: "Environment name."},
		{Name: "type", Type: tString, Doc: "development or deployment."},
		{Name: "deployment_type", Type: tString, Doc: "production, staging or NULL."},
		{Name: "dbt_version", Type: tString, Doc: "dbt version or release track."},
		{Name: "use_custom_branch", Type: tBool, Doc: "Whether a custom git branch is used."},
		{Name: "custom_branch", Type: tString, Doc: "The custom branch, else NULL."},
		{Name: "state", Type: tInt32, Doc: "Lifecycle state code (1 = active, 2 = deleted)."},
		{Name: "created_at", Type: tTS, Doc: "Creation time."},
		{Name: "updated_at", Type: tTS, Doc: "Last update."},
	},
	Rows: func(c *rowCtx) [][]any {
		return one(c.i64("id"), c.i64("account_id"), c.i64("project_id"), c.s("name"), c.s("type"), c.s("deployment_type"),
			c.s("dbt_version"), c.b("use_custom_branch"), c.s("custom_branch"), int32Of(c.i64("state")), c.ts("created_at"), c.ts("updated_at"))
	},
	Docs: fnDocs{
		Title: "dbt Cloud Environments", Category: "cloud",
		Keywords:    "dbt cloud, environments, deployment, production, environment id",
		Description: "Environments in the dbt Cloud account (where environment_id and project_id come from)",
		DocLLM: "cloud.environments lists environments (Admin API v3): id, account_id, project_id, name, type " +
			"(development/deployment), deployment_type (production/staging), dbt_version, custom branch, state. A filter " +
			"project_id = N is sent to the API. Use the production environment's id with cloud.models/tests/sources.",
		DocMD: "## cloud.environments\n\nThe discovery table for environment and project ids.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT id, name, deployment_type FROM dbt.cloud.environments WHERE type = 'deployment' ORDER BY id", Description: "Deployment environments"},
		},
	},
}

// ---------------------------------------------------------------- jobs

var cloudJobs = &cloudTable{
	Name: "jobs", Table: true, Version: "v2", Args: noArgs, Path: accountPath("jobs/"),
	Query: func(_ *CloudScanState, pf *vgi.PushdownFilters, q url.Values) {
		pushInt(pf, "project_id", "project_id", q)
		pushInt(pf, "environment_id", "environment_id", q)
		q.Set("order_by", "id")
	},
	Cols: []colSpec{
		{Name: "id", Type: tInt64, Doc: "Job definition id (numeric; runs reference it as job_id).", NotNull: true},
		{Name: "account_id", Type: tInt64, Doc: "Owning account (id in cloud.accounts)."},
		{Name: "project_id", Type: tInt64, Doc: "Project (id in cloud.projects). = filters push down."},
		{Name: "environment_id", Type: tInt64, Doc: "Environment the job runs in (id in cloud.environments). = filters push down."},
		{Name: "name", Type: tString, Doc: "Job name."},
		{Name: "description", Type: tString, Doc: "Free-text summary of what the job does."},
		{Name: "job_type", Type: tString, Doc: "scheduled, ci, merge or other."},
		{Name: "execute_steps", Type: tStrList, Doc: "dbt commands the job runs, in order."},
		{Name: "schedule_cron", Type: tString, Doc: "Cron schedule, when scheduled."},
		{Name: "schedule_enabled", Type: tBool, Doc: "triggers.schedule: whether the schedule is on."},
		{Name: "generate_docs", Type: tBool, Doc: "Whether the job generates docs (catalog.json)."},
		{Name: "run_generate_sources", Type: tBool, Doc: "Whether the job checks source freshness (sources.json)."},
		{Name: "threads", Type: tInt64, Doc: "Number of dbt threads (settings.threads)."},
		{Name: "target_name", Type: tString, Doc: "settings.target_name."},
		{Name: "dbt_version", Type: tString, Doc: "Pinned dbt version, else NULL (inherits the environment)."},
		{Name: "state", Type: tInt32, Doc: "Lifecycle state code (1 = active, 2 = deleted)."},
		{Name: "created_at", Type: tTS, Doc: "Creation time."},
		{Name: "updated_at", Type: tTS, Doc: "Last update."},
	},
	Rows: func(c *rowCtx) [][]any {
		return one(c.i64("id"), c.i64("account_id"), c.i64("project_id"), c.i64("environment_id"), c.s("name"), c.s("description"),
			c.s("job_type"), c.tags("execute_steps"), c.s("schedule", "cron"), c.b("triggers", "schedule"), c.b("generate_docs"),
			c.b("run_generate_sources"), c.i64("settings", "threads"), c.s("settings", "target_name"), c.s("dbt_version"),
			int32Of(c.i64("state")), c.ts("created_at"), c.ts("updated_at"))
	},
	Docs: fnDocs{
		Title: "dbt Cloud Jobs", Category: "cloud",
		Keywords:    "dbt cloud, jobs, schedules, cron, ci jobs, execute steps",
		Description: "Job definitions in the dbt Cloud account",
		DocLLM: "cloud.jobs lists job definitions (Admin API v2): id, project_id, environment_id, name, job_type, " +
			"execute_steps, schedule_cron, schedule_enabled, generate_docs, run_generate_sources, threads, target_name, " +
			"dbt_version, state. Filters project_id = N and environment_id = N are sent to the API.",
		DocMD: "## cloud.jobs\n\nJob definitions; `project_id` / `environment_id` filters push down.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT id, name, schedule_cron FROM dbt.cloud.jobs WHERE schedule_enabled ORDER BY id", Description: "Scheduled jobs and their cron"},
		},
	},
}

// ---------------------------------------------------------------- runs

var runCols = []colSpec{
	{Name: "id", Type: tInt64, Doc: "Run id. Also the incremental cursor: pass the largest id you have as cursor := '<id>'.", NotNull: true},
	{Name: "account_id", Type: tInt64, Doc: "Owning account."},
	{Name: "project_id", Type: tInt64, Doc: "Project. = filters push down."},
	{Name: "environment_id", Type: tInt64, Doc: "Environment. = filters push down."},
	{Name: "job_id", Type: tInt64, Doc: "Job definition id. = filters push down (job_definition_id)."},
	{Name: "job_name", Type: tString, Doc: "Job name."},
	{Name: "status", Type: tString, Doc: "Queued, Starting, Running, Success, Error or Cancelled. = / IN filters push down."},
	{Name: "status_code", Type: tInt32, Doc: "Numeric status: 1, 2, 3, 10, 20, 30."},
	{Name: "status_message", Type: tString, Doc: "Error or cancellation message."},
	{Name: "trigger_cause", Type: tString, Doc: "Why the run started (trigger.cause), e.g. 'Kicked off from UI by ...'."},
	{Name: "git_branch", Type: tString, Doc: "Git branch run."},
	{Name: "git_sha", Type: tString, Doc: "Git commit run."},
	{Name: "dbt_version", Type: tString, Doc: "dbt version used."},
	{Name: "created_at", Type: tTS, Doc: "When the run was queued. Range filters push down (created_at__range)."},
	{Name: "started_at", Type: tTS, Doc: "When execution started."},
	{Name: "finished_at", Type: tTS, Doc: "When it finished."},
	{Name: "duration_seconds", Type: tFloat64, Doc: "finished_at - started_at."},
	{Name: "queued_seconds", Type: tFloat64, Doc: "started_at - created_at."},
	{Name: "is_complete", Type: tBool, Doc: "Whether the run has finished."},
	{Name: "artifacts_saved", Type: tBool, Doc: "Whether artifacts can be read with dbt-cloud://runs/<id>/<file>."},
	{Name: "has_docs_generated", Type: tBool, Doc: "catalog.json available."},
	{Name: "has_sources_generated", Type: tBool, Doc: "sources.json available."},
	{Name: "href", Type: tString, Doc: "Link to the run in dbt Cloud."},
}

func runRow(c *rowCtx) []any {
	created, started, finished := c.ts("created_at"), c.ts("started_at"), c.ts("finished_at")
	return []any{c.i64("id"), c.i64("account_id"), c.i64("project_id"), c.i64("environment_id"), c.i64("job_definition_id"),
		c.s("job", "name"), statusName(c.F["status"]), int32Of(c.i64("status")), c.s("status_message"), c.s("trigger", "cause"),
		firstNonNull(c.s("git_branch"), c.s("trigger", "git_branch")), firstNonNull(c.s("git_sha"), c.s("trigger", "git_sha")),
		c.s("dbt_version"), created, started, finished, durationSeconds(started, finished), durationSeconds(created, started),
		c.b("is_complete"), c.b("artifacts_saved"), c.b("has_docs_generated"), c.b("has_sources_generated"), c.s("href")}
}

func runsQuery(includeSteps bool) func(st *CloudScanState, pf *vgi.PushdownFilters, q url.Values) {
	return func(_ *CloudScanState, pf *vgi.PushdownFilters, q url.Values) {
		pushInt(pf, "job_id", "job_definition_id", q)
		pushInt(pf, "environment_id", "environment_id", q)
		pushInt(pf, "project_id", "project_id", q)
		pushStatus(cloudStatusCodes(pf), q)
		pushRange(pf, "created_at", q)
		related := []string{"job", "trigger"}
		if includeSteps {
			related = append(related, "run_steps")
		}
		enc, _ := json.Marshal(related)
		q.Set("include_related", string(enc))
		q.Set("order_by", "-id")
	}
}

func cursorInit(params *vgi.ProcessParams, st *CloudScanState) error {
	c := vgi.OptionalString(params.Args, "cursor", "")
	if c == "" {
		return nil
	}
	n, err := strconv.ParseInt(strings.TrimSpace(c), 10, 64)
	if err != nil || n < 0 {
		return argErr(fmt.Sprintf("cursor must be a run id (a non-negative integer), got %q", c))
	}
	st.Cursor = n
	return nil
}

func cursorStop(c *rowCtx, st *CloudScanState) bool {
	id, ok := c.i64("id").(int64)
	return st.Cursor > 0 && ok && id <= st.Cursor
}

var cursorArg = vgi.ArgSpec{Name: "cursor", Position: -1, ArrowType: "varchar", IsConst: true, HasDefault: true, DefaultValue: "",
	Doc: "Incremental cursor: only runs with id greater than this run id are returned (store max(id) in a table and pass it back). '' = all runs."}

var cloudRuns = &cloudTable{
	Name: "runs", Version: "v2", Path: accountPath("runs/"), Args: []vgi.ArgSpec{cursorArg},
	Init: cursorInit, Stop: cursorStop, Query: runsQuery(false),
	Cols: runCols,
	Rows: func(c *rowCtx) [][]any { return [][]any{runRow(c)} },
	Docs: fnDocs{
		Title: "dbt Cloud Runs", Category: "cloud",
		Keywords:    "dbt cloud, runs, job runs, failures, run history, status, duration",
		Description: "Job runs in the dbt Cloud account, newest first, with job, status and date-range pushdown",
		DocLLM: "cloud.runs(cursor := '') lists job runs newest first (Admin API v2): id, project_id, environment_id, job_id, " +
			"job_name, status (Queued/Starting/Running/Success/Error/Cancelled), status_code, status_message, trigger_cause, " +
			"git_branch, git_sha, created_at, started_at, finished_at, duration_seconds, queued_seconds, artifacts_saved, href. " +
			"Filters job_id = N, environment_id = N, project_id = N, status = / IN and created_at ranges are sent to the API. " +
			"cursor := '<run id>' returns only newer runs for incremental loads. Read a run's artifacts with " +
			"dbt.main.nodes('dbt-cloud://runs/<id>/manifest.json').",
		DocMD: "## cloud.runs(cursor := '')\n\nRun history. Pushdown: `job_id`, `environment_id`, `project_id`, `status`, `created_at` range.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT job_name, count(*) FILTER (WHERE status = 'Error') AS failed, count(*) AS runs FROM dbt.cloud.runs() WHERE created_at >= TIMESTAMPTZ '2026-01-01' GROUP BY job_name ORDER BY failed DESC",
				Description: "Failure counts per job since a date"},
			{SQL: "SELECT id, status, duration_seconds FROM dbt.cloud.runs() WHERE job_id = 42 AND status = 'Success' ORDER BY id DESC LIMIT 10",
				Description: "Recent successful runs of one job"},
		},
	},
}

// ---------------------------------------------------------------- run_steps

var runIDArg = vgi.ArgSpec{Name: "run_id", Position: 0, ArrowType: "int64", IsConst: true, Doc: "dbt Cloud run id (cloud.runs().id)."}

func runIDInit(params *vgi.ProcessParams, st *CloudScanState) error {
	v, err := params.Args.GetScalarInt64(0)
	if err != nil {
		return argErr("run_id must be a non-NULL run id")
	}
	st.Arg = v
	return nil
}

var stepCols = []colSpec{
	{Name: "run_id", Type: tInt64, Doc: "Run the step belongs to.", NotNull: true},
	{Name: "step_id", Type: tInt64, Doc: "Step id."},
	{Name: "step_index", Type: tInt32, Doc: "1-based position in the run."},
	{Name: "name", Type: tString, Doc: "Step name, e.g. 'Invoke dbt with `dbt build`'."},
	{Name: "status", Type: tString, Doc: "Queued, Starting, Running, Success, Error or Cancelled."},
	{Name: "status_code", Type: tInt32, Doc: "Numeric status."},
	{Name: "started_at", Type: tTS, Doc: "Step start."},
	{Name: "finished_at", Type: tTS, Doc: "Step end."},
	{Name: "duration_seconds", Type: tFloat64, Doc: "finished_at - started_at."},
	{Name: "logs", Type: tString, Doc: "Step log text as returned by the API (may be truncated)."},
}

func stepRow(runID any, m map[string]json.RawMessage) []any {
	s, e := rawTime(m["started_at"]), rawTime(m["finished_at"])
	return []any{runID, rawInt(m["id"]), int32Of(rawInt(m["index"])), rawString(m["name"]), statusName(m["status"]),
		int32Of(rawInt(m["status"])), s, e, durationSeconds(s, e), rawString(m["logs"])}
}

var cloudRunSteps = &cloudTable{
	Name: "run_steps", Version: "v2", Single: true, Args: []vgi.ArgSpec{runIDArg}, Init: runIDInit,
	Path: func(st *CloudScanState) string { return "runs/" + strconv.FormatInt(st.Arg, 10) + "/" },
	Query: func(_ *CloudScanState, _ *vgi.PushdownFilters, q url.Values) {
		q.Set("include_related", `["run_steps"]`)
	},
	Cols: stepCols,
	Rows: func(c *rowCtx) [][]any {
		var rows [][]any
		for _, s := range rawList(c.F["run_steps"]) {
			rows = append(rows, stepRow(c.i64("id"), rawObject(s)))
		}
		return rows
	},
	Docs: fnDocs{
		Title: "dbt Cloud Run Steps", Category: "cloud",
		Keywords:    "dbt cloud, run steps, logs, commands, step status",
		Description: "The steps (dbt commands) of one dbt Cloud run with status, timing and logs",
		DocLLM: "cloud.run_steps(run_id) returns one row per step of the run: run_id, step_id, step_index, name, status, " +
			"status_code, started_at, finished_at, duration_seconds, logs. Use it to see which command of a failed run broke.",
		DocMD: "## cloud.run_steps(run_id)\n\nSteps of one run.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT step_index, name, status FROM dbt.cloud.run_steps(70403103) ORDER BY step_index", Description: "Steps of a run"},
		},
	},
}

// ---------------------------------------------------------------- run_artifacts

var cloudRunArtifacts = &cloudTable{
	Name: "run_artifacts", Version: "v2", Single: true, Args: []vgi.ArgSpec{runIDArg}, Init: runIDInit,
	Path: func(st *CloudScanState) string { return "runs/" + strconv.FormatInt(st.Arg, 10) + "/artifacts/" },
	Cols: []colSpec{
		{Name: "run_id", Type: tInt64, Doc: "The run.", NotNull: true},
		{Name: "path", Type: tString, Doc: "Artifact path within the run, e.g. manifest.json or compiled/....", NotNull: true},
		{Name: "artifact_url", Type: tString, Doc: "dbt-cloud:// URL to pass to the artifact functions, e.g. dbt.main.nodes(artifact_url).", NotNull: true},
	},
	Docs: fnDocs{
		Title: "dbt Cloud Run Artifacts", Category: "cloud",
		Keywords:    "dbt cloud, artifacts, manifest.json, run_results.json, download",
		Description: "Artifact files a dbt Cloud run saved, with the dbt-cloud:// URL the artifact tables read",
		DocLLM: "cloud.run_artifacts(run_id) lists the files a run saved (manifest.json, run_results.json, catalog.json, " +
			"sources.json, compiled SQL...) as run_id, path, artifact_url. Pass artifact_url (dbt-cloud://runs/<id>/<path>) " +
			"to dbt.main.nodes / run_results / catalog_columns / source_freshness to query the artifact directly.",
		DocMD: "## cloud.run_artifacts(run_id)\n\nFiles saved by a run; `artifact_url` feeds the `main` artifact tables.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT path, artifact_url FROM dbt.cloud.run_artifacts(70403103) WHERE path LIKE '%.json' AND path NOT LIKE '%/%' ORDER BY path",
				Description: "Top-level JSON artifacts of a run"},
		},
	},
}

func init() {
	// run_artifacts' data is a list of path strings, wrapped as {"_value": ...}.
	cloudRunArtifacts.Rows = func(c *rowCtx) [][]any {
		p, ok := rawString(c.F["_value"]).(string)
		if !ok {
			return nil
		}
		return one(int64(atoiOr(c.Opt)), p, "dbt-cloud://runs/"+c.Opt+"/"+p)
	}
}

func atoiOr(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

// pageDone decides whether an offset-paged scan is complete: the API's
// total_count when it reports one (servers may cap the page size below the
// requested limit), else a short or empty page.
func pageDone(n int, pageSize, offset, total int64) bool {
	if n == 0 {
		return true
	}
	if total > 0 {
		return offset >= total
	}
	return int64(n) < pageSize
}
