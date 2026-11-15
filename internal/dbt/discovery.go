// dbt Cloud Discovery API (GraphQL): applied-state models, tests and sources
// of an environment, with their latest execution and freshness.
//
// Pagination is the API's cursor (pageInfo.endCursor), kept in exported scan
// state. A unique_id = / IN filter is sent as the uniqueIds filter.

package dbt

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
)

// discoveryTable is one applied-state connection (models, tests, sources).
type discoveryTable struct {
	Name       string
	Connection string // GraphQL field under environment.applied
	FilterType string // GraphQL input type of its filter
	Fields     string // node selection
	Cols       []colSpec
	Rows       func(c *rowCtx) [][]any
	Docs       fnDocs
}

// DiscoveryState is the GraphQL cursor.
type DiscoveryState struct {
	EnvironmentID int64
	After         string
	Started       bool
	Done          bool
	UniqueIDs     []string
}

type discoveryFunc struct{ t *discoveryTable }

var _ vgi.TypedTableFunc[DiscoveryState] = (*discoveryFunc)(nil)

func (f *discoveryFunc) Name() string { return f.t.Name }

func (f *discoveryFunc) schema() *arrow.Schema { return (&artifactTable{Cols: f.t.Cols}).schema() }

func (f *discoveryFunc) Metadata() vgi.FunctionMetadata {
	return functionMetadata(f.t.Docs, (&artifactTable{Cols: f.t.Cols}).resultColumns(), vgi.FunctionMetadata{
		Stability:          vgi.StabilityVolatile,
		ProjectionPushdown: true,
		FilterPushdown:     true,
		RequiredSecrets:    []vgi.SecretRequirement{{SecretType: CloudSecretType}},
	})
}

func (f *discoveryFunc) ArgumentSpecs() []vgi.ArgSpec {
	return []vgi.ArgSpec{{Name: "environment_id", Position: -1, ArrowType: "int64", IsConst: true, HasDefault: true, DefaultValue: "0",
		Doc: "dbt Cloud environment id (the id column of dbt.cloud.environments); 0 uses the dbt_cloud secret's ENVIRONMENT_ID."}}
}

func (f *discoveryFunc) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(f.schema())
}

func (f *discoveryFunc) NewState(params *vgi.ProcessParams) (*DiscoveryState, error) {
	return &DiscoveryState{EnvironmentID: vgi.OptionalInt64(params.Args, "environment_id", 0)}, nil
}

const discoveryPageSize = 500

func (f *discoveryFunc) Process(ctx context.Context, params *vgi.ProcessParams, st *DiscoveryState, out *vgirpc.OutputCollector) error {
	if st.Done {
		return out.Finish()
	}
	cfg, err := cloudConfigFrom(params.Secrets)
	if err != nil {
		return err
	}
	if !st.Started {
		st.Started = true
		if st.EnvironmentID == 0 && cfg.EnvironmentID != "" {
			st.EnvironmentID, _ = strconv.ParseInt(cfg.EnvironmentID, 10, 64)
		}
		if st.EnvironmentID == 0 {
			return argErr(f.t.Name + " needs environment_id := <id> (see dbt.cloud.environments) or ENVIRONMENT_ID in the dbt_cloud secret")
		}
		if ids, ok := vgi.EqualOrInValues[string](params.CurrentPushdownFilters, "unique_id"); ok {
			st.UniqueIDs = ids
		}
	}
	query := fmt.Sprintf(`query ($environmentId: BigInt!, $first: Int!, $after: String, $filter: %s) {
  environment(id: $environmentId) {
    applied {
      %s(first: $first, after: $after, filter: $filter) {
        pageInfo { hasNextPage endCursor }
        edges { node { %s } }
      }
    }
  }
}`, f.t.FilterType, f.t.Connection, f.t.Fields)
	vars := map[string]any{"environmentId": st.EnvironmentID, "first": discoveryPageSize}
	if st.After != "" {
		vars["after"] = st.After
	}
	if len(st.UniqueIDs) > 0 {
		vars["filter"] = map[string]any{"uniqueIds": st.UniqueIDs}
	}
	body, _ := json.Marshal(map[string]any{"query": query, "variables": vars})
	h := http.Header{}
	h.Set("Authorization", "Bearer "+cfg.Token)
	resp, err := fetchJSON(ctx, http.MethodPost, cfg.DiscoveryURL, h, body)
	if err != nil {
		return err
	}
	var gql struct {
		Data struct {
			Environment struct {
				Applied map[string]struct {
					PageInfo struct {
						HasNextPage bool   `json:"hasNextPage"`
						EndCursor   string `json:"endCursor"`
					} `json:"pageInfo"`
					Edges []struct {
						Node map[string]json.RawMessage `json:"node"`
					} `json:"edges"`
				} `json:"applied"`
			} `json:"environment"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(resp, &gql); err != nil {
		return fmt.Errorf("decoding Discovery API response: %w", err)
	}
	if len(gql.Errors) > 0 {
		return fmt.Errorf("dbt Cloud Discovery API error: %s", gql.Errors[0].Message)
	}
	conn := gql.Data.Environment.Applied[f.t.Connection]
	var rows [][]any
	for _, e := range conn.Edges {
		rows = append(rows, f.t.Rows(&rowCtx{F: e.Node})...)
	}
	st.After = conn.PageInfo.EndCursor
	if !conn.PageInfo.HasNextPage || st.After == "" {
		st.Done = true
	}
	if rows, err = applyFilters(ctx, params.CurrentPushdownFilters, f.schema(), rows); err != nil {
		return err
	}
	if len(rows) == 0 && st.Done {
		return out.Finish()
	}
	batch, err := buildBatch(params.OutputSchema, f.schema(), rows)
	if err != nil {
		return err
	}
	return out.Emit(batch)
}

var discoveryModels = &discoveryTable{
	Name: "models", Connection: "models", FilterType: "ModelAppliedFilter",
	Fields: "uniqueId name description packageName materializedType database schema alias tags filePath " +
		"executionInfo { lastRunId lastRunStatus lastRunError lastSuccessRunId executeStartedAt executeCompletedAt executionTime }",
	Cols: []colSpec{
		{Name: "unique_id", Type: tString, Doc: "Model unique id. = / IN filters push down (uniqueIds).", NotNull: true},
		{Name: "name", Type: tString, Doc: "Model name."},
		{Name: "description", Type: tString, Doc: "Description."},
		{Name: "package_name", Type: tString, Doc: "Package."},
		{Name: "materialized", Type: tString, Doc: "Materialization."},
		{Name: "db_name", Type: tString, Doc: "Database."},
		{Name: "db_schema", Type: tString, Doc: "Schema."},
		{Name: "alias", Type: tString, Doc: "Relation identifier."},
		{Name: "tags", Type: tStrList, Doc: "Tags."},
		{Name: "file_path", Type: tString, Doc: "Source file path."},
		{Name: "last_run_id", Type: tInt64, Doc: "Run that last executed the model."},
		{Name: "last_run_status", Type: tString, Doc: "Status of that execution: success, error or skipped."},
		{Name: "last_run_error", Type: tString, Doc: "Error message of a failed last execution."},
		{Name: "last_success_run_id", Type: tInt64, Doc: "Last run in which it succeeded."},
		{Name: "execute_started_at", Type: tTS, Doc: "Start of the last execution."},
		{Name: "execute_completed_at", Type: tTS, Doc: "End of the last execution."},
		{Name: "execution_time", Type: tFloat64, Doc: "Seconds the last execution took."},
	},
	Rows: func(c *rowCtx) [][]any {
		return one(c.s("uniqueId"), c.s("name"), c.s("description"), c.s("packageName"), c.s("materializedType"),
			c.s("database"), c.s("schema"), c.s("alias"), c.tags("tags"), c.s("filePath"),
			c.i64("executionInfo", "lastRunId"), c.s("executionInfo", "lastRunStatus"), c.s("executionInfo", "lastRunError"),
			c.i64("executionInfo", "lastSuccessRunId"), c.ts("executionInfo", "executeStartedAt"),
			c.ts("executionInfo", "executeCompletedAt"), c.f64("executionInfo", "executionTime"))
	},
	Docs: fnDocs{
		Title: "dbt Cloud Models (Discovery)", Category: "discovery",
		Keywords:    "dbt cloud, discovery api, models, last run, execution time, graphql, applied state",
		Description: "Models in a dbt Cloud environment's applied state with their latest execution",
		DocLLM: "cloud.models(environment_id := N) reads the Discovery API's applied models: unique_id, name, description, " +
			"package_name, materialized, db_name, db_schema, alias, tags, file_path and the latest execution (last_run_id, " +
			"last_run_status, last_run_error, last_success_run_id, execute_started_at, execute_completed_at, execution_time). " +
			"unique_id = / IN filters are sent as the uniqueIds filter. environment_id defaults to the secret's ENVIRONMENT_ID.",
		DocMD: "## cloud.models(environment_id := N)\n\nApplied-state models from the Discovery API.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT name, last_run_status, execution_time FROM dbt.cloud.models(environment_id := 218762) WHERE last_run_status = 'error' ORDER BY name",
				Description: "Models whose last execution failed"},
		},
	},
}

var discoveryTests = &discoveryTable{
	Name: "tests", Connection: "tests", FilterType: "TestAppliedFilter",
	Fields: "uniqueId name description columnName testType " +
		"executionInfo { lastRunId lastRunStatus lastRunError executeCompletedAt executionTime }",
	Cols: []colSpec{
		{Name: "unique_id", Type: tString, Doc: "Test unique id. = / IN filters push down (uniqueIds).", NotNull: true},
		{Name: "name", Type: tString, Doc: "Test name."},
		{Name: "description", Type: tString, Doc: "Description."},
		{Name: "column_name", Type: tString, Doc: "Column under test, for column tests."},
		{Name: "test_type", Type: tString, Doc: "GENERIC or SINGULAR."},
		{Name: "last_run_id", Type: tInt64, Doc: "Run that last executed the test."},
		{Name: "last_run_status", Type: tString, Doc: "pass, fail, warn, error or skipped."},
		{Name: "last_run_error", Type: tString, Doc: "Failure message."},
		{Name: "execute_completed_at", Type: tTS, Doc: "When the last execution finished."},
		{Name: "execution_time", Type: tFloat64, Doc: "Seconds the last execution took."},
	},
	Rows: func(c *rowCtx) [][]any {
		return one(c.s("uniqueId"), c.s("name"), c.s("description"), c.s("columnName"), c.s("testType"),
			c.i64("executionInfo", "lastRunId"), c.s("executionInfo", "lastRunStatus"), c.s("executionInfo", "lastRunError"),
			c.ts("executionInfo", "executeCompletedAt"), c.f64("executionInfo", "executionTime"))
	},
	Docs: fnDocs{
		Title: "dbt Cloud Tests (Discovery)", Category: "discovery",
		Keywords:    "dbt cloud, discovery api, tests, failing tests, data quality, graphql",
		Description: "Tests in a dbt Cloud environment's applied state with their latest result",
		DocLLM: "cloud.tests(environment_id := N) reads the Discovery API's applied tests: unique_id, name, description, " +
			"column_name, test_type, last_run_id, last_run_status (pass/fail/warn/error), last_run_error, " +
			"execute_completed_at, execution_time. unique_id filters push down.",
		DocMD: "## cloud.tests(environment_id := N)\n\nApplied-state tests and their latest status.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT last_run_status, count(*) AS tests FROM dbt.cloud.tests(environment_id := 218762) GROUP BY last_run_status ORDER BY last_run_status",
				Description: "Test results in production by status"},
		},
	},
}

var discoverySources = &discoveryTable{
	Name: "sources", Connection: "sources", FilterType: "SourceAppliedFilter",
	Fields: "uniqueId name sourceName identifier database schema description loader " +
		"freshness { maxLoadedAt maxLoadedAtTimeAgoInS freshnessStatus freshnessChecked snapshottedAt freshnessRunId }",
	Cols: []colSpec{
		{Name: "unique_id", Type: tString, Doc: "Source unique id. = / IN filters push down (uniqueIds).", NotNull: true},
		{Name: "source_name", Type: tString, Doc: "Source group name."},
		{Name: "name", Type: tString, Doc: "Table name."},
		{Name: "identifier", Type: tString, Doc: "Warehouse identifier."},
		{Name: "db_name", Type: tString, Doc: "Database."},
		{Name: "db_schema", Type: tString, Doc: "Schema."},
		{Name: "description", Type: tString, Doc: "Description."},
		{Name: "loader", Type: tString, Doc: "Declared loader."},
		{Name: "freshness_status", Type: tString, Doc: "pass, warn or error."},
		{Name: "freshness_checked", Type: tBool, Doc: "Whether freshness is checked."},
		{Name: "max_loaded_at", Type: tTS, Doc: "Latest loaded_at value seen."},
		{Name: "age_seconds", Type: tFloat64, Doc: "Seconds between max_loaded_at and the check."},
		{Name: "snapshotted_at", Type: tTS, Doc: "When freshness was last checked."},
		{Name: "freshness_run_id", Type: tInt64, Doc: "Run that checked it."},
	},
	Rows: func(c *rowCtx) [][]any {
		return one(c.s("uniqueId"), c.s("sourceName"), c.s("name"), c.s("identifier"), c.s("database"), c.s("schema"),
			c.s("description"), c.s("loader"), c.s("freshness", "freshnessStatus"), c.b("freshness", "freshnessChecked"),
			c.ts("freshness", "maxLoadedAt"), c.f64("freshness", "maxLoadedAtTimeAgoInS"), c.ts("freshness", "snapshottedAt"),
			c.i64("freshness", "freshnessRunId"))
	},
	Docs: fnDocs{
		Title: "dbt Cloud Sources (Discovery)", Category: "discovery",
		Keywords:    "dbt cloud, discovery api, sources, freshness, stale data, graphql",
		Description: "Sources in a dbt Cloud environment's applied state with their latest freshness",
		DocLLM: "cloud.sources(environment_id := N) reads the Discovery API's applied sources: unique_id, source_name, name, " +
			"identifier, db_name, db_schema, description, loader, freshness_status, freshness_checked, max_loaded_at, " +
			"age_seconds, snapshotted_at, freshness_run_id. unique_id filters push down.",
		DocMD: "## cloud.sources(environment_id := N)\n\nApplied-state sources and their freshness.",
		Examples: []vgi.CatalogExample{
			{SQL: "SELECT unique_id, freshness_status, round(age_seconds / 3600) AS hours_old FROM dbt.cloud.sources(environment_id := 218762) WHERE freshness_status <> 'pass' ORDER BY unique_id",
				Description: "Stale sources in production"},
		},
	},
}
