// Command vgi-dbt-worker serves the dbt catalog to DuckDB.
//
//	vgi-dbt-worker                                  # stdio (what DuckDB launches)
//	vgi-dbt-worker --http --host 0.0.0.0 --port 8000  # HTTP, GET /health
//	vgi-dbt-worker --unix /tmp/dbt.sock         # AF_UNIX (launch: transport)
//
// Attach it from DuckDB:
//
//	ATTACH 'dbt' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-dbt-worker');
package main

import (
	"os"

	"github.com/Query-farm/vgi-go/vgi"
	// Links the DuckDB expression evaluator: pushed-down filter shapes the
	// worker does not evaluate natively fall back to it (internal/dbt/filters.go).
	_ "github.com/Query-farm/vgi-go/vgi/exprduckdb"

	"github.com/Query-farm/vgi-dbt/internal/dbt"
	"github.com/Query-farm/vgi-dbt/internal/serve"
)

func main() {
	serve.Main("vgi-dbt-worker", os.Args[1:], func() *vgi.Worker { return dbt.NewWorker() })
}
