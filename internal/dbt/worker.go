// Package dbt exposes dbt artifacts (manifest.json, run_results.json,
// catalog.json, sources.json) and dbt Cloud as SQL.
//
// Schemas:
//
//	main     artifact tables, selectors and DAG walks (offline, no credentials)
//	cloud    dbt Cloud Admin API v2/v3 and the Discovery API (dbt_cloud secret)
//	unified  runs / task_runs in the shape shared by the orchestrator workers
//
// Attach it from DuckDB:
//
//	ATTACH 'dbt' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-dbt-worker');
package dbt

import (
	"github.com/Query-farm/vgi-go/vgi"
)

// artifactTables are the generic artifact table functions in schema main.
var artifactTables = []*artifactTable{
	nodesTable, nodeColumnsTable, dependsOnTable, sourcesTable, exposuresTable, metricsTable,
	semanticModelsTable, groupsTable, macrosTable, runResultsTable, runTimingsTable,
	catalogColumnsTable, sourceFreshnessTable,
}

// cloudTables are the Admin API tables in schema cloud.
var cloudTables = []*cloudTable{
	cloudAccounts, cloudProjects, cloudEnvironments, cloudJobs, cloudRuns, cloudRunSteps, cloudRunArtifacts,
}

// discoveryTables are the Discovery API tables in schema cloud.
var discoveryTables = []*discoveryTable{discoveryModels, discoveryTests, discoverySources}

// mainDocs, cloudDocs and unifiedDocs are every function's docs per schema.
func mainDocs() []fnDocs {
	out := []fnDocs{infoDocs, manifestVersionDocs, selectorMatchDocs, parseSelectorDocs, parseUniqueIDDocs,
		normalizeRelationDocs, (&walkFn{down: true}).docs(), (&walkFn{}).docs(),
		{Examples: schemaVersionsExamples}}
	for _, t := range artifactTables {
		out = append(out, t.Docs)
	}
	return out
}

func cloudDocs() []fnDocs {
	var out []fnDocs
	for _, t := range cloudTables {
		out = append(out, t.Docs)
	}
	for _, t := range discoveryTables {
		out = append(out, t.Docs)
	}
	return out
}

func unifiedDocs() []fnDocs {
	return []fnDocs{(&unifiedFunc{}).docs(), (&unifiedFunc{tasks: true}).docs()}
}

// examplesOf flattens the example lists of a set of docs.
func examplesOf(ds []fnDocs) []exampleQuery {
	lists := make([][]vgi.CatalogExample, len(ds))
	for i, d := range ds {
		lists[i] = d.Examples
	}
	return exampleQueries(lists...)
}

// NewWorker builds the worker: catalog dbt with schemas main, cloud, unified.
func NewWorker(opts ...vgi.WorkerOption) *vgi.Worker {
	sourceURL, version := SourceURL, Version
	base := []vgi.WorkerOption{
		vgi.WithCatalogName(CatalogName),
		vgi.WithCatalogComment(CatalogComment),
		vgi.WithCatalogTags(CatalogTags),
		vgi.WithCatalogInfo(vgi.CatalogInfo{SourceURL: &sourceURL, ImplementationVersion: &version}),
		vgi.WithSchemaComments(map[string]string{"main": MainSchemaComment, "cloud": CloudSchemaComment, "unified": UnifiedSchemaComment}),
		vgi.WithSchemaTags(map[string]map[string]string{
			"main":    schemaTags(mainSchema, examplesOf(mainDocs())),
			"cloud":   schemaTags(cloudSchema, examplesOf(cloudDocs())),
			"unified": schemaTags(unifiedSchema, examplesOf(unifiedDocs())),
		}),
		vgi.WithSecretTypes(CloudSecretTypeSpec),
	}
	w := vgi.NewWorker(append(base, opts...)...)

	// main: artifacts, selectors, DAG.
	w.RegisterTable(vgi.AsTableFunction[InfoState](&artifactInfo{}))
	for _, t := range artifactTables {
		w.RegisterTable(vgi.AsTableFunction[ArtifactScanState](&artifactFunc{t: t}))
	}
	w.RegisterTable(vgi.AsTableFunction[ParseSelectorState](&parseSelectorFn{}))
	w.RegisterTableInOut(vgi.AsTableInOutFunction[struct{}](&walkFn{down: true}))
	w.RegisterTableInOut(vgi.AsTableInOutFunction[struct{}](&walkFn{down: false}))
	w.RegisterScalar(vgi.AsScalarFunction[manifestVersionArgs](&manifestVersionFn{}))
	w.RegisterScalar(&selectorMatchFn{})
	w.RegisterScalar(vgi.AsScalarFunction[parseUniqueIDArgs](&parseUniqueIDFn{}))
	w.RegisterScalar(vgi.AsScalarFunction[normalizeRelationArgs](&normalizeRelationFn{}))
	w.RegisterCatalogTable("main", schemaVersionsTable())

	// cloud: Admin API + Discovery API.
	for _, t := range cloudTables {
		if t.Table {
			w.RegisterCatalogTable("cloud", t.catalogTable())
			continue
		}
		w.RegisterTableInSchema("cloud", vgi.AsTableFunction[CloudScanState](&cloudFunc{t: t}))
	}
	for _, t := range discoveryTables {
		w.RegisterTableInSchema("cloud", vgi.AsTableFunction[DiscoveryState](&discoveryFunc{t: t}))
	}

	// unified: cross-orchestrator shape.
	w.RegisterTableInSchema("unified", vgi.AsTableFunction[UnifiedState](&unifiedFunc{}))
	w.RegisterTableInSchema("unified", vgi.AsTableFunction[UnifiedState](&unifiedFunc{tasks: true}))
	return w
}
