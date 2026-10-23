// indexer consumes product change events and bulk-indexes them into Elasticsearch.
package main

import (
	"github.com/chipontix/pg-search-sync/internal/app"
	"github.com/chipontix/pg-search-sync/internal/pipeline"
)

func main() {
	ctx, stop := app.SignalContext()
	defer stop()
	reg, metrics := app.Metrics()
	go func() {
		if err := app.Serve(ctx, ":"+app.Env("METRIC_PORT", "8091"), metrics); err != nil {
			app.Fatal("metrics server", "error", err)
		}
	}()
	if err := pipeline.RunIndexer(ctx, reg); err != nil {
		app.Fatal("indexer", "error", err)
	}
}
