// all runs the CDC connector, the indexer and the search API in one process,
// with the indexer metrics on the API port at /metrics. For tests and local
// runs; deploy the three binaries separately.
package main

import (
	"net/http"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/chipontix/pg-search-sync/internal/app"
	"github.com/chipontix/pg-search-sync/internal/pipeline"
)

func main() {
	ctx, stop := app.SignalContext()
	defer stop()
	reg, metrics := app.Metrics()
	var ready atomic.Bool
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics)
	// Ready once CDC holds the slot: a write committed earlier would be missed.
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.Handle("/", pipeline.SearchHandler())

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return pipeline.RunCDC(ctx, func() { ready.Store(true) }) })
	g.Go(func() error { return pipeline.RunIndexer(ctx, reg) })
	g.Go(func() error { return app.Serve(ctx, ":"+app.Env("PORT", "8080"), mux) })
	if err := g.Wait(); err != nil {
		app.Fatal("all", "error", err)
	}
}
