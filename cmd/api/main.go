// api serves product search from Elasticsearch.
package main

import (
	"github.com/chipontix/pg-search-sync/internal/app"
	"github.com/chipontix/pg-search-sync/internal/pipeline"
)

func main() {
	ctx, stop := app.SignalContext()
	defer stop()
	if err := app.Serve(ctx, ":"+app.Env("PORT", "8080"), pipeline.SearchHandler()); err != nil {
		app.Fatal("api", "error", err)
	}
}
