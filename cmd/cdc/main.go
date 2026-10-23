// cdc streams public.products from the PostgreSQL WAL to Kafka.
package main

import (
	"github.com/chipontix/pg-search-sync/internal/app"
	"github.com/chipontix/pg-search-sync/internal/pipeline"
)

func main() {
	ctx, stop := app.SignalContext()
	defer stop()
	if err := pipeline.RunCDC(ctx, nil); err != nil {
		app.Fatal("cdc", "error", err)
	}
}
