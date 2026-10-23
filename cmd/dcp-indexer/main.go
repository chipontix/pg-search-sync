// dcp-indexer is the second read path: Couchbase DCP → Elasticsearch with
// go-dcp-elasticsearch. DCP delivers mutations of a vBucket in order and the
// connector checkpoints after each flushed batch, so a restart replays the
// tail in order and the final state is the same; no external versions needed.
package main

import (
	"time"

	dcpes "github.com/Trendyol/go-dcp-elasticsearch"
	"github.com/Trendyol/go-dcp-elasticsearch/config"
	"github.com/Trendyol/go-dcp-elasticsearch/couchbase"
	"github.com/Trendyol/go-dcp-elasticsearch/elasticsearch/document"
	dcpconfig "github.com/Trendyol/go-dcp/config"

	"github.com/chipontix/pg-search-sync/internal/app"
	"github.com/chipontix/pg-search-sync/internal/es"
)

func mapper(event couchbase.Event) []document.ESActionDocument {
	if event.IsMutated {
		return []document.ESActionDocument{document.NewIndexAction(event.Key, event.Value, nil)}
	}
	return []document.ESActionDocument{document.NewDeleteAction(event.Key, nil)}
}

func main() {
	esURL := app.Env("ES_URL", "http://localhost:9200")
	index := app.Env("ES_INDEX", "products_cb")
	ctx, stop := app.SignalContext()
	defer stop()
	for es.New(esURL).EnsureIndex(ctx, index) != nil {
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}

	noDiscovery := 24 * time.Hour
	connector, err := dcpes.NewConnectorBuilder(config.Config{
		Elasticsearch: config.Elasticsearch{
			CollectionIndexMapping: map[string]string{"_default": index},
			Urls:                   []string{esURL},
			BatchSizeLimit:         app.EnvInt("BATCH_SIZE", 1000),
			BatchTickerDuration:    app.EnvDuration("BATCH_TICK", 100*time.Millisecond),
			// The default 5m discovery would replace the configured URL with
			// the node's container-internal publish address.
			DisableDiscoverNodesOnStart: true,
			DiscoverNodesInterval:       &noDiscovery,
		},
		Dcp: dcpconfig.Dcp{
			Hosts:      app.EnvList("CB_HOSTS", "localhost:8091"),
			Username:   app.Env("CB_USER", "admin"),
			Password:   app.Env("CB_PASSWORD", "password"),
			BucketName: app.Env("CB_BUCKET", "catalog"),
			Dcp:        dcpconfig.ExternalDcp{Group: dcpconfig.DCPGroup{Name: "products-es"}},
			Metadata: dcpconfig.Metadata{
				Type: "couchbase",
				Config: map[string]string{
					"bucket": app.Env("CB_META_BUCKET", "dcp-meta"), "scope": "_default", "collection": "_default",
				},
			},
			API:    dcpconfig.API{Port: app.EnvInt("METRIC_PORT", 18092)},
			Metric: dcpconfig.Metric{Path: "/metrics"},
		},
	}).SetMapper(mapper).Build()
	if err != nil {
		app.Fatal("dcp connector", "error", err)
	}
	go func() { <-ctx.Done(); connector.Close() }()
	connector.Start()
}
