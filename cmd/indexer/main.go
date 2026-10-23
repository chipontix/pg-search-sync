// indexer consumes product change events with kafka-konsumer and bulk-indexes
// them into Elasticsearch.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/Trendyol/kafka-konsumer/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/chipontix/pg-search-sync/internal/app"
	"github.com/chipontix/pg-search-sync/internal/catalog"
	"github.com/chipontix/pg-search-sync/internal/es"
)

var (
	itemsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "indexer_items_total",
		Help: "Bulk items by outcome.",
	}, []string{"outcome"})
	bulkSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "indexer_bulk_seconds",
		Help:    "Elasticsearch bulk request duration.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
	}, []string{"result"})
	lagSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "indexer_source_lag_seconds",
		Help:    "Time from the PostgreSQL WAL message to the bulk response (before the ES refresh).",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 14),
	})
)

type indexer struct {
	es    *es.Client
	index string
}

// consume is the kafka-konsumer BatchConsumeFn.
func (ix *indexer) consume(msgs []*kafka.Message) error {
	docs := make([]catalog.Product, 0, len(msgs))
	sources := make([]time.Time, 0, len(msgs))
	for _, m := range msgs {
		ev, err := catalog.Decode(m.Value)
		if err != nil {
			return fmt.Errorf("offset %d: %w", m.Offset, err)
		}
		docs = append(docs, ev.Doc)
		sources = append(sources, ev.SourceTime)
	}
	start := time.Now()
	statuses, err := ix.es.Bulk(context.Background(), ix.index, docs)
	if err != nil {
		bulkSeconds.WithLabelValues("error").Observe(time.Since(start).Seconds())
		return err
	}
	bulkSeconds.WithLabelValues("ok").Observe(time.Since(start).Seconds())
	for i, st := range statuses {
		switch st {
		case http.StatusOK, http.StatusCreated, http.StatusNotFound: // 404: delete of a doc never indexed
			itemsTotal.WithLabelValues("indexed").Inc()
			lagSeconds.Observe(time.Since(sources[i]).Seconds())
		default:
			itemsTotal.WithLabelValues("failed").Inc()
			err = fmt.Errorf("bulk item status %d", st)
		}
	}
	return err
}

func main() {
	ctx, stop := app.SignalContext()
	defer stop()

	ix := &indexer{
		es:    es.New(app.Env("ES_URL", "http://localhost:9200")),
		index: app.Env("ES_INDEX", "products"),
	}
	for ix.es.EnsureIndex(ctx, ix.index) != nil {
		slog.Info("waiting for elasticsearch")
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}

	consumer, err := kafka.NewConsumer(&kafka.ConsumerConfig{
		Reader: kafka.ReaderConfig{
			Brokers: app.EnvList("KAFKA_BROKERS", "localhost:19092"),
			Topic:   app.Env("TOPIC", "products.cdc"),
			GroupID: app.Env("GROUP_ID", "products-indexer"),
			MaxWait: 50 * time.Millisecond,
		},
		BatchConfiguration: &kafka.BatchConfiguration{
			MessageGroupLimit: app.EnvInt("BATCH_SIZE", 1000),
			BatchConsumeFn:    ix.consume,
		},
		MessageGroupDuration: app.EnvDuration("BATCH_WAIT", 20*time.Millisecond),
		Concurrency:          app.EnvInt("CONCURRENCY", 4),
	})
	if err != nil {
		app.Fatal("new consumer", "error", err)
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(itemsTotal, bulkSeconds, lagSeconds)
	reg.MustRegister(consumer.GetMetricCollectors()...)
	reg.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	srv := &http.Server{Addr: ":" + app.Env("METRIC_PORT", "8091"), Handler: promhttp.HandlerFor(reg, promhttp.HandlerOpts{})}
	go func() {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			app.Fatal("metrics server", "error", err)
		}
	}()

	consumer.Consume()
	slog.Info("indexer started", "index", ix.index)
	<-ctx.Done()
	consumer.Stop()
	_ = srv.Close()
}
