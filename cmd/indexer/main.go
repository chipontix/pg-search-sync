// indexer consumes product change events with kafka-konsumer and bulk-indexes
// them into Elasticsearch. Writes are idempotent: every document carries the
// PostgreSQL row version as an external version, so replays and out-of-order
// retries resolve to 409 (already have newer) instead of overwriting.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
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
		Help: "Bulk items by outcome: indexed, stale (409, newer version already indexed), retry, dead_letter.",
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
	// retryFor bounds in-process retries of transient failures (429, 5xx,
	// network). While it retries, offsets are not committed, so a crash here
	// replays the batch. Only what is still failing afterwards goes to the
	// kafka-cronsumer retry topic.
	retryFor   time.Duration
	retryTopic string
}

// consume is the kafka-konsumer BatchConsumeFn. With TransactionalRetry=false,
// only messages marked IsFailed go to the retry topic and messages marked
// SendDirectToDeadLetter go straight to the dead-letter topic.
func (ix *indexer) consume(msgs []*kafka.Message) error {
	var (
		pending []*kafka.Message
		docs    []catalog.Product
		sources []time.Time
	)
	var failed error
	for _, m := range msgs {
		ev, err := catalog.Decode(m.Value)
		if err != nil {
			deadLetter(m, err)
			failed = err
			continue
		}
		pending = append(pending, m)
		docs = append(docs, ev.Doc)
		sources = append(sources, ev.SourceTime)
	}

	deadline := time.Now().Add(ix.retryFor)
	if len(msgs) > 0 && msgs[0].Topic == ix.retryTopic {
		// Retry path: cronsumer already spaces attempts out; one try each.
		deadline = time.Now()
	}
	for attempt := 0; len(docs) > 0; attempt++ {
		start := time.Now()
		statuses, err := ix.es.BulkIndex(context.Background(), ix.index, docs)
		if err != nil {
			bulkSeconds.WithLabelValues("error").Observe(time.Since(start).Seconds())
			var se *es.StatusError
			if errors.As(err, &se) && !retryable(se.Code) {
				// The whole request was rejected for a non-transient reason;
				// retrying cannot help.
				for _, m := range pending {
					deadLetter(m, err)
				}
				return err
			}
			if !backoff(attempt, deadline) {
				for _, m := range pending {
					m.IsFailed, m.ErrDescription = true, err.Error()
				}
				itemsTotal.WithLabelValues("retry").Add(float64(len(pending)))
				slog.Warn("bulk failed, handing batch to retry topic", "docs", len(pending), "error", err)
				return err
			}
			continue
		}
		bulkSeconds.WithLabelValues("ok").Observe(time.Since(start).Seconds())

		var nextMsgs []*kafka.Message
		var nextDocs []catalog.Product
		var nextSources []time.Time
		for i, st := range statuses {
			switch {
			case st == http.StatusOK || st == http.StatusCreated:
				itemsTotal.WithLabelValues("indexed").Inc()
				if !sources[i].IsZero() {
					lagSeconds.Observe(time.Since(sources[i]).Seconds())
				}
			case st == http.StatusConflict:
				itemsTotal.WithLabelValues("stale").Inc()
			case retryable(st):
				nextMsgs, nextDocs, nextSources = append(nextMsgs, pending[i]), append(nextDocs, docs[i]), append(nextSources, sources[i])
			default:
				err := errors.New("bulk item status " + strconv.Itoa(st))
				deadLetter(pending[i], err)
				failed = err
			}
		}
		pending, docs, sources = nextMsgs, nextDocs, nextSources
		if len(docs) > 0 && !backoff(attempt, deadline) {
			err := errors.New("bulk items still rejected after " + ix.retryFor.String())
			for _, m := range pending {
				m.IsFailed, m.ErrDescription = true, err.Error()
			}
			itemsTotal.WithLabelValues("retry").Add(float64(len(pending)))
			slog.Warn("bulk items failed, handing to retry topic", "docs", len(pending))
			return err
		}
	}
	return failed
}

func deadLetter(m *kafka.Message, err error) {
	m.SendDirectToDeadLetter, m.IsFailed, m.ErrDescription = true, true, err.Error()
	itemsTotal.WithLabelValues("dead_letter").Inc()
	slog.Error("message to dead letter", "key", string(m.Key), "offset", m.Offset, "error", err)
}

func retryable(status int) bool {
	return status == http.StatusTooManyRequests || status >= 500
}

// backoff sleeps 100ms·2^attempt (max 5s) and reports false once the deadline would pass.
func backoff(attempt int, deadline time.Time) bool {
	d := min(100*time.Millisecond<<min(attempt, 6), 5*time.Second)
	if time.Now().Add(d).After(deadline) {
		return false
	}
	time.Sleep(d)
	return true
}

func main() {
	ctx, stop := app.SignalContext()
	defer stop()

	brokers := app.EnvList("KAFKA_BROKERS", "localhost:19092")
	ix := &indexer{
		es:         es.New(app.Env("ES_URL", "http://localhost:9200")),
		index:      app.Env("ES_INDEX", "products"),
		retryFor:   app.EnvDuration("BULK_RETRY_FOR", 30*time.Second),
		retryTopic: app.Env("RETRY_TOPIC", "products.retry"),
	}
	for ix.es.EnsureIndex(ctx, ix.index) != nil {
		slog.Info("waiting for elasticsearch")
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}

	transactionalRetry := false
	dlq := app.Env("DLQ_TOPIC", "products.dlq")
	consumer, err := kafka.NewConsumer(&kafka.ConsumerConfig{
		Reader: kafka.ReaderConfig{
			Brokers: brokers,
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
		TransactionalRetry:   &transactionalRetry,
		DeadLetterTopic:      dlq,
		RetryEnabled:         true,
		RetryConfiguration: kafka.RetryConfiguration{
			Brokers:         brokers,
			Topic:           ix.retryTopic,
			DeadLetterTopic: dlq,
			StartTimeCron:   app.Env("RETRY_CRON", "@every 15s"),
			WorkDuration:    app.EnvDuration("RETRY_WORK", 10*time.Second),
			MaxRetry:        app.EnvInt("RETRY_MAX", 20),
		},
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
