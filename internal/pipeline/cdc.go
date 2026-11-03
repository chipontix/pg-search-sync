package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	"github.com/Trendyol/go-pq-cdc-kafka/config"
	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	gokafka "github.com/segmentio/kafka-go"

	"github.com/chipontix/pg-search-sync/internal/app"
	"github.com/chipontix/pg-search-sync/internal/catalog"
)

// RunCDC streams public.products from the PostgreSQL WAL to Kafka with
// go-pq-cdc-kafka until ctx is done. onReady, if set, is called once the
// replication slot is held and streaming. Rows committed before the slot
// exists are captured only with SNAPSHOT=initial; then onReady fires after
// the snapshot. The library serves its own metrics on METRIC_PORT (default 2112).
func RunCDC(ctx context.Context, onReady func()) error {
	topic := app.Env("TOPIC", "products.cdc")
	snapshot, err := snapshotConfig(app.Env("SNAPSHOT", ""))
	if err != nil {
		return err
	}
	cfg := config.Connector{
		CDC: cdcconfig.Config{
			Host:     app.Env("PG_HOST", "localhost"),
			Port:     app.EnvInt("PG_PORT", 5432),
			Username: app.Env("PG_USER", "app"),
			Password: app.Env("PG_PASSWORD", "app"),
			Database: app.Env("PG_DB", "catalog"),
			Publication: publication.Config{
				CreateIfNotExists: true,
				Name:              "products_pub",
				Operations: publication.Operations{
					publication.OperationInsert, publication.OperationUpdate, publication.OperationDelete,
				},
				Tables: publication.Tables{{Name: "products", ReplicaIdentity: publication.ReplicaIdentityFull}},
			},
			Slot: slot.Config{
				CreateIfNotExists:           true,
				Name:                        app.Env("SLOT", "products_slot"),
				SlotActivityCheckerInterval: 3000,
			},
			Snapshot: snapshot,
			Metric:   cdcconfig.MetricConfig{Port: app.EnvInt("METRIC_PORT", 2112)},
			Logger:   cdcconfig.LoggerConfig{LogLevel: slog.LevelInfo},
		},
		Kafka: config.Kafka{
			TableTopicMapping:           map[string]string{"public.products": topic},
			Brokers:                     app.EnvList("KAFKA_BROKERS", "localhost:19092"),
			RequiredAcks:                -1, // all in-sync replicas; the WAL position is acked only after this
			ProducerBatchSize:           app.EnvInt("PRODUCER_BATCH_SIZE", 2000),
			ProducerBatchTickerDuration: app.EnvDuration("PRODUCER_BATCH_TICK", 50*time.Millisecond),
			Compression:                 3, // lz4
		},
	}

	connector, err := cdc.NewConnector(ctx, cfg, cdcHandler)
	if err != nil {
		return err
	}
	go func() { <-ctx.Done(); connector.Close() }()
	if onReady != nil {
		go func() {
			if connector.WaitUntilReady(ctx) == nil {
				onReady()
			}
		}()
	}
	connector.Start(ctx)
	return nil
}

// snapshotConfig decodes SNAPSHOT. "initial" copies the table once, before
// streaming, from a snapshot exported after the slot is created: a row
// changed meanwhile arrives both from the copy and from the WAL, and the
// external version keeps the newer one. Empty or "never" streams only.
func snapshotConfig(v string) (cdcconfig.SnapshotConfig, error) {
	switch v {
	case "", string(cdcconfig.SnapshotModeNever):
		return cdcconfig.SnapshotConfig{}, nil
	case string(cdcconfig.SnapshotModeInitial):
		return cdcconfig.SnapshotConfig{Enabled: true, Mode: cdcconfig.SnapshotModeInitial}, nil
	}
	return cdcconfig.SnapshotConfig{}, fmt.Errorf("SNAPSHOT=%q: want initial or never", v)
}

func cdcHandler(msg *cdc.Message) []gokafka.Message {
	if msg.Type.IsSnapshot() && msg.NewData == nil {
		return nil // BEGIN/END marker of the snapshot, no row
	}
	op, row := catalog.OpUpsert, msg.NewData
	if msg.Type.IsDelete() {
		op, row = catalog.OpDelete, msg.OldData
	}
	ev, err := catalog.FromRow(op, row, msg.EventTime)
	if err != nil {
		// A row we cannot key cannot be ordered or deduplicated downstream;
		// crash instead of acking the WAL past it.
		app.Fatal("decode row", "table", msg.TableName, "type", msg.Type, "error", err)
	}
	value, err := json.Marshal(ev)
	if err != nil {
		app.Fatal("encode event", "error", err)
	}
	return []gokafka.Message{{Key: ev.Key(), Value: value}}
}
