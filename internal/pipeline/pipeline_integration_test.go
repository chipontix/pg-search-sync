//go:build integration

package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	segkafka "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/chipontix/pg-search-sync/internal/catalog"
)

// TestPipeline runs the CDC connector and the indexer in process against real
// PostgreSQL, Redpanda and Elasticsearch containers.
func TestPipeline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	st := startStack(t, ctx)
	runPipeline(t, ctx, time.Minute)

	db, err := pgx.Connect(ctx, st.dsn())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := db.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	api := httptest.NewServer(SearchHandler())
	defer api.Close()

	exec(`INSERT INTO products (id, sku, title, brand, category, price_cents, stock) VALUES
		(1, 'A-1', 'Mavi slim jeans', 'Mavi', 'jeans', 79900, 5),
		(2, 'A-2', 'Penti cotton towel', 'Penti', 'towel', 19900, 9)`)
	eventually(t, "both inserts searchable", func() bool { return search(t, api.URL, "").Total == 2 })

	t.Run("update replaces the document", func(t *testing.T) {
		exec(`UPDATE products SET title = 'Mavi relaxed jeans', price_cents = 69900 WHERE id = 1`)
		eventually(t, "update visible", func() bool {
			r := search(t, api.URL, "q=relaxed")
			return r.Total == 1 && r.Items[0].PriceCents == 69900
		})
	})

	t.Run("delete hides the product, re-insert brings it back", func(t *testing.T) {
		exec(`DELETE FROM products WHERE id = 2`)
		eventually(t, "delete visible", func() bool { return search(t, api.URL, "q=towel").Total == 0 })
		exec(`INSERT INTO products (id, sku, title, brand, category, price_cents, stock)
			VALUES (2, 'A-2', 'Penti linen towel', 'Penti', 'towel', 24900, 1)`)
		eventually(t, "re-insert visible", func() bool { return search(t, api.URL, "q=linen+towel").Total == 1 })
	})

	t.Run("a replayed old event does not overwrite a newer document", func(t *testing.T) {
		var version int64
		if err := db.QueryRow(ctx, `SELECT version FROM products WHERE id = 1`).Scan(&version); err != nil {
			t.Fatal(err)
		}
		stale, _ := json.Marshal(catalog.Event{Op: catalog.OpUpsert, Doc: catalog.Product{
			ID: 1, Version: version - 2, Title: "stale replay", Brand: "Mavi", Category: "jeans"}})
		// Same balancer as the CDC producer, so the replay lands on the
		// partition that holds the live history of id 1.
		w := &segkafka.Writer{Addr: segkafka.TCP(st.broker), Topic: "products.cdc", Balancer: &segkafka.Hash{}}
		if err := w.WriteMessages(ctx, segkafka.Message{Key: []byte("1"), Value: stale}); err != nil {
			t.Fatal(err)
		}
		_ = w.Close()
		// A marker write after the replay: once it is visible, the replay
		// (same partition, earlier offset) has been processed too.
		exec(`UPDATE products SET stock = stock + 1 WHERE id = 1`)
		eventually(t, "marker visible", func() bool {
			r := search(t, api.URL, "sku=A-1")
			return r.Total == 1 && r.Items[0].Version > version
		})
		time.Sleep(2 * time.Second) // one more refresh in case both landed in one batch
		if r := search(t, api.URL, "q=stale"); r.Total != 0 {
			t.Fatalf("stale replay was indexed: %+v", r.Items)
		}
	})

	t.Run("delete then re-insert in one transaction", func(t *testing.T) {
		exec(`BEGIN; DELETE FROM products WHERE id = 1;
			INSERT INTO products (id, sku, title, brand, category, price_cents, stock)
			VALUES (1, 'A-1', 'Mavi skinny jeans', 'Mavi', 'jeans', 59900, 2); COMMIT`)
		eventually(t, "re-inserted row visible", func() bool { return search(t, api.URL, "q=skinny").Total == 1 })
	})
}

type stack struct{ pgHost, pgPort, broker, es string }

func (s stack) dsn() string {
	return fmt.Sprintf("postgres://app:app@%s:%s/catalog", s.pgHost, s.pgPort)
}

// startStack starts PostgreSQL, Redpanda and Elasticsearch and points the
// pipeline's env at them.
func startStack(t *testing.T, ctx context.Context) stack {
	t.Helper()
	pg, err := tcpostgres.Run(ctx, "postgres:17",
		tcpostgres.WithDatabase("catalog"), tcpostgres.WithUsername("app"), tcpostgres.WithPassword("app"),
		tcpostgres.WithInitScripts(filepath.Join("..", "..", "deploy", "postgres", "init.sql")),
		testcontainers.WithCmdArgs("-c", "wal_level=logical"),
		tcpostgres.BasicWaitStrategies(),
	)
	testcontainers.CleanupContainer(t, pg)
	if err != nil {
		t.Fatal(err)
	}
	rp, err := redpanda.Run(ctx, "redpandadata/redpanda:v25.2.1")
	testcontainers.CleanupContainer(t, rp)
	if err != nil {
		t.Fatal(err)
	}
	esC, err := testcontainers.Run(ctx, "docker.elastic.co/elasticsearch/elasticsearch:8.19.4",
		testcontainers.WithEnv(map[string]string{
			"discovery.type": "single-node", "xpack.security.enabled": "false", "ES_JAVA_OPTS": "-Xms512m -Xmx512m",
		}),
		testcontainers.WithExposedPorts("9200/tcp"),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/_cluster/health?wait_for_status=yellow").WithPort("9200/tcp").WithStartupTimeout(3*time.Minute)),
	)
	testcontainers.CleanupContainer(t, esC)
	if err != nil {
		t.Fatal(err)
	}

	var st stack
	st.pgHost, _ = pg.Host(ctx)
	pgPort, _ := pg.MappedPort(ctx, "5432/tcp")
	st.pgPort = pgPort.Port()
	st.broker, err = rp.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatal(err)
	}
	st.es, err = esC.PortEndpoint(ctx, "9200/tcp", "http")
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range []string{"products.cdc", "products.retry", "products.dlq"} {
		createTopic(t, st.broker, topic)
	}

	t.Setenv("PG_HOST", st.pgHost)
	t.Setenv("PG_PORT", st.pgPort)
	t.Setenv("KAFKA_BROKERS", st.broker)
	t.Setenv("ES_URL", st.es)
	t.Setenv("METRIC_PORT", strconv.Itoa(freePort(t)))
	t.Setenv("PRODUCER_BATCH_TICK", "20ms")
	return st
}

// runPipeline starts the CDC connector and the indexer in process, stops
// them at the end of the test and waits up to readyIn for CDC to stream.
func runPipeline(t *testing.T, ctx context.Context, readyIn time.Duration) {
	t.Helper()
	runCtx, stop := context.WithCancel(ctx)
	cdcDone, ixDone := make(chan error, 1), make(chan error, 1)
	cdcReady := make(chan struct{})
	go func() { cdcDone <- RunCDC(runCtx, func() { close(cdcReady) }) }()
	go func() { ixDone <- RunIndexer(runCtx, prometheus.NewRegistry()) }()
	t.Cleanup(func() {
		stop()
		for _, ch := range []chan error{cdcDone, ixDone} {
			select {
			case <-ch:
			case <-time.After(30 * time.Second):
				t.Error("pipeline did not stop within 30s")
			}
		}
	})

	select {
	case <-cdcReady:
	case <-time.After(readyIn):
		t.Fatalf("cdc not ready within %s", readyIn)
	}
}

type searchResult struct {
	Total int64             `json:"total"`
	Items []catalog.Product `json:"items"`
}

func search(t *testing.T, base, query string) searchResult {
	t.Helper()
	resp, err := http.Get(base + "/search?" + query)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var r searchResult
	if resp.StatusCode != http.StatusOK {
		return r // index not created yet
	}
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		t.Fatal(err)
	}
	return r
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within 60s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func createTopic(t *testing.T, broker, topic string) {
	t.Helper()
	conn, err := segkafka.Dial("tcp", broker)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.CreateTopics(segkafka.TopicConfig{Topic: topic, NumPartitions: 3, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
