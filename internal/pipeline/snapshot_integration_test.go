//go:build integration

package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestSnapshotWithConcurrentUpdates seeds rows before the replication slot
// exists, starts CDC with SNAPSHOT=initial and keeps updating random rows
// while the snapshot is read. An update during the copy reaches the index
// twice: the old version from the copy, then the new one from the WAL, which
// is streamed after the copy from an LSN taken before the snapshot was
// exported. A row changed between that LSN and the export comes the other
// way round, new version first, and only the external version keeps it.
// The index must end up equal to the table.
func TestSnapshotWithConcurrentUpdates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	st := startStack(t, ctx)
	t.Setenv("SNAPSHOT", "initial")
	const rows = 50_000 // about 7 chunks of the library's default 8 000

	db := connect(t, ctx, st)
	if _, err := db.Exec(ctx, `INSERT INTO products (sku, title, brand, category, price_cents, stock)
		SELECT 'S-' || g, 'seeded ' || g, 'Mavi', 'jeans', 1000 + g, 1 FROM generate_series(1, $1) g`, rows); err != nil {
		t.Fatal(err)
	}

	// The updater counts updates committed while the snapshot job is open:
	// after the job row is saved (snapshot exported) and before it is
	// marked completed.
	var total, during atomic.Int64
	stopUpdates := make(chan struct{})
	updaterDone := make(chan error, 1)
	u := connect(t, ctx, st)
	go func() {
		for {
			select {
			case <-stopUpdates:
				updaterDone <- nil
				return
			default:
			}
			if _, err := u.Exec(ctx, `UPDATE products SET stock = stock + 1 WHERE id = $1`, 1+rand.IntN(rows)); err != nil {
				updaterDone <- err
				return
			}
			total.Add(1)
			// The job table appears only once the snapshot starts, and a
			// query naming it fails to plan before that.
			var exists, open bool
			if err := u.QueryRow(ctx, `SELECT to_regclass('cdc_snapshot_job') IS NOT NULL`).Scan(&exists); err != nil {
				updaterDone <- err
				return
			}
			if exists {
				if err := u.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM cdc_snapshot_job WHERE NOT completed)`).Scan(&open); err != nil {
					updaterDone <- err
					return
				}
			}
			if open {
				during.Add(1)
			}
		}
	}()

	runPipeline(t, ctx, 4*time.Minute) // ready fires after the snapshot
	time.Sleep(2 * time.Second)        // a few more updates over the plain stream
	close(stopUpdates)
	if err := <-updaterDone; err != nil {
		t.Fatal(err)
	}
	t.Logf("updates: %d in total, %d while the snapshot job was open", total.Load(), during.Load())
	if during.Load() == 0 {
		t.Fatal("no update landed while the snapshot was being read; the test proved nothing")
	}

	var wantCount, wantSum int64
	if err := db.QueryRow(ctx, `SELECT count(*), sum(version) FROM products`).Scan(&wantCount, &wantSum); err != nil {
		t.Fatal(err)
	}
	// A stale document (snapshot copy indexed over a newer WAL event) has a
	// lower version, so the sum of versions catches it; the count catches a
	// missing one.
	var gotCount, gotSum int64
	deadline := time.Now().Add(2 * time.Minute)
	for {
		gotCount, gotSum = indexTotals(t, st.es)
		if gotCount == wantCount && gotSum == wantSum {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("index: %d docs, version sum %d; table: %d rows, version sum %d",
				gotCount, gotSum, wantCount, wantSum)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func connect(t *testing.T, ctx context.Context, st stack) *pgx.Conn {
	t.Helper()
	c, err := pgx.Connect(ctx, st.dsn())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close(context.Background()) })
	return c
}

// indexTotals returns the live (not tombstoned) document count and the sum
// of their versions.
func indexTotals(t *testing.T, esURL string) (count, versionSum int64) {
	t.Helper()
	body := []byte(`{"size":0,"track_total_hits":true,"query":{"term":{"deleted":false}},
		"aggs":{"v":{"sum":{"field":"version"}}}}`)
	resp, err := http.Post(esURL+"/products/_search", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, 0 // index not created yet
	}
	var r struct {
		Hits struct {
			Total struct{ Value int64 } `json:"total"`
		} `json:"hits"`
		Aggs struct {
			V struct{ Value float64 } `json:"v"`
		} `json:"aggregations"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		t.Fatal(err)
	}
	return r.Hits.Total.Value, int64(r.Aggs.V.Value) // sums stay far below 2^53
}
