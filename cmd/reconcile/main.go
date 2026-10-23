// reconcile compares PostgreSQL (source of truth) with the Elasticsearch index
// and exits non-zero if any product is missing, stale, or should be deleted.
// The pipeline is asynchronous, so it re-checks until the two converge or
// -wait expires.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/chipontix/pg-search-sync/internal/app"
	"github.com/chipontix/pg-search-sync/internal/catalog"
	"github.com/chipontix/pg-search-sync/internal/couchbase"
	"github.com/chipontix/pg-search-sync/internal/es"
)

type report struct {
	Rows    int64   `json:"source_rows"`
	Docs    int64   `json:"es_live_docs"`
	Missing int64   `json:"missing"` // in PG, absent or tombstoned in ES
	Stale   int64   `json:"stale"`   // ES holds an older version
	Extra   int64   `json:"extra"`   // live in ES, deleted in PG
	Ahead   int64   `json:"ahead"`   // ES newer than PG: should never happen
	Samples []int64 `json:"sample_ids,omitempty"`
	// Repaired counts rows touched and tombstones written by -repair.
	Repaired int `json:"repaired,omitempty"`

	touch      []int64           // missing or stale: re-emit from the source
	tombstones []catalog.Product // extra: live in ES, gone from the source
	Elapsed    string            `json:"elapsed"`
}

func (r report) ok() bool { return r.Missing+r.Stale+r.Extra+r.Ahead == 0 }

func main() {
	wait := flag.Duration("wait", 2*time.Minute, "keep re-checking until consistent or this long")
	source := flag.String("source", "postgres", "source of truth: postgres or couchbase")
	repair := flag.Bool("repair", false, "if still inconsistent after -wait, re-emit the affected rows and check again")
	flag.Parse()
	ctx := context.Background()

	var scan sourceScan
	var conn *pgx.Conn
	if *source == "couchbase" {
		scan = couchbaseScan(couchbase.New(app.Env("CB_QUERY_URL", "http://localhost:8093"),
			app.Env("CB_USER", "admin"), app.Env("CB_PASSWORD", "password")), app.Env("CB_BUCKET", "catalog"))
	} else {
		var err error
		conn, err = pgx.Connect(ctx, app.Env("PG_DSN", "postgres://app:app@localhost:5432/catalog"))
		if err != nil {
			app.Fatal("postgres", "error", err)
		}
		scan = postgresScan(conn)
	}
	client := es.New(app.Env("ES_URL", "http://localhost:9200"))
	index := app.Env("ES_INDEX", "products")

	r := converge(ctx, scan, client, index, *wait)
	if !r.ok() && *repair && conn != nil {
		n, err := repairAll(ctx, conn, client, index, r)
		if err != nil {
			app.Fatal("repair", "error", err)
		}
		slog.Warn("repaired", "rows_touched", len(r.touch), "tombstones", len(r.tombstones))
		time.Sleep(3 * time.Second) // let CDC and the indexer carry the touched rows
		r = converge(ctx, scan, client, index, *wait)
		r.Repaired = n
	}
	_ = json.NewEncoder(os.Stdout).Encode(r)
	if !r.ok() {
		os.Exit(1)
	}
}

// converge re-runs compare until both sides agree or wait expires.
func converge(ctx context.Context, scan sourceScan, client *es.Client, index string, wait time.Duration) report {
	deadline := time.Now().Add(wait)
	for {
		r, err := compare(ctx, scan, client, index)
		if err != nil {
			app.Fatal("compare", "error", err)
		}
		if r.ok() || time.Now().After(deadline) {
			return r
		}
		slog.Info("not converged yet", "missing", r.Missing, "stale", r.Stale, "extra", r.Extra)
		time.Sleep(2 * time.Second)
	}
}

// repairAll is the backstop for a document the pipeline lost: touching the
// row bumps its version (trigger), so CDC emits it again; a document whose
// row is gone gets a tombstone one version above what ES holds.
func repairAll(ctx context.Context, conn *pgx.Conn, client *es.Client, index string, r report) (int, error) {
	for ids := range slices.Chunk(r.touch, 1000) {
		if _, err := conn.Exec(ctx, "UPDATE products SET updated_at = now() WHERE id = ANY($1)", ids); err != nil {
			return 0, err
		}
	}
	for docs := range slices.Chunk(r.tombstones, 1000) {
		if _, err := client.BulkIndex(ctx, index, docs); err != nil {
			return 0, err
		}
	}
	return len(r.touch) + len(r.tombstones), nil
}

type entry struct {
	id, version int64
	deleted     bool
}

// sourceScan starts one pass over the source ordered by id and returns its
// iterator and a cleanup func.
type sourceScan func(ctx context.Context) (next func() (entry, bool, error), done func(), err error)

func postgresScan(conn *pgx.Conn) sourceScan {
	return func(ctx context.Context) (func() (entry, bool, error), func(), error) {
		// REPEATABLE READ gives one consistent snapshot of the source for the scan.
		tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
		if err != nil {
			return nil, nil, err
		}
		rows, err := tx.Query(ctx, "SELECT id, version FROM products ORDER BY id")
		if err != nil {
			_ = tx.Rollback(ctx)
			return nil, nil, err
		}
		next := func() (entry, bool, error) {
			if !rows.Next() {
				return entry{}, false, rows.Err()
			}
			var e entry
			err := rows.Scan(&e.id, &e.version)
			return e, true, err
		}
		return next, func() { rows.Close(); _ = tx.Rollback(ctx) }, nil
	}
}

// couchbaseScan reads every document's id and version through N1QL (needs
// the idx_id_version index the compose init creates).
func couchbaseScan(cb *couchbase.Client, bucket string) sourceScan {
	return func(ctx context.Context) (func() (entry, bool, error), func(), error) {
		rows, err := cb.Query(ctx, fmt.Sprintf("SELECT d.id, d.version FROM `%s` d WHERE d.id IS VALUED ORDER BY d.id", bucket))
		if err != nil {
			return nil, nil, err
		}
		next := func() (entry, bool, error) {
			if len(rows) == 0 {
				return entry{}, false, nil
			}
			var d struct{ ID, Version int64 }
			err := json.Unmarshal(rows[0], &d)
			rows = rows[1:]
			return entry{id: d.ID, version: d.Version}, true, err
		}
		return next, func() {}, nil
	}
}

// compare merge-joins both sides ordered by id.
func compare(ctx context.Context, scan sourceScan, client *es.Client, index string) (report, error) {
	start := time.Now()
	var r report
	nextSrc, done, err := scan(ctx)
	if err != nil {
		return r, err
	}
	defer done()
	nextPG := func() (entry, bool, error) {
		e, ok, err := nextSrc()
		if ok {
			r.Rows++
		}
		return e, ok, err
	}

	docs := &esScan{client: client, index: index}
	sample := func(id int64) {
		if len(r.Samples) < 10 {
			r.Samples = append(r.Samples, id)
		}
	}

	p, pok, err := nextPG()
	if err != nil {
		return r, err
	}
	d, dok, err := docs.next(ctx)
	if err != nil {
		return r, err
	}
	for pok || dok {
		switch {
		case dok && (!pok || d.id < p.id): // only in ES
			if !d.deleted {
				r.Docs++
				r.Extra++
				r.tombstones = append(r.tombstones, catalog.Product{ID: d.id, Version: d.version + 1, Deleted: true})
				sample(d.id)
			}
			if d, dok, err = docs.next(ctx); err != nil {
				return r, err
			}
		case pok && (!dok || p.id < d.id): // only in PG
			r.Missing++
			r.touch = append(r.touch, p.id)
			sample(p.id)
			if p, pok, err = nextPG(); err != nil {
				return r, err
			}
		default: // same id
			if !d.deleted {
				r.Docs++
			}
			switch {
			case d.deleted: // live in PG, tombstone in ES
				r.Missing++
				r.touch = append(r.touch, p.id)
				sample(p.id)
			case d.version < p.version:
				r.Stale++
				r.touch = append(r.touch, p.id)
				sample(p.id)
			case d.version > p.version:
				r.Ahead++
				sample(p.id)
			}
			if p, pok, err = nextPG(); err != nil {
				return r, err
			}
			if d, dok, err = docs.next(ctx); err != nil {
				return r, err
			}
		}
	}
	r.Elapsed = time.Since(start).Round(time.Millisecond).String()
	return r, nil
}

// esScan pages through the whole index ordered by id with search_after.
type esScan struct {
	client *es.Client
	index  string
	page   []entry
	after  int64
	done   bool
}

func (s *esScan) next(ctx context.Context) (entry, bool, error) {
	if len(s.page) == 0 && !s.done {
		q := fmt.Sprintf(`{"size":10000,"sort":[{"id":"asc"}],"search_after":[%d],"_source":["id","version","deleted"],"track_total_hits":false}`, s.after)
		resp, err := s.client.Search(ctx, s.index, []byte(q))
		if err != nil {
			return entry{}, false, err
		}
		_, docs, err := es.Hits(resp)
		if err != nil {
			return entry{}, false, err
		}
		for _, d := range docs {
			s.page = append(s.page, entry{id: d.ID, version: d.Version, deleted: d.Deleted})
		}
		if len(docs) == 0 {
			s.done = true
		} else {
			s.after = docs[len(docs)-1].ID
		}
	}
	if len(s.page) == 0 {
		return entry{}, false, nil
	}
	e := s.page[0]
	s.page = s.page[1:]
	return e, true, nil
}
