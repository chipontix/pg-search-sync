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
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/chipontix/pg-search-sync/internal/app"
	"github.com/chipontix/pg-search-sync/internal/es"
)

type report struct {
	Rows    int64   `json:"pg_rows"`
	Docs    int64   `json:"es_live_docs"`
	Missing int64   `json:"missing"` // in PG, absent or tombstoned in ES
	Stale   int64   `json:"stale"`   // ES holds an older version
	Extra   int64   `json:"extra"`   // live in ES, deleted in PG
	Ahead   int64   `json:"ahead"`   // ES newer than PG: should never happen
	Samples []int64 `json:"sample_ids,omitempty"`
	Elapsed string  `json:"elapsed"`
}

func (r report) ok() bool { return r.Missing+r.Stale+r.Extra+r.Ahead == 0 }

func main() {
	wait := flag.Duration("wait", 2*time.Minute, "keep re-checking until consistent or this long")
	flag.Parse()
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, app.Env("PG_DSN", "postgres://app:app@localhost:5432/catalog"))
	if err != nil {
		app.Fatal("postgres", "error", err)
	}
	client := es.New(app.Env("ES_URL", "http://localhost:9200"))
	index := app.Env("ES_INDEX", "products")

	deadline := time.Now().Add(*wait)
	for {
		r, err := compare(ctx, conn, client, index)
		if err != nil {
			app.Fatal("compare", "error", err)
		}
		if r.ok() || time.Now().After(deadline) {
			_ = json.NewEncoder(os.Stdout).Encode(r)
			if !r.ok() {
				os.Exit(1)
			}
			return
		}
		slog.Info("not converged yet", "missing", r.Missing, "stale", r.Stale, "extra", r.Extra)
		time.Sleep(2 * time.Second)
	}
}

type entry struct {
	id, version int64
	deleted     bool
}

// compare merge-joins both sides ordered by id.
func compare(ctx context.Context, conn *pgx.Conn, client *es.Client, index string) (report, error) {
	start := time.Now()
	var r report
	// REPEATABLE READ gives one consistent snapshot of the source for the scan.
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return r, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, "SELECT id, version FROM products ORDER BY id")
	if err != nil {
		return r, err
	}
	defer rows.Close()
	nextPG := func() (entry, bool, error) {
		if !rows.Next() {
			return entry{}, false, rows.Err()
		}
		var e entry
		err := rows.Scan(&e.id, &e.version)
		r.Rows++
		return e, true, err
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
				sample(d.id)
			}
			if d, dok, err = docs.next(ctx); err != nil {
				return r, err
			}
		case pok && (!dok || p.id < d.id): // only in PG
			r.Missing++
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
				sample(p.id)
			case d.version < p.version:
				r.Stale++
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
