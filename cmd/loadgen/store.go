package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/chipontix/pg-search-sync/internal/app"
	"github.com/chipontix/pg-search-sync/internal/couchbase"
)

// store is the write side under test: PostgreSQL (CDC path) or Couchbase (DCP path).
type store interface {
	maxID(ctx context.Context) (int64, error)
	update(ctx context.Context, id int64) (version int64, err error)
	insert(ctx context.Context) (id, version int64, err error)
	seed(ctx context.Context, n int) error
}

type postgresStore struct{ pool *pgxpool.Pool }

func newPostgresStore(ctx context.Context, conns int) *postgresStore {
	cfg, err := pgxpool.ParseConfig(app.Env("PG_DSN", "postgres://app:app@localhost:5432/catalog"))
	if err != nil {
		app.Fatal("pg dsn", "error", err)
	}
	cfg.MaxConns = int32(conns)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		app.Fatal("postgres", "error", err)
	}
	return &postgresStore{pool: pool}
}

func (s *postgresStore) maxID(ctx context.Context) (n int64, err error) {
	err = s.pool.QueryRow(ctx, "SELECT coalesce(max(id), 0) FROM products").Scan(&n)
	return n, err
}

func (s *postgresStore) update(ctx context.Context, id int64) (v int64, err error) {
	err = s.pool.QueryRow(ctx, "UPDATE products SET price_cents = $2, stock = $3 WHERE id = $1 RETURNING version",
		id, 100+rand.Int64N(1_000_000), rand.Int32N(1000)).Scan(&v)
	return v, err
}

func (s *postgresStore) insert(ctx context.Context) (id, v int64, err error) {
	title, brand, category := randomTitle()
	err = s.pool.QueryRow(ctx,
		`INSERT INTO products (sku, title, brand, category, price_cents, stock)
		 VALUES ('SKU-' || gen_random_uuid(), $1, $2, $3, $4, $5) RETURNING id, version`,
		title, brand, category, 100+rand.Int64N(1_000_000), rand.Int32N(1000)).Scan(&id, &v)
	return id, v, err
}

func (s *postgresStore) seed(ctx context.Context, n int) error {
	rows := make([][]any, n)
	for i := range rows {
		title, brand, category := randomTitle()
		rows[i] = []any{fmt.Sprintf("SEED-%d-%d", time.Now().UnixNano(), i), title, brand, category, 100 + rand.Int64N(1_000_000), rand.Int32N(1000)}
	}
	_, err := s.pool.CopyFrom(ctx, pgx.Identifier{"products"},
		[]string{"sku", "title", "brand", "category", "price_cents", "stock"}, pgx.CopyFromRows(rows))
	return err
}

// couchbaseStore writes through N1QL. Documents are keyed by the decimal id and
// carry their own version (a process-wide counter seeded from the clock), the
// same shape the PostgreSQL path indexes. UPSERT returns once the active node
// holds the mutation in memory (no durability level), Couchbase's default.
type couchbaseStore struct {
	cb      *couchbase.Client
	bucket  string
	version atomic.Int64
	nextID  atomic.Int64
}

func newCouchbaseStore() *couchbaseStore {
	s := &couchbaseStore{
		cb:     couchbase.New(app.Env("CB_QUERY_URL", "http://localhost:8093"), app.Env("CB_USER", "admin"), app.Env("CB_PASSWORD", "password")),
		bucket: app.Env("CB_BUCKET", "catalog"),
	}
	s.version.Store(time.Now().UnixMicro()) // < 2^53: N1QL numbers may pass through float64
	return s
}

func (s *couchbaseStore) maxID(ctx context.Context) (int64, error) {
	rows, err := s.cb.Query(ctx, fmt.Sprintf("SELECT RAW MAX(d.id) FROM `%s` d WHERE d.id IS VALUED", s.bucket))
	if err != nil {
		return 0, err
	}
	var n *int64
	if len(rows) == 1 {
		_ = json.Unmarshal(rows[0], &n)
	}
	if n == nil {
		return 0, nil
	}
	s.nextID.Store(*n)
	return *n, nil
}

func (s *couchbaseStore) upsert(ctx context.Context, id int64) (int64, error) {
	v := s.version.Add(1)
	title, brand, category := randomTitle()
	doc := map[string]any{
		"id": id, "sku": fmt.Sprintf("CB-%d", id), "title": title, "brand": brand, "category": category,
		"price_cents": 100 + rand.Int64N(1_000_000), "stock": rand.Int32N(1000), "version": v,
		"updated_at": time.Now().UTC().Format(time.RFC3339Nano), "deleted": false,
	}
	_, err := s.cb.Query(ctx, fmt.Sprintf("UPSERT INTO `%s` (KEY, VALUE) VALUES ($1, $2)", s.bucket), fmt.Sprint(id), doc)
	return v, err
}

func (s *couchbaseStore) update(ctx context.Context, id int64) (int64, error) {
	return s.upsert(ctx, id)
}

func (s *couchbaseStore) insert(ctx context.Context) (int64, int64, error) {
	id := s.nextID.Add(1)
	v, err := s.upsert(ctx, id)
	return id, v, err
}

func (s *couchbaseStore) seed(ctx context.Context, n int) error {
	start, err := s.maxID(ctx)
	if err != nil {
		return err
	}
	for lo := start + 1; lo <= start+int64(n); lo += 10000 {
		hi := min(lo+10000, start+int64(n)+1)
		_, err := s.cb.Query(ctx, fmt.Sprintf(
			"UPSERT INTO `%s` (KEY TOSTRING(i), VALUE {'id': i, 'sku': 'CB-' || TOSTRING(i), 'title': 'seed product ' || TOSTRING(i),"+
				" 'brand': 'Mavi', 'category': 'jeans', 'price_cents': i %% 1000000, 'stock': i %% 1000, 'version': $3,"+
				" 'updated_at': NOW_STR(), 'deleted': false}) SELECT i FROM ARRAY_RANGE($1, $2) AS i", s.bucket),
			lo, hi, s.version.Add(1))
		if err != nil {
			return err
		}
	}
	return nil
}
