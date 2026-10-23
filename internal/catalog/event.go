// Package catalog holds the change event that travels PostgreSQL → Kafka → Elasticsearch.
package catalog

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

const (
	OpUpsert = "upsert"
	OpDelete = "delete"
)

// Product is the indexed document.
type Product struct {
	ID         int64     `json:"id"`
	SKU        string    `json:"sku,omitempty"`
	Title      string    `json:"title,omitempty"`
	Brand      string    `json:"brand,omitempty"`
	Category   string    `json:"category,omitempty"`
	PriceCents int64     `json:"price_cents,omitempty"`
	Stock      int32     `json:"stock,omitempty"`
	Version    int64     `json:"version"`
	UpdatedAt  time.Time `json:"updated_at,omitzero"`
	Deleted    bool      `json:"deleted"`
}

// Event is the Kafka message value. Key is the product id.
type Event struct {
	Op  string  `json:"op"`
	Doc Product `json:"doc"`
	// SourceTime is the server clock PostgreSQL stamped on the WAL message;
	// used only for the CDC→index lag metric.
	SourceTime time.Time `json:"source_time"`
}

func (e Event) Key() []byte { return strconv.AppendInt(nil, e.Doc.ID, 10) }

// FromRow builds an event from a go-pq-cdc decoded row (pgx text-format types:
// bigint→int64, int→int32, text→string, timestamptz→time.Time).
func FromRow(op string, row map[string]any, sourceTime time.Time) (Event, error) {
	var p Product
	var ok bool
	if p.ID, ok = row["id"].(int64); !ok {
		return Event{}, fmt.Errorf("id: unexpected %T", row["id"])
	}
	if p.Version, ok = row["version"].(int64); !ok {
		return Event{}, fmt.Errorf("version: unexpected %T", row["version"])
	}
	if op == OpDelete {
		p.Deleted = true
		return Event{Op: op, Doc: p, SourceTime: sourceTime}, nil
	}
	p.SKU, _ = row["sku"].(string)
	p.Title, _ = row["title"].(string)
	p.Brand, _ = row["brand"].(string)
	p.Category, _ = row["category"].(string)
	p.PriceCents, _ = row["price_cents"].(int64)
	p.Stock, _ = row["stock"].(int32)
	p.UpdatedAt, _ = row["updated_at"].(time.Time)
	return Event{Op: op, Doc: p, SourceTime: sourceTime}, nil
}

func Decode(b []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(b, &e); err != nil {
		return e, err
	}
	if e.Doc.ID == 0 || e.Doc.Version == 0 {
		return e, fmt.Errorf("event without id or version")
	}
	return e, nil
}
