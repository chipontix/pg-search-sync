package pipeline

import (
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/chipontix/pg-search-sync/internal/catalog"
)

func TestSnapshotConfig(t *testing.T) {
	for v, want := range map[string]cdcconfig.SnapshotConfig{
		"":        {},
		"never":   {},
		"initial": {Enabled: true, Mode: cdcconfig.SnapshotModeInitial},
	} {
		got, err := snapshotConfig(v)
		if err != nil || got.Enabled != want.Enabled || got.Mode != want.Mode {
			t.Errorf("SNAPSHOT=%q: got %+v, %v; want %+v", v, got, err, want)
		}
	}
	// snapshot_only would copy the table and exit without streaming.
	for _, v := range []string{"snapshot_only", "Initial", "yes"} {
		if _, err := snapshotConfig(v); err == nil {
			t.Errorf("SNAPSHOT=%q accepted", v)
		}
	}
}

// TestSnapshotRowDecodes feeds the handler a row decoded the way the
// library's snapshot reader decodes it (text format, pgtype by column OID),
// which differs from the WAL path, and checks the event it produces.
func TestSnapshotRowDecodes(t *testing.T) {
	cols := []struct {
		name string
		oid  uint32
		text string
	}{
		{"id", pgtype.Int8OID, "42"},
		{"sku", pgtype.TextOID, "A-42"},
		{"title", pgtype.TextOID, "Mavi slim jeans"},
		{"brand", pgtype.TextOID, "Mavi"},
		{"category", pgtype.TextOID, "jeans"},
		{"price_cents", pgtype.Int8OID, "79900"},
		{"stock", pgtype.Int4OID, "5"},
		{"version", pgtype.Int8OID, "1234"},
		{"updated_at", pgtype.TimestamptzOID, "2026-11-03 09:15:00.123456+00"},
	}
	decoders, types := snapshot.NewDecoderCache(), pgtype.NewMap()
	row := map[string]any{}
	for _, c := range cols {
		v, err := decoders.Get(c.oid).Decode(types, []byte(c.text))
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		row[c.name] = v
	}

	out := cdcHandler(cdc.NewSnapshotMessage(&format.Snapshot{
		EventType: format.SnapshotEventTypeData, Table: "products", Schema: "public", Data: row}))
	if len(out) != 1 {
		t.Fatalf("got %d messages", len(out))
	}
	ev, err := catalog.Decode(out[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	want := catalog.Product{ID: 42, SKU: "A-42", Title: "Mavi slim jeans", Brand: "Mavi", Category: "jeans",
		PriceCents: 79900, Stock: 5, Version: 1234,
		UpdatedAt: time.Date(2026, 11, 3, 9, 15, 0, 123456000, time.UTC)}
	if ev.Op != catalog.OpUpsert || !ev.Doc.UpdatedAt.Equal(want.UpdatedAt) {
		t.Fatalf("got %+v", ev)
	}
	ev.Doc.UpdatedAt = want.UpdatedAt
	if ev.Doc != want || string(out[0].Key) != "42" {
		t.Fatalf("got %+v key %s, want %+v", ev.Doc, out[0].Key, want)
	}

	// Markers carry no row: dropped, not a crash on a missing id.
	for _, typ := range []format.SnapshotEventType{format.SnapshotEventTypeBegin, format.SnapshotEventTypeEnd} {
		if out := cdcHandler(cdc.NewSnapshotMessage(&format.Snapshot{EventType: typ, Table: "products"})); len(out) != 0 {
			t.Errorf("%s marker produced %d messages", typ, len(out))
		}
	}
}
