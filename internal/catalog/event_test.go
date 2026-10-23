package catalog

import (
	"testing"
	"time"
)

func TestDeleteBecomesNewerTombstone(t *testing.T) {
	ev, err := FromRow(OpDelete, map[string]any{"id": int64(7), "version": int64(40), "title": "x"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !ev.Doc.Deleted || ev.Doc.Version != 41 || ev.Doc.Title != "" {
		t.Fatalf("tombstone = %+v", ev.Doc)
	}
	// The next write to the same id takes the next even sequence value (≥42)
	// and must win over the tombstone.
	if ev.Doc.Version >= 42 {
		t.Fatal("tombstone would shadow a re-insert")
	}
}

func TestFromRowRejectsUnkeyedRow(t *testing.T) {
	if _, err := FromRow(OpUpsert, map[string]any{"id": int32(1), "version": int64(2)}, time.Now()); err == nil {
		t.Fatal("int32 id must be rejected, the column is bigint")
	}
}
