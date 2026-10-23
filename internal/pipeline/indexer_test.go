package pipeline

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trendyol/kafka-konsumer/v2"

	"github.com/chipontix/pg-search-sync/internal/catalog"
	"github.com/chipontix/pg-search-sync/internal/es"
)

// fakeES answers every _bulk with reply(call, itemCount): an HTTP status for
// the whole request, or 200 with one status per item.
func fakeES(t *testing.T, reply func(call, items int) (int, []int)) (*es.Client, *atomic.Int32) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		items := strings.Count(string(body), `{"index"`)
		code, statuses := reply(int(calls.Add(1)), items)
		if code != http.StatusOK {
			w.WriteHeader(code)
			return
		}
		out := struct {
			Items []map[string]map[string]int `json:"items"`
		}{}
		for _, st := range statuses {
			out.Items = append(out.Items, map[string]map[string]int{"index": {"status": st}})
		}
		_ = json.NewEncoder(w).Encode(out)
	}))
	t.Cleanup(srv.Close)
	return es.New(srv.URL), &calls
}

func messages(t *testing.T, n int) []*kafka.Message {
	msgs := make([]*kafka.Message, n)
	for i := range msgs {
		v, err := json.Marshal(catalog.Event{Op: catalog.OpUpsert, Doc: catalog.Product{ID: int64(i + 1), Version: 2}})
		if err != nil {
			t.Fatal(err)
		}
		msgs[i] = &kafka.Message{Value: v}
	}
	return msgs
}

func testIndexer(c *es.Client, retryFor time.Duration) *indexer {
	return &indexer{es: c, index: "p", retryFor: retryFor, retryTopic: "p.retry"}
}

func repeat(st, n int) []int {
	s := make([]int, n)
	for i := range s {
		s[i] = st
	}
	return s
}

func TestConsume(t *testing.T) {
	t.Run("409 is an idempotent success", func(t *testing.T) {
		c, _ := fakeES(t, func(_, n int) (int, []int) { return 200, append([]int{409}, repeat(201, n-1)...) })
		msgs := messages(t, 3)
		if err := testIndexer(c, time.Second).consume(msgs); err != nil {
			t.Fatal(err)
		}
		for _, m := range msgs {
			if m.IsFailed {
				t.Fatal("no message should fail")
			}
		}
	})

	t.Run("item 429 is retried in process, only that item", func(t *testing.T) {
		var second int
		c, calls := fakeES(t, func(call, n int) (int, []int) {
			if call == 1 {
				return 200, []int{201, 429, 201}
			}
			second = n
			return 200, repeat(201, n)
		})
		if err := testIndexer(c, 2*time.Second).consume(messages(t, 3)); err != nil {
			t.Fatal(err)
		}
		if calls.Load() != 2 || second != 1 {
			t.Fatalf("calls=%d, second bulk had %d items; want 2 calls, 1 item", calls.Load(), second)
		}
	})

	t.Run("persistent 429 hands the batch to the retry topic", func(t *testing.T) {
		c, _ := fakeES(t, func(int, int) (int, []int) { return http.StatusTooManyRequests, nil })
		msgs := messages(t, 2)
		if err := testIndexer(c, 250*time.Millisecond).consume(msgs); err == nil {
			t.Fatal("want error")
		}
		for _, m := range msgs {
			if !m.IsFailed || m.SendDirectToDeadLetter {
				t.Fatalf("want retry, got failed=%v dlq=%v", m.IsFailed, m.SendDirectToDeadLetter)
			}
		}
	})

	t.Run("retry-topic messages get a single attempt", func(t *testing.T) {
		c, calls := fakeES(t, func(int, int) (int, []int) { return http.StatusTooManyRequests, nil })
		msgs := messages(t, 1)
		msgs[0].Topic = "p.retry"
		if err := testIndexer(c, time.Minute).consume(msgs); err == nil || calls.Load() != 1 {
			t.Fatalf("err=%v calls=%d; want error after 1 call", err, calls.Load())
		}
	})

	t.Run("item 400 and undecodable messages go to dead letter", func(t *testing.T) {
		c, _ := fakeES(t, func(_, n int) (int, []int) { return 200, append([]int{400}, repeat(201, n-1)...) })
		msgs := append(messages(t, 2), &kafka.Message{Value: []byte("not json")})
		if err := testIndexer(c, time.Second).consume(msgs); err == nil {
			t.Fatal("want error")
		}
		if !msgs[0].SendDirectToDeadLetter || msgs[1].IsFailed || !msgs[2].SendDirectToDeadLetter {
			t.Fatalf("dlq flags: %v %v %v", msgs[0].SendDirectToDeadLetter, msgs[1].IsFailed, msgs[2].SendDirectToDeadLetter)
		}
	})
}
