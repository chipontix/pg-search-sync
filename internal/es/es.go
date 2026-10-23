// Package es is the slice of the Elasticsearch REST API this project needs:
// create index, bulk index/delete, search.
package es

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/chipontix/pg-search-sync/internal/catalog"
)

const Mapping = `{
  "settings": {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "1s"},
  "mappings": {"dynamic": "strict", "properties": {
    "id": {"type": "long"}, "sku": {"type": "keyword"},
    "title": {"type": "text"}, "brand": {"type": "keyword"}, "category": {"type": "keyword"},
    "price_cents": {"type": "long"}, "stock": {"type": "integer"},
    "version": {"type": "long"}, "updated_at": {"type": "date"}, "deleted": {"type": "boolean"}
  }}
}`

type Client struct {
	URL  string
	HTTP *http.Client
}

func New(url string) *Client {
	return &Client{URL: url, HTTP: &http.Client{Timeout: 30 * time.Second}}
}

// StatusError is a non-2xx response to the whole request.
type StatusError struct {
	Code int
	Body string
}

func (e *StatusError) Error() string { return fmt.Sprintf("elasticsearch %d: %s", e.Code, e.Body) }

func (c *Client) do(ctx context.Context, method, path, ctype string, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.URL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", ctype)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return b, &StatusError{Code: resp.StatusCode, Body: truncate(b, 512)}
	}
	return b, nil
}

// EnsureIndex creates the index if it does not exist.
func (c *Client) EnsureIndex(ctx context.Context, index string) error {
	_, err := c.do(ctx, http.MethodPut, "/"+index, "application/json", []byte(Mapping))
	if se, ok := err.(*StatusError); ok && se.Code == http.StatusBadRequest &&
		bytes.Contains([]byte(se.Body), []byte("resource_already_exists_exception")) {
		return nil
	}
	return err
}

// Bulk indexes live docs and deletes Deleted ones, returning the HTTP status
// of every item in input order.
func (c *Client) Bulk(ctx context.Context, index string, docs []catalog.Product) ([]int, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for i := range docs {
		if docs[i].Deleted {
			fmt.Fprintf(&buf, `{"delete":{"_index":%q,"_id":"%d"}}`+"\n", index, docs[i].ID)
			continue
		}
		fmt.Fprintf(&buf, `{"index":{"_index":%q,"_id":"%d"}}`+"\n", index, docs[i].ID)
		if err := enc.Encode(&docs[i]); err != nil {
			return nil, err
		}
	}
	b, err := c.do(ctx, http.MethodPost, "/_bulk", "application/x-ndjson", buf.Bytes())
	if err != nil {
		return nil, err
	}
	var r struct {
		Items []map[string]struct {
			Status int `json:"status"`
		} `json:"items"`
	}
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("bulk response: %w", err)
	}
	if len(r.Items) != len(docs) {
		return nil, fmt.Errorf("bulk response has %d items for %d docs", len(r.Items), len(docs))
	}
	statuses := make([]int, len(docs))
	for i, it := range r.Items {
		for _, v := range it {
			statuses[i] = v.Status
		}
	}
	return statuses, nil
}

// Search posts a query body and returns the raw response.
func (c *Client) Search(ctx context.Context, index string, query []byte) ([]byte, error) {
	return c.do(ctx, http.MethodPost, "/"+index+"/_search", "application/json", query)
}

// Hits decodes the _source of every hit.
func Hits(resp []byte) (total int64, docs []catalog.Product, err error) {
	var r struct {
		Hits struct {
			Total struct {
				Value int64 `json:"value"`
			} `json:"total"`
			Hits []struct {
				Source catalog.Product `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err = json.Unmarshal(resp, &r); err != nil {
		return 0, nil, err
	}
	docs = make([]catalog.Product, len(r.Hits.Hits))
	for i, h := range r.Hits.Hits {
		docs[i] = h.Source
	}
	return r.Hits.Total.Value, docs, nil
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n]) + "…(" + strconv.Itoa(len(b)) + " bytes)"
	}
	return string(b)
}
