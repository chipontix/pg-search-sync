// Package couchbase is a minimal N1QL-over-HTTP client (query service, port
// 8093), enough to write and scan documents without the Couchbase SDK.
package couchbase

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Client struct {
	URL, User, Password string
	HTTP                *http.Client
}

func New(url, user, password string) *Client {
	return &Client{URL: url, User: user, Password: password, HTTP: &http.Client{Timeout: 60 * time.Second}}
}

// Query runs a statement with positional ($1…) arguments and returns the rows.
func (c *Client) Query(ctx context.Context, statement string, args ...any) ([]json.RawMessage, error) {
	q := map[string]any{"statement": statement}
	if len(args) > 0 {
		q["args"] = args // the query service rejects "args": null
	}
	body, err := json.Marshal(q)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL+"/query/service", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(c.User, c.Password)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var r struct {
		Status  string            `json:"status"`
		Results []json.RawMessage `json:"results"`
		Errors  []json.RawMessage `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, fmt.Errorf("n1ql %d: %w", resp.StatusCode, err)
	}
	if r.Status != "success" {
		return nil, fmt.Errorf("n1ql %s: %s", r.Status, r.Errors)
	}
	return r.Results, nil
}
