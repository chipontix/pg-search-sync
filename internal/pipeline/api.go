package pipeline

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/chipontix/pg-search-sync/internal/app"
	"github.com/chipontix/pg-search-sync/internal/catalog"
	"github.com/chipontix/pg-search-sync/internal/es"
)

type searchResponse struct {
	Total int64             `json:"total"`
	Items []catalog.Product `json:"items"`
}

// SearchHandler serves product search from Elasticsearch (the CQRS read side).
func SearchHandler() http.Handler {
	client := es.New(app.Env("ES_URL", "http://localhost:9200"))
	index := app.Env("ES_INDEX", "products")

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("GET /search", func(w http.ResponseWriter, r *http.Request) {
		q, err := buildQuery(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp, err := client.Search(r.Context(), index, q)
		if err != nil {
			slog.Error("search", "error", err)
			http.Error(w, "search backend error", http.StatusBadGateway)
			return
		}
		total, docs, err := es.Hits(resp)
		if err != nil {
			http.Error(w, "bad search response", http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(searchResponse{Total: total, Items: docs})
	})

	return mux
}

// buildQuery maps ?q=&brand=&category=&min_price=&max_price=&limit= to an
// Elasticsearch bool query. Tombstones (deleted=true) are always excluded.
func buildQuery(r *http.Request) ([]byte, error) {
	v := r.URL.Query()
	limit := 20
	if s := v.Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > 100 {
			return nil, errors.New("limit must be 1..100")
		}
		limit = n
	}
	must := []any{}
	if q := v.Get("q"); q != "" {
		must = append(must, map[string]any{"match": map[string]any{"title": map[string]any{"query": q, "operator": "and"}}})
	}
	filter := []any{map[string]any{"term": map[string]any{"deleted": false}}}
	for _, f := range []string{"brand", "category", "sku"} {
		if s := v.Get(f); s != "" {
			filter = append(filter, map[string]any{"term": map[string]any{f: s}})
		}
	}
	rng := map[string]any{}
	for param, op := range map[string]string{"min_price": "gte", "max_price": "lte"} {
		if s := v.Get(param); s != "" {
			n, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return nil, errors.New(param + " must be an integer (cents)")
			}
			rng[op] = n
		}
	}
	if len(rng) > 0 {
		filter = append(filter, map[string]any{"range": map[string]any{"price_cents": rng}})
	}
	return json.Marshal(map[string]any{
		"size":  limit,
		"query": map[string]any{"bool": map[string]any{"must": must, "filter": filter}},
	})
}
