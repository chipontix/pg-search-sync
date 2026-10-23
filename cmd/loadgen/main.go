// loadgen writes products to PostgreSQL at a fixed rate and measures
// write→searchable latency: from the moment COMMIT returns to the moment a
// _search request (which only sees refreshed segments, unlike GET by id) first
// returns the written version.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chipontix/pg-search-sync/internal/app"
	"github.com/chipontix/pg-search-sync/internal/es"
)

type probe struct {
	version int64
	written time.Time
}

type result struct {
	TargetRate   int     `json:"target_rate"`
	AchievedRate float64 `json:"achieved_rate"`
	Writes       int64   `json:"writes"`
	WriteErrors  int64   `json:"write_errors"`
	Samples      int     `json:"samples"`
	NotFound     int     `json:"not_found"`
	P50ms        float64 `json:"p50_ms"`
	P90ms        float64 `json:"p90_ms"`
	P99ms        float64 `json:"p99_ms"`
	MaxMs        float64 `json:"max_ms"`
	DurationS    float64 `json:"duration_s"`
	PollMs       int64   `json:"poll_interval_ms"`
	// How long the probe _search itself took; a found doc's latency can be
	// overstated by up to one poll interval plus this.
	ProbeP99ms float64 `json:"probe_p99_ms"`
	ProbeMaxms float64 `json:"probe_max_ms"`
	// From the indexer's histogram over the same window: WAL message → bulk
	// acknowledged, i.e. everything except the Elasticsearch refresh wait.
	PipelineP50ms float64 `json:"pipeline_p50_ms,omitempty"`
	PipelineP99ms float64 `json:"pipeline_p99_ms,omitempty"`
	Source        string  `json:"source"`
	// cbgo_total_lag_current sampled once a second: DCP mutations the
	// connector has not yet processed, summed over vBuckets.
	DCPLagMax  float64 `json:"dcp_lag_max,omitempty"`
	DCPLagMean float64 `json:"dcp_lag_mean,omitempty"`
}

func main() {
	rate := flag.Int("rate", 1000, "writes per second")
	duration := flag.Duration("duration", 60*time.Second, "how long to write")
	warmup := flag.Duration("warmup", 10*time.Second, "writes before sampling starts")
	sampleEvery := flag.Int("sample-every", 10, "probe one write in N")
	workers := flag.Int("workers", 64, "concurrent PostgreSQL connections")
	updates := flag.Float64("updates", 0.8, "share of writes that update an existing row")
	poll := flag.Duration("poll", 20*time.Millisecond, "search poll interval")
	seed := flag.Int("seed", 0, "insert this many rows with COPY and exit")
	lagURL := flag.String("lag-metrics", app.Env("INDEXER_METRICS", ""), "indexer /metrics URL for the pipeline-only lag split")
	source := flag.String("source", "postgres", "where to write: postgres or couchbase")
	dcpURL := flag.String("dcp-metrics", app.Env("DCP_METRICS", ""), "dcp-indexer /metrics URL; samples cbgo_total_lag_current")
	flag.Parse()

	ctx := context.Background()
	var st store
	if *source == "couchbase" {
		st = newCouchbaseStore()
	} else {
		st = newPostgresStore(ctx, *workers)
	}

	if *seed > 0 {
		if err := st.seed(ctx, *seed); err != nil {
			app.Fatal("seed", "error", err)
		}
		slog.Info("seeded", "rows", *seed)
		return
	}

	maxID, err := st.maxID(ctx)
	if err != nil {
		app.Fatal("max id", "error", err)
	}
	client := es.New(app.Env("ES_URL", "http://localhost:9200"))
	index := app.Env("ES_INDEX", "products")

	var (
		mu       sync.Mutex
		pending  = map[int64]probe{}
		lat      []time.Duration
		probes   []time.Duration
		writes   atomic.Int64
		writeErr atomic.Int64
		sampling atomic.Bool
		n        atomic.Int64
	)

	// Dispatcher: one job per 1/rate seconds, on a 1ms grid. If the workers
	// fall behind the channel fills and the achieved rate drops; it is
	// reported, never hidden.
	jobs := make(chan struct{}, *rate)
	stopWrites := make(chan struct{})
	go func() {
		start := time.Now()
		var sent int64
		t := time.NewTicker(time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stopWrites:
				close(jobs)
				return
			case now := <-t.C:
				due := int64(now.Sub(start).Seconds() * float64(*rate))
				for ; sent < due; sent++ {
					select {
					case jobs <- struct{}{}:
					default: // behind schedule: drop the slot
					}
				}
			}
		}
	}()

	var wg sync.WaitGroup
	for range *workers {
		wg.Go(func() {
			for range jobs {
				var id, version int64
				var err error
				if maxID > 0 && rand.Float64() < *updates {
					id = 1 + rand.Int64N(maxID)
					version, err = st.update(ctx, id)
				} else {
					id, version, err = st.insert(ctx)
				}
				written := time.Now()
				if err != nil {
					writeErr.Add(1)
					continue
				}
				writes.Add(1)
				if sampling.Load() && n.Add(1)%int64(*sampleEvery) == 0 {
					mu.Lock()
					if _, busy := pending[id]; !busy {
						pending[id] = probe{version: version, written: written}
					}
					mu.Unlock()
				}
			}
		})
	}

	// Prober: one _search per poll with every pending id.
	stopProbe := make(chan struct{})
	probeDone := make(chan struct{})
	go func() {
		defer close(probeDone)
		t := time.NewTicker(*poll)
		defer t.Stop()
		for {
			select {
			case <-stopProbe:
				return
			case <-t.C:
			}
			mu.Lock()
			ids := make([]string, 0, len(pending))
			for id := range pending {
				ids = append(ids, fmt.Sprint(id))
				if len(ids) == 10000 {
					break
				}
			}
			mu.Unlock()
			if len(ids) == 0 {
				continue
			}
			q := fmt.Sprintf(`{"size":%d,"query":{"terms":{"id":[%s]}},"_source":["id","version"]}`, len(ids), strings.Join(ids, ","))
			sent := time.Now()
			resp, err := client.Search(ctx, index, []byte(q))
			seen := time.Now() // upper bound: the doc became visible before this
			mu.Lock()
			probes = append(probes, seen.Sub(sent))
			mu.Unlock()
			if err != nil {
				slog.Warn("probe search", "error", err)
				continue
			}
			_, docs, err := es.Hits(resp)
			if err != nil {
				continue
			}
			mu.Lock()
			for _, d := range docs {
				if p, ok := pending[d.ID]; ok && d.Version >= p.version {
					lat = append(lat, seen.Sub(p.written))
					delete(pending, d.ID)
				}
			}
			mu.Unlock()
		}
	}()

	time.Sleep(*warmup)
	before := scrapeLag(*lagURL)
	writes.Store(0)
	sampling.Store(true)
	measured := time.Now()
	dcpLag := sampleGauge(*dcpURL, "cbgo_total_lag_current", *duration)
	sampling.Store(false)
	close(stopWrites)
	wg.Wait()
	elapsed := time.Since(measured)

	// Let the stragglers arrive.
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		mu.Lock()
		left := len(pending)
		mu.Unlock()
		if left == 0 {
			break
		}
	}
	close(stopProbe)
	<-probeDone
	after := scrapeLag(*lagURL)

	mu.Lock()
	defer mu.Unlock()
	slices.Sort(lat)
	slices.Sort(probes)
	r := result{
		TargetRate:   *rate,
		AchievedRate: float64(writes.Load()) / elapsed.Seconds(),
		Writes:       writes.Load(),
		WriteErrors:  writeErr.Load(),
		Samples:      len(lat),
		NotFound:     len(pending),
		P50ms:        pct(lat, 0.50),
		P90ms:        pct(lat, 0.90),
		P99ms:        pct(lat, 0.99),
		MaxMs:        pct(lat, 1),
		DurationS:    elapsed.Seconds(),
		PollMs:       poll.Milliseconds(),
		ProbeP99ms:   pct(probes, 0.99),
		ProbeMaxms:   pct(probes, 1),
	}
	r.Source = *source
	if len(dcpLag) > 0 {
		r.DCPLagMax = slices.Max(dcpLag)
		for _, v := range dcpLag {
			r.DCPLagMean += v / float64(len(dcpLag))
		}
	}
	if before != nil && after != nil {
		r.PipelineP50ms = histQuantile(before, after, 0.50) * 1000
		r.PipelineP99ms = histQuantile(before, after, 0.99) * 1000
	}
	_ = json.NewEncoder(os.Stdout).Encode(r)
}

func pct(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := min(int(float64(len(sorted))*p), len(sorted)-1)
	return float64(sorted[i].Microseconds()) / 1000
}

var brands = []string{"Koton", "LC Waikiki", "Mavi", "DeFacto", "Penti", "Flo", "Arçelik", "Vestel", "Karaca", "English Home"}
var categories = []string{"t-shirt", "jeans", "sneakers", "dress", "kettle", "blender", "towel", "backpack"}
var adjectives = []string{"basic", "slim", "oversize", "cotton", "linen", "classic", "sport", "premium", "eco", "mini"}

func randomTitle() (title, brand, category string) {
	brand = brands[rand.IntN(len(brands))]
	category = categories[rand.IntN(len(categories))]
	return fmt.Sprintf("%s %s %s %s", brand, adjectives[rand.IntN(len(adjectives))], adjectives[rand.IntN(len(adjectives))], category), brand, category
}
