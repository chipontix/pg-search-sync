package main

import (
	"log/slog"
	"math"
	"net/http"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// scrapeLag returns the cumulative buckets of indexer_source_lag_seconds, or
// nil if url is empty or unreachable.
func scrapeLag(url string) []*dto.Bucket {
	if url == "" {
		return nil
	}
	resp, err := http.Get(url)
	if err != nil {
		slog.Warn("scrape indexer metrics", "error", err)
		return nil
	}
	defer resp.Body.Close()
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		slog.Warn("parse indexer metrics", "error", err)
		return nil
	}
	f, ok := families["indexer_source_lag_seconds"]
	if !ok || len(f.Metric) == 0 {
		return nil
	}
	return f.Metric[0].GetHistogram().GetBucket()
}

// histQuantile estimates quantile q of the observations made between two
// scrapes, interpolating linearly inside the bucket like histogram_quantile.
func histQuantile(before, after []*dto.Bucket, q float64) float64 {
	if len(before) != len(after) || len(after) == 0 {
		return 0
	}
	delta := make([]float64, len(after))
	for i := range after {
		if after[i].GetCumulativeCount() < before[i].GetCumulativeCount() {
			return 0 // the target restarted and its counters were reset
		}
		delta[i] = float64(after[i].GetCumulativeCount() - before[i].GetCumulativeCount())
	}
	total := delta[len(delta)-1]
	if total == 0 {
		return 0
	}
	rank := q * total
	lower, prev := 0.0, 0.0
	for i, c := range delta {
		upper := after[i].GetUpperBound()
		if c >= rank {
			if math.IsInf(upper, 1) {
				return lower
			}
			return lower + (upper-lower)*(rank-prev)/(c-prev)
		}
		lower, prev = upper, c
	}
	return after[len(after)-1].GetUpperBound()
}

// sampleGauge scrapes one gauge from url once a second for d and returns the
// values; it just sleeps for d if url is empty.
func sampleGauge(url, name string, d time.Duration) []float64 {
	if url == "" {
		time.Sleep(d)
		return nil
	}
	var vals []float64
	for deadline := time.Now().Add(d); time.Now().Before(deadline); time.Sleep(time.Second) {
		resp, err := http.Get(url)
		if err != nil {
			continue
		}
		parser := expfmt.NewTextParser(model.UTF8Validation)
		families, err := parser.TextToMetricFamilies(resp.Body)
		resp.Body.Close()
		if f, ok := families[name]; err == nil && ok && len(f.Metric) > 0 {
			vals = append(vals, f.Metric[0].GetGauge().GetValue())
		}
	}
	return vals
}
