package main

import (
	"math"
	"testing"

	dto "github.com/prometheus/client_model/go"
)

func buckets(bounds []float64, counts []uint64) []*dto.Bucket {
	b := make([]*dto.Bucket, len(bounds))
	for i := range bounds {
		b[i] = &dto.Bucket{UpperBound: &bounds[i], CumulativeCount: &counts[i]}
	}
	return b
}

func TestHistQuantileUsesOnlyTheWindow(t *testing.T) {
	bounds := []float64{0.1, 0.2, 0.4}
	before := buckets(bounds, []uint64{100, 100, 100}) // old, all fast
	after := buckets(bounds, []uint64{100, 150, 200})  // window: 50 in (0.1,0.2], 50 in (0.2,0.4]
	if got := histQuantile(before, after, 0.5); math.Abs(got-0.2) > 1e-9 {
		t.Fatalf("p50 = %v, want 0.2", got)
	}
	if got := histQuantile(before, after, 0.75); math.Abs(got-0.3) > 1e-9 {
		t.Fatalf("p75 = %v, want 0.3", got)
	}
}

// A window with a long outage puts the quantile in the +Inf bucket. +Inf cannot
// be encoded as JSON, and that used to lose the whole load report.
func TestHistQuantileInInfBucketIsTheHighestFiniteBound(t *testing.T) {
	bounds := []float64{0.1, 0.2, math.Inf(1)}
	got := histQuantile(buckets(bounds, []uint64{0, 0, 0}), buckets(bounds, []uint64{1, 2, 100}), 0.99)
	if got != 0.2 {
		t.Fatalf("p99 = %v, want 0.2", got)
	}
}

// A restarted indexer resets its counters; the window cannot be measured then.
func TestHistQuantileAfterCounterResetIsZero(t *testing.T) {
	bounds := []float64{0.1, 0.2}
	if got := histQuantile(buckets(bounds, []uint64{50, 100}), buckets(bounds, []uint64{1, 2}), 0.5); got != 0 {
		t.Fatalf("p50 = %v, want 0", got)
	}
}
