// Package metrics implements the Metrics Framework: Counter, Gauge, Histogram,
// Summary, and Timer instruments with label dimensions, plus the concurrent-safe
// Metrics Registry that collects them for export. Instruments are lock-free on
// the hot path (atomic counters/gauges/histogram buckets) so thousands of
// concurrent records never contend; only summaries take a mutex for their sliding
// quantile window.
//
// The framework is pull-based and exporter-agnostic: instruments accumulate
// state, and the registry's Gather() produces a neutral []Family that any
// exporter (Prometheus text, OTLP, console) renders. Business code depends only
// on the instrument interfaces.
package metrics

import (
	"sort"
	"strings"
)

// Kind enumerates the instrument types.
type Kind string

const (
	KindCounter   Kind = "counter"
	KindGauge     Kind = "gauge"
	KindHistogram Kind = "histogram"
	KindSummary   Kind = "summary"
)

// Labels is a set of dimension key/values bound to a metric series.
type Labels map[string]string

// Def defines a metric. Name and Kind are required; the rest are kind-specific.
type Def struct {
	Name string
	Kind Kind
	Help string
	// Labels are the allowed dimension names. Values are supplied via With.
	Labels []string
	// Buckets are histogram upper bounds (sorted ascending). Empty → config default.
	Buckets []float64
	// Objectives are summary quantiles in (0,1). Empty → config default.
	Objectives []float64
}

// Bucket is one cumulative histogram bucket.
type Bucket struct {
	UpperBound float64 `json:"upper_bound"`
	Count      uint64  `json:"count"`
}

// Quantile is one summary quantile estimate.
type Quantile struct {
	Quantile float64 `json:"quantile"`
	Value    float64 `json:"value"`
}

// Sample is one series (one label combination) of a metric at gather time.
type Sample struct {
	Labels    Labels     `json:"labels,omitempty"`
	Value     float64    `json:"value,omitempty"`     // counter/gauge
	Count     uint64     `json:"count,omitempty"`     // histogram/summary
	Sum       float64    `json:"sum,omitempty"`       // histogram/summary
	Buckets   []Bucket   `json:"buckets,omitempty"`   // histogram
	Quantiles []Quantile `json:"quantiles,omitempty"` // summary
}

// Family is a metric and all its series — the neutral unit exporters consume.
type Family struct {
	Name    string   `json:"name"`
	Help    string   `json:"help,omitempty"`
	Kind    Kind     `json:"kind"`
	Samples []Sample `json:"samples"`
}

// seriesKey builds a deterministic key for a label combination, ordered by the
// definition's label names so the same labels always map to the same series.
func seriesKey(labelNames []string, labels Labels) string {
	if len(labelNames) == 0 {
		return ""
	}
	var b strings.Builder
	for i, name := range labelNames {
		if i > 0 {
			b.WriteByte('\x1f') // unit separator — cannot appear in normal labels
		}
		b.WriteString(labels[name])
	}
	return b.String()
}

// normalizeLabels returns a copy containing only the defined label names.
func normalizeLabels(labelNames []string, labels Labels) Labels {
	if len(labelNames) == 0 {
		return nil
	}
	out := make(Labels, len(labelNames))
	for _, name := range labelNames {
		out[name] = labels[name]
	}
	return out
}

// sortedFloats returns a sorted copy (buckets/objectives must be ascending).
func sortedFloats(in []float64) []float64 {
	out := append([]float64(nil), in...)
	sort.Float64s(out)
	return out
}
