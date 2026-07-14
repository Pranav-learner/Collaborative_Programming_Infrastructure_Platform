// Package metrics provides observable counters and histograms for the
// persistence layer. All metrics use a Recorder interface so callers can plug
// in Prometheus, OpenTelemetry, or a no-op implementation.
package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

// Recorder is the interface that persistence subsystems use to record metrics.
// Implementations may wrap Prometheus, OTel, StatsD, or the in-memory recorder
// provided by this package.
type Recorder interface {
	IncCounter(name string, labels map[string]string)
	ObserveHistogram(name string, value float64, labels map[string]string)
}

// InMemoryRecorder is a test/dev-friendly recorder that stores metrics in memory.
type InMemoryRecorder struct {
	mu         sync.RWMutex
	counters   map[string]int64
	histograms map[string][]float64
}

// NewInMemoryRecorder creates an InMemoryRecorder.
func NewInMemoryRecorder() *InMemoryRecorder {
	return &InMemoryRecorder{
		counters:   make(map[string]int64),
		histograms: make(map[string][]float64),
	}
}

func (r *InMemoryRecorder) IncCounter(name string, _ map[string]string) {
	r.mu.Lock()
	r.counters[name]++
	r.mu.Unlock()
}

func (r *InMemoryRecorder) ObserveHistogram(name string, value float64, _ map[string]string) {
	r.mu.Lock()
	r.histograms[name] = append(r.histograms[name], value)
	r.mu.Unlock()
}

// Counter returns the current value of a named counter.
func (r *InMemoryRecorder) Counter(name string) int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.counters[name]
}

// Histogram returns all observed values for a named histogram.
func (r *InMemoryRecorder) Histogram(name string) []float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cp := make([]float64, len(r.histograms[name]))
	copy(cp, r.histograms[name])
	return cp
}

// NoopRecorder silently discards all metrics.
type NoopRecorder struct{}

func (NoopRecorder) IncCounter(string, map[string]string)               {}
func (NoopRecorder) ObserveHistogram(string, float64, map[string]string) {}

// PoolStats tracks connection pool gauges (polled periodically).
type PoolStats struct {
	OpenConnections   atomic.Int64
	IdleConnections   atomic.Int64
	InUseConnections  atomic.Int64
	WaitCount         atomic.Int64
	WaitDurationTotal atomic.Int64 // nanoseconds
}

// Metric names used across the persistence layer.
const (
	MetricTxStarted       = "persistence.tx.started"
	MetricTxCommitted     = "persistence.tx.committed"
	MetricTxRolledBack    = "persistence.tx.rolledback"
	MetricTxDuration      = "persistence.tx.duration_ms"
	MetricQueryDuration   = "persistence.query.duration_ms"
	MetricQueryCount      = "persistence.query.count"
	MetricLockConflict    = "persistence.lock.conflict"
	MetricMigrationRun    = "persistence.migration.run"
	MetricPoolOpen        = "persistence.pool.open_connections"
	MetricPoolIdle        = "persistence.pool.idle_connections"
	MetricPoolInUse       = "persistence.pool.in_use_connections"
)

// ObserveQuery is a helper that records a query's duration and increments the counter.
func ObserveQuery(rec Recorder, entity, operation string, dur time.Duration) {
	labels := map[string]string{"entity": entity, "operation": operation}
	rec.IncCounter(MetricQueryCount, labels)
	rec.ObserveHistogram(MetricQueryDuration, float64(dur.Milliseconds()), labels)
}
