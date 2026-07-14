// Package metrics provides observable counters, gauges, and histograms for the
// distributed cache & state module. All subsystems record through the Recorder
// interface so callers can plug in Prometheus, OpenTelemetry, StatsD, or the
// in-memory recorder provided here. There is no global state.
package metrics

import (
	"sync"
	"time"
)

// Recorder is the interface cache subsystems use to emit metrics.
type Recorder interface {
	IncCounter(name string, labels map[string]string)
	AddCounter(name string, delta float64, labels map[string]string)
	SetGauge(name string, value float64, labels map[string]string)
	ObserveHistogram(name string, value float64, labels map[string]string)
}

// Metric names emitted across the module. Keeping them centralized avoids
// typos and lets dashboards be defined once.
const (
	// Cache
	MetricCacheHit          = "cache.hit"
	MetricCacheMiss         = "cache.miss"
	MetricCacheSet          = "cache.set"
	MetricCacheDelete       = "cache.delete"
	MetricCacheError        = "cache.error"
	MetricCacheEviction     = "cache.eviction"
	MetricCacheInvalidation = "cache.invalidation"
	MetricCacheGetDuration  = "cache.get.duration_ms"
	MetricCacheSetDuration  = "cache.set.duration_ms"
	MetricCacheLoadDuration = "cache.load.duration_ms" // backing-store load (read-through)

	// Sessions
	MetricSessionCreated     = "cache.session.created"
	MetricSessionRenewed     = "cache.session.renewed"
	MetricSessionExpired     = "cache.session.expired"
	MetricSessionInvalidated = "cache.session.invalidated"
	MetricSessionActive      = "cache.session.active" // gauge

	// Presence / replication
	MetricPresenceReplicated = "cache.presence.replicated"
	MetricPresenceApplied    = "cache.presence.applied"
	MetricPresenceStale      = "cache.presence.stale_dropped"
	MetricReplicationLagMs   = "cache.replication.lag_ms"

	// Locks
	MetricLockAcquired  = "cache.lock.acquired"
	MetricLockContended = "cache.lock.contended"
	MetricLockReleased  = "cache.lock.released"
	MetricLockRenewed   = "cache.lock.renewed"
	MetricLockExpired   = "cache.lock.expired"
	MetricLockHeld      = "cache.lock.held" // gauge
	MetricLockWaitMs    = "cache.lock.wait_ms"

	// Pub/Sub
	MetricPubSubPublished   = "cache.pubsub.published"
	MetricPubSubReceived    = "cache.pubsub.received"
	MetricPubSubDropped     = "cache.pubsub.dropped"
	MetricPubSubReconnect   = "cache.pubsub.reconnect"
	MetricPubSubSubscribers = "cache.pubsub.subscribers" // gauge

	// TTL
	MetricTTLExpired  = "cache.ttl.expired"
	MetricTTLCallback = "cache.ttl.callback"

	// Redis health
	MetricRedisError = "cache.redis.error"
	MetricRedisUp    = "cache.redis.up" // gauge (1/0)
)

// InMemoryRecorder is a dev/test recorder storing metrics in memory.
type InMemoryRecorder struct {
	mu         sync.RWMutex
	counters   map[string]float64
	gauges     map[string]float64
	histograms map[string][]float64
}

// NewInMemoryRecorder creates an InMemoryRecorder.
func NewInMemoryRecorder() *InMemoryRecorder {
	return &InMemoryRecorder{
		counters:   make(map[string]float64),
		gauges:     make(map[string]float64),
		histograms: make(map[string][]float64),
	}
}

func (r *InMemoryRecorder) IncCounter(name string, labels map[string]string) {
	r.AddCounter(name, 1, labels)
}

func (r *InMemoryRecorder) AddCounter(name string, delta float64, _ map[string]string) {
	r.mu.Lock()
	r.counters[name] += delta
	r.mu.Unlock()
}

func (r *InMemoryRecorder) SetGauge(name string, value float64, _ map[string]string) {
	r.mu.Lock()
	r.gauges[name] = value
	r.mu.Unlock()
}

func (r *InMemoryRecorder) ObserveHistogram(name string, value float64, _ map[string]string) {
	r.mu.Lock()
	r.histograms[name] = append(r.histograms[name], value)
	r.mu.Unlock()
}

// Counter returns the current value of a named counter.
func (r *InMemoryRecorder) Counter(name string) float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.counters[name]
}

// Gauge returns the current value of a named gauge.
func (r *InMemoryRecorder) Gauge(name string) float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.gauges[name]
}

// Histogram returns a copy of all observed values for a named histogram.
func (r *InMemoryRecorder) Histogram(name string) []float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cp := make([]float64, len(r.histograms[name]))
	copy(cp, r.histograms[name])
	return cp
}

// NoopRecorder silently discards all metrics.
type NoopRecorder struct{}

// NewNoop returns a shared no-op recorder.
func NewNoop() NoopRecorder { return NoopRecorder{} }

func (NoopRecorder) IncCounter(string, map[string]string)                {}
func (NoopRecorder) AddCounter(string, float64, map[string]string)       {}
func (NoopRecorder) SetGauge(string, float64, map[string]string)         {}
func (NoopRecorder) ObserveHistogram(string, float64, map[string]string) {}

// ObserveDuration is a convenience helper that records a duration histogram in
// milliseconds and returns the elapsed time.
func ObserveDuration(rec Recorder, name string, start time.Time, labels map[string]string) time.Duration {
	d := time.Since(start)
	rec.ObserveHistogram(name, float64(d.Microseconds())/1000.0, labels)
	return d
}

var (
	_ Recorder = (*InMemoryRecorder)(nil)
	_ Recorder = NoopRecorder{}
)
