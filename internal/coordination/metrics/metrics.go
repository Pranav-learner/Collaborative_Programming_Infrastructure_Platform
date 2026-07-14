// Package metrics provides observable counters, gauges, and histograms for the
// coordination module. All subsystems record through the Recorder interface, so
// callers can plug in Prometheus, OpenTelemetry, StatsD, or the in-memory
// recorder provided here. There is no global state.
package metrics

import (
	"sync"
	"time"
)

// Recorder is the interface coordination subsystems use to emit metrics.
type Recorder interface {
	IncCounter(name string, labels map[string]string)
	AddCounter(name string, delta float64, labels map[string]string)
	SetGauge(name string, value float64, labels map[string]string)
	ObserveHistogram(name string, value float64, labels map[string]string)
}

// Metric names emitted across the module. Centralizing them avoids typos and lets
// dashboards be defined once.
const (
	// Cluster / membership
	MetricNodesTotal         = "coord.nodes.total"       // gauge
	MetricNodesActive        = "coord.nodes.active"      // gauge
	MetricNodesSchedulable   = "coord.nodes.schedulable" // gauge
	MetricNodeJoined         = "coord.node.joined"
	MetricNodeLeft           = "coord.node.left"
	MetricNodeReconnected    = "coord.node.reconnected"
	MetricMembershipChange   = "coord.membership.changed"
	MetricMembershipConflict = "coord.membership.conflict"

	// Heartbeat
	MetricHeartbeatSent     = "coord.heartbeat.sent"
	MetricHeartbeatReceived = "coord.heartbeat.received"
	MetricHeartbeatExpired  = "coord.heartbeat.expired"
	MetricHeartbeatLatency  = "coord.heartbeat.latency_ms"

	// Leader
	MetricLeaderElected  = "coord.leader.elected"
	MetricLeaderLost     = "coord.leader.lost"
	MetricLeaderRenewed  = "coord.leader.renewed"
	MetricLeaderIsLeader = "coord.leader.is_leader" // gauge (1/0)
	MetricLeaderCampaign = "coord.leader.campaign"

	// Locks
	MetricLockAcquired  = "coord.lock.acquired"
	MetricLockContended = "coord.lock.contended"
	MetricLockReleased  = "coord.lock.released"
	MetricLockRenewed   = "coord.lock.renewed"
	MetricLockExpired   = "coord.lock.expired"
	MetricLockHeld      = "coord.lock.held" // gauge
	MetricLockWaitMs    = "coord.lock.wait_ms"

	// Discovery
	MetricDiscoveryQuery   = "coord.discovery.query"
	MetricDiscoveryHit     = "coord.discovery.hit"
	MetricDiscoveryMiss    = "coord.discovery.miss"
	MetricDiscoveryLatency = "coord.discovery.latency_ms"

	// Replication
	MetricReplicationPublished = "coord.replication.published"
	MetricReplicationApplied   = "coord.replication.applied"
	MetricReplicationDropped   = "coord.replication.dropped"
	MetricReplicationFailed    = "coord.replication.failed"

	// Backend
	MetricBackendError = "coord.backend.error"
	MetricBackendUp    = "coord.backend.up" // gauge (1/0)
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

// ObserveDuration records a duration histogram in milliseconds and returns the
// elapsed time — the canonical timing helper.
func ObserveDuration(rec Recorder, name string, start time.Time, labels map[string]string) time.Duration {
	d := time.Since(start)
	if rec != nil {
		rec.ObserveHistogram(name, float64(d.Microseconds())/1000.0, labels)
	}
	return d
}

var (
	_ Recorder = (*InMemoryRecorder)(nil)
	_ Recorder = NoopRecorder{}
)
