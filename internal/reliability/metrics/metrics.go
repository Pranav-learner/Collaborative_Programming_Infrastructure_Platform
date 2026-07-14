package metrics

import "sync"

const (
	MetricRetryAttempts      = "reliability.retry.attempts"
	MetricRetryExhaustion    = "reliability.retry.exhausted"
	MetricCircuitBreakerTrips = "reliability.circuitbreaker.trips"
	MetricCircuitState       = "reliability.circuitbreaker.state" // 0: Closed, 1: Half-Open, 2: Open
	MetricBulkheadActive     = "reliability.bulkhead.active"
	MetricBulkheadRejections = "reliability.bulkhead.rejections"
	MetricRateLimitRejections = "reliability.ratelimit.rejections"
	MetricBackpressureSheds  = "reliability.backpressure.sheds"
	MetricBackupRuns         = "reliability.backup.runs"
	MetricBackupFailures     = "reliability.backup.failures"
	MetricRecoveryRuns       = "reliability.recovery.runs"
	MetricRecoveryFailures   = "reliability.recovery.failures"
)

// Recorder defines the telemetry collection interface.
type Recorder interface {
	Inc(name string)
	Dec(name string)
	Add(name string, value float64)
	Set(name string, value float64)
	Get(name string) float64
}

// InMemoryRecorder records statistics thread-safely.
type InMemoryRecorder struct {
	mu     sync.RWMutex
	values map[string]float64
}

// NewInMemoryRecorder creates a new InMemoryRecorder.
func NewInMemoryRecorder() *InMemoryRecorder {
	return &InMemoryRecorder{
		values: make(map[string]float64),
	}
}

// Inc increments a metric.
func (r *InMemoryRecorder) Inc(name string) {
	r.Add(name, 1.0)
}

// Dec decrements a metric.
func (r *InMemoryRecorder) Dec(name string) {
	r.Add(name, -1.0)
}

// Add adds value.
func (r *InMemoryRecorder) Add(name string, value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.values[name] += value
}

// Set sets the exact value.
func (r *InMemoryRecorder) Set(name string, value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.values[name] = value
}

// Get returns the metric value.
func (r *InMemoryRecorder) Get(name string) float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.values[name]
}
