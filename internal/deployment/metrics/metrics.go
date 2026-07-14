package metrics

import "sync"

const (
	MetricDeployAttempts   = "deployment.attempts.total"
	MetricDeploySuccesses  = "deployment.successes.total"
	MetricDeployFailures   = "deployment.failures.total"
	MetricRollbackRuns     = "deployment.rollbacks.total"
	MetricRollbackFailures = "deployment.rollbacks.failed"
	MetricValidationRuns   = "deployment.validations.total"
	MetricValidationErrors = "deployment.validations.errors"
	MetricProbeHealthy     = "deployment.probes.healthy"
	MetricProbeUnhealthy   = "deployment.probes.unhealthy"
)

// Recorder defines the telemetry collection interface.
type Recorder interface {
	Inc(name string)
	Dec(name string)
	Add(name string, value float64)
	Set(name string, value float64)
	Get(name string) float64
}

// InMemoryRecorder records statistics using sync.Map.
type InMemoryRecorder struct {
	mu     sync.RWMutex
	values map[string]float64
}

// NewInMemoryRecorder creates a thread-safe in-memory recorder.
func NewInMemoryRecorder() *InMemoryRecorder {
	return &InMemoryRecorder{
		values: make(map[string]float64),
	}
}

// Inc increments a metric by 1.
func (r *InMemoryRecorder) Inc(name string) {
	r.Add(name, 1.0)
}

// Dec decrements a metric by 1.
func (r *InMemoryRecorder) Dec(name string) {
	r.Add(name, -1.0)
}

// Add adds a float value to a metric.
func (r *InMemoryRecorder) Add(name string, value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.values[name] += value
}

// Set sets the exact value of a metric.
func (r *InMemoryRecorder) Set(name string, value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.values[name] = value
}

// Get returns the current metric value.
func (r *InMemoryRecorder) Get(name string) float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.values[name]
}
