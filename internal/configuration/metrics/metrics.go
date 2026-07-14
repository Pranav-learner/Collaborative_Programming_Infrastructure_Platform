// Package metrics provides observable counters for configuration subsystems.
package metrics

import "sync"

// Recorder is the metrics interface for the configuration platform.
type Recorder interface {
	Inc(name string)
	Set(name string, value float64)
}

// InMemoryRecorder stores metrics in memory for testing and dev.
type InMemoryRecorder struct {
	mu       sync.RWMutex
	counters map[string]float64
}

func NewInMemoryRecorder() *InMemoryRecorder {
	return &InMemoryRecorder{counters: make(map[string]float64)}
}

func (r *InMemoryRecorder) Inc(name string) {
	r.mu.Lock()
	r.counters[name]++
	r.mu.Unlock()
}

func (r *InMemoryRecorder) Set(name string, value float64) {
	r.mu.Lock()
	r.counters[name] = value
	r.mu.Unlock()
}

func (r *InMemoryRecorder) Get(name string) float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.counters[name]
}

// NoopRecorder discards all metrics.
type NoopRecorder struct{}

func (NoopRecorder) Inc(string)          {}
func (NoopRecorder) Set(string, float64) {}

// Metric names used across the configuration platform.
const (
	MetricConfigLoads          = "configuration.loads"
	MetricConfigReloads        = "configuration.reloads"
	MetricConfigValidations    = "configuration.validations"
	MetricConfigRollbacks      = "configuration.rollbacks"
	MetricSecretLookups        = "configuration.secret.lookups"
	MetricSecretRotations      = "configuration.secret.rotations"
	MetricFeatureFlagEvals     = "configuration.featureflag.evaluations"
	MetricProviderErrors       = "configuration.provider.errors"
	MetricActiveProviders      = "configuration.providers.active"
	MetricCurrentVersion       = "configuration.version.current"
)
