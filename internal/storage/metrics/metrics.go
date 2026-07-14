// Package metrics provides observable counters, gauges, and histograms for the
// object storage & artifact management module. All subsystems record through the
// Recorder interface, so callers can plug in Prometheus, OpenTelemetry, StatsD,
// or the in-memory recorder provided here. There is no global state.
package metrics

import (
	"sync"
	"time"
)

// Recorder is the interface storage subsystems use to emit metrics. It mirrors
// the cache module's Recorder so a single adapter can bridge both to a real TSDB.
type Recorder interface {
	IncCounter(name string, labels map[string]string)
	AddCounter(name string, delta float64, labels map[string]string)
	SetGauge(name string, value float64, labels map[string]string)
	ObserveHistogram(name string, value float64, labels map[string]string)
}

// Metric names emitted across the module. Centralizing them avoids typos and
// lets dashboards be defined once.
const (
	// Upload pipeline
	MetricUploadStarted   = "storage.upload.started"
	MetricUploadCompleted = "storage.upload.completed"
	MetricUploadFailed    = "storage.upload.failed"
	MetricUploadBytes     = "storage.upload.bytes"
	MetricUploadDuration  = "storage.upload.duration_ms"
	MetricUploadDeduped   = "storage.upload.deduplicated" // content-address hit

	// Download pipeline
	MetricDownloadStarted   = "storage.download.started"
	MetricDownloadCompleted = "storage.download.completed"
	MetricDownloadFailed    = "storage.download.failed"
	MetricDownloadBytes     = "storage.download.bytes"
	MetricDownloadDuration  = "storage.download.duration_ms"

	// Artifact lifecycle
	MetricArtifactCreated  = "storage.artifact.created"
	MetricArtifactDeleted  = "storage.artifact.deleted"
	MetricArtifactRestored = "storage.artifact.restored"
	MetricArtifactActive   = "storage.artifact.active" // gauge

	// Versioning
	MetricVersionCreated  = "storage.version.created"
	MetricVersionRollback = "storage.version.rollback"
	MetricVersionPruned   = "storage.version.pruned"

	// Content addressing / integrity
	MetricHashComputed      = "storage.hash.computed"
	MetricIntegrityOK       = "storage.integrity.ok"
	MetricIntegrityMismatch = "storage.integrity.mismatch"

	// Compression
	MetricCompressionApplied = "storage.compression.applied"
	MetricCompressionSkipped = "storage.compression.skipped"
	MetricCompressionRatio   = "storage.compression.ratio" // histogram of compressed/original
	MetricCompressionSaved   = "storage.compression.bytes_saved"

	// Retention / cleanup
	MetricRetentionApplied = "storage.retention.applied"
	MetricCleanupScanned   = "storage.cleanup.scanned"
	MetricCleanupDeleted   = "storage.cleanup.deleted"
	MetricCleanupFailed    = "storage.cleanup.failed"
	MetricCleanupDuration  = "storage.cleanup.duration_ms"

	// Backend / SDK
	MetricBackendError = "storage.backend.error"
	MetricBackendUp    = "storage.backend.up" // gauge (1/0)
	MetricSignedURL    = "storage.signed_url.issued"
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
// elapsed time — the canonical timing helper for pipeline stages.
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
