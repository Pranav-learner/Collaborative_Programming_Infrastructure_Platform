// Package config defines the configuration surface for the Observability
// Platform (Stage 5 Module 1). Configuration is injected at construction time;
// there is no global state. Each subsystem receives only the sub-struct it needs.
package config

import (
	"errors"
	"time"
)

// ErrConfig indicates an invalid configuration value.
var ErrConfig = errors.New("observability: invalid configuration")

// Logging configures the logging framework.
type Logging struct {
	// Level is the minimum level emitted ("debug","info","warn","error","fatal").
	Level string `json:"level"`
	// JSON selects JSON encoding for the built-in stdout sink (vs. text).
	JSON bool `json:"json"`
	// SampleInitial logs the first N matching records per second at each level
	// before sampling kicks in (0 disables sampling — log everything).
	SampleInitial int `json:"sample_initial"`
	// SampleThereafter logs every Kth record after the initial burst (0 → 1).
	SampleThereafter int `json:"sample_thereafter"`
	// StdoutSink enables the built-in stdout sink (in addition to exporters).
	StdoutSink bool `json:"stdout_sink"`
}

// Metrics configures the metrics framework.
type Metrics struct {
	// DefaultBuckets are the histogram bucket upper bounds used when a metric
	// definition supplies none (seconds-oriented latency buckets).
	DefaultBuckets []float64 `json:"default_buckets"`
	// SummaryWindow bounds the sliding window a Summary keeps for quantiles.
	SummaryWindow int `json:"summary_window"`
	// SummaryQuantiles are the quantiles reported by summaries (0..1).
	SummaryQuantiles []float64 `json:"summary_quantiles"`
}

// Tracing configures the distributed tracing framework.
type Tracing struct {
	// SampleRatio is the head-based sampling probability [0,1]. 1 = sample all.
	SampleRatio float64 `json:"sample_ratio"`
	// MaxAttributes caps attributes retained per span (overflow is dropped+counted).
	MaxAttributes int `json:"max_attributes"`
	// MaxEvents caps events retained per span.
	MaxEvents int `json:"max_events"`
}

// Health configures the health monitoring framework.
type Health struct {
	// Interval is how often background health checks run.
	Interval time.Duration `json:"interval"`
	// Timeout bounds a single health check.
	Timeout time.Duration `json:"timeout"`
	// CacheTTL bounds how stale a cached health result may be served.
	CacheTTL time.Duration `json:"cache_ttl"`
}

// Exporters configures the exporter framework.
type Exporters struct {
	// Enabled names the exporters to activate ("console","prometheus","otlp","noop").
	Enabled []string `json:"enabled"`
	// QueueSize bounds the async log/span queue (overflow drops + increments a
	// dropped counter — the module's back-pressure story).
	QueueSize int `json:"queue_size"`
	// BatchSize is the max batch flushed to an exporter at once.
	BatchSize int `json:"batch_size"`
	// FlushInterval forces a flush even if a batch is not full.
	FlushInterval time.Duration `json:"flush_interval"`
	// MetricsPushInterval is how often metrics are pushed to push-style exporters.
	MetricsPushInterval time.Duration `json:"metrics_push_interval"`
}

// Dashboard configures the dashboard aggregation layer.
type Dashboard struct {
	RefreshInterval time.Duration `json:"refresh_interval"`
}

// Alerts configures the alert rule framework.
type Alerts struct {
	// EvalInterval is how often alert rules are evaluated.
	EvalInterval time.Duration `json:"eval_interval"`
}

// Config is the composition root of module configuration.
type Config struct {
	// ServiceName / Environment / Version identify this deployment; they become
	// resource attributes on every signal.
	ServiceName string `json:"service_name"`
	Environment string `json:"environment"`
	Version     string `json:"version"`
	// InstanceID identifies this process (defaults to a random id if empty).
	InstanceID string `json:"instance_id"`

	Logging   Logging   `json:"logging"`
	Metrics   Metrics   `json:"metrics"`
	Tracing   Tracing   `json:"tracing"`
	Health    Health    `json:"health"`
	Exporters Exporters `json:"exporters"`
	Dashboard Dashboard `json:"dashboard"`
	Alerts    Alerts    `json:"alerts"`
}

// Default returns a production-sensible configuration: JSON logs at info, ratio
// tracing, the console + prometheus exporters, and the OpenTelemetry-shaped OTLP
// exporter as the default telemetry representation.
func Default() Config {
	return Config{
		ServiceName: "cpip",
		Environment: "development",
		Version:     "0.0.0",
		Logging: Logging{
			Level:            "info",
			JSON:             true,
			SampleInitial:    0,
			SampleThereafter: 100,
			StdoutSink:       true,
		},
		Metrics: Metrics{
			DefaultBuckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
			SummaryWindow:    2048,
			SummaryQuantiles: []float64{0.5, 0.9, 0.95, 0.99},
		},
		Tracing: Tracing{
			SampleRatio:   1.0,
			MaxAttributes: 128,
			MaxEvents:     128,
		},
		Health: Health{
			Interval: 10 * time.Second,
			Timeout:  2 * time.Second,
			CacheTTL: 5 * time.Second,
		},
		Exporters: Exporters{
			// Prometheus (pull) for metrics + OTLP (the OpenTelemetry default) for
			// all signals. Console is opt-in for local dev; human-readable console
			// LOG output is served by the logging stdout sink to avoid duplication.
			Enabled:             []string{"prometheus", "otlp"},
			QueueSize:           8192,
			BatchSize:           512,
			FlushInterval:       5 * time.Second,
			MetricsPushInterval: 15 * time.Second,
		},
		Dashboard: Dashboard{RefreshInterval: 15 * time.Second},
		Alerts:    Alerts{EvalInterval: 15 * time.Second},
	}
}

// Validate normalizes zero-valued fields to defaults and rejects nonsensical
// values, returning a normalized copy.
func (c Config) Validate() (Config, error) {
	d := Default()
	if c.ServiceName == "" {
		c.ServiceName = d.ServiceName
	}
	if c.Environment == "" {
		c.Environment = d.Environment
	}
	if c.Version == "" {
		c.Version = d.Version
	}

	if c.Logging.Level == "" {
		c.Logging.Level = d.Logging.Level
	}
	switch c.Logging.Level {
	case "debug", "info", "warn", "error", "fatal":
	default:
		return Config{}, wrap("logging.level must be one of debug|info|warn|error|fatal")
	}
	if c.Logging.SampleThereafter < 0 {
		return Config{}, wrap("logging.sample_thereafter must be >= 0")
	}

	if len(c.Metrics.DefaultBuckets) == 0 {
		c.Metrics.DefaultBuckets = d.Metrics.DefaultBuckets
	}
	if c.Metrics.SummaryWindow <= 0 {
		c.Metrics.SummaryWindow = d.Metrics.SummaryWindow
	}
	if len(c.Metrics.SummaryQuantiles) == 0 {
		c.Metrics.SummaryQuantiles = d.Metrics.SummaryQuantiles
	}
	for _, q := range c.Metrics.SummaryQuantiles {
		if q < 0 || q > 1 {
			return Config{}, wrap("metrics.summary_quantiles must be within [0,1]")
		}
	}

	if c.Tracing.SampleRatio < 0 || c.Tracing.SampleRatio > 1 {
		return Config{}, wrap("tracing.sample_ratio must be within [0,1]")
	}
	if c.Tracing.MaxAttributes <= 0 {
		c.Tracing.MaxAttributes = d.Tracing.MaxAttributes
	}
	if c.Tracing.MaxEvents <= 0 {
		c.Tracing.MaxEvents = d.Tracing.MaxEvents
	}

	if c.Health.Interval <= 0 {
		c.Health.Interval = d.Health.Interval
	}
	if c.Health.Timeout <= 0 {
		c.Health.Timeout = d.Health.Timeout
	}
	if c.Health.CacheTTL < 0 {
		c.Health.CacheTTL = d.Health.CacheTTL
	}

	if len(c.Exporters.Enabled) == 0 {
		c.Exporters.Enabled = d.Exporters.Enabled
	}
	if c.Exporters.QueueSize <= 0 {
		c.Exporters.QueueSize = d.Exporters.QueueSize
	}
	if c.Exporters.BatchSize <= 0 {
		c.Exporters.BatchSize = d.Exporters.BatchSize
	}
	if c.Exporters.FlushInterval <= 0 {
		c.Exporters.FlushInterval = d.Exporters.FlushInterval
	}
	if c.Exporters.MetricsPushInterval <= 0 {
		c.Exporters.MetricsPushInterval = d.Exporters.MetricsPushInterval
	}

	if c.Dashboard.RefreshInterval <= 0 {
		c.Dashboard.RefreshInterval = d.Dashboard.RefreshInterval
	}
	if c.Alerts.EvalInterval <= 0 {
		c.Alerts.EvalInterval = d.Alerts.EvalInterval
	}
	return c, nil
}

func wrap(msg string) error { return &configError{msg: msg} }

type configError struct{ msg string }

func (e *configError) Error() string        { return "observability/config: " + e.msg }
func (e *configError) Is(target error) bool { return target == ErrConfig }
