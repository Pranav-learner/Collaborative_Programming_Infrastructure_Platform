// Package dashboard implements the Dashboard Aggregation Layer: it rolls the
// platform's raw metric families, health snapshot, trace statistics, and exporter
// health into unified, subsystem-oriented dashboard models. It is the read model
// a future UI / REST API renders — it does NOT draw Grafana panels (that is a
// deployment concern); it produces the vendor-neutral data those panels bind to.
//
// Metrics are grouped into the platform's subsystems (runtime, execution,
// sandbox, database, redis, storage, cluster, application) by name prefix, so a
// single Build call yields one coherent picture of the whole system.
package dashboard

import (
	"context"
	"sort"
	"strings"
	"time"

	"cpip/internal/observability/config"
	"cpip/internal/observability/events"
	"cpip/internal/observability/exporters"
	"cpip/internal/observability/health"
	"cpip/internal/observability/metrics"
	"cpip/internal/observability/tracing"
)

// Panel is one metric series rendered for display.
type Panel struct {
	Metric string         `json:"metric"`
	Labels metrics.Labels `json:"labels,omitempty"`
	Kind   metrics.Kind   `json:"kind"`
	Value  float64        `json:"value,omitempty"`
	Count  uint64         `json:"count,omitempty"`
	Sum    float64        `json:"sum,omitempty"`
	Help   string         `json:"help,omitempty"`
}

// Section groups panels for one subsystem.
type Section struct {
	Key    string  `json:"key"`
	Title  string  `json:"title"`
	Panels []Panel `json:"panels"`
}

// Dashboard is the unified, point-in-time view of the platform.
type Dashboard struct {
	ServiceName string             `json:"service_name"`
	Environment string             `json:"environment"`
	GeneratedAt time.Time          `json:"generated_at"`
	Health      health.Snapshot    `json:"health"`
	Traces      tracing.Stats      `json:"traces"`
	Exporters   []exporters.Health `json:"exporters"`
	Sections    []Section          `json:"sections"`
}

// subsystem maps a display key to its metric-name prefixes.
type subsystem struct {
	key      string
	title    string
	prefixes []string
}

// defaultSubsystems is the canonical grouping across CPIP modules.
var defaultSubsystems = []subsystem{
	{"runtime", "Runtime", []string{"runtime.", "runtime_"}},
	{"execution", "Execution", []string{"execution.", "exec.", "job.", "queue."}},
	{"sandbox", "Sandbox", []string{"sandbox.", "sbx."}},
	{"database", "Database", []string{"db.", "persistence.", "postgres."}},
	{"redis", "Redis / Cache", []string{"cache.", "redis."}},
	{"storage", "Object Storage", []string{"storage."}},
	{"cluster", "Cluster / Coordination", []string{"coord.", "cluster."}},
	{"application", "Application", []string{"app.", "http."}},
	{"observability", "Observability (self)", []string{"obs."}},
}

// Builder assembles dashboards from the platform's telemetry sources. All sources
// are read-only; the builder never mutates them.
type Builder struct {
	cfg    config.Config
	gather func() []metrics.Family
	health *health.Registry
	traces *tracing.Registry
	exp    *exporters.Manager
	bus    *events.Bus
	subs   []subsystem

	cancel context.CancelFunc
	done   chan struct{}
}

// Params configures a Builder.
type Params struct {
	Config    config.Config
	Gather    func() []metrics.Family
	Health    *health.Registry
	Traces    *tracing.Registry
	Exporters *exporters.Manager
	Events    *events.Bus
}

// New constructs a dashboard Builder.
func New(p Params) *Builder {
	return &Builder{
		cfg:    p.Config,
		gather: p.Gather,
		health: p.Health,
		traces: p.Traces,
		exp:    p.Exporters,
		bus:    p.Events,
		subs:   defaultSubsystems,
	}
}

// Build produces a fresh dashboard snapshot.
func (b *Builder) Build(ctx context.Context) Dashboard {
	d := Dashboard{
		ServiceName: b.cfg.ServiceName,
		Environment: b.cfg.Environment,
		GeneratedAt: time.Now().UTC(),
	}
	if b.health != nil {
		d.Health = b.health.CheckAll(ctx)
	}
	if b.traces != nil {
		d.Traces = b.traces.Stats()
	}
	if b.exp != nil {
		d.Exporters = b.exp.Health()
	}

	// Group metric families into subsystem sections.
	buckets := make(map[string][]Panel, len(b.subs))
	var families []metrics.Family
	if b.gather != nil {
		families = b.gather()
	}
	for _, f := range families {
		key := b.classify(f.Name)
		for _, s := range f.Samples {
			buckets[key] = append(buckets[key], Panel{
				Metric: f.Name, Labels: s.Labels, Kind: f.Kind,
				Value: s.Value, Count: s.Count, Sum: s.Sum, Help: f.Help,
			})
		}
	}
	for _, sub := range b.subs {
		panels := buckets[sub.key]
		if len(panels) == 0 {
			continue
		}
		sort.Slice(panels, func(i, j int) bool { return panels[i].Metric < panels[j].Metric })
		d.Sections = append(d.Sections, Section{Key: sub.key, Title: sub.title, Panels: panels})
	}
	return d
}

// classify assigns a metric name to a subsystem key (default "application").
func (b *Builder) classify(name string) string {
	for _, sub := range b.subs {
		for _, p := range sub.prefixes {
			if strings.HasPrefix(name, p) {
				return sub.key
			}
		}
	}
	return "application"
}

// Start launches a background refresh loop that rebuilds the dashboard and emits
// DashboardUpdated so subscribers (a future UI/API) can react.
func (b *Builder) Start(ctx context.Context) {
	if b.done != nil {
		return
	}
	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	b.cancel = cancel
	b.done = done
	go func() {
		defer close(done)
		ticker := time.NewTicker(b.cfg.Dashboard.RefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-loopCtx.Done():
				return
			case <-ticker.C:
				d := b.Build(loopCtx)
				b.bus.Emit(events.DashboardUpdated, "dashboard", func(e *events.Event) {
					e.Payload = map[string]any{"sections": len(d.Sections), "status": string(d.Health.Status)}
				})
			}
		}
	}()
}

// Stop halts the refresh loop.
func (b *Builder) Stop() {
	cancel, done := b.cancel, b.done
	b.cancel = nil
	b.done = nil
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}
