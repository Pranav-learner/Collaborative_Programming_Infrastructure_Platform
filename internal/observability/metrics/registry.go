package metrics

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"cpip/internal/observability/config"
	"cpip/internal/observability/registry"
)

// ErrKindConflict indicates a metric was requested with a kind different from the
// one it was first registered with.
var ErrKindConflict = errors.New("observability/metrics: metric kind conflict")

// metric is a registered metric and all its label series.
type metric struct {
	def   Def
	quant []float64 // summary quantiles (resolved)
	winSz int       // summary window (resolved)

	mu          sync.RWMutex
	seriesByKey map[string]*series
}

func newMetric(def Def) *metric {
	def.Buckets = sortedFloats(def.Buckets)
	def.Objectives = sortedFloats(def.Objectives)
	return &metric{
		def:         def,
		quant:       def.Objectives,
		seriesByKey: make(map[string]*series),
	}
}

// seriesFor resolves or creates the series for a label combination.
func (m *metric) seriesFor(labels Labels) *series {
	key := seriesKey(m.def.Labels, labels)
	m.mu.RLock()
	s, ok := m.seriesByKey[key]
	m.mu.RUnlock()
	if ok {
		return s
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.seriesByKey[key]; ok {
		return s
	}
	s = m.newSeries(labels)
	m.seriesByKey[key] = s
	return s
}

func (m *metric) newSeries(labels Labels) *series {
	s := &series{labels: normalizeLabels(m.def.Labels, labels)}
	switch m.def.Kind {
	case KindHistogram:
		s.bounds = m.def.Buckets
		s.bucketHits = make([]atomic.Uint64, len(s.bounds))
	case KindSummary:
		s.windowSz = m.winSz
		s.quants = m.quant
	}
	return s
}

func (m *metric) gather() Family {
	m.mu.RLock()
	all := make([]*series, 0, len(m.seriesByKey))
	for _, s := range m.seriesByKey {
		all = append(all, s)
	}
	m.mu.RUnlock()

	fam := Family{Name: m.def.Name, Help: m.def.Help, Kind: m.def.Kind}
	for _, s := range all {
		switch m.def.Kind {
		case KindCounter, KindGauge:
			fam.Samples = append(fam.Samples, Sample{Labels: s.labels, Value: s.value.load()})
		case KindHistogram:
			fam.Samples = append(fam.Samples, s.snapshotHist())
		case KindSummary:
			fam.Samples = append(fam.Samples, s.snapshotSummary())
		}
	}
	sort.Slice(fam.Samples, func(i, j int) bool {
		return seriesKey(m.def.Labels, fam.Samples[i].Labels) < seriesKey(m.def.Labels, fam.Samples[j].Labels)
	})
	return fam
}

// Registry is the concurrent-safe Metrics Registry: it holds registered metrics
// and gathers them for export. It maintains registered metrics, their labels and
// collectors, and produces aggregated statistics.
type Registry struct {
	reg *registry.Registry[*metric]
}

// NewRegistry constructs an empty Metrics Registry.
func NewRegistry() *Registry { return &Registry{reg: registry.New[*metric]()} }

// getOrCreate returns the metric for def, creating it on first use. It errors if
// the name exists with a different kind (registry conflict handling).
func (r *Registry) getOrCreate(def Def) (*metric, error) {
	if existing, ok := r.reg.Get(def.Name); ok {
		if existing.def.Kind != def.Kind {
			return nil, fmt.Errorf("%w: %q is %s not %s", ErrKindConflict, def.Name, existing.def.Kind, def.Kind)
		}
		return existing, nil
	}
	return r.reg.GetOrCreate(def.Name, func() *metric { return newMetric(def) }), nil
}

// Gather snapshots every metric into neutral families for export.
func (r *Registry) Gather() []Family {
	metrics := r.reg.List()
	out := make([]Family, 0, len(metrics))
	for _, m := range metrics {
		out = append(out, m.gather())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Names returns all registered metric names.
func (r *Registry) Names() []string { return r.reg.Names() }

// Len returns the number of registered metrics.
func (r *Registry) Len() int { return r.reg.Len() }

// Meter is the front door business services use to create instruments. It
// resolves configuration defaults (buckets, summary window/quantiles) and
// registers each metric in the Registry, returning the same instrument for
// repeated calls with the same name (idempotent registration).
type Meter struct {
	reg *Registry
	cfg config.Metrics
}

// NewMeter constructs a Meter over a Registry.
func NewMeter(reg *Registry, cfg config.Metrics) *Meter {
	return &Meter{reg: reg, cfg: cfg}
}

// Registry exposes the underlying registry (for exporters/dashboards).
func (m *Meter) Registry() *Registry { return m.reg }

func (m *Meter) resolve(def Def, kind Kind) (*metric, error) {
	def.Kind = kind
	if kind == KindHistogram && len(def.Buckets) == 0 {
		def.Buckets = m.cfg.DefaultBuckets
	}
	if kind == KindSummary {
		if len(def.Objectives) == 0 {
			def.Objectives = m.cfg.SummaryQuantiles
		}
	}
	met, err := m.reg.getOrCreate(def)
	if err != nil {
		return nil, err
	}
	if kind == KindSummary {
		met.winSz = m.cfg.SummaryWindow
	}
	return met, nil
}

// Counter registers/returns a counter. On a kind conflict it returns a no-op
// counter so a misuse never panics a caller on a hot path (the conflict is
// surfaced via TryCounter for callers that want the error).
func (m *Meter) Counter(def Def) Counter {
	c, err := m.TryCounter(def)
	if err != nil {
		return noopCounter{}
	}
	return c
}

// TryCounter is Counter with an explicit error on registry conflict.
func (m *Meter) TryCounter(def Def) (Counter, error) {
	met, err := m.resolve(def, KindCounter)
	if err != nil {
		return nil, err
	}
	return counterHandle{met, met.seriesFor(nil)}, nil
}

// Gauge registers/returns a gauge.
func (m *Meter) Gauge(def Def) Gauge {
	met, err := m.resolve(def, KindGauge)
	if err != nil {
		return noopGauge{}
	}
	return gaugeHandle{met, met.seriesFor(nil)}
}

// Histogram registers/returns a histogram.
func (m *Meter) Histogram(def Def) Histogram {
	met, err := m.resolve(def, KindHistogram)
	if err != nil {
		return noopHist{}
	}
	return histHandle{met, met.seriesFor(nil)}
}

// Summary registers/returns a summary.
func (m *Meter) Summary(def Def) Summary {
	met, err := m.resolve(def, KindSummary)
	if err != nil {
		return noopSummary{}
	}
	return summaryHandle{met, met.seriesFor(nil)}
}

// Timer registers/returns a histogram-backed timer.
func (m *Meter) Timer(def Def) *Timer { return NewTimer(m.Histogram(def)) }

// --- no-op instruments (returned on conflict so hot paths never panic) ---

type noopCounter struct{}

func (noopCounter) Inc()                {}
func (noopCounter) Add(float64)         {}
func (noopCounter) Get() float64        { return 0 }
func (noopCounter) With(Labels) Counter { return noopCounter{} }

type noopGauge struct{}

func (noopGauge) Set(float64)       {}
func (noopGauge) Add(float64)       {}
func (noopGauge) Sub(float64)       {}
func (noopGauge) Inc()              {}
func (noopGauge) Dec()              {}
func (noopGauge) Get() float64      { return 0 }
func (noopGauge) With(Labels) Gauge { return noopGauge{} }

type noopHist struct{}

func (noopHist) Observe(float64)       {}
func (noopHist) With(Labels) Histogram { return noopHist{} }

type noopSummary struct{}

func (noopSummary) Observe(float64)     {}
func (noopSummary) With(Labels) Summary { return noopSummary{} }
