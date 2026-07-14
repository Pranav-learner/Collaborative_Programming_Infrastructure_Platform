package exporters

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"cpip/internal/observability/config"
	"cpip/internal/observability/events"
	"cpip/internal/observability/logger"
	"cpip/internal/observability/logging"
	"cpip/internal/observability/metrics"
	"cpip/internal/observability/registry"
	"cpip/internal/observability/tracing"
)

// Manager is the Exporter Framework's fan-out hub. It receives the platform's
// signals (as a logging.Sink and a tracing.SpanExporter, plus a periodic metrics
// push), buffers logs and spans in bounded async queues, and dispatches batches
// to every registered exporter with per-exporter error isolation. It observes
// ITSELF: throughput, drops, queue depth, and per-exporter health are recorded as
// metrics in the very registry it exports.
type Manager struct {
	cfg    config.Exporters
	bus    *events.Bus
	log    *logger.Logger
	meter  *metrics.Meter
	gather func() []metrics.Family

	reg      *registry.Registry[Exporter]
	statesMu sync.RWMutex
	states   map[string]*exporterState

	logBatch  *batcher[logging.Record]
	spanBatch *batcher[tracing.SpanData]

	cancel  context.CancelFunc
	done    chan struct{}
	started atomic.Bool

	// self-metrics
	mExported metrics.Counter
	mErrors   metrics.Counter
	mDropped  metrics.Counter
	mQueue    metrics.Gauge
	mUp       metrics.Gauge
}

type exporterState struct {
	exported atomic.Int64
	errors   atomic.Int64
	dropped  atomic.Int64
	healthy  atomic.Bool
	lastErr  atomic.Pointer[string]
	lastAt   atomic.Int64 // unix nano
}

// Params configures a Manager.
type Params struct {
	Config config.Exporters
	Events *events.Bus
	Logger *logger.Logger
	// Meter registers the manager's self-observability metrics.
	Meter *metrics.Meter
	// Gather pulls metric families for the periodic push (metrics.Registry.Gather).
	Gather func() []metrics.Family
}

// NewManager constructs an Exporter Manager and its self-metrics.
func NewManager(p Params) *Manager {
	m := &Manager{
		cfg:    p.Config,
		bus:    p.Events,
		log:    p.Logger.With("subsystem", "exporters"),
		meter:  p.Meter,
		gather: p.Gather,
		reg:    registry.New[Exporter](),
		states: make(map[string]*exporterState),
	}
	if m.meter != nil {
		m.mExported = m.meter.Counter(metrics.Def{Name: "obs_exporter_exported_total", Help: "signals exported", Labels: []string{"exporter", "signal"}})
		m.mErrors = m.meter.Counter(metrics.Def{Name: "obs_exporter_errors_total", Help: "exporter errors", Labels: []string{"exporter"}})
		m.mDropped = m.meter.Counter(metrics.Def{Name: "obs_exporter_dropped_total", Help: "dropped signals (queue overflow)", Labels: []string{"signal"}})
		m.mQueue = m.meter.Gauge(metrics.Def{Name: "obs_exporter_queue_size", Help: "async queue depth", Labels: []string{"signal"}})
		m.mUp = m.meter.Gauge(metrics.Def{Name: "obs_exporter_up", Help: "exporter healthy (1/0)", Labels: []string{"exporter"}})
	} else {
		m.mExported, m.mErrors, m.mDropped = noCounter{}, noCounter{}, noCounter{}
		m.mQueue, m.mUp = noGauge{}, noGauge{}
	}

	m.logBatch = newBatcher(p.Config.QueueSize, p.Config.BatchSize, p.Config.FlushInterval,
		func(batch []logging.Record) { m.fanLogs(batch) },
		func() { m.mDropped.With(metrics.Labels{"signal": "logs"}).Inc(); m.emitDropped("logs") },
		func(n int) { m.mQueue.With(metrics.Labels{"signal": "logs"}).Set(float64(n)) },
	)
	m.spanBatch = newBatcher(p.Config.QueueSize, p.Config.BatchSize, p.Config.FlushInterval,
		func(batch []tracing.SpanData) { m.fanSpans(batch) },
		func() { m.mDropped.With(metrics.Labels{"signal": "spans"}).Inc(); m.emitDropped("spans") },
		func(n int) { m.mQueue.With(metrics.Labels{"signal": "spans"}).Set(float64(n)) },
	)
	return m
}

// Register adds an exporter (idempotent by name) and emits ExporterRegistered.
func (m *Manager) Register(e Exporter) error {
	if err := m.reg.Register(e.Name(), e); err != nil {
		return err
	}
	st := &exporterState{}
	st.healthy.Store(true)
	m.statesMu.Lock()
	m.states[e.Name()] = st
	m.statesMu.Unlock()
	m.mUp.With(metrics.Labels{"exporter": e.Name()}).Set(1)
	m.bus.Emit(events.ExporterRegistered, "exporters", func(ev *events.Event) { ev.Name = e.Name() })
	return nil
}

// Exporters returns the registered exporters.
func (m *Manager) Exporters() []Exporter { return m.reg.List() }

// Start launches the batch workers and the periodic metrics push loop.
func (m *Manager) Start(ctx context.Context) {
	if m.started.Swap(true) {
		return
	}
	m.logBatch.start()
	m.spanBatch.start()

	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	m.cancel = cancel
	m.done = done
	go func() {
		defer close(done)
		ticker := time.NewTicker(m.cfg.MetricsPushInterval)
		defer ticker.Stop()
		for {
			select {
			case <-loopCtx.Done():
				return
			case <-ticker.C:
				m.PushMetrics(loopCtx)
			}
		}
	}()
}

// PushMetrics gathers and fans metric families to every exporter now.
func (m *Manager) PushMetrics(ctx context.Context) {
	if m.gather == nil {
		return
	}
	m.fanMetrics(ctx, m.gather())
}

// --- logging.Sink implementation ---

// Name identifies the manager as a log sink.
func (m *Manager) Name() string { return "exporters" }

// Write enqueues a log record for async export.
func (m *Manager) Write(r logging.Record) { m.logBatch.add(r) }

// Flush blocks until queued logs and spans are dispatched.
func (m *Manager) Flush() error {
	m.logBatch.flushNow()
	m.spanBatch.flushNow()
	return nil
}

// Close flushes and shuts the manager down.
func (m *Manager) Close() error { return m.Shutdown(context.Background()) }

// --- tracing.SpanExporter implementation ---

// ExportSpans enqueues completed spans for async export.
func (m *Manager) ExportSpans(spans []tracing.SpanData) {
	for _, s := range spans {
		m.spanBatch.add(s)
	}
}

// --- fan-out with per-exporter isolation ---

func (m *Manager) fanLogs(batch []logging.Record) {
	ctx := context.Background()
	for _, e := range m.reg.List() {
		m.dispatch(e.Name(), "logs", len(batch), func() error { return e.ExportLogs(ctx, batch) })
	}
}

func (m *Manager) fanSpans(batch []tracing.SpanData) {
	ctx := context.Background()
	for _, e := range m.reg.List() {
		m.dispatch(e.Name(), "spans", len(batch), func() error { return e.ExportSpans(ctx, batch) })
	}
}

func (m *Manager) fanMetrics(ctx context.Context, families []metrics.Family) {
	for _, e := range m.reg.List() {
		m.dispatch(e.Name(), "metrics", len(families), func() error { return e.ExportMetrics(ctx, families) })
	}
}

// dispatch runs one export against one exporter, isolating and accounting errors.
func (m *Manager) dispatch(name, signal string, n int, fn func() error) {
	st := m.state(name)
	err := fn()
	if errors.Is(err, ErrUnsupported) {
		return // benign: this exporter doesn't handle this signal
	}
	if err != nil {
		st.errors.Add(1)
		s := err.Error()
		st.lastErr.Store(&s)
		st.healthy.Store(false)
		m.mErrors.With(metrics.Labels{"exporter": name}).Inc()
		m.mUp.With(metrics.Labels{"exporter": name}).Set(0)
		m.log.ExporterError(context.Background(), name, "export_"+signal, err)
		m.bus.Emit(events.ExporterFailed, "exporters", func(ev *events.Event) {
			ev.Name = name
			ev.Payload = map[string]any{"signal": signal, "error": s}
		})
		return
	}
	st.exported.Add(int64(n))
	st.lastAt.Store(time.Now().UnixNano())
	if !st.healthy.Swap(true) {
		m.mUp.With(metrics.Labels{"exporter": name}).Set(1)
	}
	m.mExported.With(metrics.Labels{"exporter": name, "signal": signal}).Add(float64(n))
}

func (m *Manager) state(name string) *exporterState {
	m.statesMu.RLock()
	st := m.states[name]
	m.statesMu.RUnlock()
	if st == nil {
		st = &exporterState{}
		st.healthy.Store(true)
		m.statesMu.Lock()
		m.states[name] = st
		m.statesMu.Unlock()
	}
	return st
}

func (m *Manager) emitDropped(signal string) {
	m.bus.Emit(events.EventsDropped, "exporters", func(ev *events.Event) {
		ev.Payload = map[string]any{"signal": signal}
	})
}

// Health is a per-exporter health snapshot for self-observability.
type Health struct {
	Name     string `json:"name"`
	Healthy  bool   `json:"healthy"`
	Exported int64  `json:"exported"`
	Errors   int64  `json:"errors"`
	LastErr  string `json:"last_error,omitempty"`
	LastAt   string `json:"last_export,omitempty"`
}

// Health returns each exporter's health.
func (m *Manager) Health() []Health {
	var out []Health
	for _, name := range m.reg.Names() {
		st := m.state(name)
		h := Health{Name: name, Healthy: st.healthy.Load(), Exported: st.exported.Load(), Errors: st.errors.Load()}
		if p := st.lastErr.Load(); p != nil {
			h.LastErr = *p
		}
		if ns := st.lastAt.Load(); ns > 0 {
			h.LastAt = time.Unix(0, ns).UTC().Format(time.RFC3339Nano)
		}
		out = append(out, h)
	}
	return out
}

// QueueDepth reports the current async queue depths (logs, spans).
func (m *Manager) QueueDepth() (logs, spans int) {
	return m.logBatch.depth(), m.spanBatch.depth()
}

// Shutdown drains queues, flushes exporters, and stops loops.
func (m *Manager) Shutdown(ctx context.Context) error {
	if m.cancel != nil {
		m.cancel()
	}
	if m.done != nil {
		<-m.done
	}
	m.logBatch.shutdown()
	m.spanBatch.shutdown()
	var firstErr error
	for _, e := range m.reg.List() {
		if err := e.Shutdown(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// --- no-op self-metrics (when no meter is provided) ---

type noCounter struct{}

func (noCounter) Inc()                                {}
func (noCounter) Add(float64)                         {}
func (noCounter) Get() float64                        { return 0 }
func (noCounter) With(metrics.Labels) metrics.Counter { return noCounter{} }

type noGauge struct{}

func (noGauge) Set(float64)                       {}
func (noGauge) Add(float64)                       {}
func (noGauge) Sub(float64)                       {}
func (noGauge) Inc()                              {}
func (noGauge) Dec()                              {}
func (noGauge) Get() float64                      { return 0 }
func (noGauge) With(metrics.Labels) metrics.Gauge { return noGauge{} }

var (
	_ logging.Sink         = (*Manager)(nil)
	_ tracing.SpanExporter = (*Manager)(nil)
)
