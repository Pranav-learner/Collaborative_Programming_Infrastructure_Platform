package logging

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"cpip/internal/observability/correlation"
	"cpip/internal/observability/registry"
)

// Logger is the interface business services depend on. It is context-aware:
// every emission pulls correlation IDs from ctx automatically, so callers never
// thread IDs manually.
type Logger interface {
	Debug(ctx context.Context, msg string, fields ...Field)
	Info(ctx context.Context, msg string, fields ...Field)
	Warn(ctx context.Context, msg string, fields ...Field)
	Error(ctx context.Context, msg string, fields ...Field)
	// Log emits at an explicit level.
	Log(ctx context.Context, level Level, msg string, fields ...Field)
	// With returns a child logger that always includes fields.
	With(fields ...Field) Logger
	// WithComponent returns a child logger tagged with a component label.
	WithComponent(component string) Logger
	// Enabled reports whether a level would be emitted (cheap guard for hot paths).
	Enabled(level Level) bool
}

// EmitHook is invoked for every emitted (post-sampling) record. The telemetry
// provider uses it to bump throughput metrics without coupling logging to metrics.
type EmitHook func(Record)

// Manager owns the sinks, level, sampler, and logger registry. It is the Logging
// Registry of the objectives: it maintains registered loggers and sinks with
// concurrent-safe operations.
type Manager struct {
	level   atomic.Int32 // Level
	sampler Sampler

	mu    sync.RWMutex
	sinks []Sink

	sinkReg   *registry.Registry[Sink]
	loggerReg *registry.Registry[Logger]

	onEmit  atomic.Pointer[EmitHook]
	dropped atomic.Int64
}

// Params configures a Manager.
type Params struct {
	Level   Level
	Sampler Sampler
	// Sinks are the initial destinations (e.g. a stdout WriterSink).
	Sinks []Sink
}

// NewManager constructs a logging Manager.
func NewManager(p Params) *Manager {
	m := &Manager{
		sampler:   p.Sampler,
		sinkReg:   registry.New[Sink](),
		loggerReg: registry.New[Logger](),
	}
	if m.sampler == nil {
		m.sampler = AllSampler{}
	}
	m.level.Store(int32(p.Level))
	for _, s := range p.Sinks {
		_ = m.AddSink(s)
	}
	return m
}

// NewDefault returns a Manager writing JSON to stdout at info level — the
// zero-config logger for bootstrapping.
func NewDefault() *Manager {
	return NewManager(Params{Level: LevelInfo, Sinks: []Sink{NewWriterSink("stdout", os.Stdout, true)}})
}

// SetEmitHook installs the post-sampling emit hook (nil clears it).
func (m *Manager) SetEmitHook(h EmitHook) {
	if h == nil {
		m.onEmit.Store(nil)
		return
	}
	m.onEmit.Store(&h)
}

// SetLevel changes the minimum emitted level at runtime.
func (m *Manager) SetLevel(l Level) { m.level.Store(int32(l)) }

// Level returns the current minimum level.
func (m *Manager) Level() Level { return Level(m.level.Load()) }

// AddSink registers a sink (idempotent by name).
func (m *Manager) AddSink(s Sink) error {
	if err := m.sinkReg.Register(s.Name(), s); err != nil {
		return err
	}
	m.mu.Lock()
	m.sinks = append(m.sinks, s)
	m.mu.Unlock()
	return nil
}

// Sinks returns the current sink set.
func (m *Manager) Sinks() []Sink {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]Sink(nil), m.sinks...)
}

// Dropped returns the number of records dropped by a sink-less manager (defensive
// self-observability; normally zero).
func (m *Manager) Dropped() int64 { return m.dropped.Load() }

// Logger returns the root logger.
func (m *Manager) Logger() Logger { return &logger{mgr: m} }

// NamedLogger returns (registering on first use) a named logger tagged with a
// component label — the entry the Logging Registry hands back consistently.
func (m *Manager) NamedLogger(name string) Logger {
	return m.loggerReg.GetOrCreate(name, func() Logger {
		return &logger{mgr: m, component: name}
	})
}

// Flush flushes every sink.
func (m *Manager) Flush() error {
	var firstErr error
	for _, s := range m.Sinks() {
		if err := s.Flush(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Close flushes and closes every sink.
func (m *Manager) Close() error {
	var firstErr error
	for _, s := range m.Sinks() {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// emit is the single fan-out point: it filters by level, applies sampling,
// enriches with correlation IDs, and writes to every sink.
func (m *Manager) emit(ctx context.Context, level Level, component, msg string, fields []Field) {
	if level < m.Level() {
		return
	}
	rec := Record{
		Time:      time.Now(),
		Level:     level,
		Message:   msg,
		Component: component,
		Fields:    fields,
		IDs:       correlation.From(ctx),
	}
	if !m.sampler.Sample(rec) {
		return
	}

	m.mu.RLock()
	sinks := m.sinks
	m.mu.RUnlock()
	if len(sinks) == 0 {
		m.dropped.Add(1)
	}
	for _, s := range sinks {
		s.Write(rec)
	}
	if h := m.onEmit.Load(); h != nil {
		(*h)(rec)
	}
}

// logger is the concrete Logger bound to a Manager, optional component, and
// preset fields.
type logger struct {
	mgr       *Manager
	component string
	fields    []Field
}

func (l *logger) Enabled(level Level) bool { return level >= l.mgr.Level() }

func (l *logger) Debug(ctx context.Context, msg string, fields ...Field) {
	l.Log(ctx, LevelDebug, msg, fields...)
}
func (l *logger) Info(ctx context.Context, msg string, fields ...Field) {
	l.Log(ctx, LevelInfo, msg, fields...)
}
func (l *logger) Warn(ctx context.Context, msg string, fields ...Field) {
	l.Log(ctx, LevelWarn, msg, fields...)
}
func (l *logger) Error(ctx context.Context, msg string, fields ...Field) {
	l.Log(ctx, LevelError, msg, fields...)
}

func (l *logger) Log(ctx context.Context, level Level, msg string, fields ...Field) {
	if level < l.mgr.Level() {
		return
	}
	merged := fields
	if len(l.fields) > 0 {
		merged = make([]Field, 0, len(l.fields)+len(fields))
		merged = append(merged, l.fields...)
		merged = append(merged, fields...)
	}
	l.mgr.emit(ctx, level, l.component, msg, merged)
}

func (l *logger) With(fields ...Field) Logger {
	nf := make([]Field, 0, len(l.fields)+len(fields))
	nf = append(nf, l.fields...)
	nf = append(nf, fields...)
	return &logger{mgr: l.mgr, component: l.component, fields: nf}
}

func (l *logger) WithComponent(component string) Logger {
	return &logger{mgr: l.mgr, component: component, fields: l.fields}
}

var _ Logger = (*logger)(nil)
