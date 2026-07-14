package manager

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	execjob "cpip/internal/execution/job"
	execlang "cpip/internal/execution/language"
	"cpip/internal/runtime/adapter"
	"cpip/internal/runtime/config"
	"cpip/internal/runtime/events"
	"cpip/internal/runtime/metrics"
	"cpip/internal/runtime/pipeline"
	"cpip/internal/runtime/stream"
	"cpip/internal/runtime/types"
)

// Manager is the composition root and public facade of the runtime subsystem.
type Manager struct {
	cfg      config.Config
	adapters *adapter.AdapterRegistry
	langReg  *execlang.Registry
	bus      *events.Bus
	metrics  metrics.Recorder
	log      *slog.Logger

	mu     sync.RWMutex
	active map[string]*pipeline.Pipeline
}

// NewManager creates a new Manager instance.
func NewManager(
	cfg config.Config,
	langReg *execlang.Registry,
	bus *events.Bus,
	rec metrics.Recorder,
	log *slog.Logger,
) *Manager {
	if log == nil {
		log = slog.Default()
	}
	if rec == nil {
		rec = metrics.NoopRecorder{}
	}
	if bus == nil {
		bus = events.NewBus(events.Options{})
	}
	return &Manager{
		cfg:      cfg,
		adapters: adapter.NewAdapterRegistry(),
		langReg:  langReg,
		bus:      bus,
		metrics:  rec,
		log:      log.With("subsystem", "runtime_manager"),
		active:   make(map[string]*pipeline.Pipeline),
	}
}

// ExecuteJob runs a job using the appropriate adapter, streaming output live.
func (m *Manager) ExecuteJob(ctx context.Context, job execjob.Job, workerID string) (types.Session, *stream.StreamManager, error) {
	m.log.Info("starting job execution", "job_id", job.ID, "language", job.Language)

	// Validate language is registered in execution registry
	if m.langReg != nil {
		if _, err := m.langReg.Get(job.Language); err != nil {
			return types.Session{}, nil, fmt.Errorf("%w: language %s not registered", types.ErrInvalidLanguage, job.Language)
		}
	}

	p := pipeline.NewPipeline(m.cfg, m.adapters, m.langReg, m.bus, m.metrics, m.log)

	// Register active pipeline
	m.mu.Lock()
	m.active[job.ID] = p
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		delete(m.active, job.ID)
		m.mu.Unlock()
	}()

	// Run execution pipeline
	sess, err := p.Execute(ctx, job, workerID)
	return sess, p.GetStreamManager(), err
}

// CancelSession cancels a running execution session.
func (m *Manager) CancelSession(jobID string) bool {
	m.mu.RLock()
	p, ok := m.active[jobID]
	m.mu.RUnlock()

	if !ok {
		m.log.Debug("cancellation requested for non-active job", "job_id", jobID)
		return false
	}

	sess := p.GetSession()
	if sess.Cancel != nil {
		m.log.Info("cancelling active session", "job_id", jobID)
		sess.Cancel()
		return true
	}

	return false
}

// GetSession retrieves the session state of a running execution.
func (m *Manager) GetSession(jobID string) (types.Session, bool) {
	m.mu.RLock()
	p, ok := m.active[jobID]
	m.mu.RUnlock()

	if !ok {
		return types.Session{}, false
	}
	return p.GetSession(), true
}

// GetStreamManager retrieves the stream manager of an active session.
func (m *Manager) GetStreamManager(jobID string) (*stream.StreamManager, bool) {
	m.mu.RLock()
	p, ok := m.active[jobID]
	m.mu.RUnlock()

	if !ok {
		return nil, false
	}
	return p.GetStreamManager(), true
}

// Adapters returns the adapter registry for registering custom executors.
func (m *Manager) Adapters() *adapter.AdapterRegistry {
	return m.adapters
}

// Events returns the central event bus.
func (m *Manager) Events() *events.Bus {
	return m.bus
}
