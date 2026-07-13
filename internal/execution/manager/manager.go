// Package manager is the composition root of the execution orchestrator
// subsystem. It wires the configuration, language registry, validation pipeline,
// execution-context manager, scheduler, job registry, storage, event bus, and
// metrics/logging seams into a running Orchestrator, decorates the request path
// with middleware, owns the background archival sweep, and exposes the minimal
// public Service interface consumed by the gateway, room manager, presence,
// collaboration engine, and future queue/runtime/REST/gRPC layers.
package manager

import (
	stdctx "context"
	"log/slog"
	"sync"
	"time"

	"cpip/internal/execution/config"
	execctx "cpip/internal/execution/context"
	"cpip/internal/execution/events"
	"cpip/internal/execution/job"
	"cpip/internal/execution/language"
	execlog "cpip/internal/execution/logger"
	"cpip/internal/execution/metrics"
	"cpip/internal/execution/middleware"
	"cpip/internal/execution/orchestrator"
	"cpip/internal/execution/registry"
	"cpip/internal/execution/scheduler"
	"cpip/internal/execution/storage"
	"cpip/internal/execution/validation"
	"cpip/internal/id"
)

// Service is the minimal public surface exposed to the rest of the platform.
type Service interface {
	// Intake.
	Submit(ctx stdctx.Context, req job.Request) (job.Job, error)
	Cancel(ctx stdctx.Context, jobID string) error
	Retry(ctx stdctx.Context, jobID string) error

	// Lifecycle marks (driven by future queue/worker/runtime modules).
	MarkDispatched(ctx stdctx.Context, jobID, workerID string) error
	MarkStarted(ctx stdctx.Context, jobID string) error
	MarkStreaming(ctx stdctx.Context, jobID string) error
	MarkCompleted(ctx stdctx.Context, jobID string) error
	MarkFailed(ctx stdctx.Context, jobID, reason string) error
	MarkTimedOut(ctx stdctx.Context, jobID string) error

	// Queries.
	Status(jobID string) (job.Job, error)
	Statistics(jobID string) (job.Statistics, error)
	ByUser(userID string) []job.Job
	ByRoom(roomID string) []job.Job
	BySession(sessionID string) []job.Job
	ByState(s job.State) []job.Job
	ByLanguage(lang string) []job.Job
	Stats() registry.Stats
	Languages() []language.Language

	// Observability.
	Events() *events.Bus
}

// Manager is the top-level façade over the orchestrator subsystem.
type Manager struct {
	cfg     config.Config
	orch    *orchestrator.Orchestrator
	submit  middleware.Submitter
	langs   *language.Registry
	bus     *events.Bus
	archive storage.Archive
	metrics metrics.Recorder
	log     *slog.Logger

	ctx    stdctx.Context
	cancel stdctx.CancelFunc
	wg     sync.WaitGroup
}

// Compile-time assertion that Manager satisfies the public Service interface.
var _ Service = (*Manager)(nil)

// Params configures the Manager. Only Config is meaningful without defaults;
// every other dependency defaults to a production-sensible in-memory / no-op
// implementation so the subsystem is usable out of the box.
type Params struct {
	Config           config.Config
	Metrics          metrics.Recorder
	Logger           *slog.Logger
	Scheduler        scheduler.Scheduler
	Languages        *language.Registry
	Repository       storage.Repository
	Archive          storage.Archive
	Authorizer       validation.Authorizer
	CustomValidators []validation.Validator
	TraceIDFactory   execctx.TraceIDFactory
	// NowFunc overrides the clock (tests). Nil uses time.Now.
	NowFunc func() time.Time
}

// NewManager constructs and wires a Manager. It normalizes the configuration and
// fills in default dependencies for anything the caller omitted.
func NewManager(p Params) (*Manager, error) {
	cfg, err := p.Config.Validate()
	if err != nil {
		return nil, err
	}

	base := p.Logger
	if base == nil {
		base = slog.Default()
	}
	if p.Metrics == nil {
		p.Metrics = metrics.NewNoop()
	}
	if p.Scheduler == nil {
		p.Scheduler = scheduler.NewMemory(0)
	}
	if p.Languages == nil {
		p.Languages = language.Default()
	}
	if p.Repository == nil {
		p.Repository = storage.NewMemoryRepository()
	}
	if p.Archive == nil {
		p.Archive = storage.NewMemoryArchive()
	}
	if p.Authorizer == nil {
		p.Authorizer = validation.AllowAll
	}
	if p.TraceIDFactory == nil {
		p.TraceIDFactory = func() (string, string) { return id.NewWithPrefix("trace"), id.NewWithPrefix("span") }
	}

	// The bus is counter-free: lifecycle metrics are recorded explicitly by the
	// orchestrator at each transition, not inferred from publish volume.
	bus := events.New(events.Options{})

	reg := registry.New()
	ctxMgr := execctx.NewManager(p.TraceIDFactory)
	pipeline := validation.NewPipeline(
		validation.DefaultValidators(cfg, p.Languages, p.Authorizer, p.CustomValidators...),
		validation.WithMetrics(p.Metrics),
	)

	orch := orchestrator.New(orchestrator.Deps{
		Config:    cfg,
		Registry:  reg,
		Language:  p.Languages,
		Pipeline:  pipeline,
		Context:   ctxMgr,
		Scheduler: p.Scheduler,
		Bus:       bus,
		Metrics:   p.Metrics,
		Store:     p.Repository,
		Logger:    base,
		NowFunc:   p.NowFunc,
	})

	// Decorate the request path: Recovery outermost (catches panics), then Logging.
	submit := middleware.Chain(orch,
		middleware.Recovery(base),
		middleware.Logging(base),
	)

	ctx, cancel := stdctx.WithCancel(stdctx.Background())
	return &Manager{
		cfg:     cfg,
		orch:    orch,
		submit:  submit,
		langs:   p.Languages,
		bus:     bus,
		archive: p.Archive,
		metrics: p.Metrics,
		log:     execlog.Named(base, "manager"),
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

// Start launches the background archival sweep.
func (m *Manager) Start() {
	m.wg.Add(1)
	go m.archiveLoop()
}

// Stop cancels the background loop, waits for it to drain, and closes the bus.
func (m *Manager) Stop() {
	m.cancel()
	m.wg.Wait()
	m.bus.Close()
}

func (m *Manager) archiveLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.cfg.ArchiveSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			if n := m.orch.ArchiveFinished(m.ctx, m.archive); n > 0 {
				m.log.Debug("archival sweep", "archived", n)
			}
		}
	}
}

// --- Service implementation (request path via middleware) ---------------------

// Submit validates and schedules an execution request.
func (m *Manager) Submit(ctx stdctx.Context, req job.Request) (job.Job, error) {
	return m.submit.SubmitExecution(ctx, req)
}

// Cancel cancels a job.
func (m *Manager) Cancel(ctx stdctx.Context, jobID string) error {
	return m.submit.Cancel(ctx, jobID)
}

// Retry re-enters a recoverable job.
func (m *Manager) Retry(ctx stdctx.Context, jobID string) error {
	return m.submit.Retry(ctx, jobID)
}

// --- Service implementation (lifecycle marks, delegated to the orchestrator) --

func (m *Manager) MarkDispatched(ctx stdctx.Context, jobID, workerID string) error {
	return m.orch.MarkDispatched(ctx, jobID, workerID)
}
func (m *Manager) MarkStarted(ctx stdctx.Context, jobID string) error {
	return m.orch.MarkStarted(ctx, jobID)
}
func (m *Manager) MarkStreaming(ctx stdctx.Context, jobID string) error {
	return m.orch.MarkStreaming(ctx, jobID)
}
func (m *Manager) MarkCompleted(ctx stdctx.Context, jobID string) error {
	return m.orch.MarkCompleted(ctx, jobID)
}
func (m *Manager) MarkFailed(ctx stdctx.Context, jobID, reason string) error {
	return m.orch.MarkFailed(ctx, jobID, reason)
}
func (m *Manager) MarkTimedOut(ctx stdctx.Context, jobID string) error {
	return m.orch.MarkTimedOut(ctx, jobID)
}

// --- Service implementation (queries) ----------------------------------------

func (m *Manager) Status(jobID string) (job.Job, error)            { return m.orch.Status(jobID) }
func (m *Manager) Statistics(jobID string) (job.Statistics, error) { return m.orch.Statistics(jobID) }
func (m *Manager) ByUser(userID string) []job.Job                  { return m.orch.ByUser(userID) }
func (m *Manager) ByRoom(roomID string) []job.Job                  { return m.orch.ByRoom(roomID) }
func (m *Manager) BySession(sessionID string) []job.Job            { return m.orch.BySession(sessionID) }
func (m *Manager) ByState(s job.State) []job.Job                   { return m.orch.ByState(s) }
func (m *Manager) ByLanguage(lang string) []job.Job                { return m.orch.ByLanguage(lang) }
func (m *Manager) Stats() registry.Stats                           { return m.orch.Stats() }
func (m *Manager) Languages() []language.Language                  { return m.langs.List() }

// Events returns the event bus for subscribers.
func (m *Manager) Events() *events.Bus { return m.bus }

// Orchestrator exposes the underlying orchestrator for advanced integration
// (e.g. future modules that need the execution context of a job).
func (m *Manager) Orchestrator() *orchestrator.Orchestrator { return m.orch }
