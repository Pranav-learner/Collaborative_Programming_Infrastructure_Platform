// Package context builds and owns the per-job execution context: the cancellable
// Go context, tracing/correlation identifiers, security metadata, the resolved
// resource profile, and forward-looking worker/sandbox assignment fields.
//
// The execution context is the live, cancellable counterpart to the immutable
// Job value. The Manager owns every context's lifetime and is the single place
// cancellation is delivered, so the orchestrator never juggles CancelFuncs.
package context

import (
	"context"
	"sync"
	"time"

	"cpip/internal/execution/job"
)

// SecurityMetadata captures the authenticated principal and its authorization
// decision for downstream enforcement (sandbox network policy, audit).
type SecurityMetadata struct {
	UserID        string
	Roles         []string
	Authenticated bool
	Authorized    bool
	NetworkAccess bool
}

// Tracing carries correlation identifiers threaded through the whole execution.
type Tracing struct {
	RequestID     string
	CorrelationID string
	TraceID       string
	SpanID        string
}

// ExecutionContext is the live context for a single job. Its Context is derived
// from the caller's context with the job's deadline applied, and is cancelled
// when the job is cancelled, completed, or its context is released.
type ExecutionContext struct {
	Context context.Context
	cancel  context.CancelFunc

	JobID     string
	UserID    string
	SessionID string
	RoomID    string
	Language  string

	Tracing   Tracing
	Security  SecurityMetadata
	Resources job.ResourceProfile
	Deadline  time.Time
	CreatedAt time.Time

	// Forward-looking assignments, populated by future modules.
	WorkerID  string
	SandboxID string

	cancelOnce sync.Once
}

// Cancel cancels the underlying Go context. It is idempotent.
func (e *ExecutionContext) Cancel() {
	e.cancelOnce.Do(func() {
		if e.cancel != nil {
			e.cancel()
		}
	})
}

// Spec describes the inputs required to build an ExecutionContext.
type Spec struct {
	Job      job.Job
	Tracing  Tracing
	Security SecurityMetadata
	Now      time.Time
}

// Manager builds and owns execution contexts, keyed by job ID. It is safe for
// concurrent use.
type Manager struct {
	mu     sync.RWMutex
	byJob  map[string]*ExecutionContext
	tracer TraceIDFactory
}

// TraceIDFactory generates trace and span identifiers. It is injected so the
// tracing vendor stays out of this package; a default is provided.
type TraceIDFactory func() (traceID, spanID string)

// NewManager constructs a context Manager. A nil factory yields empty trace IDs.
func NewManager(f TraceIDFactory) *Manager {
	return &Manager{byJob: make(map[string]*ExecutionContext), tracer: f}
}

// Create derives a new ExecutionContext from parent for the given spec, stores
// it, and returns it. The context deadline is the job's timeout from now. Any
// existing context for the same job ID is cancelled and replaced (which occurs
// on retry).
func (m *Manager) Create(parent context.Context, spec Spec) *ExecutionContext {
	now := spec.Now
	if now.IsZero() {
		now = time.Now()
	}
	deadline := now.Add(spec.Job.Timeout)

	ctx, cancel := context.WithDeadline(parent, deadline)

	tracing := spec.Tracing
	if tracing.TraceID == "" && m.tracer != nil {
		tracing.TraceID, tracing.SpanID = m.tracer()
	}

	ec := &ExecutionContext{
		Context:   ctx,
		cancel:    cancel,
		JobID:     spec.Job.ID,
		UserID:    spec.Job.UserID,
		SessionID: spec.Job.SessionID,
		RoomID:    spec.Job.RoomID,
		Language:  spec.Job.Language,
		Tracing:   tracing,
		Security:  spec.Security,
		Resources: spec.Job.Resources,
		Deadline:  deadline,
		CreatedAt: now,
	}

	m.mu.Lock()
	if prev, ok := m.byJob[spec.Job.ID]; ok {
		prev.Cancel()
	}
	m.byJob[spec.Job.ID] = ec
	m.mu.Unlock()
	return ec
}

// Get returns the execution context for a job, if present.
func (m *Manager) Get(jobID string) (*ExecutionContext, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ec, ok := m.byJob[jobID]
	return ec, ok
}

// Cancel cancels the execution context for a job, if present, and reports
// whether one was found.
func (m *Manager) Cancel(jobID string) bool {
	m.mu.RLock()
	ec, ok := m.byJob[jobID]
	m.mu.RUnlock()
	if ok {
		ec.Cancel()
	}
	return ok
}

// Release cancels and removes the execution context for a job, freeing its
// resources. It is called when a job reaches a terminal state.
func (m *Manager) Release(jobID string) {
	m.mu.Lock()
	ec, ok := m.byJob[jobID]
	delete(m.byJob, jobID)
	m.mu.Unlock()
	if ok {
		ec.Cancel()
	}
}

// Count returns the number of live execution contexts.
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.byJob)
}

// Assign records the worker and sandbox assignment for a job's context. Empty
// values are ignored, so callers may set either independently.
func (m *Manager) Assign(jobID, workerID, sandboxID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ec, ok := m.byJob[jobID]
	if !ok {
		return
	}
	if workerID != "" {
		ec.WorkerID = workerID
	}
	if sandboxID != "" {
		ec.SandboxID = sandboxID
	}
}
