// Package scheduler defines the abstraction the orchestrator uses to hand jobs
// off for execution, decoupled from any concrete queue. The production
// implementations (Redis Streams, RabbitMQ, Kafka) arrive in later modules; this
// package ships the interface plus an in-memory scheduler for development and
// testing and a no-op scheduler.
//
// The orchestrator depends only on Scheduler. It never assumes ordering,
// delivery, or worker semantics beyond the contract below.
package scheduler

import (
	stdctx "context"
	"sort"
	"sync"

	"cpip/internal/execution/job"
)

// Scheduler accepts jobs for eventual execution and supports cancellation,
// retry, and reprioritization. Implementations must be safe for concurrent use.
type Scheduler interface {
	// Schedule enqueues a job for execution. It returns job.ErrSchedulerUnavailable
	// if the job cannot be accepted (e.g. the backing queue is unavailable/full).
	Schedule(ctx stdctx.Context, j job.Job) error
	// Cancel removes a not-yet-dispatched job from the schedule. Cancelling an
	// unknown job is a no-op that returns nil.
	Cancel(ctx stdctx.Context, jobID string) error
	// Retry re-enqueues a job for another attempt.
	Retry(ctx stdctx.Context, j job.Job) error
	// Reprioritize updates the scheduling priority of a pending job.
	Reprioritize(ctx stdctx.Context, jobID string, p job.Priority) error
}

// --- No-op scheduler ---------------------------------------------------------

// Noop is a Scheduler that accepts and discards everything. It is useful when
// the orchestrator is driven purely by direct lifecycle calls in tests.
type Noop struct{}

// NewNoop constructs a Noop scheduler.
func NewNoop() Noop { return Noop{} }

func (Noop) Schedule(stdctx.Context, job.Job) error                  { return nil }
func (Noop) Cancel(stdctx.Context, string) error                     { return nil }
func (Noop) Retry(stdctx.Context, job.Job) error                     { return nil }
func (Noop) Reprioritize(stdctx.Context, string, job.Priority) error { return nil }

// --- In-memory scheduler -----------------------------------------------------

// entry is a scheduled job with its priority and monotonic sequence, used to
// produce a deterministic priority ordering.
type entry struct {
	jobID    string
	priority job.Priority
	seq      uint64
}

// Memory is an in-memory Scheduler for development and tests. It records
// scheduled jobs in a priority order (higher priority first, FIFO within a
// priority) and can be capacity-bounded to exercise the unavailable path. It
// does not execute anything — workers are a future module.
type Memory struct {
	mu       sync.Mutex
	entries  map[string]entry
	seq      uint64
	capacity int // 0 means unbounded
}

// NewMemory constructs an in-memory scheduler. capacity <= 0 means unbounded.
func NewMemory(capacity int) *Memory {
	return &Memory{entries: make(map[string]entry), capacity: capacity}
}

// Schedule records the job, honoring the capacity bound.
func (m *Memory) Schedule(_ stdctx.Context, j job.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.entries[j.ID]; !exists && m.capacity > 0 && len(m.entries) >= m.capacity {
		return job.ErrSchedulerUnavailable
	}
	m.seq++
	m.entries[j.ID] = entry{jobID: j.ID, priority: j.Priority, seq: m.seq}
	return nil
}

// Cancel removes a scheduled job.
func (m *Memory) Cancel(_ stdctx.Context, jobID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.entries, jobID)
	return nil
}

// Retry re-records the job at the tail of its priority class.
func (m *Memory) Retry(ctx stdctx.Context, j job.Job) error {
	return m.Schedule(ctx, j)
}

// Reprioritize updates a pending job's priority, or is a no-op if unknown.
func (m *Memory) Reprioritize(_ stdctx.Context, jobID string, p job.Priority) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.entries[jobID]; ok {
		e.priority = p
		m.entries[jobID] = e
	}
	return nil
}

// Len returns the number of scheduled jobs.
func (m *Memory) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}

// Has reports whether a job is currently scheduled.
func (m *Memory) Has(jobID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.entries[jobID]
	return ok
}

// Drain returns the scheduled job IDs in priority order (highest priority first,
// FIFO within a priority) and clears the schedule. It models a worker pool
// pulling ready work.
func (m *Memory) Drain() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	es := make([]entry, 0, len(m.entries))
	for _, e := range m.entries {
		es = append(es, e)
	}
	sort.Slice(es, func(i, j int) bool {
		if es[i].priority != es[j].priority {
			return es[i].priority > es[j].priority
		}
		return es[i].seq < es[j].seq
	})
	ids := make([]string, len(es))
	for i, e := range es {
		ids[i] = e.jobID
	}
	m.entries = make(map[string]entry)
	return ids
}
