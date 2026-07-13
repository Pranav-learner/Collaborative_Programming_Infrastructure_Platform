// Package storage defines the persistence abstractions for the execution
// orchestrator and provides in-memory implementations. The live registry is the
// source of truth during a job's lifetime; these interfaces are the seam through
// which jobs are durably persisted and archived. PostgreSQL implementations
// arrive in a later module; the orchestrator depends only on the interfaces.
package storage

import (
	stdctx "context"
	"sort"
	"sync"
	"time"

	"cpip/internal/execution/job"
)

// Repository persists live job records. Implementations must be safe for
// concurrent use.
type Repository interface {
	// Save inserts or updates a job record.
	Save(ctx stdctx.Context, j job.Job) error
	// Load returns a job record by ID, or job.ErrJobNotFound.
	Load(ctx stdctx.Context, id string) (job.Job, error)
	// Delete removes a job record. Deleting an unknown job is not an error.
	Delete(ctx stdctx.Context, id string) error
	// Count returns the number of persisted job records.
	Count(ctx stdctx.Context) (int, error)
}

// Archive stores terminal job records past their live retention window. It is a
// write-mostly, read-rarely store separate from the hot Repository.
type Archive interface {
	// Archive stores a finished job record with the time it was archived.
	Archive(ctx stdctx.Context, j job.Job, at time.Time) error
	// Get returns an archived job by ID, or job.ErrJobNotFound.
	Get(ctx stdctx.Context, id string) (job.Job, error)
	// List returns archived jobs, most-recently-archived first, bounded by limit
	// (0 = unbounded).
	List(ctx stdctx.Context, limit int) ([]job.Job, error)
	// Count returns the number of archived records.
	Count(ctx stdctx.Context) (int, error)
}

// --- In-memory Repository ----------------------------------------------------

// MemoryRepository is a concurrency-safe, in-memory Repository.
type MemoryRepository struct {
	mu   sync.RWMutex
	jobs map[string]job.Job
}

// NewMemoryRepository constructs an in-memory Repository.
func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{jobs: make(map[string]job.Job)}
}

// Save stores a clone of the job.
func (r *MemoryRepository) Save(_ stdctx.Context, j job.Job) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.jobs[j.ID] = j.Clone()
	return nil
}

// Load returns a clone of the stored job.
func (r *MemoryRepository) Load(_ stdctx.Context, id string) (job.Job, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	j, ok := r.jobs[id]
	if !ok {
		return job.Job{}, job.ErrJobNotFound
	}
	return j.Clone(), nil
}

// Delete removes a job record.
func (r *MemoryRepository) Delete(_ stdctx.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.jobs, id)
	return nil
}

// Count returns the number of records.
func (r *MemoryRepository) Count(_ stdctx.Context) (int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.jobs), nil
}

// --- In-memory Archive -------------------------------------------------------

type archivedRecord struct {
	job        job.Job
	archivedAt time.Time
}

// MemoryArchive is a concurrency-safe, in-memory Archive.
type MemoryArchive struct {
	mu      sync.RWMutex
	records map[string]archivedRecord
}

// NewMemoryArchive constructs an in-memory Archive.
func NewMemoryArchive() *MemoryArchive {
	return &MemoryArchive{records: make(map[string]archivedRecord)}
}

// Archive stores a clone of the finished job.
func (a *MemoryArchive) Archive(_ stdctx.Context, j job.Job, at time.Time) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.records[j.ID] = archivedRecord{job: j.Clone(), archivedAt: at}
	return nil
}

// Get returns an archived job.
func (a *MemoryArchive) Get(_ stdctx.Context, id string) (job.Job, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	rec, ok := a.records[id]
	if !ok {
		return job.Job{}, job.ErrJobNotFound
	}
	return rec.job.Clone(), nil
}

// List returns archived jobs, most-recent first.
func (a *MemoryArchive) List(_ stdctx.Context, limit int) ([]job.Job, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	recs := make([]archivedRecord, 0, len(a.records))
	for _, rec := range a.records {
		recs = append(recs, rec)
	}
	sort.Slice(recs, func(i, j int) bool { return recs[i].archivedAt.After(recs[j].archivedAt) })
	if limit > 0 && len(recs) > limit {
		recs = recs[:limit]
	}
	out := make([]job.Job, len(recs))
	for i, rec := range recs {
		out[i] = rec.job.Clone()
	}
	return out, nil
}

// Count returns the number of archived records.
func (a *MemoryArchive) Count(_ stdctx.Context) (int, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.records), nil
}
