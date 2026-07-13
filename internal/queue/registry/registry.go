package registry

import (
	"sync"
	"time"

	"cpip/internal/queue/types"
)

// Registry manages the set of active workers in the cluster. It provides thread-safe
// query and state transition mechanisms.
type Registry struct {
	mu      sync.RWMutex
	workers map[string]*types.Worker
}

// New constructs a new Worker Registry.
func New() *Registry {
	return &Registry{
		workers: make(map[string]*types.Worker),
	}
}

// Register adds a worker to the registry, setting its state to WorkerStarting
// and recording its start time.
func (r *Registry) Register(w types.Worker) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.workers[w.ID]; exists {
		return types.ErrDuplicateWorker
	}

	now := time.Now()
	w.State = types.WorkerStarting
	w.StartedAt = now
	w.LastHeartbeat = now
	w.Transitions = 1

	r.workers[w.ID] = &w
	return nil
}

// Deregister removes a worker from the registry.
func (r *Registry) Deregister(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.workers[id]; !exists {
		return types.ErrWorkerNotFound
	}

	delete(r.workers, id)
	return nil
}

// Get retrieves a deep copy of a worker by ID.
func (r *Registry) Get(id string) (types.Worker, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	w, exists := r.workers[id]
	if !exists {
		return types.Worker{}, types.ErrWorkerNotFound
	}

	return w.Clone(), nil
}

// UpdateState transitions a worker's state, checking transition legality.
func (r *Registry) UpdateState(id string, to types.WorkerState) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	w, exists := r.workers[id]
	if !exists {
		return types.ErrWorkerNotFound
	}

	if !types.CanTransitionWorker(w.State, to) {
		return types.ErrIllegalWorkerTransition
	}

	w.State = to
	w.Transitions++
	return nil
}

// UpdateHeartbeat refreshes the last heartbeat timestamp and updates health status.
func (r *Registry) UpdateHeartbeat(id string, health types.Health) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	w, exists := r.workers[id]
	if !exists {
		return types.ErrWorkerNotFound
	}

	w.LastHeartbeat = time.Now()
	w.Health = health
	return nil
}

// UpdateCurrentJob updates the job ID and message ID currently assigned to the worker.
func (r *Registry) UpdateCurrentJob(id string, jobID, msgID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	w, exists := r.workers[id]
	if !exists {
		return types.ErrWorkerNotFound
	}

	w.CurrentJob = jobID
	w.CurrentMsg = msgID
	if jobID != "" {
		w.Stats.LastJobAt = time.Now()
	}
	return nil
}

// UpdateStats mutates a worker's stats in-place under the registry lock.
func (r *Registry) UpdateStats(id string, fn func(*types.WorkerStats)) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	w, exists := r.workers[id]
	if !exists {
		return types.ErrWorkerNotFound
	}

	fn(&w.Stats)
	return nil
}

// ByState returns all workers matching the given state.
func (r *Registry) ByState(s types.WorkerState) []types.Worker {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []types.Worker
	for _, w := range r.workers {
		if w.State == s {
			result = append(result, w.Clone())
		}
	}
	return result
}

// ByCapability returns all workers capable of executing jobs of the given language/capability.
func (r *Registry) ByCapability(cap string) []types.Worker {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []types.Worker
	for _, w := range r.workers {
		if w.HasCapability(cap) {
			result = append(result, w.Clone())
		}
	}
	return result
}

// ByCurrentJob returns the worker currently processing the specified job ID.
func (r *Registry) ByCurrentJob(jobID string) (types.Worker, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, w := range r.workers {
		if w.CurrentJob == jobID {
			return w.Clone(), nil
		}
	}
	return types.Worker{}, types.ErrWorkerNotFound
}

// List returns a snapshot list of all registered workers.
func (r *Registry) List() []types.Worker {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]types.Worker, 0, len(r.workers))
	for _, w := range r.workers {
		result = append(result, w.Clone())
	}
	return result
}
