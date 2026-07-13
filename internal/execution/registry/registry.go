// Package registry is the concurrency-safe, in-memory index of live jobs. It is
// the orchestrator's authoritative store of job state during a job's lifetime
// and maintains secondary indexes for lookup by user, room, session, language,
// and state.
//
// The registry owns the canonical *job.Job under a single RWMutex and hands out
// value copies (via Job.Clone), so a job obtained from a query is an immutable
// snapshot that cannot race with concurrent mutations. All state changes flow
// through Transition, which enforces the lifecycle state machine and keeps the
// state index consistent.
package registry

import (
	"sync"
	"time"

	"cpip/internal/execution/job"
)

// Registry maintains the live job index.
type Registry struct {
	mu        sync.RWMutex
	byID      map[string]*job.Job
	byUser    index
	byRoom    index
	bySession index
	byLang    index
	byState   map[job.State]set
}

// index maps a secondary key to the set of job IDs carrying that key.
type index map[string]set

// set is a set of job IDs.
type set map[string]struct{}

// New constructs an empty Registry.
func New() *Registry {
	return &Registry{
		byID:      make(map[string]*job.Job),
		byUser:    make(index),
		byRoom:    make(index),
		bySession: make(index),
		byLang:    make(index),
		byState:   make(map[job.State]set),
	}
}

// Add registers a new job. It returns job.ErrDuplicateJob if the ID exists.
func (r *Registry) Add(j job.Job) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.byID[j.ID]; exists {
		return job.ErrDuplicateJob
	}
	stored := j.Clone()
	r.byID[j.ID] = &stored
	r.byUser.add(j.UserID, j.ID)
	r.byRoom.add(j.RoomID, j.ID)
	r.bySession.add(j.SessionID, j.ID)
	r.byLang.add(j.Language, j.ID)
	r.stateAdd(j.State, j.ID)
	return nil
}

// Get returns an immutable snapshot of a job.
func (r *Registry) Get(id string) (job.Job, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	j, ok := r.byID[id]
	if !ok {
		return job.Job{}, false
	}
	return j.Clone(), true
}

// Transition moves a job to a new state, enforcing the lifecycle state machine,
// and applies an optional mutation (for outcome, timestamps, assignments) under
// the same lock so the change is atomic. It returns the previous state on
// success, or job.ErrJobNotFound / job.ErrIllegalTransition on failure.
//
// The mutate callback must not change any indexed identity field (UserID, RoomID,
// SessionID, Language); those are immutable for a job's lifetime.
func (r *Registry) Transition(id string, to job.State, mutate func(*job.Job)) (job.State, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	j, ok := r.byID[id]
	if !ok {
		return 0, job.ErrJobNotFound
	}
	if !job.CanTransition(j.State, to) {
		return j.State, job.ErrIllegalTransition
	}
	from := j.State
	j.State = to
	j.Transitions++
	if mutate != nil {
		mutate(j)
	}
	r.stateRemove(from, id)
	r.stateAdd(to, id)
	return from, nil
}

// Update applies a mutation to a job without a state change (e.g. marking a
// cancellation request, recording a worker assignment). It must not modify the
// job's State or indexed identity fields. Returns job.ErrJobNotFound if absent.
func (r *Registry) Update(id string, mutate func(*job.Job)) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	j, ok := r.byID[id]
	if !ok {
		return job.ErrJobNotFound
	}
	before := j.State
	mutate(j)
	// Defend the invariant: Update never changes state (that is Transition's job).
	if j.State != before {
		j.State = before
	}
	return nil
}

// Remove deletes a job and returns its final snapshot.
func (r *Registry) Remove(id string) (job.Job, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	j, ok := r.byID[id]
	if !ok {
		return job.Job{}, false
	}
	snapshot := j.Clone()
	delete(r.byID, id)
	r.byUser.remove(j.UserID, id)
	r.byRoom.remove(j.RoomID, id)
	r.bySession.remove(j.SessionID, id)
	r.byLang.remove(j.Language, id)
	r.stateRemove(j.State, id)
	return snapshot, true
}

// State returns a job's current state without cloning the whole job.
func (r *Registry) State(id string) (job.State, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	j, ok := r.byID[id]
	if !ok {
		return 0, false
	}
	return j.State, true
}

// --- Queries -----------------------------------------------------------------

// ByUser returns snapshots of all jobs for a user.
func (r *Registry) ByUser(userID string) []job.Job { return r.byIndex(r.byUser, userID) }

// ByRoom returns snapshots of all jobs for a room.
func (r *Registry) ByRoom(roomID string) []job.Job { return r.byIndex(r.byRoom, roomID) }

// BySession returns snapshots of all jobs for a session.
func (r *Registry) BySession(sessionID string) []job.Job { return r.byIndex(r.bySession, sessionID) }

// ByLanguage returns snapshots of all jobs for a language.
func (r *Registry) ByLanguage(lang string) []job.Job { return r.byIndex(r.byLang, lang) }

// ByState returns snapshots of all jobs in a state.
func (r *Registry) ByState(s job.State) []job.Job {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := r.byState[s]
	out := make([]job.Job, 0, len(ids))
	for id := range ids {
		out = append(out, r.byID[id].Clone())
	}
	return out
}

func (r *Registry) byIndex(idx index, key string) []job.Job {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := idx[key]
	out := make([]job.Job, 0, len(ids))
	for id := range ids {
		out = append(out, r.byID[id].Clone())
	}
	return out
}

// FinishedBefore returns snapshots of finished (non-archived) jobs whose
// completion time is at or before the cutoff — the archival candidate set.
func (r *Registry) FinishedBefore(cutoff time.Time) []job.Job {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []job.Job
	for _, s := range []job.State{job.StateCompleted, job.StateFailed, job.StateTimedOut, job.StateCancelled} {
		for id := range r.byState[s] {
			j := r.byID[id]
			ref := j.CompletedAt
			if ref.IsZero() {
				ref = j.CreatedAt
			}
			if !ref.After(cutoff) {
				out = append(out, j.Clone())
			}
		}
	}
	return out
}

// --- Statistics --------------------------------------------------------------

// Stats is a point-in-time summary of the registry.
type Stats struct {
	Total        int
	ByState      map[job.State]int
	ActiveJobs   int
	FinishedJobs int
}

// Stats returns a snapshot of registry counts.
func (r *Registry) Stats() Stats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s := Stats{Total: len(r.byID), ByState: make(map[job.State]int, len(r.byState))}
	for state, ids := range r.byState {
		n := len(ids)
		s.ByState[state] = n
		if state.IsFinished() {
			s.FinishedJobs += n
		} else {
			s.ActiveJobs += n
		}
	}
	return s
}

// Count returns the total number of jobs.
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.byID)
}

// --- index helpers -----------------------------------------------------------

func (idx index) add(key, id string) {
	if key == "" {
		return
	}
	s, ok := idx[key]
	if !ok {
		s = make(set)
		idx[key] = s
	}
	s[id] = struct{}{}
}

func (idx index) remove(key, id string) {
	if key == "" {
		return
	}
	if s, ok := idx[key]; ok {
		delete(s, id)
		if len(s) == 0 {
			delete(idx, key)
		}
	}
}

func (r *Registry) stateAdd(s job.State, id string) {
	set_, ok := r.byState[s]
	if !ok {
		set_ = make(set)
		r.byState[s] = set_
	}
	set_[id] = struct{}{}
}

func (r *Registry) stateRemove(s job.State, id string) {
	if set_, ok := r.byState[s]; ok {
		delete(set_, id)
		if len(set_) == 0 {
			delete(r.byState, s)
		}
	}
}
