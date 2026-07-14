package registry

import (
	"errors"
	"sync"

	"cpip/internal/sandbox/types"
)

var (
	ErrSandboxNotFound = errors.New("sandbox not found")
)

// SandboxRegistry maintains a thread-safe index of all active and expiring sandbox sessions.
type SandboxRegistry struct {
	mu       sync.RWMutex
	sessions map[string]*types.SandboxSession
}

// NewSandboxRegistry initializes a SandboxRegistry instance.
func NewSandboxRegistry() *SandboxRegistry {
	return &SandboxRegistry{
		sessions: make(map[string]*types.SandboxSession),
	}
}

// Register registers a new sandbox session.
func (r *SandboxRegistry) Register(sess *types.SandboxSession) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessions[sess.ID] = sess
}

// Unregister deletes a session from the active catalog.
func (r *SandboxRegistry) Unregister(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.sessions, id)
}

// Get retrieves a sandbox session by ID.
func (r *SandboxRegistry) Get(id string) (*types.SandboxSession, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	sess, exists := r.sessions[id]
	if !exists {
		return nil, ErrSandboxNotFound
	}
	return sess, nil
}

// List returns a snapshot of all registered sandbox sessions.
func (r *SandboxRegistry) List() []*types.SandboxSession {
	r.mu.RLock()
	defer r.mu.RUnlock()
	list := make([]*types.SandboxSession, 0, len(r.sessions))
	for _, sess := range r.sessions {
		list = append(list, sess)
	}
	return list
}
