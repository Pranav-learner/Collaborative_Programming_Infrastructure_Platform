// Package registry maintains indices of active presence sessions for fast lookups.
package registry

import (
	"errors"
	"sync"

	"cpip/internal/presence/types"
)

var (
	ErrSessionConflict  = errors.New("registry: session already exists under another connection")
	ErrPresenceNotFound = errors.New("registry: presence not found")
)

// Registry manages presence entities mapped by connection, user, room, and session.
type Registry struct {
	mu        sync.RWMutex
	presences map[string]*types.Presence
	byUser    map[string]map[string]struct{}
	byRoom    map[string]map[string]struct{}
	bySession map[string]string
}

// New constructs a Registry.
func New() *Registry {
	return &Registry{
		presences: make(map[string]*types.Presence),
		byUser:    make(map[string]map[string]struct{}),
		byRoom:    make(map[string]map[string]struct{}),
		bySession: make(map[string]string),
	}
}

// Register registers a participant's presence. If the connection ID already exists,
// it updates it. If the SessionID exists under a different connection, it returns ErrSessionConflict.
func (reg *Registry) Register(p types.Presence) error {
	reg.mu.Lock()
	defer reg.mu.Unlock()

	// Check session conflict
	if p.SessionID != "" {
		if existingConnID, exists := reg.bySession[p.SessionID]; exists && existingConnID != p.ConnID {
			return ErrSessionConflict
		}
	}

	// Clean up old index state if connection ID already existed (overwrite case)
	if old, exists := reg.presences[p.ConnID]; exists {
		reg.removeIndicesLocked(old)
	}

	copied := p.Clone()
	reg.presences[p.ConnID] = &copied

	// Index by UserID
	if p.UserID != "" {
		if _, exists := reg.byUser[p.UserID]; !exists {
			reg.byUser[p.UserID] = make(map[string]struct{})
		}
		reg.byUser[p.UserID][p.ConnID] = struct{}{}
	}

	// Index by RoomID
	if p.RoomID != "" {
		if _, exists := reg.byRoom[p.RoomID]; !exists {
			reg.byRoom[p.RoomID] = make(map[string]struct{})
		}
		reg.byRoom[p.RoomID][p.ConnID] = struct{}{}
	}

	// Index by SessionID
	if p.SessionID != "" {
		reg.bySession[p.SessionID] = p.ConnID
	}

	return nil
}

// Deregister removes a presence by connection ID.
func (reg *Registry) Deregister(connID string) (types.Presence, error) {
	reg.mu.Lock()
	defer reg.mu.Unlock()

	p, exists := reg.presences[connID]
	if !exists {
		return types.Presence{}, ErrPresenceNotFound
	}

	reg.removeIndicesLocked(p)
	delete(reg.presences, connID)

	return *p, nil
}

// Get retrieves a clone of the presence for a connection ID.
func (reg *Registry) Get(connID string) (types.Presence, bool) {
	reg.mu.RLock()
	defer reg.mu.RUnlock()

	p, exists := reg.presences[connID]
	if !exists {
		return types.Presence{}, false
	}
	return p.Clone(), true
}

// GetBySession retrieves a clone of the presence for a session ID.
func (reg *Registry) GetBySession(sessID string) (types.Presence, bool) {
	reg.mu.RLock()
	defer reg.mu.RUnlock()

	connID, exists := reg.bySession[sessID]
	if !exists {
		return types.Presence{}, false
	}
	p, exists := reg.presences[connID]
	if !exists {
		return types.Presence{}, false
	}
	return p.Clone(), true
}

// ListByRoom returns all active presences in a given room.
func (reg *Registry) ListByRoom(roomID string) []types.Presence {
	reg.mu.RLock()
	defer reg.mu.RUnlock()

	connIDs, exists := reg.byRoom[roomID]
	if !exists {
		return nil
	}

	out := make([]types.Presence, 0, len(connIDs))
	for id := range connIDs {
		if p, ok := reg.presences[id]; ok {
			out = append(out, p.Clone())
		}
	}
	return out
}

// ListByUser returns all active presences for a user ID (handles multi-device connections).
func (reg *Registry) ListByUser(userID string) []types.Presence {
	reg.mu.RLock()
	defer reg.mu.RUnlock()

	connIDs, exists := reg.byUser[userID]
	if !exists {
		return nil
	}

	out := make([]types.Presence, 0, len(connIDs))
	for id := range connIDs {
		if p, ok := reg.presences[id]; ok {
			out = append(out, p.Clone())
		}
	}
	return out
}

// ListByState returns all presences matching a state.
func (reg *Registry) ListByState(state types.State) []types.Presence {
	reg.mu.RLock()
	defer reg.mu.RUnlock()

	var out []types.Presence
	for _, p := range reg.presences {
		if p.State == state {
			out = append(out, p.Clone())
		}
	}
	return out
}

// UpdateState transitions the state of a connection ID.
func (reg *Registry) UpdateState(connID string, newState types.State) (types.Presence, error) {
	reg.mu.Lock()
	defer reg.mu.Unlock()

	p, exists := reg.presences[connID]
	if !exists {
		return types.Presence{}, ErrPresenceNotFound
	}

	if !types.CanTransition(p.State, newState) {
		return types.Presence{}, types.ErrInvalidStateTransition
	}

	p.State = newState
	return p.Clone(), nil
}

// Mutate executes a callback under write lock to safely modify a presence inside the registry.
func (reg *Registry) Mutate(connID string, mutateFn func(*types.Presence) error) (types.Presence, error) {
	reg.mu.Lock()
	defer reg.mu.Unlock()

	p, exists := reg.presences[connID]
	if !exists {
		return types.Presence{}, ErrPresenceNotFound
	}

	// Save index fields before mutation
	oldUser := p.UserID
	oldRoom := p.RoomID
	oldSession := p.SessionID

	if err := mutateFn(p); err != nil {
		return types.Presence{}, err
	}

	// Update indices if User, Room or Session was mutated
	if p.UserID != oldUser {
		if oldUser != "" {
			delete(reg.byUser[oldUser], connID)
		}
		if p.UserID != "" {
			if _, ok := reg.byUser[p.UserID]; !ok {
				reg.byUser[p.UserID] = make(map[string]struct{})
			}
			reg.byUser[p.UserID][connID] = struct{}{}
		}
	}

	if p.RoomID != oldRoom {
		if oldRoom != "" {
			delete(reg.byRoom[oldRoom], connID)
		}
		if p.RoomID != "" {
			if _, ok := reg.byRoom[p.RoomID]; !ok {
				reg.byRoom[p.RoomID] = make(map[string]struct{})
			}
			reg.byRoom[p.RoomID][connID] = struct{}{}
		}
	}

	if p.SessionID != oldSession {
		if oldSession != "" {
			delete(reg.bySession, oldSession)
		}
		if p.SessionID != "" {
			reg.bySession[p.SessionID] = connID
		}
	}

	return p.Clone(), nil
}

// Len returns the total number of registered connection presences.
func (reg *Registry) Len() int {
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	return len(reg.presences)
}

func (reg *Registry) removeIndicesLocked(p *types.Presence) {
	if p.UserID != "" {
		if m, exists := reg.byUser[p.UserID]; exists {
			delete(m, p.ConnID)
			if len(m) == 0 {
				delete(reg.byUser, p.UserID)
			}
		}
	}
	if p.RoomID != "" {
		if m, exists := reg.byRoom[p.RoomID]; exists {
			delete(m, p.ConnID)
			if len(m) == 0 {
				delete(reg.byRoom, p.RoomID)
			}
		}
	}
	if p.SessionID != "" {
		delete(reg.bySession, p.SessionID)
	}
}
