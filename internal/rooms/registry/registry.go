// Package registry provides concurrency-safe storage and indexing of active rooms.
package registry

import (
	"errors"
	"sync"

	"cpip/internal/rooms/metrics"
	"cpip/internal/rooms/room"
)

var (
	// ErrRoomNotFound is returned when trying to deregister or perform operations on an absent room.
	ErrRoomNotFound = errors.New("registry: room not found")
	// ErrRoomExists is returned when trying to register a room with a duplicate ID.
	ErrRoomExists   = errors.New("registry: room already exists")
)

// Registry manages the collection of active room runtime instances.
type Registry struct {
	mu      sync.RWMutex
	rooms   map[string]*room.Room
	metrics metrics.Recorder
}

// New builds a Registry.
func New(m metrics.Recorder) *Registry {
	return &Registry{
		rooms:   make(map[string]*room.Room),
		metrics: m,
	}
}

// Register adds a room to the registry, reporting active room metrics.
func (reg *Registry) Register(r *room.Room) error {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if _, ok := reg.rooms[r.ID()]; ok {
		return ErrRoomExists
	}
	reg.rooms[r.ID()] = r
	reg.metrics.SetActiveRooms(len(reg.rooms))
	return nil
}

// Deregister removes a room from the registry.
func (reg *Registry) Deregister(id string) (*room.Room, error) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	r, ok := reg.rooms[id]
	if !ok {
		return nil, ErrRoomNotFound
	}
	delete(reg.rooms, id)
	reg.metrics.SetActiveRooms(len(reg.rooms))
	return r, nil
}

// Get finds a room by ID.
func (reg *Registry) Get(id string) (*room.Room, bool) {
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	r, ok := reg.rooms[id]
	return r, ok
}

// List returns a list of all active rooms.
func (reg *Registry) List() []*room.Room {
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	out := make([]*room.Room, 0, len(reg.rooms))
	for _, r := range reg.rooms {
		out = append(out, r)
	}
	return out
}

// FindByName returns all rooms matching the given name.
func (reg *Registry) FindByName(name string) []*room.Room {
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	var out []*room.Room
	for _, r := range reg.rooms {
		if r.Name() == name {
			out = append(out, r)
		}
	}
	return out
}

// Len returns the number of active rooms.
func (reg *Registry) Len() int {
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	return len(reg.rooms)
}
