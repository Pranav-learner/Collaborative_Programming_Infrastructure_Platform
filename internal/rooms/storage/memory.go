package storage

import (
	"context"
	"sync"
)

// MemoryRepository is an in-memory Repository. It is the default persistence
// adapter for this module: the runtime is fully functional with it, and it backs
// the test suite. A production deployment swaps it for a PostgreSQL adapter by
// dependency injection — no calling code changes because both satisfy Repository.
//
// It deep-copies snapshots on the way in and out so a caller can never mutate
// stored state by retaining a reference, mirroring how a real database boundary
// behaves.
type MemoryRepository struct {
	mu    sync.RWMutex
	rooms map[string]Snapshot
}

// NewMemoryRepository returns an empty in-memory repository.
func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{rooms: make(map[string]Snapshot)}
}

// Save inserts or replaces the snapshot for snap.ID.
func (m *MemoryRepository) Save(ctx context.Context, snap Snapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	m.rooms[snap.ID] = cloneSnapshot(snap)
	m.mu.Unlock()
	return nil
}

// Load returns a copy of the snapshot for id, or ErrNotFound.
func (m *MemoryRepository) Load(ctx context.Context, id string) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	m.mu.RLock()
	snap, ok := m.rooms[id]
	m.mu.RUnlock()
	if !ok {
		return Snapshot{}, ErrNotFound
	}
	return cloneSnapshot(snap), nil
}

// Delete removes the snapshot for id, returning ErrNotFound if absent.
func (m *MemoryRepository) Delete(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.rooms[id]; !ok {
		return ErrNotFound
	}
	delete(m.rooms, id)
	return nil
}

// List returns copies of all persisted snapshots.
func (m *MemoryRepository) List(ctx context.Context) ([]Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Snapshot, 0, len(m.rooms))
	for _, snap := range m.rooms {
		out = append(out, cloneSnapshot(snap))
	}
	return out, nil
}

// Len returns the number of persisted rooms (test/introspection helper).
func (m *MemoryRepository) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.rooms)
}

// cloneSnapshot deep-copies a snapshot's slices and maps so stored and returned
// values never alias the caller's.
func cloneSnapshot(s Snapshot) Snapshot {
	cp := s
	if s.Participants != nil {
		parts := make([]ParticipantSnapshot, len(s.Participants))
		for i, p := range s.Participants {
			parts[i] = p
			parts[i].Metadata = cloneMap(p.Metadata)
		}
		cp.Participants = parts
	}
	cp.Metadata = cloneMap(s.Metadata)
	return cp
}

func cloneMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// Compile-time assurance.
var _ Repository = (*MemoryRepository)(nil)
