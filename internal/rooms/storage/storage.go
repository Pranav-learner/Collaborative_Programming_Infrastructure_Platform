// Package storage defines the persistence boundary for rooms: a Repository
// interface plus a self-contained Snapshot model that later database adapters
// (PostgreSQL, and a Redis-backed distributed registry) implement.
//
// The runtime is authoritative and works entirely in memory; persistence is a
// write-through side effect and a source for restoring rooms after a restart. To
// keep those two models from coupling, the persistence Snapshot is deliberately
// distinct from the runtime room.Room / room.View: it carries only serializable
// value types and no behavior. The manager maps between the runtime view and the
// snapshot at the boundary, so a change to the persistence schema never ripples
// into the runtime entity and vice versa.
//
// storage depends only on the leaf packages lifecycle and permissions, so it can
// be imported by the manager without cycles and a future adapter package can
// import it freely.
package storage

import (
	"context"
	"errors"
	"time"

	"cpip/internal/rooms/lifecycle"
	"cpip/internal/rooms/permissions"
)

// ErrNotFound is returned by Repository.Load/Delete when no room with the given
// id is persisted. Callers compare with errors.Is.
var ErrNotFound = errors.New("storage: room not found")

// ParticipantSnapshot is the persisted form of a room member.
type ParticipantSnapshot struct {
	UserID    string
	Role      permissions.Role
	JoinedAt  time.Time
	LastSeen  time.Time
	Connected bool
	Metadata  map[string]any
}

// Snapshot is the persisted form of a room. It is a flat, serializable value
// with no pointers to runtime state.
type Snapshot struct {
	ID              string
	Name            string
	OwnerID         string
	State           lifecycle.State
	CreatedAt       time.Time
	LastActivity    time.Time
	Participants    []ParticipantSnapshot
	MaxParticipants int
	Visibility      uint8
	Metadata        map[string]any
}

// Repository persists room snapshots. Implementations MUST be safe for
// concurrent use and MUST honor context cancellation. All methods are
// write-through best-effort from the manager's perspective: a persistence error
// is logged and surfaced but never blocks or fails a runtime room operation
// (the in-memory state remains authoritative).
type Repository interface {
	// Save inserts or updates the snapshot for snap.ID.
	Save(ctx context.Context, snap Snapshot) error
	// Load returns the snapshot for id, or ErrNotFound.
	Load(ctx context.Context, id string) (Snapshot, error)
	// Delete removes the snapshot for id. Deleting an absent id returns
	// ErrNotFound.
	Delete(ctx context.Context, id string) error
	// List returns all persisted snapshots (used to restore rooms on startup).
	List(ctx context.Context) ([]Snapshot, error)
}
