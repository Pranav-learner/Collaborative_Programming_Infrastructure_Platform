package types

import (
	"errors"
	"time"
)

// DocumentState represents the current state of a document in its lifecycle.
type DocumentState uint8

const (
	// StateCreated represents a document that is registered but not yet loaded or initialized.
	StateCreated DocumentState = iota
	// StateInitialized represents a document that is allocated and ready for edits.
	StateInitialized
	// StateActive represents a document that is actively being edited by connected peers.
	StateActive
	// StateDirty represents a document that has unsaved modifications since the last snapshot.
	StateDirty
	// StateSnapshotPending represents a document whose snapshot is currently scheduled/saving.
	StateSnapshotPending
	// StatePersisted represents a document whose current state is successfully persisted.
	StatePersisted
	// StateRecovered represents a document reconstructed from a snapshot and updates.
	StateRecovered
	// StateArchived represents a document that has been unloaded from active memory.
	StateArchived
	// StateDestroyed represents a document that has been deleted.
	StateDestroyed
)

// String returns the string representation of a DocumentState.
func (s DocumentState) String() string {
	switch s {
	case StateCreated:
		return "Created"
	case StateInitialized:
		return "Initialized"
	case StateActive:
		return "Active"
	case StateDirty:
		return "Dirty"
	case StateSnapshotPending:
		return "SnapshotPending"
	case StatePersisted:
		return "Persisted"
	case StateRecovered:
		return "Recovered"
	case StateArchived:
		return "Archived"
	case StateDestroyed:
		return "Destroyed"
	default:
		return "Unknown"
	}
}

// CanTransition validates document lifecycle transitions.
func CanTransition(from, to DocumentState) bool {
	switch from {
	case StateCreated:
		return to == StateInitialized || to == StateDestroyed
	case StateInitialized:
		return to == StateActive || to == StateArchived || to == StateDestroyed
	case StateActive:
		return to == StateDirty || to == StateArchived || to == StateSnapshotPending || to == StateDestroyed
	case StateDirty:
		return to == StateSnapshotPending || to == StatePersisted || to == StateActive || to == StateArchived || to == StateDestroyed
	case StateSnapshotPending:
		return to == StatePersisted || to == StateActive || to == StateDirty || to == StateDestroyed
	case StatePersisted:
		return to == StateActive || to == StateArchived || to == StateDestroyed
	case StateRecovered:
		return to == StateActive || to == StateInitialized || to == StateDestroyed
	case StateArchived:
		return to == StateRecovered || to == StateDestroyed
	case StateDestroyed:
		return false
	default:
		return false
	}
}

// DocumentMetadata stores structural metadata about a document.
type DocumentMetadata struct {
	ID        string    `json:"id"`
	RoomID    string    `json:"room_id"`
	FilePath  string    `json:"file_path"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Update represents a single binary delta update block.
type Update struct {
	ID        string    `json:"id"`
	DocID     string    `json:"doc_id"`
	Data      []byte    `json:"data"`
	Timestamp time.Time `json:"timestamp"`
	Version   uint64    `json:"version"`
}

// Snapshot represents a full serialized document snapshot at a point in time.
type Snapshot struct {
	ID          string    `json:"id"`
	DocID       string    `json:"doc_id"`
	StateVector []byte    `json:"state_vector"`
	Data        []byte    `json:"data"`
	Timestamp   time.Time `json:"timestamp"`
	Version     uint64    `json:"version"`
}

var (
	ErrInvalidDocumentState  = errors.New("collaboration: invalid document state transition")
	ErrDocumentNotFound      = errors.New("collaboration: document not found")
	ErrDocumentSizeExceeded  = errors.New("collaboration: document size exceeds configured limit")
	ErrSnapshotNotFound      = errors.New("collaboration: snapshot not found")
	ErrRegistryConflict      = errors.New("collaboration: document ID already exists in registry")
)
