// Package types defines the shared, dependency-free domain model for the
// collaboration engine: document lifecycle states, synchronization status,
// wire structures (updates, snapshots), participant descriptors, and the
// canonical error set.
//
// This package imports only the standard library so that every other
// collaboration package may depend on it without risking an import cycle.
package types

import (
	"errors"
	"time"
)

// DocumentState represents the current state of a document in its lifecycle.
//
// The lifecycle is a formal state machine; permitted transitions are enforced
// by CanTransition. The nominal happy path is:
//
//	Created → Initialized → Active → Dirty → SnapshotPending → Persisted
//	                                                              ↓
//	                                            Archived ← ── ── ─┘
//	Archived → Recovered → Active   (on re-open)
//	<any>    → Destroyed             (on delete)
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

// IsTerminal reports whether the state admits no further transitions.
func (s DocumentState) IsTerminal() bool { return s == StateDestroyed }

// CanTransition validates document lifecycle transitions. It is the single
// source of truth for the lifecycle state machine and is consulted by both the
// document entity and the registry.
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
		return to == StateActive || to == StateDirty || to == StateArchived || to == StateDestroyed
	case StateRecovered:
		return to == StateActive || to == StateInitialized || to == StateArchived || to == StateDestroyed
	case StateArchived:
		return to == StateRecovered || to == StateDestroyed
	case StateDestroyed:
		return false
	default:
		return false
	}
}

// SyncStatus describes the synchronization status of a document or participant.
type SyncStatus uint8

const (
	// SyncIdle indicates no synchronization is in progress.
	SyncIdle SyncStatus = iota
	// SyncInProgress indicates a synchronization handshake is underway.
	SyncInProgress
	// SyncSynced indicates the peer/document is fully synchronized.
	SyncSynced
	// SyncFailed indicates the last synchronization attempt failed.
	SyncFailed
)

// String returns the string representation of a SyncStatus.
func (s SyncStatus) String() string {
	switch s {
	case SyncIdle:
		return "Idle"
	case SyncInProgress:
		return "InProgress"
	case SyncSynced:
		return "Synced"
	case SyncFailed:
		return "Failed"
	default:
		return "Unknown"
	}
}

// SnapshotKind distinguishes full snapshots from incremental deltas.
type SnapshotKind uint8

const (
	// SnapshotFull is a complete, self-contained serialization of document state.
	SnapshotFull SnapshotKind = iota
	// SnapshotIncremental is a delta relative to the preceding snapshot's state vector.
	SnapshotIncremental
)

// String returns the string representation of a SnapshotKind.
func (k SnapshotKind) String() string {
	switch k {
	case SnapshotFull:
		return "Full"
	case SnapshotIncremental:
		return "Incremental"
	default:
		return "Unknown"
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

// Update represents a single binary delta update block in the durable update log.
type Update struct {
	ID        string    `json:"id"`
	DocID     string    `json:"doc_id"`
	Data      []byte    `json:"data"`
	Timestamp time.Time `json:"timestamp"`
	Version   uint64    `json:"version"`
}

// Snapshot represents a serialized document snapshot at a point in time. A
// snapshot may be Full (self-contained) or Incremental (a delta since BaseVersion).
type Snapshot struct {
	ID          string       `json:"id"`
	DocID       string       `json:"doc_id"`
	Kind        SnapshotKind `json:"kind"`
	StateVector []byte       `json:"state_vector"`
	Data        []byte       `json:"data"`
	Compressed  bool         `json:"compressed"`
	BaseVersion uint64       `json:"base_version"`
	Version     uint64       `json:"version"`
	Size        int64        `json:"size"`
	Timestamp   time.Time    `json:"timestamp"`
}

// Participant describes a peer connected to a document. The ID is the stable
// participant/user identifier owned by the presence subsystem; ClientID is the
// underlying Yjs client identifier used for update attribution.
type Participant struct {
	ID           string     `json:"id"`
	ClientID     uint32     `json:"client_id"`
	JoinedAt     time.Time  `json:"joined_at"`
	LastSyncedAt time.Time  `json:"last_synced_at"`
	SyncStatus   SyncStatus `json:"sync_status"`
}

// Statistics is an immutable snapshot of a document's runtime counters, safe to
// copy across goroutine boundaries.
type Statistics struct {
	Version          uint64    `json:"version"`
	EditCount        uint64    `json:"edit_count"`
	UpdatesApplied   uint64    `json:"updates_applied"`
	UpdatesGenerated uint64    `json:"updates_generated"`
	BytesApplied     uint64    `json:"bytes_applied"`
	BytesGenerated   uint64    `json:"bytes_generated"`
	ParticipantCount int       `json:"participant_count"`
	SizeBytes        int64     `json:"size_bytes"`
	LastModified     time.Time `json:"last_modified"`
}

// SnapshotMeta records the last snapshot taken for a document.
type SnapshotMeta struct {
	LastSnapshotID      string       `json:"last_snapshot_id"`
	LastSnapshotKind    SnapshotKind `json:"last_snapshot_kind"`
	LastSnapshotVersion uint64       `json:"last_snapshot_version"`
	LastSnapshotAt      time.Time    `json:"last_snapshot_at"`
	SnapshotCount       uint64       `json:"snapshot_count"`
}

// RecoveryMeta records the last recovery performed for a document.
type RecoveryMeta struct {
	Recovered        bool      `json:"recovered"`
	FromSnapshotID   string    `json:"from_snapshot_id"`
	UpdatesReplayed  int       `json:"updates_replayed"`
	RecoveredAt      time.Time `json:"recovered_at"`
	RecoveredVersion uint64    `json:"recovered_version"`
}

// PersistenceMeta records the durability status of a document.
type PersistenceMeta struct {
	LastPersistedVersion uint64    `json:"last_persisted_version"`
	LastPersistedAt      time.Time `json:"last_persisted_at"`
	PendingUpdates       int       `json:"pending_updates"`
}

// The canonical collaboration error set. Callers should compare with errors.Is.
var (
	// ErrInvalidDocumentState indicates an illegal lifecycle state transition.
	ErrInvalidDocumentState = errors.New("collaboration: invalid document state transition")
	// ErrDocumentNotFound indicates the requested document does not exist.
	ErrDocumentNotFound = errors.New("collaboration: document not found")
	// ErrUnknownDocument is an alias returned by public APIs for an unknown document ID.
	ErrUnknownDocument = ErrDocumentNotFound
	// ErrDocumentDestroyed indicates the document has been destroyed and is unusable.
	ErrDocumentDestroyed = errors.New("collaboration: document has been destroyed")
	// ErrDocumentSizeExceeded indicates a document exceeds the configured maximum size.
	ErrDocumentSizeExceeded = errors.New("collaboration: document size exceeds configured limit")
	// ErrUpdateSizeExceeded indicates a single update exceeds the configured maximum size.
	ErrUpdateSizeExceeded = errors.New("collaboration: update size exceeds configured limit")
	// ErrMalformedUpdate indicates an update payload is empty or structurally invalid.
	ErrMalformedUpdate = errors.New("collaboration: malformed update payload")
	// ErrCorruptedUpdate indicates an update could not be decoded/applied by the CRDT engine.
	ErrCorruptedUpdate = errors.New("collaboration: corrupted update payload")
	// ErrInvalidStateVector indicates a state vector could not be decoded.
	ErrInvalidStateVector = errors.New("collaboration: invalid state vector")
	// ErrSyncTimeout indicates a synchronization operation exceeded its deadline.
	ErrSyncTimeout = errors.New("collaboration: synchronization timeout")
	// ErrSnapshotNotFound indicates no snapshot exists for the document.
	ErrSnapshotNotFound = errors.New("collaboration: snapshot not found")
	// ErrSnapshotFailure indicates a snapshot could not be created or persisted.
	ErrSnapshotFailure = errors.New("collaboration: snapshot failure")
	// ErrRecoveryFailure indicates a document could not be recovered.
	ErrRecoveryFailure = errors.New("collaboration: recovery failure")
	// ErrRecoveryTimeout indicates a recovery operation exceeded its deadline.
	ErrRecoveryTimeout = errors.New("collaboration: recovery timeout")
	// ErrConsistencyCheckFailed indicates a post-recovery consistency check failed.
	ErrConsistencyCheckFailed = errors.New("collaboration: consistency verification failed")
	// ErrConcurrentModification indicates a conflicting concurrent structural mutation.
	ErrConcurrentModification = errors.New("collaboration: concurrent modification conflict")
	// ErrVersionMismatch indicates an operation targeted an unexpected document version.
	ErrVersionMismatch = errors.New("collaboration: version mismatch")
	// ErrRegistryConflict indicates a document ID already exists in the registry.
	ErrRegistryConflict = errors.New("collaboration: document ID already exists in registry")
)
