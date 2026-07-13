package registry

import (
	"sync"
	"time"

	"cpip/internal/collaboration/types"
	"cpip/internal/collaboration/yjs"
)

// DocumentEntry represents an active document managed in memory.
//
// All fields are guarded by the owning Registry's mutex. Callers must not mutate
// an entry returned by Get without holding that lock; the read-oriented accessors
// (Participants, snapshots via DocumentInfo) return copies for safe consumption.
type DocumentEntry struct {
	ID       string
	RoomID   string
	FilePath string
	State    types.DocumentState
	Doc      *yjs.DocWrapper
	// Version is the monotonic document version: it advances on every applied
	// update and never regresses (in particular it is NOT reset by snapshots).
	// It orders the durable update log and anchors snapshot base versions.
	Version uint64
	// EditCount counts edits since the last snapshot; it drives the snapshot
	// edit-threshold and is reset to zero when a snapshot is recorded.
	EditCount   int
	LastAccess  time.Time
	IsDirty     bool
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Snapshot    types.SnapshotMeta
	Recovery    types.RecoveryMeta
	Persistence types.PersistenceMeta

	participants map[string]types.Participant
}

// Registry manages thread-safe storage and state of active collaborative documents.
type Registry struct {
	mu      sync.RWMutex
	entries map[string]*DocumentEntry
}

// New constructs a Registry.
func New() *Registry {
	return &Registry{
		entries: make(map[string]*DocumentEntry),
	}
}

// Register adds a new document entry to the registry.
func (r *Registry) Register(entry *DocumentEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.entries[entry.ID]; exists {
		return types.ErrRegistryConflict
	}

	if entry.LastAccess.IsZero() {
		entry.LastAccess = time.Now()
	}
	if entry.participants == nil {
		entry.participants = make(map[string]types.Participant)
	}
	r.entries[entry.ID] = entry
	return nil
}

// Get retrieves a document entry by ID, automatically updating its LastAccess time.
func (r *Registry) Get(docID string) (*DocumentEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.entries[docID]
	if !ok {
		return nil, false
	}
	entry.LastAccess = time.Now()
	return entry, true
}

// Unregister removes a document entry from the registry.
func (r *Registry) Unregister(docID string) (*DocumentEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.entries[docID]
	if !ok {
		return nil, false
	}
	delete(r.entries, docID)
	return entry, true
}

// Transition moves a document to a new state if the transition is valid.
func (r *Registry) Transition(docID string, to types.DocumentState) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.entries[docID]
	if !ok {
		return types.ErrDocumentNotFound
	}

	if !types.CanTransition(entry.State, to) {
		return types.ErrInvalidDocumentState
	}

	entry.State = to
	entry.UpdatedAt = time.Now()
	return nil
}

// MarkEdited advances the monotonic version, increments the since-snapshot edit
// count, marks the document dirty, drives lifecycle transitions, and returns the
// new monotonic version — all atomically under the registry lock.
func (r *Registry) MarkEdited(docID string) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.entries[docID]
	if !ok {
		return 0
	}

	now := time.Now()
	entry.LastAccess = now
	entry.UpdatedAt = now
	entry.Version++
	entry.EditCount++
	entry.IsDirty = true
	entry.Persistence.PendingUpdates++

	// Handle transitions atomically along the edit path.
	if entry.State == types.StateInitialized || entry.State == types.StatePersisted || entry.State == types.StateRecovered {
		if types.CanTransition(entry.State, types.StateActive) {
			entry.State = types.StateActive
		}
	}
	if entry.State == types.StateActive {
		if types.CanTransition(entry.State, types.StateDirty) {
			entry.State = types.StateDirty
		}
	}

	return entry.Version
}

// SetDirty sets the dirty flag of a document.
func (r *Registry) SetDirty(docID string, isDirty bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if entry, ok := r.entries[docID]; ok {
		entry.IsDirty = isDirty
		entry.UpdatedAt = time.Now()
	}
}

// IncrementEdits increments the edit count of a document and returns the new value.
func (r *Registry) IncrementEdits(docID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	if entry, ok := r.entries[docID]; ok {
		entry.EditCount++
		entry.UpdatedAt = time.Now()
		return entry.EditCount
	}
	return 0
}

// ResetEdits resets the edit count of a document.
func (r *Registry) ResetEdits(docID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if entry, ok := r.entries[docID]; ok {
		entry.EditCount = 0
		entry.UpdatedAt = time.Now()
	}
}

// AddParticipant records a participant as connected to a document and returns
// the resulting participant count and whether the document exists. Re-adding an
// existing participant updates its descriptor idempotently.
func (r *Registry) AddParticipant(docID string, p types.Participant) (int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.entries[docID]
	if !ok {
		return 0, false
	}
	if entry.participants == nil {
		entry.participants = make(map[string]types.Participant)
	}
	entry.participants[p.ID] = p
	entry.LastAccess = time.Now()
	return len(entry.participants), true
}

// RemoveParticipant drops a participant from a document and returns the resulting
// participant count and whether the participant had been present.
func (r *Registry) RemoveParticipant(docID, participantID string) (int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.entries[docID]
	if !ok {
		return 0, false
	}
	if _, present := entry.participants[participantID]; !present {
		return len(entry.participants), false
	}
	delete(entry.participants, participantID)
	entry.LastAccess = time.Now()
	return len(entry.participants), true
}

// SetParticipantSync updates a participant's synchronization status and last-sync
// timestamp. It is a no-op if the document or participant is unknown.
func (r *Registry) SetParticipantSync(docID, participantID string, status types.SyncStatus, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.entries[docID]
	if !ok {
		return
	}
	p, present := entry.participants[participantID]
	if !present {
		return
	}
	p.SyncStatus = status
	p.LastSyncedAt = at
	entry.participants[participantID] = p
}

// Participants returns a copy of the participants connected to a document.
func (r *Registry) Participants(docID string) []types.Participant {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entry, ok := r.entries[docID]
	if !ok {
		return nil
	}
	out := make([]types.Participant, 0, len(entry.participants))
	for _, p := range entry.participants {
		out = append(out, p)
	}
	return out
}

// ParticipantCount returns the number of participants connected to a document.
func (r *Registry) ParticipantCount(docID string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if entry, ok := r.entries[docID]; ok {
		return len(entry.participants)
	}
	return 0
}

// Count returns the number of active documents in the registry.
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.entries)
}

// SnapshotPlan is a lock-safe view of the inputs needed to take a snapshot: the
// live document handle, the monotonic version to record, and the previous
// snapshot metadata used to decide full-vs-incremental. Doc is a stable pointer
// for the entry's lifetime.
type SnapshotPlan struct {
	Doc     *yjs.DocWrapper
	RoomID  string
	Version uint64
	Prev    types.SnapshotMeta
	Dirty   bool
}

// PlanSnapshot returns a lock-safe SnapshotPlan for a document.
func (r *Registry) PlanSnapshot(docID string) (SnapshotPlan, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entry, ok := r.entries[docID]
	if !ok {
		return SnapshotPlan{}, false
	}
	return SnapshotPlan{
		Doc:     entry.Doc,
		RoomID:  entry.RoomID,
		Version: entry.Version,
		Prev:    entry.Snapshot,
		Dirty:   entry.IsDirty,
	}, true
}

// RecordSnapshot atomically records a completed snapshot: it advances the
// snapshot metadata, updates persistence metadata, resets the since-snapshot
// edit count, and clears the dirty flag. It is the durable-progress counterpart
// to MarkEdited.
func (r *Registry) RecordSnapshot(docID string, snap types.Snapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.entries[docID]
	if !ok {
		return
	}
	entry.Snapshot.LastSnapshotID = snap.ID
	entry.Snapshot.LastSnapshotKind = snap.Kind
	entry.Snapshot.LastSnapshotVersion = snap.Version
	entry.Snapshot.LastSnapshotAt = snap.Timestamp
	entry.Snapshot.SnapshotCount++

	entry.Persistence.LastPersistedVersion = snap.Version
	entry.Persistence.LastPersistedAt = snap.Timestamp
	entry.Persistence.PendingUpdates = 0

	entry.EditCount = 0
	entry.IsDirty = false
	entry.UpdatedAt = time.Now()
}

// SetRecoveryMeta records recovery metadata for a document.
func (r *Registry) SetRecoveryMeta(docID string, meta types.RecoveryMeta) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if entry, ok := r.entries[docID]; ok {
		entry.Recovery = meta
		entry.UpdatedAt = time.Now()
	}
}

// DocumentInfo represents a read-only snapshot copy of a document's metadata and state.
type DocumentInfo struct {
	ID               string
	RoomID           string
	FilePath         string
	State            types.DocumentState
	Version          uint64
	LastAccess       time.Time
	IsDirty          bool
	EditCount        int
	ParticipantCount int
	CreatedAt        time.Time
	UpdatedAt        time.Time
	Snapshot         types.SnapshotMeta
	Recovery         types.RecoveryMeta
	Persistence      types.PersistenceMeta
}

// ListDirty returns read-only snapshot copies of all document entries marked as dirty.
func (r *Registry) ListDirty() []DocumentInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var dirty []DocumentInfo
	for _, entry := range r.entries {
		if entry.IsDirty {
			dirty = append(dirty, entry.info())
		}
	}
	return dirty
}

// ListIdle returns the IDs of all active document entries that have been idle past the threshold.
func (r *Registry) ListIdle(timeout time.Duration) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := time.Now()
	var idle []string
	for _, entry := range r.entries {
		// Only archive active/persisted documents
		if entry.State == types.StateActive || entry.State == types.StatePersisted || entry.State == types.StateDirty {
			if now.Sub(entry.LastAccess) > timeout {
				idle = append(idle, entry.ID)
			}
		}
	}
	return idle
}

// ListAll returns read-only snapshot copies of all document entries.
func (r *Registry) ListAll() []DocumentInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var all []DocumentInfo
	for _, entry := range r.entries {
		all = append(all, entry.info())
	}
	return all
}

// info returns a lock-safe DocumentInfo copy. Callers must hold the registry lock.
func (e *DocumentEntry) info() DocumentInfo {
	return DocumentInfo{
		ID:               e.ID,
		RoomID:           e.RoomID,
		FilePath:         e.FilePath,
		State:            e.State,
		Version:          e.Version,
		LastAccess:       e.LastAccess,
		IsDirty:          e.IsDirty,
		EditCount:        e.EditCount,
		ParticipantCount: len(e.participants),
		CreatedAt:        e.CreatedAt,
		UpdatedAt:        e.UpdatedAt,
		Snapshot:         e.Snapshot,
		Recovery:         e.Recovery,
		Persistence:      e.Persistence,
	}
}

// Info returns a lock-safe DocumentInfo copy for a single document.
func (r *Registry) Info(docID string) (DocumentInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entry, ok := r.entries[docID]
	if !ok {
		return DocumentInfo{}, false
	}
	return entry.info(), true
}
