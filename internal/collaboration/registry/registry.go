package registry

import (
	"sync"
	"time"

	"cpip/internal/collaboration/types"
	"cpip/internal/collaboration/yjs"
)

// DocumentEntry represents an active document managed in memory.
type DocumentEntry struct {
	ID         string
	RoomID     string
	FilePath   string
	State      types.DocumentState
	Doc        *yjs.DocWrapper
	LastAccess time.Time
	IsDirty    bool
	EditCount  int
	CreatedAt  time.Time
	UpdatedAt  time.Time
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

// MarkEdited updates last access, increments the edit count, marks the document as dirty,
// and handles lifecycle transitions atomically, returning the new edit count.
func (r *Registry) MarkEdited(docID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.entries[docID]
	if !ok {
		return 0
	}

	entry.LastAccess = time.Now()
	entry.EditCount++
	entry.IsDirty = true
	entry.UpdatedAt = time.Now()

	// Handle transitions atomically
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

	return entry.EditCount
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

// DocumentInfo represents a read-only snapshot copy of a document's metadata and state.
type DocumentInfo struct {
	ID         string
	RoomID     string
	FilePath   string
	State      types.DocumentState
	LastAccess time.Time
	IsDirty    bool
	EditCount  int
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// ListDirty returns read-only snapshot copies of all document entries marked as dirty.
func (r *Registry) ListDirty() []DocumentInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var dirty []DocumentInfo
	for _, entry := range r.entries {
		if entry.IsDirty {
			dirty = append(dirty, DocumentInfo{
				ID:         entry.ID,
				RoomID:     entry.RoomID,
				FilePath:   entry.FilePath,
				State:      entry.State,
				LastAccess: entry.LastAccess,
				IsDirty:    entry.IsDirty,
				EditCount:  entry.EditCount,
				CreatedAt:  entry.CreatedAt,
				UpdatedAt:  entry.UpdatedAt,
			})
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
		all = append(all, DocumentInfo{
			ID:         entry.ID,
			RoomID:     entry.RoomID,
			FilePath:   entry.FilePath,
			State:      entry.State,
			LastAccess: entry.LastAccess,
			IsDirty:    entry.IsDirty,
			EditCount:  entry.EditCount,
			CreatedAt:  entry.CreatedAt,
			UpdatedAt:  entry.UpdatedAt,
		})
	}
	return all
}
