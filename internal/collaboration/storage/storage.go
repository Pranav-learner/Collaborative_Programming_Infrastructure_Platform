package storage

import (
	"context"
	"sync"
	"time"

	"cpip/internal/collaboration/types"
)

// Repository defines the storage interface for document metadata, delta updates, and snapshots.
type Repository interface {
	GetMetadata(ctx context.Context, docID string) (types.DocumentMetadata, error)
	SaveMetadata(ctx context.Context, meta types.DocumentMetadata) error

	SaveUpdate(ctx context.Context, update types.Update) error
	GetUpdates(ctx context.Context, docID string, sinceVersion uint64) ([]types.Update, error)
	DeleteUpdates(ctx context.Context, docID string, beforeVersion uint64) error

	SaveSnapshot(ctx context.Context, snapshot types.Snapshot) error
	GetLatestSnapshot(ctx context.Context, docID string) (types.Snapshot, error)
	GetSnapshots(ctx context.Context, docID string) ([]types.Snapshot, error)
	DeleteSnapshots(ctx context.Context, docID string) error
	PruneSnapshots(ctx context.Context, docID string, keepCount int) error

	DeleteDocument(ctx context.Context, docID string) error
}

// InMemoryRepository is a thread-safe, in-memory implementation of Repository.
type InMemoryRepository struct {
	mu        sync.RWMutex
	meta      map[string]types.DocumentMetadata
	updates   map[string][]types.Update
	snapshots map[string][]types.Snapshot
}

// NewInMemoryRepository constructs an InMemoryRepository.
func NewInMemoryRepository() *InMemoryRepository {
	return &InMemoryRepository{
		meta:      make(map[string]types.DocumentMetadata),
		updates:   make(map[string][]types.Update),
		snapshots: make(map[string][]types.Snapshot),
	}
}

// GetMetadata retrieves metadata for a document.
func (r *InMemoryRepository) GetMetadata(ctx context.Context, docID string) (types.DocumentMetadata, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	meta, ok := r.meta[docID]
	if !ok {
		return types.DocumentMetadata{}, types.ErrDocumentNotFound
	}
	return meta, nil
}

// SaveMetadata stores or updates metadata for a document.
func (r *InMemoryRepository) SaveMetadata(ctx context.Context, meta types.DocumentMetadata) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.meta[meta.ID] = meta
	return nil
}

// SaveUpdate appends a new delta update block to the document's update history.
func (r *InMemoryRepository) SaveUpdate(ctx context.Context, update types.Update) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if update.Timestamp.IsZero() {
		update.Timestamp = time.Now()
	}
	r.updates[update.DocID] = append(r.updates[update.DocID], update)
	return nil
}

// GetUpdates retrieves all delta updates for a document whose version is greater than sinceVersion.
func (r *InMemoryRepository) GetUpdates(ctx context.Context, docID string, sinceVersion uint64) ([]types.Update, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	all := r.updates[docID]
	var filtered []types.Update
	for _, u := range all {
		if u.Version > sinceVersion {
			filtered = append(filtered, u)
		}
	}
	return filtered, nil
}

// DeleteUpdates deletes all updates for a document whose version is less than beforeVersion.
func (r *InMemoryRepository) DeleteUpdates(ctx context.Context, docID string, beforeVersion uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	all := r.updates[docID]
	var kept []types.Update
	for _, u := range all {
		if u.Version >= beforeVersion {
			kept = append(kept, u)
		}
	}
	r.updates[docID] = kept
	return nil
}

// SaveSnapshot stores a new document snapshot and maintains chronological ordering.
func (r *InMemoryRepository) SaveSnapshot(ctx context.Context, snapshot types.Snapshot) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if snapshot.Timestamp.IsZero() {
		snapshot.Timestamp = time.Now()
	}
	r.snapshots[snapshot.DocID] = append(r.snapshots[snapshot.DocID], snapshot)
	return nil
}

// GetLatestSnapshot retrieves the most recent snapshot for a document.
func (r *InMemoryRepository) GetLatestSnapshot(ctx context.Context, docID string) (types.Snapshot, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	snaps, ok := r.snapshots[docID]
	if !ok || len(snaps) == 0 {
		return types.Snapshot{}, types.ErrSnapshotNotFound
	}
	// Return the last snapshot (newest)
	return snaps[len(snaps)-1], nil
}

// GetSnapshots retrieves all snapshots for a document in chronological order.
func (r *InMemoryRepository) GetSnapshots(ctx context.Context, docID string) ([]types.Snapshot, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	snaps, ok := r.snapshots[docID]
	if !ok || len(snaps) == 0 {
		return nil, types.ErrSnapshotNotFound
	}
	// Return a copy to prevent external mutation
	copied := make([]types.Snapshot, len(snaps))
	copy(copied, snaps)
	return copied, nil
}

// PruneSnapshots removes older snapshots keeping only the newest keepCount snapshots.
func (r *InMemoryRepository) PruneSnapshots(ctx context.Context, docID string, keepCount int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	snaps, ok := r.snapshots[docID]
	if !ok || len(snaps) <= keepCount {
		return nil
	}
	r.snapshots[docID] = snaps[len(snaps)-keepCount:]
	return nil
}

// DeleteSnapshots removes all snapshots for a document.
func (r *InMemoryRepository) DeleteSnapshots(ctx context.Context, docID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.snapshots, docID)
	return nil
}

// DeleteDocument removes the document metadata, updates, and snapshots.
func (r *InMemoryRepository) DeleteDocument(ctx context.Context, docID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.meta, docID)
	delete(r.updates, docID)
	delete(r.snapshots, docID)
	return nil
}
