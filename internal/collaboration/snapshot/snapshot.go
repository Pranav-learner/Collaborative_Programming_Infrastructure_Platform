package snapshot

import (
	"context"
	"fmt"
	"time"

	"cpip/internal/collaboration/storage"
	"cpip/internal/collaboration/types"
	"cpip/internal/collaboration/yjs"
	"cpip/internal/id"
)

// Manager coordinates snapshot taking and retention policies.
type Manager struct {
	repo           storage.Repository
	retentionCount int
}

// NewManager constructs a snapshot Manager.
func NewManager(repo storage.Repository, retentionCount int) *Manager {
	if retentionCount <= 0 {
		retentionCount = 5
	}
	return &Manager{
		repo:           repo,
		retentionCount: retentionCount,
	}
}

// TakeSnapshot serializes the current state of a document, saves it as a snapshot,
// and enforces snapshot retention limits.
func (m *Manager) TakeSnapshot(ctx context.Context, docID string, doc *yjs.DocWrapper, version uint64) (types.Snapshot, error) {
	// Encode entire document state as V1 update bytes
	snapshotData := doc.EncodeStateAsUpdate(nil)
	
	// Encode state vector
	svData := doc.EncodeStateVector()

	snapshot := types.Snapshot{
		ID:          id.NewWithPrefix("snap"),
		DocID:       docID,
		StateVector: svData,
		Data:        snapshotData,
		Timestamp:   time.Now(),
		Version:     version,
	}

	if err := m.repo.SaveSnapshot(ctx, snapshot); err != nil {
		return types.Snapshot{}, fmt.Errorf("failed to save snapshot: %w", err)
	}

	// Enforce retention policy
	if err := m.repo.PruneSnapshots(ctx, docID, m.retentionCount); err != nil {
		// Log or handle prune failure, but don't fail the snapshot taking itself
		// since the snapshot has already been successfully saved.
	}

	return snapshot, nil
}
