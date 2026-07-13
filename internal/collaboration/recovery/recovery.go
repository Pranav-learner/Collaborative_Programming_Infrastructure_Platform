package recovery

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"cpip/internal/collaboration/storage"
	"cpip/internal/collaboration/types"
	"cpip/internal/collaboration/yjs"
)

// Manager coordinates document state recovery.
type Manager struct {
	repo storage.Repository
}

// NewManager constructs a recovery Manager.
func NewManager(repo storage.Repository) *Manager {
	return &Manager{
		repo: repo,
	}
}

// RecoverDocument reconstructs the current state of a document from its latest snapshot
// and any subsequent incremental updates.
func (m *Manager) RecoverDocument(ctx context.Context, docID string) (*yjs.DocWrapper, uint64, error) {
	doc := yjs.NewDocWrapper()
	var startVersion uint64 = 0

	// 1. Attempt to load the latest snapshot
	snapshot, err := m.repo.GetLatestSnapshot(ctx, docID)
	if err == nil {
		// Snapshot found, apply it to the document
		if err := doc.ApplyUpdate(snapshot.Data); err != nil {
			return nil, 0, fmt.Errorf("failed to apply snapshot update: %w", err)
		}
		startVersion = snapshot.Version
	} else if !errors.Is(err, types.ErrSnapshotNotFound) {
		// Some other database error
		return nil, 0, fmt.Errorf("failed to query latest snapshot: %w", err)
	}

	// 2. Query and sort incremental updates after startVersion
	updates, err := m.repo.GetUpdates(ctx, docID, startVersion)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query incremental updates: %w", err)
	}

	// If no snapshot exists and no updates exist, the document is not found
	if startVersion == 0 && len(updates) == 0 {
		return nil, 0, types.ErrDocumentNotFound
	}

	// Sort updates chronologically by version
	sort.Slice(updates, func(i, j int) bool {
		return updates[i].Version < updates[j].Version
	})

	// 3. Sequentially replay updates onto the document state
	finalVersion := startVersion
	for _, up := range updates {
		if err := doc.ApplyUpdate(up.Data); err != nil {
			return nil, 0, fmt.Errorf("failed to apply incremental update (version %d): %w", up.Version, err)
		}
		finalVersion = up.Version
	}

	return doc, finalVersion, nil
}
