// Package collaboration implements the core document collaboration, snapshotting, and synchronization engine.
package collaboration

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"cpip/internal/collaboration/config"
	"cpip/internal/collaboration/events"
	"cpip/internal/collaboration/metrics"
	"cpip/internal/collaboration/recovery"
	"cpip/internal/collaboration/registry"
	"cpip/internal/collaboration/snapshot"
	"cpip/internal/collaboration/storage"
	collabSync "cpip/internal/collaboration/sync"
	"cpip/internal/collaboration/types"
	"cpip/internal/collaboration/yjs"
	"cpip/internal/id"
)

// Manager is the top-level orchestrator for the collaboration system.
type Manager struct {
	cfg     config.Config
	reg     *registry.Registry
	repo    storage.Repository
	syncEng *collabSync.Engine
	snapMgr *snapshot.Manager
	recMgr  *recovery.Manager
	bus     *events.Bus
	metrics metrics.Recorder
	log     *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Params configures the collaboration.Manager.
type Params struct {
	Config  config.Config
	Repo    storage.Repository
	Metrics metrics.Recorder
	Logger  *slog.Logger
}

// NewManager constructs a collaboration.Manager.
func NewManager(p Params) *Manager {
	if p.Metrics == nil {
		p.Metrics = metrics.NewNoop()
	}
	if p.Logger == nil {
		p.Logger = slog.Default()
	}

	bus := events.New(events.Options{
		OnPublish: func() {
			p.Metrics.EventPublished()
		},
		OnDrop: func() {
			p.Metrics.EventDropped()
		},
	})

	reg := registry.New()
	syncEng := collabSync.NewEngine()
	snapMgr := snapshot.NewManager(p.Repo, p.Config.RetentionCount)
	recMgr := recovery.NewManager(p.Repo)

	ctx, cancel := context.WithCancel(context.Background())

	return &Manager{
		cfg:     p.Config,
		reg:     reg,
		repo:    p.Repo,
		syncEng: syncEng,
		snapMgr: snapMgr,
		recMgr:  recMgr,
		bus:     bus,
		metrics: p.Metrics,
		log:     p.Logger,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Start spawns background snapshot and cleanup workers.
func (m *Manager) Start() {
	m.wg.Add(2)
	go m.saverLoop()
	go m.janitorLoop()
}

// Stop shuts down the background loops and events bus.
func (m *Manager) Stop() {
	m.cancel()
	m.wg.Wait()
	m.bus.Close()
}

// Registry returns the active document registry.
func (m *Manager) Registry() *registry.Registry {
	return m.reg
}

// Events returns the event bus.
func (m *Manager) Events() *events.Bus {
	return m.bus
}

// GetOrCreateDocument retrieves an existing document from cache/storage or creates a new one.
func (m *Manager) GetOrCreateDocument(ctx context.Context, docID, roomID, filePath string) (*yjs.DocWrapper, error) {
	// 1. Try to load from registry
	if entry, ok := m.reg.Get(docID); ok {
		return entry.Doc, nil
	}

	// 2. Query repository metadata to see if it exists
	meta, err := m.repo.GetMetadata(ctx, docID)
	if err == nil {
		// Document exists in persistent storage. Perform recovery.
		doc, finalVer, err := m.recMgr.RecoverDocument(ctx, docID)
		if err != nil {
			return nil, fmt.Errorf("failed to recover document: %w", err)
		}

		entry := &registry.DocumentEntry{
			ID:         docID,
			RoomID:     roomID,
			FilePath:   filePath,
			State:      types.StateRecovered,
			Doc:        doc,
			LastAccess: time.Now(),
			IsDirty:    false,
			EditCount:  int(finalVer),
			CreatedAt:  meta.CreatedAt,
			UpdatedAt:  meta.UpdatedAt,
		}

		if err := m.reg.Register(entry); err != nil {
			return nil, err
		}

		if err := m.reg.Transition(docID, types.StateActive); err != nil {
			return nil, err
		}

		m.bus.Publish(events.Event{
			Type:      events.DocumentRecovered,
			DocID:     docID,
			RoomID:    roomID,
			Timestamp: time.Now(),
		})
		m.bus.Publish(events.Event{
			Type:      events.DocumentInitialized,
			DocID:     docID,
			RoomID:    roomID,
			Timestamp: time.Now(),
		})
		return doc, nil
	}

	if !errors.Is(err, types.ErrDocumentNotFound) {
		return nil, fmt.Errorf("failed to query metadata: %w", err)
	}

	// 3. Create a brand-new document
	now := time.Now()
	newMeta := types.DocumentMetadata{
		ID:        docID,
		RoomID:    roomID,
		FilePath:  filePath,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := m.repo.SaveMetadata(ctx, newMeta); err != nil {
		return nil, fmt.Errorf("failed to save metadata: %w", err)
	}

	doc := yjs.NewDocWrapper()
	entry := &registry.DocumentEntry{
		ID:         docID,
		RoomID:     roomID,
		FilePath:   filePath,
		State:      types.StateCreated,
		Doc:        doc,
		LastAccess: now,
		IsDirty:    false,
		EditCount:  0,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	if err := m.reg.Register(entry); err != nil {
		return nil, err
	}

	if err := m.reg.Transition(docID, types.StateInitialized); err != nil {
		return nil, err
	}

	m.metrics.DocumentCreated()
	m.bus.Publish(events.Event{
		Type:      events.DocumentCreated,
		DocID:     docID,
		RoomID:    roomID,
		Timestamp: now,
	})
	m.bus.Publish(events.Event{
		Type:      events.DocumentInitialized,
		DocID:     docID,
		RoomID:    roomID,
		Timestamp: now,
	})

	return doc, nil
}

// HandleSyncStep1 returns the client the server delta update relative to the client's state vector.
func (m *Manager) HandleSyncStep1(ctx context.Context, docID string, clientStateVector []byte) ([]byte, error) {
	entry, ok := m.reg.Get(docID)
	if !ok {
		return nil, types.ErrDocumentNotFound
	}

	updateBytes, err := m.syncEng.GenerateSyncStep2(entry.Doc, clientStateVector)
	if err != nil {
		return nil, fmt.Errorf("failed generating sync step 2: %w", err)
	}

	m.metrics.SyncHandshakeCompleted()
	m.bus.Publish(events.Event{
		Type:      events.SyncStepCompleted,
		DocID:     docID,
		RoomID:    entry.RoomID,
		Timestamp: time.Now(),
	})

	return updateBytes, nil
}

// ApplyIncrementalUpdate integrates client updates, logs updates to storage, and flags documents as dirty.
func (m *Manager) ApplyIncrementalUpdate(ctx context.Context, docID string, updateBytes []byte) error {
	if int64(len(updateBytes)) > m.cfg.MaxDocumentSize {
		return types.ErrDocumentSizeExceeded
	}

	entry, ok := m.reg.Get(docID)
	if !ok {
		return types.ErrDocumentNotFound
	}

	// Apply update in-memory
	if err := m.syncEng.ApplyUpdate(entry.Doc, updateBytes); err != nil {
		return fmt.Errorf("failed to apply update to YDoc: %w", err)
	}

	// Atomically mark document as edited and update lifecycle state
	newEditCount := m.reg.MarkEdited(docID)

	// Save update incrementally to disk/log for crash durability
	up := types.Update{
		ID:        id.NewWithPrefix("up"),
		DocID:     docID,
		Data:      updateBytes,
		Timestamp: time.Now(),
		Version:   uint64(newEditCount),
	}

	if err := m.repo.SaveUpdate(ctx, up); err != nil {
		return fmt.Errorf("failed to persist incremental update: %w", err)
	}

	m.metrics.UpdateApplied(len(updateBytes))
	return nil
}

// SaveSnapshot forces taking a snapshot of the current state and rotating old ones.
func (m *Manager) SaveSnapshot(ctx context.Context, docID string) error {
	entry, ok := m.reg.Get(docID)
	if !ok {
		return types.ErrDocumentNotFound
	}

	// Transition to SnapshotPending
	if err := m.reg.Transition(docID, types.StateSnapshotPending); err != nil {
		return err
	}

	start := time.Now()
	version := uint64(entry.EditCount)

	// Take snapshot and save
	_, err := m.snapMgr.TakeSnapshot(ctx, docID, entry.Doc, version)
	if err != nil {
		// Roll back state to Dirty on failure
		_ = m.reg.Transition(docID, types.StateDirty)
		return fmt.Errorf("snapshot generation failed: %w", err)
	}

	// Prune incremental updates that are now compiled in the snapshot
	if err := m.repo.DeleteUpdates(ctx, docID, version); err != nil {
		m.log.Warn("failed to delete old incremental updates", "docID", docID, "err", err)
	}

	// Transition to Persisted
	if err := m.reg.Transition(docID, types.StatePersisted); err != nil {
		return err
	}

	m.reg.SetDirty(docID, false)
	m.reg.ResetEdits(docID)

	durationMs := time.Since(start).Milliseconds()
	m.metrics.SnapshotCreated(durationMs)
	m.metrics.DocumentSaved()

	m.bus.Publish(events.Event{
		Type:      events.SnapshotCreated,
		DocID:     docID,
		RoomID:    entry.RoomID,
		Timestamp: time.Now(),
	})
	m.bus.Publish(events.Event{
		Type:      events.DocumentSaved,
		DocID:     docID,
		RoomID:    entry.RoomID,
		Timestamp: time.Now(),
	})

	return nil
}

// ArchiveDocument forces flushing and unloading the document from active memory.
func (m *Manager) ArchiveDocument(ctx context.Context, docID string) error {
	entry, ok := m.reg.Get(docID)
	if !ok {
		return types.ErrDocumentNotFound
	}

	// 1. If dirty, flush state via snapshot first
	if entry.IsDirty {
		if err := m.SaveSnapshot(ctx, docID); err != nil {
			return fmt.Errorf("failed to save snapshot during archive: %w", err)
		}
	}

	// 2. Transition state to Archived
	if err := m.reg.Transition(docID, types.StateArchived); err != nil {
		return err
	}

	// 3. Remove from active cache
	m.reg.Unregister(docID)

	m.metrics.DocumentArchived()
	m.bus.Publish(events.Event{
		Type:      events.DocumentArchived,
		DocID:     docID,
		RoomID:    entry.RoomID,
		Timestamp: time.Now(),
	})

	return nil
}

// DeleteDocument cleans up all memory and persistent updates/snapshots for a document.
func (m *Manager) DeleteDocument(ctx context.Context, docID string) error {
	entry, ok := m.reg.Unregister(docID)
	var roomID string
	if ok {
		roomID = entry.RoomID
		_ = m.reg.Transition(docID, types.StateDestroyed)
	}

	if err := m.repo.DeleteDocument(ctx, docID); err != nil {
		return fmt.Errorf("failed to delete document storage: %w", err)
	}

	m.metrics.DocumentDestroyed()
	m.bus.Publish(events.Event{
		Type:      events.DocumentDestroyed,
		DocID:     docID,
		RoomID:    roomID,
		Timestamp: time.Now(),
	})

	return nil
}

func (m *Manager) saverLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.cfg.BackgroundSaveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.saveDirtyDocuments()
		}
	}
}

func (m *Manager) saveDirtyDocuments() {
	dirty := m.reg.ListDirty()
	for _, entry := range dirty {
		timeSinceLastSave := time.Since(entry.UpdatedAt)
		if timeSinceLastSave >= m.cfg.SnapshotInterval || entry.EditCount >= m.cfg.SnapshotEditsThreshold {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := m.SaveSnapshot(ctx, entry.ID); err != nil {
				m.log.Error("failed to save scheduled snapshot", "docID", entry.ID, "err", err)
			}
			cancel()
		}
	}
}

func (m *Manager) janitorLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.cfg.BackgroundSaveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.archiveIdleDocuments()
		}
	}
}

func (m *Manager) archiveIdleDocuments() {
	idleDocIDs := m.reg.ListIdle(m.cfg.IdleTimeout)
	for _, docID := range idleDocIDs {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := m.ArchiveDocument(ctx, docID); err != nil {
			m.log.Error("failed to archive idle document", "docID", docID, "err", err)
		}
		cancel()
	}
}
