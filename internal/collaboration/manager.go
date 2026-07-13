// Package collaboration implements the core collaborative-document engine: it
// owns the lifecycle of live Yjs documents, drives the synchronization protocol,
// schedules snapshots, recovers documents from durable storage, and publishes
// domain events for the rest of the platform (gateway, presence, execution).
//
// The Manager is the composition root. It wires the registry (live in-memory
// index), the synchronization engine, the snapshot and recovery managers, the
// pluggable storage repository, the event bus, and the metrics/logging seams,
// then runs two background loops: a saver (snapshots dirty documents) and a
// janitor (archives idle documents). All CRDT work is delegated to the yjs
// package; the Manager never implements CRDT logic itself.
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
	collablog "cpip/internal/collaboration/logger"
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

// Service is the minimal public surface the collaboration engine exposes to the
// rest of the platform (WebSocket gateway, room manager, presence system,
// future REST/gRPC facades and execution infrastructure). It is intentionally
// small: callers manage documents, exchange sync frames, and observe events.
type Service interface {
	// Document lifecycle.
	GetOrCreateDocument(ctx context.Context, docID, roomID, filePath string) (*yjs.DocWrapper, error)
	ArchiveDocument(ctx context.Context, docID string) error
	DeleteDocument(ctx context.Context, docID string) error

	// Synchronization.
	ServerStateVector(docID string) ([]byte, error)
	HandleSyncStep1(ctx context.Context, docID string, clientStateVector []byte) ([]byte, error)
	InitialSync(ctx context.Context, docID string) ([]byte, error)
	ApplyIncrementalUpdate(ctx context.Context, docID string, update []byte) error
	BatchUpdates(ctx context.Context, docID string, updates [][]byte) error

	// Participants.
	JoinDocument(ctx context.Context, docID, participantID string) error
	LeaveDocument(ctx context.Context, docID, participantID string) error
	MarkParticipantSynced(docID, participantID string)
	Participants(docID string) []types.Participant

	// Durability & introspection.
	SaveSnapshot(ctx context.Context, docID string) error
	Statistics(docID string) (types.Statistics, error)
	Events() *events.Bus
}

// Manager is the top-level orchestrator for the collaboration engine.
type Manager struct {
	cfg     config.Config
	yjsOpts yjs.Options

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

// Compile-time assertion that Manager satisfies the public Service interface.
var _ Service = (*Manager)(nil)

// Params configures the collaboration Manager. Only Repo is strictly required;
// Metrics and Logger default to no-op implementations and Config is normalized.
type Params struct {
	Config  config.Config
	Repo    storage.Repository
	Metrics metrics.Recorder
	Logger  *slog.Logger
}

// NewManager constructs a collaboration Manager, normalizing the configuration
// and wiring every collaborator via dependency injection.
func NewManager(p Params) *Manager {
	if p.Metrics == nil {
		p.Metrics = metrics.NewNoop()
	}
	base := p.Logger
	if base == nil {
		base = slog.Default()
	}
	log := collablog.Named(base, "manager")

	cfg, err := p.Config.Validate()
	if err != nil {
		log.Warn("invalid collaboration config; falling back to defaults", "err", err)
		cfg = config.Default()
	}

	bus := events.New(events.Options{
		OnPublish: p.Metrics.EventPublished,
		OnDrop:    p.Metrics.EventDropped,
	})

	yjsOpts := yjs.Options{GC: cfg.EnableGC}

	snapMgr := snapshot.NewManager(p.Repo, snapshot.Options{
		RetentionCount:       cfg.RetentionCount,
		IncrementalThreshold: cfg.IncrementalSnapshotThreshold,
		Compress:             cfg.EnableCompression,
		CompressionThreshold: cfg.CompressionThreshold,
		Metrics:              p.Metrics,
		Logger:               collablog.Named(base, "snapshot"),
	})

	recMgr := recovery.NewManager(p.Repo, recovery.Options{
		Snapshots:  snapMgr,
		Metrics:    p.Metrics,
		YjsOptions: yjsOpts,
	})

	ctx, cancel := context.WithCancel(context.Background())

	return &Manager{
		cfg:     cfg,
		yjsOpts: yjsOpts,
		reg:     registry.New(),
		repo:    p.Repo,
		syncEng: collabSync.NewEngine(collabSync.WithMetrics(p.Metrics), collabSync.WithBatchSize(cfg.BatchSize)),
		snapMgr: snapMgr,
		recMgr:  recMgr,
		bus:     bus,
		metrics: p.Metrics,
		log:     log,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Start spawns the background saver and janitor workers.
func (m *Manager) Start() {
	m.wg.Add(2)
	go m.saverLoop()
	go m.janitorLoop()
}

// Stop cancels the background loops, waits for them to drain, and closes the bus.
func (m *Manager) Stop() {
	m.cancel()
	m.wg.Wait()
	m.bus.Close()
}

// Registry returns the active document registry (used by observability tooling).
func (m *Manager) Registry() *registry.Registry { return m.reg }

// Events returns the event bus for subscribers.
func (m *Manager) Events() *events.Bus { return m.bus }

// attachUpdateHandler installs the CRDT update observer for a document. Every
// committed transaction — local edit or applied remote update — is republished
// as an UpdateGenerated event carrying the raw update, so the gateway can fan it
// out to peers. The handler runs while the document's write lock is held, so it
// does only cheap, non-reentrant work (metrics + a non-blocking publish).
func (m *Manager) attachUpdateHandler(docID, roomID, filePath string, doc *yjs.DocWrapper) {
	doc.SetUpdateHandler(func(update []byte, origin any) {
		remote := origin == yjs.OriginRemote
		m.metrics.UpdateGenerated(len(update))
		m.bus.Publish(events.Event{
			Type:   events.UpdateGenerated,
			DocID:  docID,
			RoomID: roomID,
			Payload: events.UpdatePayload{
				Data:     update,
				Remote:   remote,
				FilePath: filePath,
			},
		})
	})
}

// GetOrCreateDocument returns the live document for docID, loading it from cache,
// recovering it from durable storage, or creating it fresh — in that order.
func (m *Manager) GetOrCreateDocument(ctx context.Context, docID, roomID, filePath string) (*yjs.DocWrapper, error) {
	// 1. Fast path: already live in the registry.
	if entry, ok := m.reg.Get(docID); ok {
		return entry.Doc, nil
	}

	// 2. Known to durable storage: recover it.
	meta, err := m.repo.GetMetadata(ctx, docID)
	if err == nil {
		return m.recoverInto(ctx, docID, roomID, filePath, meta)
	}
	if !errors.Is(err, types.ErrDocumentNotFound) {
		return nil, fmt.Errorf("query metadata: %w", err)
	}

	// 3. Brand-new document.
	return m.createNew(ctx, docID, roomID, filePath)
}

// recoverInto reconstructs a document from storage and registers it Active.
func (m *Manager) recoverInto(ctx context.Context, docID, roomID, filePath string, meta types.DocumentMetadata) (*yjs.DocWrapper, error) {
	rctx, cancel := context.WithTimeout(ctx, m.cfg.RecoveryTimeout)
	defer cancel()

	res, err := m.recMgr.RecoverDocument(rctx, docID)
	if err != nil {
		if errors.Is(rctx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("%w: %v", types.ErrRecoveryTimeout, err)
		}
		return nil, fmt.Errorf("recover document: %w", err)
	}

	entry := &registry.DocumentEntry{
		ID:         docID,
		RoomID:     roomID,
		FilePath:   filePath,
		State:      types.StateRecovered,
		Doc:        res.Doc,
		Version:    res.Version,
		LastAccess: time.Now(),
		CreatedAt:  meta.CreatedAt,
		UpdatedAt:  meta.UpdatedAt,
		Recovery: types.RecoveryMeta{
			Recovered:        true,
			FromSnapshotID:   res.FromSnapshotID,
			UpdatesReplayed:  res.UpdatesReplayed,
			RecoveredAt:      time.Now(),
			RecoveredVersion: res.Version,
		},
	}
	if err := m.reg.Register(entry); err != nil {
		res.Doc.Destroy()
		return nil, err
	}
	m.attachUpdateHandler(docID, roomID, filePath, res.Doc)

	if err := m.reg.Transition(docID, types.StateActive); err != nil {
		return nil, err
	}

	m.publish(events.DocumentRecovered, docID, roomID, res.Version)
	m.publish(events.DocumentLoaded, docID, roomID, res.Version)
	return res.Doc, nil
}

// createNew allocates, persists metadata for, and registers a fresh document.
func (m *Manager) createNew(ctx context.Context, docID, roomID, filePath string) (*yjs.DocWrapper, error) {
	now := time.Now()
	newMeta := types.DocumentMetadata{
		ID: docID, RoomID: roomID, FilePath: filePath, CreatedAt: now, UpdatedAt: now,
	}
	if err := m.repo.SaveMetadata(ctx, newMeta); err != nil {
		return nil, fmt.Errorf("save metadata: %w", err)
	}

	doc := yjs.New(m.yjsOpts)
	entry := &registry.DocumentEntry{
		ID: docID, RoomID: roomID, FilePath: filePath,
		State: types.StateCreated, Doc: doc, LastAccess: now, CreatedAt: now, UpdatedAt: now,
	}
	if err := m.reg.Register(entry); err != nil {
		doc.Destroy()
		return nil, err
	}
	m.attachUpdateHandler(docID, roomID, filePath, doc)

	if err := m.reg.Transition(docID, types.StateInitialized); err != nil {
		return nil, err
	}

	m.metrics.DocumentCreated()
	m.publish(events.DocumentCreated, docID, roomID, 0)
	m.publish(events.DocumentInitialized, docID, roomID, 0)
	return doc, nil
}

// ServerStateVector returns the authoritative document's state vector, the
// opening message a peer needs to compute what the server is missing.
func (m *Manager) ServerStateVector(docID string) ([]byte, error) {
	entry, ok := m.reg.Get(docID)
	if !ok {
		return nil, types.ErrDocumentNotFound
	}
	return m.syncEng.GenerateSyncStep1(entry.Doc), nil
}

// InitialSync returns a self-contained update encoding the full document, for a
// participant that is joining with no prior state (late join).
func (m *Manager) InitialSync(ctx context.Context, docID string) ([]byte, error) {
	entry, ok := m.reg.Get(docID)
	if !ok {
		return nil, types.ErrDocumentNotFound
	}
	m.metrics.LateJoinSync()
	return m.syncEng.InitialState(entry.Doc), nil
}

// HandleSyncStep1 replies to a peer's state vector with the delta update the
// peer is missing (sync step 2). This serves late-join, incremental, and
// reconnect synchronization uniformly.
func (m *Manager) HandleSyncStep1(ctx context.Context, docID string, clientStateVector []byte) ([]byte, error) {
	entry, ok := m.reg.Get(docID)
	if !ok {
		return nil, types.ErrDocumentNotFound
	}
	// Version is mutated concurrently under the registry lock; read a lock-safe
	// snapshot rather than the live entry field. RoomID is immutable post-create.
	info, _ := m.reg.Info(docID)
	roomID, version := info.RoomID, info.Version

	m.metrics.SyncStarted()
	m.publish(events.SynchronizationStarted, docID, roomID, version)

	update, err := m.syncEng.GenerateSyncStep2(entry.Doc, clientStateVector)
	if err != nil {
		m.metrics.SyncFailed()
		m.publish(events.SynchronizationFailed, docID, roomID, version)
		return nil, fmt.Errorf("sync step 2: %w", err)
	}

	m.metrics.SyncHandshakeCompleted()
	m.publish(events.SyncStepCompleted, docID, roomID, version)
	return update, nil
}

// ApplyIncrementalUpdate validates and merges a peer's update into the document,
// appends it to the durable update log for crash recovery, and advances the
// document version. It enforces the configured update- and document-size limits.
func (m *Manager) ApplyIncrementalUpdate(ctx context.Context, docID string, update []byte) error {
	if len(update) == 0 {
		m.metrics.UpdateRejected("malformed")
		return types.ErrMalformedUpdate
	}
	if int64(len(update)) > m.cfg.MaxUpdateSize {
		m.metrics.UpdateRejected("update_size")
		return types.ErrUpdateSizeExceeded
	}

	entry, ok := m.reg.Get(docID)
	if !ok {
		return types.ErrDocumentNotFound
	}
	if entry.Doc.Size() > m.cfg.MaxDocumentSize {
		m.metrics.UpdateRejected("document_size")
		return types.ErrDocumentSizeExceeded
	}

	if err := m.syncEng.ApplyUpdate(entry.Doc, update); err != nil {
		m.metrics.UpdateRejected("corrupted")
		return fmt.Errorf("apply update: %w", err)
	}

	version := m.reg.MarkEdited(docID)

	if err := m.repo.SaveUpdate(ctx, types.Update{
		ID:        id.NewWithPrefix("up"),
		DocID:     docID,
		Data:      update,
		Timestamp: time.Now(),
		Version:   version,
	}); err != nil {
		return fmt.Errorf("persist update: %w", err)
	}

	m.publish(events.UpdateApplied, docID, entry.RoomID, version)

	// Flush eagerly when the unpersisted-update backlog grows too large.
	if info, ok := m.reg.Info(docID); ok && info.Persistence.PendingUpdates >= m.cfg.MaxPendingUpdatesLimit {
		if err := m.SaveSnapshot(ctx, docID); err != nil {
			m.log.Warn("forced snapshot flush failed", "doc_id", docID, "err", err)
		}
	}
	return nil
}

// BatchUpdates applies a burst of updates to a document in one call, reducing
// per-frame API overhead for high-throughput or offline-flush edit streams. Each
// frame is validated, applied, and durably logged in order (batch sync).
func (m *Manager) BatchUpdates(ctx context.Context, docID string, updates [][]byte) error {
	if len(updates) == 0 {
		return types.ErrMalformedUpdate
	}
	applied := 0
	for i, u := range updates {
		if len(u) == 0 {
			continue
		}
		if err := m.ApplyIncrementalUpdate(ctx, docID, u); err != nil {
			return fmt.Errorf("batch frame %d: %w", i, err)
		}
		applied++
	}
	if applied == 0 {
		return types.ErrMalformedUpdate
	}
	m.metrics.BatchSync(applied)
	return nil
}

// JoinDocument records a participant as connected to a document and publishes a
// ParticipantJoined event. The document must already be loaded.
func (m *Manager) JoinDocument(ctx context.Context, docID, participantID string) error {
	entry, ok := m.reg.Get(docID)
	if !ok {
		return types.ErrDocumentNotFound
	}
	p := types.Participant{
		ID:         participantID,
		JoinedAt:   time.Now(),
		SyncStatus: types.SyncInProgress,
	}
	if _, ok := m.reg.AddParticipant(docID, p); !ok {
		return types.ErrDocumentNotFound
	}
	m.metrics.ParticipantJoined()
	m.bus.Publish(events.Event{
		Type: events.ParticipantJoined, DocID: docID, RoomID: entry.RoomID, ParticipantID: participantID,
	})
	return nil
}

// LeaveDocument removes a participant from a document and publishes a
// ParticipantLeft event.
func (m *Manager) LeaveDocument(ctx context.Context, docID, participantID string) error {
	entry, ok := m.reg.Get(docID)
	if !ok {
		return types.ErrDocumentNotFound
	}
	if _, removed := m.reg.RemoveParticipant(docID, participantID); !removed {
		return nil
	}
	m.metrics.ParticipantLeft()
	m.bus.Publish(events.Event{
		Type: events.ParticipantLeft, DocID: docID, RoomID: entry.RoomID, ParticipantID: participantID,
	})
	return nil
}

// MarkParticipantSynced flags a participant as fully synchronized and publishes a
// ParticipantSynchronized event.
func (m *Manager) MarkParticipantSynced(docID, participantID string) {
	m.reg.SetParticipantSync(docID, participantID, types.SyncSynced, time.Now())
	m.metrics.ParticipantSynchronized()
	roomID := ""
	if info, ok := m.reg.Info(docID); ok {
		roomID = info.RoomID
	}
	m.bus.Publish(events.Event{
		Type: events.ParticipantSynchronized, DocID: docID, RoomID: roomID, ParticipantID: participantID,
	})
}

// Participants returns the participants currently connected to a document.
func (m *Manager) Participants(docID string) []types.Participant {
	return m.reg.Participants(docID)
}

// SaveSnapshot captures the current document state as a snapshot, prunes the
// now-redundant update-log tail, and records durable progress. It drives the
// document through the SnapshotPending → Persisted lifecycle transitions.
func (m *Manager) SaveSnapshot(ctx context.Context, docID string) error {
	plan, ok := m.reg.PlanSnapshot(docID)
	if !ok {
		return types.ErrDocumentNotFound
	}

	if err := m.reg.Transition(docID, types.StateSnapshotPending); err != nil {
		return err
	}
	m.publish(events.DocumentSnapshotCreated, docID, plan.RoomID, plan.Version)

	snap, err := m.snapMgr.Create(ctx, docID, plan.Doc, plan.Version, plan.Prev)
	if err != nil {
		_ = m.reg.Transition(docID, types.StateDirty)
		return fmt.Errorf("create snapshot: %w", err)
	}

	// The snapshot subsumes every update up to and including its version.
	if err := m.repo.DeleteUpdates(ctx, docID, snap.Version+1); err != nil {
		m.log.Warn("prune update log failed", "doc_id", docID, "err", err)
	}

	m.reg.RecordSnapshot(docID, snap)
	if err := m.reg.Transition(docID, types.StatePersisted); err != nil {
		return err
	}

	m.metrics.DocumentSaved()
	m.publish(events.DocumentSaved, docID, plan.RoomID, snap.Version)
	m.publish(events.DocumentPersisted, docID, plan.RoomID, snap.Version)
	return nil
}

// ArchiveDocument flushes a dirty document, unloads it from active memory, and
// releases its CRDT resources. A subsequent GetOrCreateDocument recovers it.
func (m *Manager) ArchiveDocument(ctx context.Context, docID string) error {
	info, ok := m.reg.Info(docID)
	if !ok {
		return types.ErrDocumentNotFound
	}
	if info.IsDirty {
		if err := m.SaveSnapshot(ctx, docID); err != nil {
			return fmt.Errorf("flush before archive: %w", err)
		}
	}
	if err := m.reg.Transition(docID, types.StateArchived); err != nil {
		return err
	}
	entry, ok := m.reg.Unregister(docID)
	if ok && entry.Doc != nil {
		entry.Doc.Destroy()
	}

	m.metrics.DocumentArchived()
	m.publish(events.DocumentArchived, docID, info.RoomID, info.Version)
	return nil
}

// DeleteDocument permanently removes a document from memory and durable storage.
func (m *Manager) DeleteDocument(ctx context.Context, docID string) error {
	entry, ok := m.reg.Unregister(docID)
	var roomID string
	if ok {
		roomID = entry.RoomID
		_ = m.reg.Transition(docID, types.StateDestroyed) // best-effort; entry already removed
		if entry.Doc != nil {
			entry.Doc.Destroy()
		}
	}
	if err := m.repo.DeleteDocument(ctx, docID); err != nil {
		return fmt.Errorf("delete document storage: %w", err)
	}

	m.metrics.DocumentDestroyed()
	m.publish(events.DocumentDestroyed, docID, roomID, 0)
	return nil
}

// Statistics returns an immutable snapshot of a document's runtime counters.
func (m *Manager) Statistics(docID string) (types.Statistics, error) {
	entry, ok := m.reg.Get(docID)
	if !ok {
		return types.Statistics{}, types.ErrDocumentNotFound
	}
	info, _ := m.reg.Info(docID)
	return types.Statistics{
		Version:          info.Version,
		EditCount:        uint64(info.EditCount),
		ParticipantCount: info.ParticipantCount,
		SizeBytes:        entry.Doc.Size(),
		LastModified:     info.UpdatedAt,
	}, nil
}

// publish emits an event on the bus with the current timestamp.
func (m *Manager) publish(t events.Type, docID, roomID string, version uint64) {
	m.bus.Publish(events.Event{
		Type: t, DocID: docID, RoomID: roomID, Version: version, Timestamp: time.Now(),
	})
}

// --- Background loops ---------------------------------------------------------

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

// saveDirtyDocuments snapshots dirty documents that have aged past the snapshot
// interval or crossed the edit threshold since their last snapshot.
func (m *Manager) saveDirtyDocuments() {
	for _, info := range m.reg.ListDirty() {
		if time.Since(info.UpdatedAt) < m.cfg.SnapshotInterval && info.EditCount < m.cfg.SnapshotEditsThreshold {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), m.cfg.SyncTimeout)
		if err := m.SaveSnapshot(ctx, info.ID); err != nil {
			m.log.Error("scheduled snapshot failed", "doc_id", info.ID, "err", err)
		}
		cancel()
	}
}

func (m *Manager) janitorLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.cfg.GCInterval)
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

// archiveIdleDocuments unloads documents idle past the configured timeout.
func (m *Manager) archiveIdleDocuments() {
	for _, docID := range m.reg.ListIdle(m.cfg.IdleTimeout) {
		ctx, cancel := context.WithTimeout(context.Background(), m.cfg.RecoveryTimeout)
		if err := m.ArchiveDocument(ctx, docID); err != nil {
			m.log.Error("archive idle document failed", "doc_id", docID, "err", err)
		}
		cancel()
	}
}
