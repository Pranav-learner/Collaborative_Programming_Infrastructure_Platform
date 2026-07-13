// Package snapshot implements durable point-in-time capture of collaborative
// documents. It supports two snapshot kinds — full (a self-contained encoding of
// the entire document) and incremental (a delta relative to the previous
// snapshot's state vector) — and reconstructs live state from a snapshot chain.
//
// Snapshots bound recovery cost: without them, recovering a long-lived document
// would mean replaying its entire update log. The manager takes a full snapshot
// periodically (every IncrementalThreshold snapshots) and cheap incremental
// snapshots in between, so a recovery replays at most one full snapshot, a
// bounded run of increments, and the tail of the update log.
//
// Payloads are optionally gzip-compressed at rest when they exceed a threshold.
// Compression is transparent to callers: Reconstruct returns decompressed bytes.
package snapshot

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/reearth/ygo/crdt"

	"cpip/internal/collaboration/metrics"
	"cpip/internal/collaboration/storage"
	"cpip/internal/collaboration/types"
	"cpip/internal/collaboration/yjs"
	"cpip/internal/id"
)

// clock abstracts time so snapshots timestamps are testable/deterministic.
type clock func() time.Time

// Manager coordinates snapshot creation, compression, retention, and
// reconstruction. It is safe for concurrent use; all mutable state lives in the
// injected Repository, which is itself concurrency-safe.
type Manager struct {
	repo    storage.Repository
	metrics metrics.Recorder
	log     *slog.Logger
	now     clock

	retentionCount       int
	incrementalThreshold int
	compress             bool
	compressionThreshold int
}

// Options configures a snapshot Manager.
type Options struct {
	// RetentionCount is how many snapshots to retain per document. <= 0 defaults to 5.
	RetentionCount int
	// IncrementalThreshold is how many snapshots to take between full snapshots.
	// 0 disables incremental snapshots (every snapshot is full).
	IncrementalThreshold int
	// Compress enables gzip compression of snapshot payloads at rest.
	Compress bool
	// CompressionThreshold is the minimum payload size (bytes) eligible for
	// compression; smaller payloads are stored verbatim.
	CompressionThreshold int
	// Metrics records snapshot telemetry; nil yields a Noop.
	Metrics metrics.Recorder
	// Logger is the structured logger; nil yields slog.Default.
	Logger *slog.Logger
}

// NewManager constructs a snapshot Manager from the given repository and options.
func NewManager(repo storage.Repository, opts Options) *Manager {
	if opts.RetentionCount <= 0 {
		opts.RetentionCount = 5
	}
	if opts.IncrementalThreshold < 0 {
		opts.IncrementalThreshold = 0
	}
	if opts.CompressionThreshold <= 0 {
		opts.CompressionThreshold = 4 * 1024
	}
	if opts.Metrics == nil {
		opts.Metrics = metrics.NewNoop()
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Manager{
		repo:                 repo,
		metrics:              opts.Metrics,
		log:                  opts.Logger,
		now:                  time.Now,
		retentionCount:       opts.RetentionCount,
		incrementalThreshold: opts.IncrementalThreshold,
		compress:             opts.Compress,
		compressionThreshold: opts.CompressionThreshold,
	}
}

// Create captures the current state of a document as a snapshot, persists it,
// and enforces the retention policy. It decides between a full and an
// incremental snapshot from prev (the document's last-snapshot metadata):
//
//   - the first snapshot, or every IncrementalThreshold-th snapshot, is full;
//   - snapshots in between are incremental deltas relative to the previous
//     snapshot's state vector.
//
// It returns the stored snapshot descriptor. The document's version is the
// caller's monotonic edit/version counter, recorded for recovery ordering.
func (m *Manager) Create(ctx context.Context, docID string, doc *yjs.DocWrapper, version uint64, prev types.SnapshotMeta) (types.Snapshot, error) {
	start := m.now()
	kind, baseSV, baseVersion := m.plan(ctx, docID, prev)

	var raw []byte
	switch kind {
	case types.SnapshotIncremental:
		raw = doc.EncodeStateAsUpdate(baseSV)
		m.metrics.SnapshotIncremental()
	default:
		raw = doc.EncodeStateAsUpdate(nil)
		m.metrics.SnapshotFull()
	}

	stored, compressed, err := m.maybeCompress(raw)
	if err != nil {
		m.metrics.SnapshotFailed()
		return types.Snapshot{}, fmt.Errorf("%w: compress: %v", types.ErrSnapshotFailure, err)
	}

	snap := types.Snapshot{
		ID:          id.NewWithPrefix("snap"),
		DocID:       docID,
		Kind:        kind,
		StateVector: doc.EncodeStateVector(),
		Data:        stored,
		Compressed:  compressed,
		BaseVersion: baseVersion,
		Version:     version,
		Size:        int64(len(stored)),
		Timestamp:   start,
	}

	if err := m.repo.SaveSnapshot(ctx, snap); err != nil {
		m.metrics.SnapshotFailed()
		return types.Snapshot{}, fmt.Errorf("%w: save: %v", types.ErrSnapshotFailure, err)
	}

	// Retention is best-effort: the snapshot is already durable, so a prune
	// failure must not fail the operation. It is retried on the next snapshot.
	if err := m.repo.PruneSnapshots(ctx, docID, m.retentionCount); err != nil {
		m.log.Warn("snapshot prune failed", "doc_id", docID, "err", err)
	}

	m.metrics.SnapshotCreated(m.now().Sub(start).Milliseconds())
	return snap, nil
}

// plan decides the kind of the next snapshot and, for incremental snapshots,
// resolves the base state vector and base version to diff against. It falls back
// to a full snapshot whenever the base cannot be established.
func (m *Manager) plan(ctx context.Context, docID string, prev types.SnapshotMeta) (types.SnapshotKind, crdt.StateVector, uint64) {
	if m.incrementalThreshold <= 0 || prev.SnapshotCount == 0 || prev.LastSnapshotID == "" {
		return types.SnapshotFull, nil, 0
	}
	// Every IncrementalThreshold-th snapshot is full to bound the chain length.
	if prev.SnapshotCount%uint64(m.incrementalThreshold) == 0 {
		return types.SnapshotFull, nil, 0
	}
	latest, err := m.repo.GetLatestSnapshot(ctx, docID)
	if err != nil {
		return types.SnapshotFull, nil, 0
	}
	sv, err := yjs.DecodeStateVector(latest.StateVector)
	if err != nil {
		m.log.Warn("snapshot base state vector undecodable; falling back to full", "doc_id", docID, "err", err)
		return types.SnapshotFull, nil, 0
	}
	return types.SnapshotIncremental, sv, latest.Version
}

// Reconstruct returns the ordered, decompressed update payloads that rebuild the
// document's snapshotted state, together with the version at the newest
// snapshot. Applying the returned payloads in order to a fresh document (in any
// order, in fact — CRDT updates are commutative) reproduces the snapshotted
// state. When no snapshot exists it returns ErrSnapshotNotFound.
//
// The chain is assembled from the most recent full snapshot forward, followed by
// every incremental snapshot layered on top of it.
func (m *Manager) Reconstruct(ctx context.Context, docID string) (payloads [][]byte, version uint64, fromID string, err error) {
	snaps, err := m.repo.GetSnapshots(ctx, docID)
	if err != nil {
		return nil, 0, "", err // ErrSnapshotNotFound propagates verbatim
	}
	if len(snaps) == 0 {
		return nil, 0, "", types.ErrSnapshotNotFound
	}

	// Find the most recent full snapshot; everything from there is the chain.
	base := 0
	for i := len(snaps) - 1; i >= 0; i-- {
		if snaps[i].Kind == types.SnapshotFull {
			base = i
			break
		}
	}
	chain := snaps[base:]

	payloads = make([][]byte, 0, len(chain))
	for _, s := range chain {
		data, derr := m.decompress(s)
		if derr != nil {
			return nil, 0, "", fmt.Errorf("%w: decompress %s: %v", types.ErrSnapshotFailure, s.ID, derr)
		}
		payloads = append(payloads, data)
	}
	newest := chain[len(chain)-1]
	return payloads, newest.Version, chain[0].ID, nil
}

// maybeCompress gzip-compresses data when compression is enabled and the payload
// exceeds the threshold. It returns the stored bytes and whether they were
// compressed. Compression is skipped if it fails to shrink the payload.
func (m *Manager) maybeCompress(data []byte) ([]byte, bool, error) {
	if !m.compress || len(data) < m.compressionThreshold {
		return data, false, nil
	}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(data); err != nil {
		return nil, false, err
	}
	if err := zw.Close(); err != nil {
		return nil, false, err
	}
	if buf.Len() >= len(data) {
		return data, false, nil // compression did not help; store verbatim
	}
	return buf.Bytes(), true, nil
}

// decompress returns the raw update bytes of a snapshot, transparently inflating
// gzip-compressed payloads.
func (m *Manager) decompress(s types.Snapshot) ([]byte, error) {
	if !s.Compressed {
		return s.Data, nil
	}
	zr, err := gzip.NewReader(bytes.NewReader(s.Data))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	out, err := io.ReadAll(zr)
	if err != nil {
		return nil, err
	}
	return out, nil
}
