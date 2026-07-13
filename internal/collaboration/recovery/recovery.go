// Package recovery reconstructs live document state from durable storage. It is
// used both to rehydrate an archived (unloaded) document on demand and to
// restore state after a crash. Recovery composes two durable sources:
//
//  1. the snapshot chain (a full snapshot plus any incremental snapshots), which
//     provides a bounded starting point; and
//  2. the update log — every incremental update accepted since the last
//     snapshot — which is replayed on top to reach the latest state.
//
// After reconstruction the manager verifies consistency: it re-derives the
// recovered document's state vector and confirms the replayed version is
// monotonic with respect to the snapshot base. Callers receive a RecoveryResult
// describing exactly what was replayed, enabling audit and metrics.
package recovery

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"cpip/internal/collaboration/metrics"
	"cpip/internal/collaboration/snapshot"
	"cpip/internal/collaboration/storage"
	"cpip/internal/collaboration/types"
	"cpip/internal/collaboration/yjs"
)

// Manager coordinates document recovery. It is safe for concurrent use; it holds
// no per-document mutable state.
type Manager struct {
	repo    storage.Repository
	snaps   *snapshot.Manager
	metrics metrics.Recorder
	yjsOpts yjs.Options
}

// Options configures a recovery Manager.
type Options struct {
	// Snapshots reconstructs the snapshot chain. Required.
	Snapshots *snapshot.Manager
	// Metrics records recovery telemetry; nil yields a Noop.
	Metrics metrics.Recorder
	// YjsOptions configures documents materialized during recovery (e.g. GC).
	YjsOptions yjs.Options
}

// NewManager constructs a recovery Manager.
func NewManager(repo storage.Repository, opts Options) *Manager {
	if opts.Metrics == nil {
		opts.Metrics = metrics.NewNoop()
	}
	return &Manager{
		repo:    repo,
		snaps:   opts.Snapshots,
		metrics: opts.Metrics,
		yjsOpts: opts.YjsOptions,
	}
}

// Result describes the outcome of a recovery operation.
type Result struct {
	// Doc is the reconstructed, ready-to-use document.
	Doc *yjs.DocWrapper
	// Version is the document version after replay (the highest applied update
	// version, or the snapshot version if the update log was empty).
	Version uint64
	// FromSnapshotID identifies the base snapshot of the recovery chain, if any.
	FromSnapshotID string
	// SnapshotPayloads is how many snapshot payloads (full + incremental) applied.
	SnapshotPayloads int
	// UpdatesReplayed is how many update-log entries were replayed.
	UpdatesReplayed int
	// StateVector is the recovered document's serialized state vector, for
	// post-recovery consistency verification by the caller.
	StateVector []byte
	// Consistent reports whether the post-recovery verification passed.
	Consistent bool
	// Duration is the wall-clock time the recovery took.
	Duration time.Duration
}

// RecoverDocument reconstructs a document from its snapshot chain and update log.
// It returns ErrDocumentNotFound when neither a snapshot nor any updates exist,
// and ErrConsistencyCheckFailed when post-recovery verification fails.
func (m *Manager) RecoverDocument(ctx context.Context, docID string) (*Result, error) {
	start := time.Now()
	m.metrics.RecoveryStarted()

	doc := yjs.New(m.yjsOpts)

	res := &Result{Doc: doc}

	// 1. Rebuild from the snapshot chain, if one exists.
	payloads, snapVersion, fromID, err := m.snaps.Reconstruct(ctx, docID)
	switch {
	case err == nil:
		for _, p := range payloads {
			if aerr := doc.ApplyUpdate(p); aerr != nil {
				doc.Destroy()
				m.metrics.RecoveryFailed()
				return nil, fmt.Errorf("%w: apply snapshot payload: %v", types.ErrRecoveryFailure, aerr)
			}
		}
		res.FromSnapshotID = fromID
		res.SnapshotPayloads = len(payloads)
		res.Version = snapVersion
	case errors.Is(err, types.ErrSnapshotNotFound):
		// No snapshot: recovery proceeds from the update log alone.
	default:
		doc.Destroy()
		m.metrics.RecoveryFailed()
		return nil, fmt.Errorf("%w: reconstruct snapshot: %v", types.ErrRecoveryFailure, err)
	}

	// 2. Replay the tail of the update log recorded after the snapshot.
	updates, err := m.repo.GetUpdates(ctx, docID, snapVersion)
	if err != nil {
		doc.Destroy()
		m.metrics.RecoveryFailed()
		return nil, fmt.Errorf("%w: load updates: %v", types.ErrRecoveryFailure, err)
	}

	// A document with neither a snapshot nor updates does not exist.
	if res.SnapshotPayloads == 0 && len(updates) == 0 {
		doc.Destroy()
		m.metrics.RecoveryFailed()
		return nil, types.ErrDocumentNotFound
	}

	sort.Slice(updates, func(i, j int) bool { return updates[i].Version < updates[j].Version })
	for _, u := range updates {
		if aerr := doc.ApplyUpdate(u.Data); aerr != nil {
			doc.Destroy()
			m.metrics.RecoveryFailed()
			return nil, fmt.Errorf("%w: replay update v%d: %v", types.ErrRecoveryFailure, u.Version, aerr)
		}
		res.UpdatesReplayed++
		if u.Version > res.Version {
			res.Version = u.Version
		}
	}
	m.metrics.MissedUpdatesReplayed(res.UpdatesReplayed)

	// 3. Verify consistency: the state vector must decode and the recovered
	//    version must not regress below the snapshot base.
	res.StateVector = doc.EncodeStateVector()
	res.Consistent = m.verify(res, snapVersion)
	if !res.Consistent {
		doc.Destroy()
		m.metrics.RecoveryFailed()
		return nil, types.ErrConsistencyCheckFailed
	}

	res.Duration = time.Since(start)
	m.metrics.RecoveryCompleted(res.Duration.Milliseconds())
	m.metrics.DocumentRecovered()
	return res, nil
}

// verify performs post-recovery consistency validation: the recovered state
// vector must decode, and the recovered version must be monotonic with respect
// to the snapshot base version.
func (m *Manager) verify(res *Result, snapVersion uint64) bool {
	if _, err := yjs.DecodeStateVector(res.StateVector); err != nil {
		return false
	}
	if res.Version < snapVersion {
		return false
	}
	return true
}
