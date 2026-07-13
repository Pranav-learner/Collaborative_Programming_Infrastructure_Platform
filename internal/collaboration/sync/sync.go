// Package sync implements the synchronization engine that drives the Yjs sync
// protocol on top of the yjs.DocWrapper. It is intentionally stateless: all
// document state lives inside the CRDT engine, and the engine here only
// orchestrates the handshake, computes deltas, and merges/batches update blocks.
//
// The synchronization model is the standard Yjs two-step handshake:
//
//	Step 1 (SV exchange): each side sends its state vector — a compact map of
//	                      {clientID: clock} describing everything it already knows.
//	Step 2 (delta):       each side replies with exactly the operations the peer
//	                      is missing, computed from the peer's state vector.
//
// This yields initial synchronization, incremental live updates, late-join
// synchronization (empty peer SV → full state), reconnect synchronization
// (stale peer SV → only the missed operations), and offline synchronization
// (a peer applies its queued local updates, then exchanges SVs) — all without
// the engine implementing any CRDT logic itself.
package sync

import (
	"fmt"

	"github.com/reearth/ygo/crdt"

	"cpip/internal/collaboration/metrics"
	"cpip/internal/collaboration/types"
	"cpip/internal/collaboration/yjs"
)

// Engine handles the synchronization handshake and update exchange for
// collaborative documents. It is safe for concurrent use: it holds no
// per-document state, and all document access goes through the thread-safe
// DocWrapper.
type Engine struct {
	metrics   metrics.Recorder
	batchSize int
}

// Option customizes an Engine at construction time.
type Option func(*Engine)

// WithMetrics injects a metrics recorder. When unset, a Noop recorder is used.
func WithMetrics(m metrics.Recorder) Option {
	return func(e *Engine) {
		if m != nil {
			e.metrics = m
		}
	}
}

// WithBatchSize bounds how many update blocks Batch merges in a single pass.
// Values <= 0 leave the default in place.
func WithBatchSize(n int) Option {
	return func(e *Engine) {
		if n > 0 {
			e.batchSize = n
		}
	}
}

// NewEngine constructs a synchronization Engine. With no options it uses a Noop
// metrics recorder and an unbounded batch size, preserving the zero-argument
// call site used by earlier revisions.
func NewEngine(opts ...Option) *Engine {
	e := &Engine{metrics: metrics.NewNoop(), batchSize: 0}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// GenerateSyncStep1 returns the local document's state vector, serialized. This
// is the opening message of the handshake: "here is everything I already know".
func (e *Engine) GenerateSyncStep1(doc *yjs.DocWrapper) []byte {
	return doc.EncodeStateVector()
}

// GenerateSyncStep2 processes the remote peer's state vector and produces the
// delta update carrying exactly the operations the peer is missing.
//
// An empty (or nil) remote state vector means the peer knows nothing — the late
// join case — so the entire document is returned. A populated state vector that
// is merely stale — the reconnect case — yields only the missed operations,
// which is the core bandwidth optimization of the protocol.
func (e *Engine) GenerateSyncStep2(doc *yjs.DocWrapper, remoteStateVector []byte) ([]byte, error) {
	if len(remoteStateVector) == 0 {
		update := doc.EncodeStateAsUpdate(nil)
		e.metrics.UpdateGenerated(len(update))
		e.metrics.LateJoinSync()
		return update, nil
	}

	sv, err := crdt.DecodeStateVectorV1(remoteStateVector)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", types.ErrInvalidStateVector, err)
	}

	update := doc.EncodeStateAsUpdate(sv)
	e.metrics.UpdateGenerated(len(update))
	return update, nil
}

// ApplyUpdate integrates a peer's delta update into the local document. It
// rejects empty payloads early and classifies engine decode failures as
// corrupted-update errors so callers can distinguish protocol faults from
// transport faults.
func (e *Engine) ApplyUpdate(doc *yjs.DocWrapper, update []byte) error {
	if len(update) == 0 {
		return types.ErrMalformedUpdate
	}
	if err := doc.ApplyUpdate(update); err != nil {
		return fmt.Errorf("%w: %v", types.ErrCorruptedUpdate, err)
	}
	e.metrics.UpdateApplied(len(update))
	return nil
}

// InitialState returns a self-contained update encoding the full document. It is
// the payload delivered to a brand-new participant that has no prior state, and
// the basis of a full snapshot.
func (e *Engine) InitialState(doc *yjs.DocWrapper) []byte {
	return doc.EncodeStateAsUpdate(nil)
}

// Delta computes the portion of a standalone update that the peer described by
// remoteStateVector is missing, without instantiating a document. This is the
// stateless bandwidth optimization used when relaying a captured update to a
// peer whose state vector is known: it strips operations the peer already has.
//
// An empty remote state vector returns the update unchanged (the peer needs all
// of it).
func (e *Engine) Delta(update, remoteStateVector []byte) ([]byte, error) {
	if len(update) == 0 {
		return nil, types.ErrMalformedUpdate
	}
	if len(remoteStateVector) == 0 {
		return update, nil
	}
	sv, err := crdt.DecodeStateVectorV1(remoteStateVector)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", types.ErrInvalidStateVector, err)
	}
	delta, err := yjs.DiffUpdate(update, sv)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", types.ErrCorruptedUpdate, err)
	}
	return delta, nil
}

// ApplyBatch applies a burst of update blocks to a document in a single pass and
// returns how many were applied. This is the batch-synchronization primitive:
// applying frames sequentially through the CRDT engine is always correct,
// including for chained deltas that build on the document's existing state.
//
// It deliberately does NOT pre-merge the frames into one blob: merging chained
// deltas over a non-empty base is not lossless in the underlying engine, whereas
// sequential application is. The document's write lock is acquired and released
// per frame, so other writers are never starved for the length of the batch.
func (e *Engine) ApplyBatch(doc *yjs.DocWrapper, updates [][]byte) (int, error) {
	applied := 0
	for i, u := range updates {
		if len(u) == 0 {
			continue
		}
		if err := e.ApplyUpdate(doc, u); err != nil {
			return applied, fmt.Errorf("batch frame %d: %w", i, err)
		}
		applied++
	}
	if applied == 0 {
		return 0, types.ErrMalformedUpdate
	}
	e.metrics.BatchSync(applied)
	return applied, nil
}

// Merge coalesces update blocks that form a self-contained set — every operation
// they reference is present within the set, as produced by a peer flushing its
// entire offline edit history from an empty base — into one compact update, for
// relay or storage. It is NOT safe for merging deltas that depend on operations
// outside the set; apply those with ApplyBatch instead.
func (e *Engine) Merge(updates ...[]byte) ([]byte, error) {
	nonEmpty := make([][]byte, 0, len(updates))
	for _, u := range updates {
		if len(u) > 0 {
			nonEmpty = append(nonEmpty, u)
		}
	}
	if len(nonEmpty) == 0 {
		return nil, types.ErrMalformedUpdate
	}
	merged, err := yjs.MergeUpdates(nonEmpty...)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", types.ErrCorruptedUpdate, err)
	}
	return merged, nil
}

// StateVectorOf decodes a serialized state vector, normalizing an empty payload
// to an empty (rather than nil-error) vector. Used by callers that need to
// reason about what a peer already knows.
func (e *Engine) StateVectorOf(data []byte) (crdt.StateVector, error) {
	sv, err := yjs.DecodeStateVector(data)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", types.ErrInvalidStateVector, err)
	}
	return sv, nil
}
