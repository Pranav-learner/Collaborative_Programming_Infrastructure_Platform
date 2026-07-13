// Package yjs is the adapter over the Yjs-compatible CRDT engine
// (github.com/reearth/ygo/crdt). It is the ONLY package permitted to touch the
// CRDT library directly; every other collaboration package operates through the
// thread-safe DocWrapper abstraction defined here.
//
// The wrapper never implements CRDT algorithms itself — it delegates conflict
// resolution, update encoding/merging, and state-vector computation entirely to
// the underlying engine. Its responsibilities are (1) serializing access to the
// non-reentrant CRDT document with a single writer lock, (2) exposing a small,
// intention-revealing API to the rest of the engine, and (3) surfacing generated
// updates via an observer hook.
package yjs

import (
	"sync"

	"github.com/reearth/ygo/crdt"
)

// DefaultText is the name of the root text type used for single-file documents.
const DefaultText = "content"

// UpdateHandler is invoked with the binary update produced by every committed
// transaction (both local mutations and applied remote updates), along with the
// origin passed to the mutating call.
//
// IMPORTANT: the handler is called while the wrapper's internal write lock is
// held. It MUST NOT call back into any DocWrapper method, or it will deadlock.
// Handlers should do cheap, lock-free work only (e.g. update atomic counters).
type UpdateHandler func(update []byte, origin any)

// OriginRemote is the origin tag attached to updates applied from a peer.
type originTag string

// OriginRemote marks updates applied via ApplyUpdate (i.e. originating from a
// remote peer). Local edits carry a nil origin.
const OriginRemote originTag = "remote"

// Options configures a new DocWrapper.
type Options struct {
	// GC enables engine garbage collection of deleted content. Disable when the
	// document's full history must be reconstructable from CaptureSnapshot.
	GC bool
	// ClientID, when non-nil, pins the underlying CRDT client identifier. When
	// nil a random client ID is generated.
	ClientID *uint32
	// GUID, when non-empty, pins the document GUID.
	GUID string
}

// DocWrapper encapsulates a Yjs document (crdt.Doc) with a thread-safe API and
// multi-file (named shared type) support.
type DocWrapper struct {
	mu    sync.RWMutex
	ydoc  *crdt.Doc
	texts map[string]*crdt.YText

	hmu      sync.RWMutex
	onUpdate UpdateHandler
	unsub    func()
}

// New constructs a DocWrapper with the given options and initializes the
// default text type.
func New(opts Options) *DocWrapper {
	docOpts := []crdt.DocOption{crdt.WithGC(opts.GC)}
	if opts.ClientID != nil {
		docOpts = append(docOpts, crdt.WithClientID(crdt.ClientID(*opts.ClientID)))
	}
	if opts.GUID != "" {
		docOpts = append(docOpts, crdt.WithGUID(opts.GUID))
	}

	ydoc := crdt.New(docOpts...)
	w := &DocWrapper{
		ydoc:  ydoc,
		texts: make(map[string]*crdt.YText),
	}
	// Materialize the default text type so single-file documents work out of the box.
	w.texts[DefaultText] = ydoc.GetText(DefaultText)

	// Bridge engine update notifications to the (optional) user handler.
	w.unsub = ydoc.OnUpdate(func(update []byte, origin any) {
		w.hmu.RLock()
		h := w.onUpdate
		w.hmu.RUnlock()
		if h != nil {
			h(update, origin)
		}
	})
	return w
}

// NewDocWrapper constructs a DocWrapper with GC enabled. Retained for backwards
// compatibility with earlier call sites.
func NewDocWrapper() *DocWrapper {
	return New(Options{GC: true})
}

// SetUpdateHandler registers (or clears, with nil) the handler invoked on every
// committed update. See UpdateHandler for the concurrency contract.
func (w *DocWrapper) SetUpdateHandler(h UpdateHandler) {
	w.hmu.Lock()
	w.onUpdate = h
	w.hmu.Unlock()
}

// ClientID returns the underlying CRDT client identifier.
func (w *DocWrapper) ClientID() uint32 {
	return uint32(w.ydoc.ClientID())
}

// GUID returns the document GUID.
func (w *DocWrapper) GUID() string { return w.ydoc.GUID() }

// text returns the named text type, lazily materializing it. Callers must hold
// at least a read lock; materialization upgrades are guarded by the caller.
func (w *DocWrapper) textLocked(name string) *crdt.YText {
	if t, ok := w.texts[name]; ok {
		return t
	}
	t := w.ydoc.GetText(name)
	w.texts[name] = t
	return t
}

// ApplyUpdate merges a binary V1 update into the document state. The update is
// tagged with OriginRemote so the update handler can distinguish applied remote
// updates from local edits.
func (w *DocWrapper) ApplyUpdate(update []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return crdt.ApplyUpdateV1(w.ydoc, update, OriginRemote)
}

// EncodeStateAsUpdate serializes the document state as a V1 update relative to
// the supplied state vector. Pass nil to encode the entire document.
func (w *DocWrapper) EncodeStateAsUpdate(sv crdt.StateVector) []byte {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return crdt.EncodeStateAsUpdateV1(w.ydoc, sv)
}

// EncodeStateVector returns the document's current state vector, serialized.
func (w *DocWrapper) EncodeStateVector() []byte {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return crdt.EncodeStateVectorV1(w.ydoc)
}

// StateVector returns the document's current state vector as a decoded map.
func (w *DocWrapper) StateVector() crdt.StateVector {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.ydoc.StateVector()
}

// Size returns the serialized size of the full document state, in bytes. This is
// the figure compared against configured document-size limits.
func (w *DocWrapper) Size() int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return int64(len(crdt.EncodeStateAsUpdateV1(w.ydoc, nil)))
}

// GetText returns the raw text content of the default single-file text type.
func (w *DocWrapper) GetText() string {
	return w.GetTextIn(DefaultText)
}

// GetTextIn returns the raw text content of the named text type (multi-file).
func (w *DocWrapper) GetTextIn(name string) string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.textLocked(name).ToString()
}

// InsertText inserts plain text at index within the default text type.
func (w *DocWrapper) InsertText(index int, content string) {
	w.InsertTextIn(DefaultText, index, content)
}

// InsertTextIn inserts plain text at index within the named text type.
func (w *DocWrapper) InsertTextIn(name string, index int, content string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	t := w.textLocked(name)
	w.ydoc.Transact(func(txn *crdt.Transaction) {
		t.Insert(txn, index, content, nil)
	}, nil)
}

// DeleteText deletes a range within the default text type.
func (w *DocWrapper) DeleteText(index, length int) {
	w.DeleteTextIn(DefaultText, index, length)
}

// DeleteTextIn deletes a range within the named text type.
func (w *DocWrapper) DeleteTextIn(name string, index, length int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	t := w.textLocked(name)
	w.ydoc.Transact(func(txn *crdt.Transaction) {
		t.Delete(txn, index, length)
	}, nil)
}

// Files returns the names of all materialized text types (open files).
func (w *DocWrapper) Files() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	names := make([]string, 0, len(w.texts))
	for name := range w.texts {
		names = append(names, name)
	}
	return names
}

// Destroy releases the update observer and the underlying document. The wrapper
// must not be used afterward.
func (w *DocWrapper) Destroy() {
	w.hmu.Lock()
	w.onUpdate = nil
	w.hmu.Unlock()

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.unsub != nil {
		w.unsub()
		w.unsub = nil
	}
	w.ydoc.Destroy()
}

// YDoc returns the raw crdt.Doc instance. Intended for advanced use within the
// yjs package's test suite; other packages should not depend on it.
func (w *DocWrapper) YDoc() *crdt.Doc { return w.ydoc }

// --- Stateless engine helpers -------------------------------------------------
//
// These wrap pure engine functions that operate on update bytes without a live
// document. They power bandwidth optimization, batch synchronization, and
// offline delta computation.

// DecodeStateVector decodes a serialized state vector.
func DecodeStateVector(data []byte) (crdt.StateVector, error) {
	if len(data) == 0 {
		return crdt.StateVector{}, nil
	}
	return crdt.DecodeStateVectorV1(data)
}

// MergeUpdates losslessly merges multiple V1 updates into a single compacted
// update. Used for batch synchronization and update-log compaction.
func MergeUpdates(updates ...[]byte) ([]byte, error) {
	return crdt.MergeUpdatesV1(updates...)
}

// DiffUpdate computes the portion of an update that is missing relative to the
// supplied state vector — a bandwidth optimization that avoids re-sending known
// operations without instantiating a document.
func DiffUpdate(update []byte, sv crdt.StateVector) ([]byte, error) {
	return crdt.DiffUpdateV1(update, sv)
}

// StateVectorFromUpdate derives a serialized state vector directly from an
// update payload, without loading it into a document.
func StateVectorFromUpdate(update []byte) ([]byte, error) {
	return crdt.EncodeStateVectorFromUpdate(update)
}
