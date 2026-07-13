package yjs

import (
	"sync"

	"github.com/reearth/ygo/crdt"
)

// DocWrapper encapsulates a Yjs document (crdt.Doc) with a thread-safe API.
type DocWrapper struct {
	mu   sync.RWMutex
	ydoc *crdt.Doc
	text *crdt.YText
}

// NewDocWrapper creates a new DocWrapper and initializes the default text type.
func NewDocWrapper() *DocWrapper {
	ydoc := crdt.New()
	text := ydoc.GetText("content")
	return &DocWrapper{
		ydoc: ydoc,
		text: text,
	}
}

// ApplyUpdate V1 merges binary updates into the document state.
func (w *DocWrapper) ApplyUpdate(update []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return crdt.ApplyUpdateV1(w.ydoc, update, nil)
}

// EncodeStateAsUpdate V1 serializes the document state vector diff.
// Pass nil to encode the entire document.
func (w *DocWrapper) EncodeStateAsUpdate(sv crdt.StateVector) []byte {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return crdt.EncodeStateAsUpdateV1(w.ydoc, sv)
}

// EncodeStateVector V1 returns the document's current state vector.
func (w *DocWrapper) EncodeStateVector() []byte {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return crdt.EncodeStateVectorV1(w.ydoc)
}

// GetText returns the raw text content of the shared document.
func (w *DocWrapper) GetText() string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.text.ToString()
}

// InsertText inserts plain text at the specified index within a transaction.
func (w *DocWrapper) InsertText(index int, content string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ydoc.Transact(func(txn *crdt.Transaction) {
		w.text.Insert(txn, index, content, nil)
	}, nil)
}

// DeleteText deletes a range of text of specified length at the index.
func (w *DocWrapper) DeleteText(index, length int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ydoc.Transact(func(txn *crdt.Transaction) {
		w.text.Delete(txn, index, length)
	}, nil)
}

// YDoc returns the raw crdt.Doc instance.
func (w *DocWrapper) YDoc() *crdt.Doc {
	return w.ydoc
}
