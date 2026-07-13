package sync

import (
	"github.com/reearth/ygo/crdt"

	"cpip/internal/collaboration/yjs"
)

// Engine handles the synchronization handshake and updates for collaborative documents.
type Engine struct{}

// NewEngine constructs a Sync Engine.
func NewEngine() *Engine {
	return &Engine{}
}

// GenerateSyncStep1 returns the local document's state vector, serialized.
func (e *Engine) GenerateSyncStep1(doc *yjs.DocWrapper) []byte {
	return doc.EncodeStateVector()
}

// GenerateSyncStep2 processes the remote peer's state vector and produces a delta update.
func (e *Engine) GenerateSyncStep2(doc *yjs.DocWrapper, remoteStateVector []byte) ([]byte, error) {
	// If the remote state vector is empty, we return the entire local document update
	if len(remoteStateVector) == 0 {
		return doc.EncodeStateAsUpdate(nil), nil
	}

	sv, err := crdt.DecodeStateVectorV1(remoteStateVector)
	if err != nil {
		return nil, err
	}

	return doc.EncodeStateAsUpdate(sv), nil
}

// ApplyUpdate integrates the remote peer's delta update into the local document.
func (e *Engine) ApplyUpdate(doc *yjs.DocWrapper, update []byte) error {
	return doc.ApplyUpdate(update)
}
