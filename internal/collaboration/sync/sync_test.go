package sync

import (
	"errors"
	"testing"

	"cpip/internal/collaboration/types"
	"cpip/internal/collaboration/yjs"
)

// converge performs a full two-step handshake from src into dst and returns dst's text.
func converge(t *testing.T, e *Engine, src, dst *yjs.DocWrapper) {
	t.Helper()
	dstSV := e.GenerateSyncStep1(dst)
	update, err := e.GenerateSyncStep2(src, dstSV)
	if err != nil {
		t.Fatalf("step2: %v", err)
	}
	if err := e.ApplyUpdate(dst, update); err != nil {
		t.Fatalf("apply: %v", err)
	}
}

func TestHandshakeConvergence(t *testing.T) {
	e := NewEngine()
	alice := yjs.New(yjs.Options{GC: true})
	bob := yjs.New(yjs.Options{GC: true})

	alice.InsertText(0, "Hello ")
	alice.InsertText(6, "World!")

	converge(t, e, alice, bob)

	if got := bob.GetText(); got != "Hello World!" {
		t.Fatalf("bob text = %q, want %q", got, "Hello World!")
	}
}

func TestLateJoinFullState(t *testing.T) {
	e := NewEngine()
	server := yjs.New(yjs.Options{GC: true})
	server.InsertText(0, "existing content")

	// A late joiner sends an empty state vector; step2 must return full state.
	update, err := e.GenerateSyncStep2(server, nil)
	if err != nil {
		t.Fatalf("step2: %v", err)
	}
	joiner := yjs.New(yjs.Options{GC: true})
	if err := e.ApplyUpdate(joiner, update); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := joiner.GetText(); got != "existing content" {
		t.Fatalf("joiner text = %q", got)
	}
}

func TestReconnectDeltaIsSmallerThanFull(t *testing.T) {
	e := NewEngine()
	server := yjs.New(yjs.Options{GC: true})
	server.InsertText(0, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa") // 30 chars

	// Peer already has the current state.
	peerSV := e.GenerateSyncStep1(server)

	// Server makes one more small edit after the peer's SV was captured.
	server.InsertText(30, "b")

	delta, err := e.GenerateSyncStep2(server, peerSV)
	if err != nil {
		t.Fatalf("step2: %v", err)
	}
	full := e.InitialState(server)
	if len(delta) >= len(full) {
		t.Fatalf("reconnect delta (%d) not smaller than full state (%d)", len(delta), len(full))
	}
}

func TestApplyBatchSequential(t *testing.T) {
	e := NewEngine()

	// Capture a stream of chained deltas built on a non-empty base.
	src := yjs.New(yjs.Options{GC: true})
	src.InsertText(0, "base")
	base := e.InitialState(src)

	var updates [][]byte
	src.SetUpdateHandler(func(u []byte, _ any) {
		cp := make([]byte, len(u))
		copy(cp, u)
		updates = append(updates, cp)
	})
	src.InsertText(4, "-a")
	src.InsertText(6, "-b")
	src.InsertText(8, "-c")

	// Applying the batch sequentially over the same base is lossless.
	dst := yjs.New(yjs.Options{GC: true})
	if err := e.ApplyUpdate(dst, base); err != nil {
		t.Fatalf("seed base: %v", err)
	}
	n, err := e.ApplyBatch(dst, updates)
	if err != nil {
		t.Fatalf("apply batch: %v", err)
	}
	if n != 3 {
		t.Fatalf("applied = %d, want 3", n)
	}
	if got := dst.GetText(); got != "base-a-b-c" {
		t.Fatalf("batch apply text = %q, want base-a-b-c", got)
	}
}

func TestMergeSelfContainedSet(t *testing.T) {
	e := NewEngine()
	src := yjs.New(yjs.Options{GC: true})

	var updates [][]byte
	src.SetUpdateHandler(func(u []byte, _ any) {
		cp := make([]byte, len(u))
		copy(cp, u)
		updates = append(updates, cp)
	})
	src.InsertText(0, "a")
	src.InsertText(1, "b")
	src.InsertText(2, "c")

	// The set is complete from an empty base, so a merge is lossless here.
	merged, err := e.Merge(updates...)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	dst := yjs.New(yjs.Options{GC: true})
	if err := e.ApplyUpdate(dst, merged); err != nil {
		t.Fatalf("apply merged: %v", err)
	}
	if got := dst.GetText(); got != "abc" {
		t.Fatalf("merged apply text = %q, want abc", got)
	}
}

func TestApplyRejectsMalformed(t *testing.T) {
	e := NewEngine()
	dst := yjs.New(yjs.Options{GC: true})

	if err := e.ApplyUpdate(dst, nil); !errors.Is(err, types.ErrMalformedUpdate) {
		t.Fatalf("empty update err = %v, want ErrMalformedUpdate", err)
	}
	if err := e.ApplyUpdate(dst, []byte{0xff, 0xff, 0xff, 0xff}); !errors.Is(err, types.ErrCorruptedUpdate) {
		t.Fatalf("garbage update err = %v, want ErrCorruptedUpdate", err)
	}
}

func TestGenerateStep2RejectsInvalidStateVector(t *testing.T) {
	e := NewEngine()
	src := yjs.New(yjs.Options{GC: true})
	src.InsertText(0, "x")

	if _, err := e.GenerateSyncStep2(src, []byte{0xff, 0xfe, 0xfd}); !errors.Is(err, types.ErrInvalidStateVector) {
		t.Fatalf("invalid SV err = %v, want ErrInvalidStateVector", err)
	}
}
