package snapshot

import (
	"context"
	"errors"
	"strings"
	"testing"

	"cpip/internal/collaboration/storage"
	"cpip/internal/collaboration/types"
	"cpip/internal/collaboration/yjs"
)

// rebuild applies a snapshot chain to a fresh document and returns its text.
func rebuild(t *testing.T, m *Manager, docID string) string {
	t.Helper()
	payloads, _, _, err := m.Reconstruct(context.Background(), docID)
	if err != nil {
		t.Fatalf("reconstruct: %v", err)
	}
	doc := yjs.New(yjs.Options{GC: true})
	for _, p := range payloads {
		if err := doc.ApplyUpdate(p); err != nil {
			t.Fatalf("apply payload: %v", err)
		}
	}
	return doc.GetText()
}

func TestFullSnapshotRoundTrip(t *testing.T) {
	repo := storage.NewInMemoryRepository()
	m := NewManager(repo, Options{RetentionCount: 5})

	doc := yjs.New(yjs.Options{GC: true})
	doc.InsertText(0, "full snapshot content")

	snap, err := m.Create(context.Background(), "d1", doc, 1, types.SnapshotMeta{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if snap.Kind != types.SnapshotFull {
		t.Fatalf("kind = %v, want Full", snap.Kind)
	}
	if got := rebuild(t, m, "d1"); got != "full snapshot content" {
		t.Fatalf("rebuilt = %q", got)
	}
}

func TestIncrementalSnapshotChain(t *testing.T) {
	repo := storage.NewInMemoryRepository()
	// Full every 3rd snapshot; the 2nd is incremental.
	m := NewManager(repo, Options{RetentionCount: 10, IncrementalThreshold: 3})

	doc := yjs.New(yjs.Options{GC: true})
	ctx := context.Background()
	var prev types.SnapshotMeta

	record := func(version uint64) types.Snapshot {
		snap, err := m.Create(ctx, "d1", doc, version, prev)
		if err != nil {
			t.Fatalf("create v%d: %v", version, err)
		}
		prev.LastSnapshotID = snap.ID
		prev.LastSnapshotKind = snap.Kind
		prev.LastSnapshotVersion = snap.Version
		prev.SnapshotCount++
		return snap
	}

	doc.InsertText(0, "one ")
	s1 := record(1)
	doc.InsertText(4, "two ")
	s2 := record(2)
	doc.InsertText(8, "three")
	_ = record(3)

	if s1.Kind != types.SnapshotFull {
		t.Fatalf("s1 kind = %v, want Full", s1.Kind)
	}
	if s2.Kind != types.SnapshotIncremental {
		t.Fatalf("s2 kind = %v, want Incremental", s2.Kind)
	}
	if got := rebuild(t, m, "d1"); got != "one two three" {
		t.Fatalf("rebuilt = %q, want %q", got, "one two three")
	}
}

func TestCompressionRoundTrip(t *testing.T) {
	repo := storage.NewInMemoryRepository()
	m := NewManager(repo, Options{RetentionCount: 5, Compress: true, CompressionThreshold: 64})

	doc := yjs.New(yjs.Options{GC: true})
	big := strings.Repeat("collaborative-", 5000) // highly compressible, well over threshold
	doc.InsertText(0, big)

	snap, err := m.Create(context.Background(), "d1", doc, 1, types.SnapshotMeta{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !snap.Compressed {
		t.Fatal("expected snapshot to be compressed")
	}
	if got := rebuild(t, m, "d1"); got != big {
		t.Fatalf("decompressed content mismatch (len got %d want %d)", len(got), len(big))
	}
}

func TestRetentionPrunesOldSnapshots(t *testing.T) {
	repo := storage.NewInMemoryRepository()
	m := NewManager(repo, Options{RetentionCount: 2})

	doc := yjs.New(yjs.Options{GC: true})
	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		doc.InsertText(0, "x")
		if _, err := m.Create(ctx, "d1", doc, uint64(i), types.SnapshotMeta{}); err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
	}
	snaps, err := repo.GetSnapshots(ctx, "d1")
	if err != nil {
		t.Fatalf("get snapshots: %v", err)
	}
	if len(snaps) != 2 {
		t.Fatalf("retained %d snapshots, want 2", len(snaps))
	}
}

func TestReconstructMissing(t *testing.T) {
	repo := storage.NewInMemoryRepository()
	m := NewManager(repo, Options{})
	if _, _, _, err := m.Reconstruct(context.Background(), "nope"); !errors.Is(err, types.ErrSnapshotNotFound) {
		t.Fatalf("err = %v, want ErrSnapshotNotFound", err)
	}
}
