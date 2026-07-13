package storage

import (
	"context"
	"errors"
	"testing"

	"cpip/internal/collaboration/types"
)

func TestMetadataRoundTrip(t *testing.T) {
	r := NewInMemoryRepository()
	ctx := context.Background()

	if _, err := r.GetMetadata(ctx, "missing"); !errors.Is(err, types.ErrDocumentNotFound) {
		t.Fatalf("missing meta err = %v", err)
	}
	meta := types.DocumentMetadata{ID: "d1", RoomID: "r1", FilePath: "f.go"}
	if err := r.SaveMetadata(ctx, meta); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := r.GetMetadata(ctx, "d1")
	if err != nil || got.RoomID != "r1" {
		t.Fatalf("get meta = %+v, err %v", got, err)
	}
}

func TestUpdateLogFilteringAndDeletion(t *testing.T) {
	r := NewInMemoryRepository()
	ctx := context.Background()

	for v := uint64(1); v <= 5; v++ {
		if err := r.SaveUpdate(ctx, types.Update{DocID: "d1", Version: v, Data: []byte{byte(v)}}); err != nil {
			t.Fatalf("save update: %v", err)
		}
	}
	got, _ := r.GetUpdates(ctx, "d1", 2)
	if len(got) != 3 {
		t.Fatalf("updates since v2 = %d, want 3", len(got))
	}
	// Delete everything strictly below version 4.
	if err := r.DeleteUpdates(ctx, "d1", 4); err != nil {
		t.Fatalf("delete: %v", err)
	}
	remaining, _ := r.GetUpdates(ctx, "d1", 0)
	if len(remaining) != 2 {
		t.Fatalf("remaining = %d, want 2", len(remaining))
	}
}

func TestSnapshotStorageAndPrune(t *testing.T) {
	r := NewInMemoryRepository()
	ctx := context.Background()

	if _, err := r.GetLatestSnapshot(ctx, "d1"); !errors.Is(err, types.ErrSnapshotNotFound) {
		t.Fatalf("latest missing err = %v", err)
	}
	for i := 1; i <= 4; i++ {
		if err := r.SaveSnapshot(ctx, types.Snapshot{ID: string(rune('a' + i)), DocID: "d1", Version: uint64(i)}); err != nil {
			t.Fatalf("save snap: %v", err)
		}
	}
	latest, err := r.GetLatestSnapshot(ctx, "d1")
	if err != nil || latest.Version != 4 {
		t.Fatalf("latest = %+v err %v", latest, err)
	}
	if err := r.PruneSnapshots(ctx, "d1", 2); err != nil {
		t.Fatalf("prune: %v", err)
	}
	snaps, _ := r.GetSnapshots(ctx, "d1")
	if len(snaps) != 2 || snaps[0].Version != 3 {
		t.Fatalf("after prune = %+v", snaps)
	}
}

func TestDeleteDocumentClearsAll(t *testing.T) {
	r := NewInMemoryRepository()
	ctx := context.Background()
	_ = r.SaveMetadata(ctx, types.DocumentMetadata{ID: "d1"})
	_ = r.SaveUpdate(ctx, types.Update{DocID: "d1", Version: 1})
	_ = r.SaveSnapshot(ctx, types.Snapshot{ID: "s1", DocID: "d1", Version: 1})

	if err := r.DeleteDocument(ctx, "d1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := r.GetMetadata(ctx, "d1"); !errors.Is(err, types.ErrDocumentNotFound) {
		t.Fatalf("meta still present: %v", err)
	}
	if ups, _ := r.GetUpdates(ctx, "d1", 0); len(ups) != 0 {
		t.Fatalf("updates still present: %d", len(ups))
	}
}
