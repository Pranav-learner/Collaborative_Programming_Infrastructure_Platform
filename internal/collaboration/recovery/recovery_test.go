package recovery

import (
	"context"
	"errors"
	"testing"
	"time"

	"cpip/internal/collaboration/snapshot"
	"cpip/internal/collaboration/storage"
	"cpip/internal/collaboration/types"
	"cpip/internal/collaboration/yjs"
)

func newManagers(t *testing.T) (*storage.InMemoryRepository, *Manager) {
	t.Helper()
	repo := storage.NewInMemoryRepository()
	snaps := snapshot.NewManager(repo, snapshot.Options{RetentionCount: 5})
	rec := NewManager(repo, Options{Snapshots: snaps, YjsOptions: yjs.Options{GC: true}})
	return repo, rec
}

func TestRecoverFromSnapshotAndUpdates(t *testing.T) {
	repo, rec := newManagers(t)
	ctx := context.Background()

	// Author a document and snapshot it at version 1.
	doc := yjs.New(yjs.Options{GC: true})
	doc.InsertText(0, "base ")
	snaps := snapshot.NewManager(repo, snapshot.Options{RetentionCount: 5})
	if _, err := snaps.Create(ctx, "d1", doc, 1, types.SnapshotMeta{}); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Two further edits land only in the update log (versions 2 and 3).
	doc.InsertText(5, "two ")
	_ = repo.SaveUpdate(ctx, types.Update{DocID: "d1", Data: doc.EncodeStateAsUpdate(nil), Version: 2, Timestamp: time.Now()})
	doc.InsertText(9, "three")
	_ = repo.SaveUpdate(ctx, types.Update{DocID: "d1", Data: doc.EncodeStateAsUpdate(nil), Version: 3, Timestamp: time.Now()})

	res, err := rec.RecoverDocument(ctx, "d1")
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	defer res.Doc.Destroy()

	if got := res.Doc.GetText(); got != "base two three" {
		t.Fatalf("recovered text = %q, want %q", got, "base two three")
	}
	if res.Version != 3 {
		t.Fatalf("version = %d, want 3", res.Version)
	}
	if res.UpdatesReplayed != 2 {
		t.Fatalf("updates replayed = %d, want 2", res.UpdatesReplayed)
	}
	if !res.Consistent {
		t.Fatal("expected consistent recovery")
	}
	if res.FromSnapshotID == "" {
		t.Fatal("expected a source snapshot id")
	}
}

func TestRecoverFromUpdatesOnly(t *testing.T) {
	repo, rec := newManagers(t)
	ctx := context.Background()

	doc := yjs.New(yjs.Options{GC: true})
	doc.InsertText(0, "no snapshot here")
	_ = repo.SaveUpdate(ctx, types.Update{DocID: "d1", Data: doc.EncodeStateAsUpdate(nil), Version: 1, Timestamp: time.Now()})

	res, err := rec.RecoverDocument(ctx, "d1")
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	defer res.Doc.Destroy()
	if got := res.Doc.GetText(); got != "no snapshot here" {
		t.Fatalf("text = %q", got)
	}
	if res.SnapshotPayloads != 0 {
		t.Fatalf("snapshot payloads = %d, want 0", res.SnapshotPayloads)
	}
}

func TestRecoverUnknownDocument(t *testing.T) {
	_, rec := newManagers(t)
	if _, err := rec.RecoverDocument(context.Background(), "ghost"); !errors.Is(err, types.ErrDocumentNotFound) {
		t.Fatalf("err = %v, want ErrDocumentNotFound", err)
	}
}
