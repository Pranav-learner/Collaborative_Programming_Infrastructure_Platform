package storage

import (
	stdctx "context"
	"errors"
	"testing"
	"time"

	"cpip/internal/execution/job"
)

func TestRepositoryRoundTrip(t *testing.T) {
	r := NewMemoryRepository()
	ctx := stdctx.Background()

	if _, err := r.Load(ctx, "missing"); !errors.Is(err, job.ErrJobNotFound) {
		t.Fatalf("missing load err = %v", err)
	}
	j := job.Job{ID: "j1", UserID: "u1", Metadata: map[string]string{"k": "v"}}
	if err := r.Save(ctx, j); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := r.Load(ctx, "j1")
	if err != nil || got.UserID != "u1" {
		t.Fatalf("load = %+v err %v", got, err)
	}
	// Stored a clone: mutating the loaded copy must not affect the store.
	got.Metadata["k"] = "mutated"
	again, _ := r.Load(ctx, "j1")
	if again.Metadata["k"] != "v" {
		t.Fatal("repository did not isolate stored copy")
	}

	n, _ := r.Count(ctx)
	if n != 1 {
		t.Fatalf("count = %d", n)
	}
	_ = r.Delete(ctx, "j1")
	if n, _ := r.Count(ctx); n != 0 {
		t.Fatalf("count after delete = %d", n)
	}
}

func TestArchiveRoundTripAndOrdering(t *testing.T) {
	a := NewMemoryArchive()
	ctx := stdctx.Background()
	base := time.Now()

	_ = a.Archive(ctx, job.Job{ID: "old"}, base)
	_ = a.Archive(ctx, job.Job{ID: "new"}, base.Add(time.Hour))

	if _, err := a.Get(ctx, "old"); err != nil {
		t.Fatalf("get old: %v", err)
	}
	if _, err := a.Get(ctx, "ghost"); !errors.Is(err, job.ErrJobNotFound) {
		t.Fatalf("get ghost err = %v", err)
	}

	list, _ := a.List(ctx, 0)
	if len(list) != 2 || list[0].ID != "new" {
		t.Fatalf("list = %+v, want new first", list)
	}
	limited, _ := a.List(ctx, 1)
	if len(limited) != 1 || limited[0].ID != "new" {
		t.Fatalf("limited list = %+v", limited)
	}
	if n, _ := a.Count(ctx); n != 2 {
		t.Fatalf("count = %d", n)
	}
}
