package metadata

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"cpip/internal/storage/artifacts"
)

func mkArtifact(id, lineage string) *artifacts.Artifact {
	now := time.Now().UTC()
	return &artifacts.Artifact{
		ID: id, ObjectKey: "cas/" + id, Bucket: "artifacts", ContentHash: "sha256:" + id,
		Size: 10, Type: artifacts.ExecutionLog, LineageID: lineage,
		State: artifacts.Available, CreatedAt: now, UpdatedAt: now,
	}
}

func TestAppendVersionAssignsSequentialVersionsAndSingleHead(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	for i := 0; i < 5; i++ {
		a := mkArtifact(fmt.Sprintf("art%d", i), "lin1")
		if err := s.AppendVersion(ctx, a); err != nil {
			t.Fatal(err)
		}
		if a.Version != int64(i+1) {
			t.Fatalf("version %d, want %d", a.Version, i+1)
		}
	}
	hist, err := s.ListLineage(ctx, "lin1")
	if err != nil {
		t.Fatal(err)
	}
	heads := 0
	for _, h := range hist {
		if h.IsLatest {
			heads++
		}
	}
	if heads != 1 {
		t.Fatalf("expected exactly one head, got %d", heads)
	}
	latest, err := s.GetLatest(ctx, "lin1")
	if err != nil {
		t.Fatal(err)
	}
	if latest.Version != 5 {
		t.Fatalf("latest version %d, want 5", latest.Version)
	}
}

func TestConcurrentAppendVersionNoDuplicateVersions(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	const n = 50
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			a := mkArtifact(fmt.Sprintf("c%d", i), "linC")
			if err := s.AppendVersion(ctx, a); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("append error: %v", err)
	}
	hist, _ := s.ListLineage(ctx, "linC")
	if len(hist) != n {
		t.Fatalf("expected %d versions, got %d", n, len(hist))
	}
	seen := map[int64]bool{}
	heads := 0
	for _, h := range hist {
		if seen[h.Version] {
			t.Fatalf("duplicate version %d", h.Version)
		}
		seen[h.Version] = true
		if h.IsLatest {
			heads++
		}
	}
	if heads != 1 {
		t.Fatalf("expected one head after concurrent appends, got %d", heads)
	}
}

func TestUpdateStateGuardsTransitions(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	a := mkArtifact("a1", "lin")
	if err := s.AppendVersion(ctx, a); err != nil {
		t.Fatal(err)
	}
	// Wrong expected state is rejected.
	if err := s.UpdateState(ctx, "a1", artifacts.Pending, artifacts.Deleting); !errors.Is(err, artifacts.ErrIllegalTransition) {
		t.Fatalf("expected illegal transition, got %v", err)
	}
	// Illegal edge is rejected.
	if err := s.UpdateState(ctx, "a1", artifacts.Available, artifacts.Deleted); !errors.Is(err, artifacts.ErrIllegalTransition) {
		t.Fatalf("expected illegal edge rejection, got %v", err)
	}
	// Legal edge succeeds and stamps DeletedAt on Deleted.
	if err := s.UpdateState(ctx, "a1", artifacts.Available, artifacts.Deleting); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateState(ctx, "a1", artifacts.Deleting, artifacts.Deleted); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(ctx, "a1")
	if got.DeletedAt == nil {
		t.Fatalf("DeletedAt should be set after delete")
	}
}

func TestSetLatestRollback(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	var ids []string
	for i := 0; i < 3; i++ {
		a := mkArtifact(fmt.Sprintf("v%d", i), "lin")
		_ = s.AppendVersion(ctx, a)
		ids = append(ids, a.ID)
	}
	if err := s.SetLatest(ctx, "lin", ids[0]); err != nil {
		t.Fatal(err)
	}
	latest, _ := s.GetLatest(ctx, "lin")
	if latest.ID != ids[0] {
		t.Fatalf("rollback head = %s, want %s", latest.ID, ids[0])
	}
}

func TestFindByContentHashDedup(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	a := mkArtifact("d1", "lin")
	a.ContentHash = "sha256:deadbeef"
	_ = s.AppendVersion(ctx, a)

	got, err := s.FindByContentHash(ctx, "artifacts", "sha256:deadbeef")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "d1" {
		t.Fatalf("dedup lookup returned %s", got.ID)
	}
	if _, err := s.FindByContentHash(ctx, "artifacts", "sha256:missing"); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestFindExpiredHonorsLegalHold(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	past := time.Now().Add(-time.Hour)

	held := mkArtifact("held", "l1")
	held.Retention = artifacts.RetentionPolicy{Mode: artifacts.RetainUntil, ExpireAt: &past, LegalHold: true}
	_ = s.AppendVersion(ctx, held)

	exp := mkArtifact("exp", "l2")
	exp.Retention = artifacts.RetentionPolicy{Mode: artifacts.RetainUntil, ExpireAt: &past}
	_ = s.AppendVersion(ctx, exp)

	found, err := s.FindExpired(ctx, time.Now(), 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 || found[0].ID != "exp" {
		t.Fatalf("expected only 'exp' expired, got %+v", found)
	}
}

func TestListQueryFilterAndPagination(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	for i := 0; i < 10; i++ {
		a := mkArtifact(fmt.Sprintf("o%d", i), fmt.Sprintf("lin%d", i))
		a.Owner = "alice"
		if i%2 == 0 {
			a.Owner = "bob"
		}
		_ = s.AppendVersion(ctx, a)
	}
	alice, err := s.List(ctx, Query{Owner: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	if len(alice) != 5 {
		t.Fatalf("expected 5 for alice, got %d", len(alice))
	}
	page, _ := s.List(ctx, Query{Limit: 3})
	if len(page) != 3 {
		t.Fatalf("expected page size 3, got %d", len(page))
	}
	n, _ := s.Count(ctx, Query{Owner: "bob"})
	if n != 5 {
		t.Fatalf("count bob = %d, want 5", n)
	}
}
