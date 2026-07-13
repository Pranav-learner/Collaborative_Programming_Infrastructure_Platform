package registry

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"cpip/internal/collaboration/types"
	"cpip/internal/collaboration/yjs"
)

func newEntry(id string) *DocumentEntry {
	return &DocumentEntry{
		ID:     id,
		RoomID: "room-" + id,
		State:  types.StateCreated,
		Doc:    yjs.New(yjs.Options{GC: true}),
	}
}

func TestRegisterAndConflict(t *testing.T) {
	r := New()
	if err := r.Register(newEntry("d1")); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := r.Register(newEntry("d1")); err != types.ErrRegistryConflict {
		t.Fatalf("duplicate register err = %v, want ErrRegistryConflict", err)
	}
	if r.Count() != 1 {
		t.Fatalf("count = %d, want 1", r.Count())
	}
}

func TestParticipantLifecycle(t *testing.T) {
	r := New()
	_ = r.Register(newEntry("d1"))

	n, ok := r.AddParticipant("d1", types.Participant{ID: "alice"})
	if !ok || n != 1 {
		t.Fatalf("add alice: n=%d ok=%v", n, ok)
	}
	n, _ = r.AddParticipant("d1", types.Participant{ID: "bob"})
	if n != 2 {
		t.Fatalf("add bob: n=%d, want 2", n)
	}
	// Idempotent re-add.
	n, _ = r.AddParticipant("d1", types.Participant{ID: "bob"})
	if n != 2 {
		t.Fatalf("re-add bob: n=%d, want 2", n)
	}
	if r.ParticipantCount("d1") != 2 {
		t.Fatalf("participant count = %d, want 2", r.ParticipantCount("d1"))
	}

	r.SetParticipantSync("d1", "alice", types.SyncSynced, time.Now())
	found := false
	for _, p := range r.Participants("d1") {
		if p.ID == "alice" && p.SyncStatus != types.SyncSynced {
			t.Fatalf("alice sync status = %v", p.SyncStatus)
		}
		if p.ID == "alice" {
			found = true
		}
	}
	if !found {
		t.Fatal("alice not present")
	}

	n, removed := r.RemoveParticipant("d1", "alice")
	if !removed || n != 1 {
		t.Fatalf("remove alice: n=%d removed=%v", n, removed)
	}
	if _, removed := r.RemoveParticipant("d1", "ghost"); removed {
		t.Fatal("removing absent participant reported removed")
	}
}

func TestMonotonicVersionSurvivesSnapshot(t *testing.T) {
	r := New()
	_ = r.Register(newEntry("d1"))

	if v := r.MarkEdited("d1"); v != 1 {
		t.Fatalf("v1 = %d", v)
	}
	if v := r.MarkEdited("d1"); v != 2 {
		t.Fatalf("v2 = %d", v)
	}
	r.RecordSnapshot("d1", types.Snapshot{ID: "s1", Version: 2})

	info, _ := r.Info("d1")
	if info.EditCount != 0 {
		t.Fatalf("edit count after snapshot = %d, want 0", info.EditCount)
	}
	if info.IsDirty {
		t.Fatal("dirty after snapshot")
	}
	// Version must NOT regress after a snapshot.
	if v := r.MarkEdited("d1"); v != 3 {
		t.Fatalf("post-snapshot version = %d, want 3", v)
	}
}

func TestConcurrentRegistryAccess(t *testing.T) {
	r := New()
	const docs = 50
	for i := 0; i < docs; i++ {
		_ = r.Register(newEntry(fmt.Sprintf("d%d", i)))
	}

	var wg sync.WaitGroup
	for i := 0; i < docs; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("d%d", i)
			for j := 0; j < 100; j++ {
				r.MarkEdited(id)
				r.AddParticipant(id, types.Participant{ID: fmt.Sprintf("p%d", j)})
				_ = r.Participants(id)
				_ = r.ListDirty()
				_ = r.ParticipantCount(id)
				r.RemoveParticipant(id, fmt.Sprintf("p%d", j))
			}
		}(i)
	}
	wg.Wait()

	if r.Count() != docs {
		t.Fatalf("count = %d, want %d", r.Count(), docs)
	}
}
