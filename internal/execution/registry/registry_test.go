package registry

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"cpip/internal/execution/job"
)

func mkJob(id, user, room, sess, lang string) job.Job {
	return job.Job{
		ID: id, UserID: user, RoomID: room, SessionID: sess, Language: lang,
		State: job.StatePending, CreatedAt: time.Now(),
	}
}

func TestAddDuplicateAndGet(t *testing.T) {
	r := New()
	if err := r.Add(mkJob("j1", "u1", "r1", "s1", "go")); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := r.Add(mkJob("j1", "u1", "r1", "s1", "go")); err != job.ErrDuplicateJob {
		t.Fatalf("dup add err = %v, want ErrDuplicateJob", err)
	}
	got, ok := r.Get("j1")
	if !ok || got.UserID != "u1" {
		t.Fatalf("get = %+v ok=%v", got, ok)
	}
	// Snapshot isolation: mutating the returned copy must not affect the store.
	got.UserID = "mutated"
	again, _ := r.Get("j1")
	if again.UserID != "u1" {
		t.Fatal("Get returned a mutable alias, not a snapshot")
	}
}

func TestTransitionEnforcesStateMachine(t *testing.T) {
	r := New()
	_ = r.Add(mkJob("j1", "u1", "r1", "s1", "go"))

	from, err := r.Transition("j1", job.StateValidated, nil)
	if err != nil || from != job.StatePending {
		t.Fatalf("valid transition: from=%s err=%v", from, err)
	}
	// Illegal: Validated → Running.
	if _, err := r.Transition("j1", job.StateRunning, nil); err != job.ErrIllegalTransition {
		t.Fatalf("illegal transition err = %v, want ErrIllegalTransition", err)
	}
	// Unknown job.
	if _, err := r.Transition("ghost", job.StateValidated, nil); err != job.ErrJobNotFound {
		t.Fatalf("unknown job err = %v", err)
	}
}

func TestStateIndexReindexes(t *testing.T) {
	r := New()
	_ = r.Add(mkJob("j1", "u1", "r1", "s1", "go"))
	if n := len(r.ByState(job.StatePending)); n != 1 {
		t.Fatalf("pending count = %d", n)
	}
	_, _ = r.Transition("j1", job.StateValidated, nil)
	if n := len(r.ByState(job.StatePending)); n != 0 {
		t.Fatalf("pending count after transition = %d, want 0", n)
	}
	if n := len(r.ByState(job.StateValidated)); n != 1 {
		t.Fatalf("validated count = %d, want 1", n)
	}
}

func TestSecondaryIndexes(t *testing.T) {
	r := New()
	_ = r.Add(mkJob("j1", "u1", "r1", "s1", "go"))
	_ = r.Add(mkJob("j2", "u1", "r1", "s2", "python3"))
	_ = r.Add(mkJob("j3", "u2", "r2", "s3", "go"))

	if got := len(r.ByUser("u1")); got != 2 {
		t.Errorf("ByUser(u1) = %d, want 2", got)
	}
	if got := len(r.ByRoom("r1")); got != 2 {
		t.Errorf("ByRoom(r1) = %d, want 2", got)
	}
	if got := len(r.ByLanguage("go")); got != 2 {
		t.Errorf("ByLanguage(go) = %d, want 2", got)
	}
	if got := len(r.BySession("s2")); got != 1 {
		t.Errorf("BySession(s2) = %d, want 1", got)
	}
}

func TestRemoveDeindexes(t *testing.T) {
	r := New()
	_ = r.Add(mkJob("j1", "u1", "r1", "s1", "go"))
	if _, ok := r.Remove("j1"); !ok {
		t.Fatal("remove reported not found")
	}
	if _, ok := r.Get("j1"); ok {
		t.Fatal("job still present after remove")
	}
	if len(r.ByUser("u1")) != 0 || len(r.ByState(job.StatePending)) != 0 {
		t.Fatal("indexes not cleaned up after remove")
	}
}

func TestUpdateCannotChangeState(t *testing.T) {
	r := New()
	_ = r.Add(mkJob("j1", "u1", "r1", "s1", "go"))
	_ = r.Update("j1", func(j *job.Job) {
		j.State = job.StateCompleted // must be defended against
		j.WorkerID = "w1"
	})
	got, _ := r.Get("j1")
	if got.State != job.StatePending {
		t.Fatalf("Update changed state to %s; must be defended", got.State)
	}
	if got.WorkerID != "w1" {
		t.Fatal("Update did not apply non-state mutation")
	}
}

func TestFinishedBeforeAndStats(t *testing.T) {
	r := New()
	old := mkJob("old", "u1", "r1", "s1", "go")
	_ = r.Add(old)
	_, _ = r.Transition("old", job.StateValidated, nil)
	_, _ = r.Transition("old", job.StateQueued, nil)
	_, _ = r.Transition("old", job.StateCancelled, func(j *job.Job) {
		j.CompletedAt = time.Now().Add(-time.Hour)
	})

	_ = r.Add(mkJob("active", "u1", "r1", "s2", "go"))

	finished := r.FinishedBefore(time.Now().Add(-time.Minute))
	if len(finished) != 1 || finished[0].ID != "old" {
		t.Fatalf("FinishedBefore = %+v, want [old]", finished)
	}

	st := r.Stats()
	if st.Total != 2 || st.FinishedJobs != 1 || st.ActiveJobs != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestConcurrentRegistryAccess(t *testing.T) {
	r := New()
	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("j%d", i)
			_ = r.Add(mkJob(id, fmt.Sprintf("u%d", i%10), "r1", "s1", "go"))
			_, _ = r.Transition(id, job.StateValidated, nil)
			_, _ = r.Transition(id, job.StateQueued, nil)
			_ = r.ByUser(fmt.Sprintf("u%d", i%10))
			_ = r.ByRoom("r1")
			_ = r.Stats()
			_, _ = r.Get(id)
		}(i)
	}
	wg.Wait()
	if r.Count() != n {
		t.Fatalf("count = %d, want %d", r.Count(), n)
	}
	if got := len(r.ByState(job.StateQueued)); got != n {
		t.Fatalf("queued = %d, want %d", got, n)
	}
}
