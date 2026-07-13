package context

import (
	stdctx "context"
	"sync"
	"testing"
	"time"

	"cpip/internal/execution/job"
)

func spec(id string, timeout time.Duration) Spec {
	return Spec{
		Job:      job.Job{ID: id, UserID: "u1", Language: "go", Timeout: timeout},
		Security: SecurityMetadata{UserID: "u1", Authenticated: true, Authorized: true},
	}
}

func TestCreateAndGet(t *testing.T) {
	m := NewManager(func() (string, string) { return "trace-1", "span-1" })
	ec := m.Create(stdctx.Background(), spec("j1", time.Minute))

	if ec.Tracing.TraceID != "trace-1" {
		t.Fatalf("trace id = %q", ec.Tracing.TraceID)
	}
	if ec.Deadline.Before(time.Now()) {
		t.Fatal("deadline should be in the future")
	}
	got, ok := m.Get("j1")
	if !ok || got != ec {
		t.Fatal("Get did not return the stored context")
	}
	if m.Count() != 1 {
		t.Fatalf("count = %d", m.Count())
	}
}

func TestCancelSignalsContext(t *testing.T) {
	m := NewManager(nil)
	ec := m.Create(stdctx.Background(), spec("j1", time.Minute))
	if !m.Cancel("j1") {
		t.Fatal("cancel reported not found")
	}
	select {
	case <-ec.Context.Done():
	default:
		t.Fatal("context not cancelled")
	}
}

func TestReleaseCancelsAndRemoves(t *testing.T) {
	m := NewManager(nil)
	ec := m.Create(stdctx.Background(), spec("j1", time.Minute))
	m.Release("j1")
	if _, ok := m.Get("j1"); ok {
		t.Fatal("context not removed on release")
	}
	select {
	case <-ec.Context.Done():
	default:
		t.Fatal("context not cancelled on release")
	}
}

func TestDeadlineApplied(t *testing.T) {
	m := NewManager(nil)
	ec := m.Create(stdctx.Background(), spec("j1", 20*time.Millisecond))
	select {
	case <-ec.Context.Done():
	case <-time.After(time.Second):
		t.Fatal("deadline did not fire")
	}
}

func TestRecreateCancelsPrevious(t *testing.T) {
	m := NewManager(nil)
	first := m.Create(stdctx.Background(), spec("j1", time.Minute))
	second := m.Create(stdctx.Background(), spec("j1", time.Minute))
	if first == second {
		t.Fatal("recreate should yield a new context")
	}
	select {
	case <-first.Context.Done():
	default:
		t.Fatal("previous context not cancelled on recreate")
	}
}

func TestConcurrentContextAccess(t *testing.T) {
	m := NewManager(nil)
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := string(rune('a' + i%26))
			m.Create(stdctx.Background(), spec(id, time.Minute))
			m.Assign(id, "worker", "sandbox")
			_, _ = m.Get(id)
			m.Cancel(id)
			m.Release(id)
		}(i)
	}
	wg.Wait()
}
