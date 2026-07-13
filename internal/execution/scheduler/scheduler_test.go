package scheduler

import (
	stdctx "context"
	"fmt"
	"sync"
	"testing"

	"cpip/internal/execution/job"
)

func mk(id string, p job.Priority) job.Job { return job.Job{ID: id, Priority: p} }

func TestMemorySchedule(t *testing.T) {
	s := NewMemory(0)
	ctx := stdctx.Background()
	if err := s.Schedule(ctx, mk("j1", job.PriorityNormal)); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if !s.Has("j1") || s.Len() != 1 {
		t.Fatal("job not recorded")
	}
	if err := s.Cancel(ctx, "j1"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if s.Has("j1") {
		t.Fatal("job not removed after cancel")
	}
}

func TestMemoryCapacityUnavailable(t *testing.T) {
	s := NewMemory(2)
	ctx := stdctx.Background()
	_ = s.Schedule(ctx, mk("j1", job.PriorityNormal))
	_ = s.Schedule(ctx, mk("j2", job.PriorityNormal))
	if err := s.Schedule(ctx, mk("j3", job.PriorityNormal)); err != job.ErrSchedulerUnavailable {
		t.Fatalf("over-capacity err = %v, want ErrSchedulerUnavailable", err)
	}
	// Re-scheduling an existing job stays within capacity.
	if err := s.Schedule(ctx, mk("j1", job.PriorityHigh)); err != nil {
		t.Fatalf("re-schedule existing: %v", err)
	}
}

func TestMemoryDrainPriorityOrder(t *testing.T) {
	s := NewMemory(0)
	ctx := stdctx.Background()
	_ = s.Schedule(ctx, mk("low", job.PriorityLow))
	_ = s.Schedule(ctx, mk("crit", job.PriorityCritical))
	_ = s.Schedule(ctx, mk("norm1", job.PriorityNormal))
	_ = s.Schedule(ctx, mk("norm2", job.PriorityNormal))

	order := s.Drain()
	if len(order) != 4 || order[0] != "crit" {
		t.Fatalf("drain order = %v, want crit first", order)
	}
	// FIFO within the same priority.
	posNorm1, posNorm2 := indexOf(order, "norm1"), indexOf(order, "norm2")
	if posNorm1 > posNorm2 {
		t.Fatalf("FIFO within priority violated: %v", order)
	}
	if order[len(order)-1] != "low" {
		t.Fatalf("lowest priority not last: %v", order)
	}
	if s.Len() != 0 {
		t.Fatal("drain did not clear the schedule")
	}
}

func TestReprioritize(t *testing.T) {
	s := NewMemory(0)
	ctx := stdctx.Background()
	_ = s.Schedule(ctx, mk("j1", job.PriorityLow))
	_ = s.Schedule(ctx, mk("j2", job.PriorityNormal))
	_ = s.Reprioritize(ctx, "j1", job.PriorityCritical)
	if s.Drain()[0] != "j1" {
		t.Fatal("reprioritized job not first")
	}
}

func TestNoopScheduler(t *testing.T) {
	var s Scheduler = NewNoop()
	ctx := stdctx.Background()
	if err := s.Schedule(ctx, mk("j", job.PriorityNormal)); err != nil {
		t.Fatalf("noop schedule: %v", err)
	}
	if err := s.Retry(ctx, mk("j", job.PriorityNormal)); err != nil {
		t.Fatalf("noop retry: %v", err)
	}
}

func TestConcurrentScheduling(t *testing.T) {
	s := NewMemory(0)
	ctx := stdctx.Background()
	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("j%d", i)
			_ = s.Schedule(ctx, mk(id, job.Priority(i%4)))
			_ = s.Has(id)
			if i%3 == 0 {
				_ = s.Cancel(ctx, id)
			}
		}(i)
	}
	wg.Wait()
	// Roughly two-thirds remain; exact count depends on the %3 cancellation.
	if s.Len() == 0 || s.Len() > 500 {
		t.Fatalf("unexpected length after concurrent ops: %d", s.Len())
	}
}

func indexOf(s []string, v string) int {
	for i, x := range s {
		if x == v {
			return i
		}
	}
	return -1
}
