package health

import (
	"context"
	"testing"
	"time"

	"cpip/internal/observability/config"
	"cpip/internal/observability/events"
	obslogger "cpip/internal/observability/logger"
)

func newRegistry() *Registry {
	return NewRegistry(Params{
		Config: config.Health{Interval: time.Second, Timeout: 200 * time.Millisecond, CacheTTL: 0},
		Events: events.NewBus(), Logger: obslogger.New(nil),
	})
}

func TestAggregateWorstOf(t *testing.T) {
	r := newRegistry()
	r.RegisterFunc("a", func(context.Context) Result { return Up("ok") }, Options{Critical: true})
	r.RegisterFunc("b", func(context.Context) Result { return Degraded("slow") }, Options{Critical: true})
	snap := r.CheckAll(context.Background())
	if snap.Status != StatusDegraded {
		t.Fatalf("aggregate = %s, want degraded", snap.Status)
	}
	// A critical Down dominates.
	r.RegisterFunc("c", func(context.Context) Result { return Down("dead") }, Options{Critical: true})
	if r.CheckAll(context.Background()).Status != StatusDown {
		t.Fatalf("critical down should down the aggregate")
	}
}

func TestNonCriticalDegrades(t *testing.T) {
	r := newRegistry()
	r.RegisterFunc("up", func(context.Context) Result { return Up("") }, Options{Critical: true})
	r.RegisterFunc("optional", func(context.Context) Result { return Down("gone") }, Options{Critical: false})
	if snap := r.CheckAll(context.Background()); snap.Status != StatusDegraded {
		t.Fatalf("non-critical down should degrade not down, got %s", snap.Status)
	}
}

func TestLivenessReadinessSeparation(t *testing.T) {
	r := newRegistry()
	r.RegisterFunc("alive", func(context.Context) Result { return Up("") }, Options{Kinds: []Kind{Liveness}})
	r.RegisterFunc("dep", func(context.Context) Result { return Down("db down") }, Options{Kinds: []Kind{Readiness}, Critical: true})

	if r.Liveness(context.Background()).Status != StatusUp {
		t.Fatalf("liveness should be up (independent of readiness dep)")
	}
	if r.Readiness(context.Background()).Status != StatusDown {
		t.Fatalf("readiness should be down")
	}
}

func TestStartupGatesReadiness(t *testing.T) {
	r := newRegistry()
	started := false
	r.RegisterFunc("startup", func(context.Context) Result {
		if started {
			return Up("")
		}
		return Down("starting")
	}, Options{Kinds: []Kind{Startup, Readiness}, Critical: true})

	if r.Readiness(context.Background()).Status == StatusUp {
		t.Fatal("readiness should not be up before startup completes")
	}
	started = true
	if r.Readiness(context.Background()).Status != StatusUp {
		t.Fatal("readiness should be up after startup completes")
	}
}

func TestCheckTimeout(t *testing.T) {
	r := newRegistry()
	r.RegisterFunc("slow", func(ctx context.Context) Result {
		<-ctx.Done() // never returns Up before the timeout
		return Up("")
	}, Options{Critical: true})
	snap := r.CheckAll(context.Background())
	if snap.Status != StatusDown {
		t.Fatalf("timed-out check should be down, got %s", snap.Status)
	}
}

func TestPanicInCheckIsDown(t *testing.T) {
	r := newRegistry()
	r.RegisterFunc("boom", func(context.Context) Result { panic("kaboom") }, Options{Critical: true})
	if r.CheckAll(context.Background()).Status != StatusDown {
		t.Fatal("a panicking check must be reported down, not crash")
	}
}
