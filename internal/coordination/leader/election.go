package leader

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"cpip/internal/coordination/config"
	"cpip/internal/coordination/events"
	"cpip/internal/coordination/logger"
	"cpip/internal/coordination/metrics"
	"cpip/internal/coordination/types"
)

// Callback is invoked on a leadership transition. Callbacks run on the election's
// own goroutine and must not block; dispatch heavy work elsewhere.
type Callback func(ctx context.Context)

// Election is the per-scope leadership runtime for one candidate. It runs a
// single loop that campaigns while a follower and renews while a leader, firing
// OnElected / OnLost on transitions. Leadership loss is detected the instant a
// renewal fails (lease lapsed or superseded).
type Election struct {
	scope       string
	candidateID string
	elector     Elector
	cfg         config.Leader
	bus         *events.Bus
	rec         metrics.Recorder
	log         *logger.Logger

	isLeader atomic.Bool
	leaderID atomic.Value // string

	mu        sync.Mutex
	onElected []Callback
	onLost    []Callback
	cancel    context.CancelFunc
	done      chan struct{}
}

func newElection(scope, candidateID string, el Elector, cfg config.Leader, bus *events.Bus, rec metrics.Recorder, log *logger.Logger) *Election {
	e := &Election{scope: scope, candidateID: candidateID, elector: el, cfg: cfg, bus: bus, rec: rec, log: log}
	e.leaderID.Store("")
	return e
}

// Scope returns the election scope (a named leadership domain).
func (e *Election) Scope() string { return e.scope }

// CandidateID returns this candidate's identity.
func (e *Election) CandidateID() string { return e.candidateID }

// IsLeader reports whether this candidate currently holds leadership.
func (e *Election) IsLeader() bool { return e.isLeader.Load() }

// LeaderID returns the last-observed leader id (may be empty if unknown).
func (e *Election) LeaderID() string {
	v, _ := e.leaderID.Load().(string)
	return v
}

// Leader queries the backend for the authoritative current leader.
func (e *Election) Leader(ctx context.Context) (string, error) {
	id, found, err := e.elector.Leader(ctx, e.scope)
	if err != nil {
		return "", err
	}
	if !found {
		return "", types.ErrNoLeader
	}
	e.leaderID.Store(id)
	return id, nil
}

// OnElected registers a callback fired when this candidate becomes leader.
func (e *Election) OnElected(fn Callback) {
	e.mu.Lock()
	e.onElected = append(e.onElected, fn)
	e.mu.Unlock()
}

// OnLost registers a callback fired when this candidate loses leadership.
func (e *Election) OnLost(fn Callback) {
	e.mu.Lock()
	e.onLost = append(e.onLost, fn)
	e.mu.Unlock()
}

// Start launches the election loop. Idempotent.
func (e *Election) Start(ctx context.Context) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.done != nil {
		return
	}
	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	e.cancel = cancel
	e.done = done
	go func() {
		defer close(done)
		e.run(loopCtx)
	}()
}

func (e *Election) run(ctx context.Context) {
	// Campaign immediately, then pace by lease/retry.
	for {
		var wait time.Duration
		if e.isLeader.Load() {
			wait = e.renewTick(ctx)
		} else {
			wait = e.campaignTick(ctx)
		}
		select {
		case <-ctx.Done():
			// Best-effort resign on shutdown so a successor is elected promptly.
			if e.isLeader.Load() {
				rctx, c := context.WithTimeout(context.Background(), e.cfg.RenewInterval)
				_ = e.elector.Resign(rctx, e.scope, e.candidateID)
				c()
				e.transitionLost(ctx, "shutdown")
			}
			return
		case <-time.After(wait):
		}
	}
}

func (e *Election) campaignTick(ctx context.Context) time.Duration {
	e.rec.IncCounter(metrics.MetricLeaderCampaign, map[string]string{"scope": e.scope})
	won, err := e.elector.Campaign(ctx, e.scope, e.candidateID, e.cfg.Lease)
	if err != nil {
		e.rec.IncCounter(metrics.MetricBackendError, map[string]string{"op": "campaign"})
		return e.cfg.RetryInterval
	}
	if won {
		e.transitionWon(ctx)
		return e.cfg.RenewInterval
	}
	// Track who the current leader is for observability.
	if id, found, _ := e.elector.Leader(ctx, e.scope); found {
		e.leaderID.Store(id)
	}
	return e.cfg.RetryInterval
}

func (e *Election) renewTick(ctx context.Context) time.Duration {
	ok, err := e.elector.Renew(ctx, e.scope, e.candidateID, e.cfg.Lease)
	if err != nil {
		e.rec.IncCounter(metrics.MetricBackendError, map[string]string{"op": "renew"})
		// Transient backend error: keep leadership but retry quickly; the lease
		// TTL still protects against a partitioned leader.
		return e.cfg.RetryInterval
	}
	if !ok {
		e.transitionLost(ctx, "renew_failed")
		return e.cfg.RetryInterval
	}
	e.rec.IncCounter(metrics.MetricLeaderRenewed, map[string]string{"scope": e.scope})
	return e.cfg.RenewInterval
}

func (e *Election) transitionWon(ctx context.Context) {
	if e.isLeader.Swap(true) {
		return // already leader
	}
	e.leaderID.Store(e.candidateID)
	e.rec.IncCounter(metrics.MetricLeaderElected, map[string]string{"scope": e.scope})
	e.rec.SetGauge(metrics.MetricLeaderIsLeader, 1, map[string]string{"scope": e.scope})
	e.log.Leader(ctx, "elected", e.scope, e.candidateID, nil)
	e.bus.Emit(events.LeaderElected, "leader", func(ev *events.Event) {
		ev.LeaderID = e.candidateID
		ev.Resource = e.scope
	})
	e.fire(ctx, e.snapshot(e.onElected))
}

func (e *Election) transitionLost(ctx context.Context, reason string) {
	if !e.isLeader.Swap(false) {
		return // wasn't leader
	}
	e.leaderID.Store("")
	e.rec.IncCounter(metrics.MetricLeaderLost, map[string]string{"scope": e.scope, "reason": reason})
	e.rec.SetGauge(metrics.MetricLeaderIsLeader, 0, map[string]string{"scope": e.scope})
	e.log.Leader(ctx, "lost:"+reason, e.scope, e.candidateID, types.ErrLeadershipLost)
	e.bus.Emit(events.LeaderLost, "leader", func(ev *events.Event) {
		ev.LeaderID = e.candidateID
		ev.Resource = e.scope
		ev.Payload = map[string]any{"reason": reason}
	})
	e.fire(ctx, e.snapshot(e.onLost))
}

// Resign voluntarily gives up leadership now; the loop resumes campaigning.
func (e *Election) Resign(ctx context.Context) error {
	if !e.isLeader.Load() {
		return types.ErrNotLeader
	}
	if err := e.elector.Resign(ctx, e.scope, e.candidateID); err != nil {
		return err
	}
	e.log.Leader(ctx, "resigned", e.scope, e.candidateID, nil)
	e.bus.Emit(events.LeaderStepped, "leader", func(ev *events.Event) {
		ev.LeaderID = e.candidateID
		ev.Resource = e.scope
	})
	e.transitionLost(ctx, "resigned")
	return nil
}

// Transfer hands leadership to another candidate. The caller must currently be
// the leader. After a successful transfer this candidate steps down; the target's
// election loop confirms ownership on its next tick.
func (e *Election) Transfer(ctx context.Context, to string) error {
	if !e.isLeader.Load() {
		return types.ErrNotLeader
	}
	ok, err := e.elector.Transfer(ctx, e.scope, e.candidateID, to, e.cfg.Lease)
	if err != nil {
		return err
	}
	if !ok {
		return types.ErrLeadershipLost
	}
	e.log.Leader(ctx, "transferred_to:"+to, e.scope, e.candidateID, nil)
	e.bus.Emit(events.LeaderStepped, "leader", func(ev *events.Event) {
		ev.LeaderID = e.candidateID
		ev.Resource = e.scope
		ev.Payload = map[string]any{"transferred_to": to}
	})
	e.transitionLost(ctx, "transferred")
	return nil
}

// Stop halts the election loop (resigning if leader) and waits for it to exit.
func (e *Election) Stop() {
	e.mu.Lock()
	cancel, done := e.cancel, e.done
	e.cancel = nil
	e.done = nil
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (e *Election) snapshot(cbs []Callback) []Callback {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]Callback(nil), cbs...)
}

func (e *Election) fire(ctx context.Context, cbs []Callback) {
	for _, fn := range cbs {
		fn(ctx)
	}
}
