// Package health implements the Health Monitoring Framework: component and
// dependency health checks, the liveness / readiness / startup probe model,
// health aggregation, and cached snapshots. It answers the two questions every
// orchestrator asks — "is this process alive?" (liveness) and "can it serve
// traffic yet?" (readiness) — plus "has it finished starting?" (startup).
//
// Checks are plain functions registered with the Registry; a background runner
// executes them on an interval with per-check timeouts and caches the results, so
// probe handlers answer in O(1) without stampeding a slow dependency. Aggregation
// is worst-of, with non-critical failures degrading rather than downing the
// service.
package health

import (
	"context"
	"sort"
	"sync"
	"time"

	"cpip/internal/observability/config"
	"cpip/internal/observability/events"
	"cpip/internal/observability/logger"
)

// Status is a health verdict.
type Status string

const (
	StatusUp       Status = "up"
	StatusDegraded Status = "degraded"
	StatusDown     Status = "down"
	StatusUnknown  Status = "unknown"
)

func severity(s Status) int {
	switch s {
	case StatusUp:
		return 0
	case StatusUnknown:
		return 1
	case StatusDegraded:
		return 2
	case StatusDown:
		return 3
	default:
		return 1
	}
}

// Kind tags what probe(s) a check participates in.
type Kind string

const (
	Liveness  Kind = "liveness"
	Readiness Kind = "readiness"
	Startup   Kind = "startup"
)

// Result is the outcome of a single check.
type Result struct {
	Status   Status         `json:"status"`
	Message  string         `json:"message,omitempty"`
	Details  map[string]any `json:"details,omitempty"`
	Duration time.Duration  `json:"duration"`
	Time     time.Time      `json:"time"`
}

// Check is a named health probe. Implementations must honor ctx cancellation
// (the runner enforces a per-check timeout).
type Check interface {
	Name() string
	Check(ctx context.Context) Result
}

// CheckFunc adapts a function to a Check.
type CheckFunc struct {
	CheckName string
	Fn        func(ctx context.Context) Result
}

// Name implements Check.
func (c CheckFunc) Name() string { return c.CheckName }

// Check implements Check.
func (c CheckFunc) Check(ctx context.Context) Result { return c.Fn(ctx) }

// NewCheck builds a Check from a name and function.
func NewCheck(name string, fn func(ctx context.Context) Result) Check {
	return CheckFunc{CheckName: name, Fn: fn}
}

// Up / Down / Degraded are Result constructors for common cases.
func Up(msg string) Result       { return Result{Status: StatusUp, Message: msg} }
func Down(msg string) Result     { return Result{Status: StatusDown, Message: msg} }
func Degraded(msg string) Result { return Result{Status: StatusDegraded, Message: msg} }

// Options customize a registered check.
type Options struct {
	// Kinds are the probes this check participates in (default: liveness+readiness).
	Kinds []Kind
	// Critical, when false, makes a Down result degrade (not down) the aggregate —
	// used for optional dependencies whose loss reduces but does not stop service.
	Critical bool
	// Dependency marks the check as an external dependency (for reporting).
	Dependency bool
}

type registered struct {
	check    Check
	kinds    map[Kind]bool
	critical bool
	depend   bool
}

type cached struct {
	result Result
	reg    *registered
}

// Snapshot is an aggregated health view over a set of checks.
type Snapshot struct {
	Status     Status            `json:"status"`
	Components map[string]Result `json:"components"`
	Time       time.Time         `json:"time"`
}

// Registry is the Health Registry: it registers checks, runs them (on a schedule
// and on demand), caches results, and aggregates snapshots for each probe kind.
type Registry struct {
	cfg config.Health
	bus *events.Bus
	log *logger.Logger
	now func() time.Time

	mu      sync.RWMutex
	checks  map[string]*registered
	results map[string]cached
	lastRun time.Time
	startOK bool

	cancel context.CancelFunc
	done   chan struct{}
}

// Params configures a Registry.
type Params struct {
	Config config.Health
	Events *events.Bus
	Logger *logger.Logger
}

// NewRegistry constructs a Health Registry.
func NewRegistry(p Params) *Registry {
	return &Registry{
		cfg:     p.Config,
		bus:     p.Events,
		log:     p.Logger.With("subsystem", "health"),
		now:     time.Now,
		checks:  make(map[string]*registered),
		results: make(map[string]cached),
	}
}

// Register adds a check. Re-registering a name replaces it.
func (r *Registry) Register(check Check, opts Options) {
	kinds := opts.Kinds
	if len(kinds) == 0 {
		kinds = []Kind{Liveness, Readiness}
	}
	km := make(map[Kind]bool, len(kinds))
	for _, k := range kinds {
		km[k] = true
	}
	r.mu.Lock()
	r.checks[check.Name()] = &registered{check: check, kinds: km, critical: opts.Critical, depend: opts.Dependency}
	r.mu.Unlock()
}

// RegisterFunc is a convenience for Register(NewCheck(...), opts).
func (r *Registry) RegisterFunc(name string, fn func(ctx context.Context) Result, opts Options) {
	r.Register(NewCheck(name, fn), opts)
}

// Unregister removes a check.
func (r *Registry) Unregister(name string) {
	r.mu.Lock()
	delete(r.checks, name)
	delete(r.results, name)
	r.mu.Unlock()
}

// Names returns registered check names.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.checks))
	for n := range r.checks {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Run executes every check concurrently (bounded by per-check timeout), updates
// the cache, and emits HealthChanged on any component transition. Returns the
// full snapshot.
func (r *Registry) Run(ctx context.Context) Snapshot {
	r.mu.RLock()
	regs := make([]*registered, 0, len(r.checks))
	for _, rg := range r.checks {
		regs = append(regs, rg)
	}
	prev := make(map[string]Status, len(r.results))
	for n, c := range r.results {
		prev[n] = c.result.Status
	}
	r.mu.RUnlock()

	type outcome struct {
		name   string
		result Result
		reg    *registered
	}
	results := make([]outcome, len(regs))
	var wg sync.WaitGroup
	for i, rg := range regs {
		wg.Add(1)
		go func(i int, rg *registered) {
			defer wg.Done()
			results[i] = outcome{name: rg.check.Name(), result: r.runOne(ctx, rg), reg: rg}
		}(i, rg)
	}
	wg.Wait()

	r.mu.Lock()
	for _, o := range results {
		r.results[o.name] = cached{result: o.result, reg: o.reg}
	}
	r.lastRun = r.now()
	r.mu.Unlock()

	// Emit transitions.
	for _, o := range results {
		if old, ok := prev[o.name]; ok && old != o.result.Status {
			r.emitChange(o.name, old, o.result.Status)
		} else if !ok && o.result.Status != StatusUp {
			r.emitChange(o.name, StatusUnknown, o.result.Status)
		}
	}
	return r.aggregate(nil)
}

func (r *Registry) runOne(ctx context.Context, rg *registered) Result {
	cctx, cancel := context.WithTimeout(ctx, r.cfg.Timeout)
	defer cancel()
	start := r.now()
	done := make(chan Result, 1)
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				done <- Result{Status: StatusDown, Message: "panic in health check"}
			}
		}()
		done <- rg.check.Check(cctx)
	}()
	select {
	case res := <-done:
		res.Duration = r.now().Sub(start)
		if res.Time.IsZero() {
			res.Time = r.now()
		}
		if res.Status == "" {
			res.Status = StatusUnknown
		}
		return res
	case <-cctx.Done():
		return Result{Status: StatusDown, Message: "health check timed out", Duration: r.now().Sub(start), Time: r.now()}
	}
}

// snapshot returns an aggregated view. When the background runner is active it
// serves the cache (refreshing only past CacheTTL); with no runner it runs the
// checks on demand so every call is fresh.
func (r *Registry) snapshot(ctx context.Context, kind *Kind) Snapshot {
	if r.isRunnerStopped() {
		r.Run(ctx) // on-demand mode: always fresh
		return r.aggregate(kind)
	}
	r.mu.RLock()
	empty := len(r.results) == 0
	stale := !r.lastRun.IsZero() && r.cfg.CacheTTL > 0 && r.now().Sub(r.lastRun) > r.cfg.CacheTTL
	r.mu.RUnlock()
	if empty || stale {
		r.Run(ctx)
	}
	return r.aggregate(kind)
}

func (r *Registry) isRunnerStopped() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.done == nil
}

// aggregate builds a snapshot from cached results, filtered by kind (nil = all).
func (r *Registry) aggregate(kind *Kind) Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	snap := Snapshot{Status: StatusUp, Components: map[string]Result{}, Time: r.now()}
	worst := 0
	any := false
	for name, c := range r.results {
		if kind != nil && !c.reg.kinds[*kind] {
			continue
		}
		any = true
		snap.Components[name] = c.result
		effective := c.result.Status
		// Non-critical failures degrade rather than down the aggregate.
		if !c.reg.critical && effective == StatusDown {
			effective = StatusDegraded
		}
		if s := severity(effective); s > worst {
			worst = s
		}
	}
	if !any {
		snap.Status = StatusUnknown
		return snap
	}
	switch {
	case worst >= severity(StatusDown):
		snap.Status = StatusDown
	case worst >= severity(StatusDegraded):
		snap.Status = StatusDegraded
	case worst >= severity(StatusUnknown):
		snap.Status = StatusUnknown
	default:
		snap.Status = StatusUp
	}
	// Readiness additionally gates on startup completion.
	if kind != nil && *kind == Readiness && !r.startupComplete() {
		if severity(snap.Status) < severity(StatusDown) {
			snap.Status = StatusDown
		}
	}
	return snap
}

func (r *Registry) startupComplete() bool {
	for _, c := range r.results {
		if c.reg.kinds[Startup] && c.result.Status != StatusUp {
			return false
		}
	}
	return true
}

// Liveness returns the liveness snapshot.
func (r *Registry) Liveness(ctx context.Context) Snapshot { k := Liveness; return r.snapshot(ctx, &k) }

// Readiness returns the readiness snapshot (gated on startup).
func (r *Registry) Readiness(ctx context.Context) Snapshot {
	k := Readiness
	return r.snapshot(ctx, &k)
}

// StartupSnapshot returns the startup snapshot.
func (r *Registry) StartupSnapshot(ctx context.Context) Snapshot {
	k := Startup
	return r.snapshot(ctx, &k)
}

// CheckAll returns the aggregate over every check.
func (r *Registry) CheckAll(ctx context.Context) Snapshot { return r.snapshot(ctx, nil) }

// Healthy reports whether the aggregate liveness status is Up.
func (r *Registry) Healthy(ctx context.Context) bool { return r.Liveness(ctx).Status == StatusUp }

func (r *Registry) emitChange(name string, from, to Status) {
	r.log.Slog().Info("health_changed", "component", name, "from", string(from), "to", string(to))
	r.bus.Emit(events.HealthChanged, "health", func(e *events.Event) {
		e.Name = name
		e.Payload = map[string]any{"from": string(from), "to": string(to)}
	})
}

// Start launches the background runner.
func (r *Registry) Start(ctx context.Context) {
	r.mu.Lock()
	if r.done != nil {
		r.mu.Unlock()
		return
	}
	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	r.cancel = cancel
	r.done = done
	r.mu.Unlock()

	go func() {
		defer close(done)
		ticker := time.NewTicker(r.cfg.Interval)
		defer ticker.Stop()
		r.Run(loopCtx) // prime the cache immediately
		for {
			select {
			case <-loopCtx.Done():
				return
			case <-ticker.C:
				r.Run(loopCtx)
			}
		}
	}()
}

// Stop halts the background runner.
func (r *Registry) Stop() {
	r.mu.Lock()
	cancel, done := r.cancel, r.done
	r.cancel = nil
	r.done = nil
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}
