// Package locks implements the Distributed Lock Service over the coordination
// backend. A lock is a key holding a unique, unguessable owner token; acquisition
// is an atomic SET NX with TTL, and release/renew are token-fenced atomic
// operations so a caller can only ever release or extend a lock it still owns —
// the fencing that prevents the classic "expired-then-deleted-by-someone-else"
// bug.
//
// Safety properties:
//   - Mutual exclusion: SET NX guarantees a single owner per resource.
//   - Deadlock freedom:  every lease has a TTL, so a crashed holder's lock
//     self-expires; acquisition is bounded by AcquireTimeout and never blocks
//     forever.
//   - Liveness under long work: an optional watchdog auto-renews the lease at a
//     fraction of its duration, so honest long-running work keeps the lock while
//     a crash still releases it within one lease.
//   - Fencing: the token doubles as a monotonic-ish fence so downstream systems
//     can reject writes from a superseded holder.
//
// The design is deliberately Redlock-COMPATIBLE (token fencing, clock-drift-
// adjusted validity) so a future multi-node Redlock quorum can be dropped in
// behind the same Lock API without touching callers.
package locks

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"cpip/internal/coordination/backend"
	"cpip/internal/coordination/config"
	"cpip/internal/coordination/events"
	"cpip/internal/coordination/keys"
	"cpip/internal/coordination/logger"
	"cpip/internal/coordination/metrics"
	"cpip/internal/coordination/types"
)

// Manager mints and manages distributed locks.
type Manager struct {
	backend backend.Backend
	cfg     config.Lock
	kb      keys.Builder
	nodeID  string
	bus     *events.Bus
	rec     metrics.Recorder
	log     *logger.Logger

	held atomic.Int64
}

// Params configures a Manager.
type Params struct {
	Backend backend.Backend
	Config  config.Lock
	Keys    keys.Builder
	NodeID  string
	Events  *events.Bus
	Metrics metrics.Recorder
	Logger  *logger.Logger
}

// New constructs a lock Manager.
func New(p Params) *Manager {
	rec := p.Metrics
	if rec == nil {
		rec = metrics.NewNoop()
	}
	return &Manager{
		backend: p.Backend,
		cfg:     p.Config,
		kb:      p.Keys,
		nodeID:  p.NodeID,
		bus:     p.Events,
		rec:     rec,
		log:     p.Logger.With("subsystem", "locks"),
	}
}

// Options customize a single acquisition.
type Options struct {
	Lease          time.Duration
	AcquireTimeout time.Duration
	AutoRenew      bool
}

// Lock is a held distributed lock, safe for concurrent Release/Renew.
type Lock struct {
	mgr      *Manager
	resource string
	fullKey  string
	token    string

	mu         sync.Mutex
	lease      time.Duration
	acquiredAt time.Time

	released atomic.Bool
	lost     atomic.Bool
	stopWD   chan struct{}
	wdOnce   sync.Once
}

func (m *Manager) mintToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	if m.nodeID != "" {
		return m.nodeID + ":" + hex.EncodeToString(b)
	}
	return hex.EncodeToString(b)
}

// Acquire blocks until the lock on resource is obtained or AcquireTimeout elapses.
func (m *Manager) Acquire(ctx context.Context, resource string, opts *Options) (*Lock, error) {
	o := m.resolve(opts)
	fullKey := m.kb.LockKey(resource)
	token := m.mintToken()
	start := time.Now()
	deadline := start.Add(o.AcquireTimeout)

	for {
		ok, err := m.backend.SetNX(ctx, fullKey, token, o.Lease)
		if err != nil {
			m.rec.IncCounter(metrics.MetricBackendError, map[string]string{"op": "lock_acquire"})
			return nil, err
		}
		if ok {
			return m.newLock(ctx, resource, fullKey, token, o, start), nil
		}
		m.rec.IncCounter(metrics.MetricLockContended, map[string]string{"resource": resource})
		if o.AcquireTimeout <= 0 || time.Now().After(deadline) {
			m.log.Lock(ctx, "timeout", resource, token, time.Since(start), types.ErrLockNotAcquired)
			return nil, fmt.Errorf("%w: resource %q after %s", types.ErrLockNotAcquired, resource, o.AcquireTimeout)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(m.cfg.RetryInterval):
		}
	}
}

// TryAcquire attempts a single non-blocking acquisition.
func (m *Manager) TryAcquire(ctx context.Context, resource string, opts *Options) (*Lock, error) {
	o := m.resolve(opts)
	o.AcquireTimeout = 0
	fullKey := m.kb.LockKey(resource)
	token := m.mintToken()
	ok, err := m.backend.SetNX(ctx, fullKey, token, o.Lease)
	if err != nil {
		return nil, err
	}
	if !ok {
		m.rec.IncCounter(metrics.MetricLockContended, map[string]string{"resource": resource})
		return nil, types.ErrLockNotAcquired
	}
	return m.newLock(ctx, resource, fullKey, token, o, time.Now()), nil
}

// WithLock acquires the lock, runs fn, and releases it even if fn panics.
func (m *Manager) WithLock(ctx context.Context, resource string, opts *Options, fn func(ctx context.Context) error) error {
	l, err := m.Acquire(ctx, resource, opts)
	if err != nil {
		return err
	}
	defer func() { _ = l.Release(context.WithoutCancel(ctx)) }()
	return fn(ctx)
}

func (m *Manager) newLock(ctx context.Context, resource, fullKey, token string, o Options, start time.Time) *Lock {
	l := &Lock{mgr: m, resource: resource, fullKey: fullKey, token: token, lease: o.Lease, acquiredAt: time.Now(), stopWD: make(chan struct{})}
	m.held.Add(1)
	m.rec.SetGauge(metrics.MetricLockHeld, float64(m.held.Load()), nil)
	m.rec.IncCounter(metrics.MetricLockAcquired, map[string]string{"resource": resource})
	metrics.ObserveDuration(m.rec, metrics.MetricLockWaitMs, start, map[string]string{"resource": resource})
	m.log.Lock(ctx, "acquired", resource, token, time.Since(start), nil)
	m.bus.Emit(events.LockAcquired, "locks", func(e *events.Event) { e.Resource = resource; e.Origin = m.nodeID })
	if o.AutoRenew {
		l.startWatchdog(ctx)
	}
	return l
}

func (m *Manager) resolve(opts *Options) Options {
	o := Options{Lease: m.cfg.DefaultLease, AcquireTimeout: m.cfg.AcquireTimeout}
	if opts != nil {
		if opts.Lease > 0 {
			o.Lease = opts.Lease
		}
		if opts.AcquireTimeout > 0 {
			o.AcquireTimeout = opts.AcquireTimeout
		}
		o.AutoRenew = opts.AutoRenew
	}
	return o
}

// HeldCount returns the number of locks this manager currently holds.
func (m *Manager) HeldCount() int64 { return m.held.Load() }

// --- Lock methods ---

// Token returns the fencing token.
func (l *Lock) Token() string { return l.token }

// Resource returns the locked resource name.
func (l *Lock) Resource() string { return l.resource }

func (l *Lock) leaseSnapshot() (time.Duration, time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lease, l.acquiredAt
}

func (l *Lock) setLease(lease time.Duration, at time.Time) {
	l.mu.Lock()
	l.lease = lease
	l.acquiredAt = at
	l.mu.Unlock()
}

// ValidUntil returns the clock-drift-adjusted expiry — the instant after which
// the holder must NOT assume it still owns the lock (Redlock semantics).
func (l *Lock) ValidUntil() time.Time {
	lease, at := l.leaseSnapshot()
	drift := time.Duration(float64(lease) * l.mgr.cfg.ClockDriftFactor)
	return at.Add(lease - drift)
}

// IsLost reports whether the watchdog observed a failed renewal.
func (l *Lock) IsLost() bool { return l.lost.Load() }

// Renew extends the lease iff this lock still owns the key (token match).
func (l *Lock) Renew(ctx context.Context, lease time.Duration) error {
	if l.released.Load() {
		return types.ErrLockNotHeld
	}
	if lease <= 0 {
		lease, _ = l.leaseSnapshot()
	}
	ok, err := l.mgr.backend.CompareAndExpire(ctx, l.fullKey, l.token, lease)
	if err != nil {
		return err
	}
	if !ok {
		l.lost.Store(true)
		l.mgr.rec.IncCounter(metrics.MetricLockExpired, map[string]string{"resource": l.resource})
		l.mgr.bus.Emit(events.LockExpired, "locks", func(e *events.Event) { e.Resource = l.resource })
		return fmt.Errorf("%w: resource %q", types.ErrLockNotHeld, l.resource)
	}
	l.setLease(lease, time.Now())
	l.mgr.rec.IncCounter(metrics.MetricLockRenewed, map[string]string{"resource": l.resource})
	l.mgr.bus.Emit(events.LockAcquired, "locks", func(e *events.Event) { e.Resource = l.resource })
	return nil
}

// Release relinquishes the lock iff still owned. Idempotent against double calls.
func (l *Lock) Release(ctx context.Context) error {
	if !l.released.CompareAndSwap(false, true) {
		return nil
	}
	l.stopWatchdog()

	ok, err := l.mgr.backend.CompareAndDelete(ctx, l.fullKey, l.token)
	l.mgr.held.Add(-1)
	l.mgr.rec.SetGauge(metrics.MetricLockHeld, float64(l.mgr.held.Load()), nil)
	if err != nil {
		return err
	}
	if !ok {
		l.mgr.log.Lock(ctx, "release_not_held", l.resource, l.token, 0, types.ErrLockNotHeld)
		return fmt.Errorf("%w: resource %q (lease likely expired)", types.ErrLockNotHeld, l.resource)
	}
	_, at := l.leaseSnapshot()
	l.mgr.rec.IncCounter(metrics.MetricLockReleased, map[string]string{"resource": l.resource})
	l.mgr.log.Lock(ctx, "released", l.resource, l.token, time.Since(at), nil)
	l.mgr.bus.Emit(events.LockReleased, "locks", func(e *events.Event) { e.Resource = l.resource; e.Origin = l.mgr.nodeID })
	return nil
}

func (l *Lock) startWatchdog(ctx context.Context) {
	lease, _ := l.leaseSnapshot()
	interval := time.Duration(float64(lease) * l.mgr.cfg.AutoRenewFraction)
	if interval <= 0 {
		interval = lease / 2
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-l.stopWD:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				curLease, _ := l.leaseSnapshot()
				rctx, cancel := context.WithTimeout(context.Background(), interval)
				err := l.Renew(rctx, curLease)
				cancel()
				if err != nil {
					l.mgr.log.Lock(ctx, "watchdog_lost", l.resource, l.token, 0, err)
					return
				}
			}
		}
	}()
}

func (l *Lock) stopWatchdog() { l.wdOnce.Do(func() { close(l.stopWD) }) }
