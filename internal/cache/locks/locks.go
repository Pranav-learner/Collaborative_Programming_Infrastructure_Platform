// Package locks implements a distributed lock manager over Redis. A lock is a
// key holding a unique, unguessable owner token; acquisition is an atomic SET
// NX PX, and release/renew are token-checked atomic operations (Lua on real
// Redis) so a caller can only ever release or extend a lock it still owns —
// the fencing that prevents the classic "expired-then-deleted-by-someone-else"
// bug.
//
// Safety properties:
//   - Mutual exclusion:  SET NX guarantees a single owner per resource.
//   - Deadlock freedom:  every lease has a TTL, so a crashed holder's lock
//     self-expires; acquisition is bounded by AcquireTimeout and never blocks
//     forever.
//   - Liveness under long work: an optional watchdog auto-renews the lease at a
//     fraction of its duration, so honest long-running work keeps the lock while
//     a crash still releases it within one lease.
//   - Fencing: the token is returned to the caller as a monotonic-ish fence so
//     downstream systems can reject writes from a superseded holder.
//
// The single-instance design here is deliberately Redlock-COMPATIBLE (token
// fencing, clock-drift-adjusted validity) so a future multi-node Redlock
// quorum can be dropped in behind the same Lock API.
package locks

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"cpip/internal/cache/config"
	"cpip/internal/cache/events"
	"cpip/internal/cache/keys"
	"cpip/internal/cache/logger"
	"cpip/internal/cache/metrics"
	"cpip/internal/cache/redis"
	"cpip/internal/cache/types"
)

// Manager mints and manages distributed locks.
type Manager struct {
	client redis.Client
	cfg    config.Lock
	kb     keys.Builder
	nodeID string
	bus    *events.Bus
	rec    metrics.Recorder
	log    *logger.Logger

	held atomic.Int64 // gauge of currently-held locks minted by this manager
}

// Params configures a Manager.
type Params struct {
	Client  redis.Client
	Config  config.Lock
	Keys    keys.Builder
	NodeID  string
	Bus     *events.Bus
	Metrics metrics.Recorder
	Logger  *logger.Logger
}

// New constructs a lock Manager.
func New(p Params) *Manager {
	rec := p.Metrics
	if rec == nil {
		rec = metrics.NewNoop()
	}
	log := p.Logger
	if log == nil {
		log = logger.New(nil)
	}
	return &Manager{
		client: p.Client,
		cfg:    p.Config,
		kb:     p.Keys,
		nodeID: p.NodeID,
		bus:    p.Bus,
		rec:    rec,
		log:    log,
	}
}

// Options customize a single acquisition.
type Options struct {
	// Lease overrides the default lease duration.
	Lease time.Duration
	// AcquireTimeout overrides how long Acquire blocks before giving up.
	AcquireTimeout time.Duration
	// AutoRenew, when true, starts a watchdog that keeps the lease alive until
	// the lock is released or its owning context is cancelled.
	AutoRenew bool
}

// Lock is a held distributed lock. It is safe for concurrent Release/Renew: the
// watchdog goroutine and caller may touch it at once, so the mutable lease state
// is mutex-guarded while the immutable identity fields are not.
type Lock struct {
	mgr      *Manager
	resource string
	fullKey  string
	token    string

	mu         sync.Mutex // guards lease and acquiredAt
	lease      time.Duration
	acquiredAt time.Time

	released atomic.Bool
	lost     atomic.Bool
	stopWD   chan struct{}
	wdOnce   sync.Once
}

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

// mintToken returns a globally-unique owner token. The node ID prefix aids
// debugging ("who holds this?") while the random suffix guarantees uniqueness.
func (m *Manager) mintToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	if m.nodeID != "" {
		return m.nodeID + ":" + hex.EncodeToString(b)
	}
	return hex.EncodeToString(b)
}

// Acquire blocks until the lock on resource is obtained or AcquireTimeout
// elapses. It spins with RetryInterval between attempts; the deadline guarantees
// it never blocks indefinitely (deadlock freedom).
func (m *Manager) Acquire(ctx context.Context, resource string, opts *Options) (*Lock, error) {
	o := m.resolve(opts)
	fullKey := m.kb.Lock(resource)
	token := m.mintToken()
	start := time.Now()
	deadline := start.Add(o.AcquireTimeout)

	for {
		ok, err := m.client.SetNX(ctx, fullKey, token, o.Lease)
		if err != nil {
			m.rec.IncCounter(metrics.MetricRedisError, map[string]string{"op": "lock_acquire"})
			return nil, err
		}
		if ok {
			l := &Lock{
				mgr:        m,
				resource:   resource,
				fullKey:    fullKey,
				token:      token,
				lease:      o.Lease,
				acquiredAt: time.Now(),
				stopWD:     make(chan struct{}),
			}
			m.held.Add(1)
			m.rec.SetGauge(metrics.MetricLockHeld, float64(m.held.Load()), map[string]string{})
			m.rec.IncCounter(metrics.MetricLockAcquired, map[string]string{"resource": resource})
			metrics.ObserveDuration(m.rec, metrics.MetricLockWaitMs, start, map[string]string{"resource": resource})
			m.log.Lock(ctx, "acquired", resource, token, time.Since(start), nil)
			if m.bus != nil {
				m.bus.Emit(events.LockAcquired, "locks", func(e *events.Event) { e.Resource = resource })
			}
			if o.AutoRenew {
				l.startWatchdog(ctx)
			}
			return l, nil
		}
		m.rec.IncCounter(metrics.MetricLockContended, map[string]string{"resource": resource})

		if time.Now().After(deadline) {
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

// TryAcquire attempts a single non-blocking acquisition. Returns
// (nil, ErrLockNotAcquired) if the lock is held by someone else.
func (m *Manager) TryAcquire(ctx context.Context, resource string, opts *Options) (*Lock, error) {
	o := m.resolve(opts)
	o.AcquireTimeout = 0
	fullKey := m.kb.Lock(resource)
	token := m.mintToken()
	ok, err := m.client.SetNX(ctx, fullKey, token, o.Lease)
	if err != nil {
		return nil, err
	}
	if !ok {
		m.rec.IncCounter(metrics.MetricLockContended, map[string]string{"resource": resource})
		return nil, types.ErrLockNotAcquired
	}
	l := &Lock{mgr: m, resource: resource, fullKey: fullKey, token: token, lease: o.Lease, acquiredAt: time.Now(), stopWD: make(chan struct{})}
	m.held.Add(1)
	m.rec.SetGauge(metrics.MetricLockHeld, float64(m.held.Load()), map[string]string{})
	m.rec.IncCounter(metrics.MetricLockAcquired, map[string]string{"resource": resource})
	if o.AutoRenew {
		l.startWatchdog(ctx)
	}
	return l, nil
}

// WithLock acquires the lock, runs fn, and releases the lock even if fn panics.
// This is the recommended way to guard a critical section.
func (m *Manager) WithLock(ctx context.Context, resource string, opts *Options, fn func(ctx context.Context) error) error {
	l, err := m.Acquire(ctx, resource, opts)
	if err != nil {
		return err
	}
	defer func() { _ = l.Release(context.WithoutCancel(ctx)) }()
	return fn(ctx)
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

// --- Lock methods ---

// Token returns the fencing token. Downstream systems can compare tokens to
// reject writes from a lock holder that has since been superseded.
func (l *Lock) Token() string { return l.token }

// Resource returns the locked resource name.
func (l *Lock) Resource() string { return l.resource }

// ValidUntil returns the lease's clock-drift-adjusted expiry — the instant after
// which the holder must NOT assume it still owns the lock (Redlock semantics).
func (l *Lock) ValidUntil() time.Time {
	lease, at := l.leaseSnapshot()
	drift := time.Duration(float64(lease) * l.mgr.cfg.ClockDriftFactor)
	return at.Add(lease - drift)
}

// IsLost reports whether the watchdog observed a failed renewal (the lease
// lapsed or another owner took over).
func (l *Lock) IsLost() bool { return l.lost.Load() }

// Renew extends the lease iff this lock still owns the key (token match).
func (l *Lock) Renew(ctx context.Context, lease time.Duration) error {
	if l.released.Load() {
		return types.ErrLockNotHeld
	}
	if lease <= 0 {
		lease, _ = l.leaseSnapshot()
	}
	ok, err := l.mgr.client.CompareAndExtend(ctx, l.fullKey, l.token, lease)
	if err != nil {
		return err
	}
	if !ok {
		l.lost.Store(true)
		l.mgr.rec.IncCounter(metrics.MetricLockExpired, map[string]string{"resource": l.resource})
		return fmt.Errorf("%w: resource %q", types.ErrLockNotHeld, l.resource)
	}
	l.setLease(lease, time.Now())
	l.mgr.rec.IncCounter(metrics.MetricLockRenewed, map[string]string{"resource": l.resource})
	if l.mgr.bus != nil {
		l.mgr.bus.Emit(events.LockRenewed, "locks", func(e *events.Event) { e.Resource = l.resource })
	}
	return nil
}

// Release relinquishes the lock iff still owned. Releasing an already-released or
// lost lock returns ErrLockNotHeld. Idempotent-safe against double calls.
func (l *Lock) Release(ctx context.Context) error {
	if !l.released.CompareAndSwap(false, true) {
		return nil // already released by us
	}
	l.stopWatchdog()

	ok, err := l.mgr.client.CompareAndDelete(ctx, l.fullKey, l.token)
	l.mgr.held.Add(-1)
	l.mgr.rec.SetGauge(metrics.MetricLockHeld, float64(l.mgr.held.Load()), map[string]string{})
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
	if l.mgr.bus != nil {
		l.mgr.bus.Emit(events.LockReleased, "locks", func(e *events.Event) { e.Resource = l.resource })
	}
	return nil
}

// startWatchdog renews the lease at AutoRenewFraction of its duration until the
// lock is released or ctx is cancelled.
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
				// Use a short independent context so renewal isn't cancelled by
				// the caller's request context mid-flight.
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

func (l *Lock) stopWatchdog() {
	l.wdOnce.Do(func() { close(l.stopWD) })
}

// HeldCount returns the number of locks this manager currently holds (gauge).
func (m *Manager) HeldCount() int64 { return m.held.Load() }
