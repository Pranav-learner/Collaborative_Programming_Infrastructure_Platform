// Package ttl centralizes expiration policy for the module: default and
// per-cache TTLs, absolute vs sliding expiration, jitter to avoid synchronized
// mass-expiry (thundering herd), expiration callbacks, and a cleanup scheduler.
//
// Redis owns the authoritative expiry (via PEXPIRE); this package adds the
// application-level scheduling Redis cannot do without keyspace notifications:
// firing a Go callback and a TTLExpired event when a tracked key's lease
// elapses. It is a best-effort local scheduler, safe under high concurrency.
package ttl

import (
	"context"
	"hash/fnv"
	"sync"
	"time"

	"cpip/internal/cache/config"
	"cpip/internal/cache/events"
	"cpip/internal/cache/metrics"
)

// Mode selects how a cache's TTL is refreshed.
type Mode uint8

const (
	// Absolute expiry: the key dies a fixed duration after it was written.
	Absolute Mode = iota
	// Sliding expiry: each access extends the lease by the TTL. Used for
	// sessions where activity should keep the entry alive.
	Sliding
)

// Policy is a per-cache expiration policy.
type Policy struct {
	TTL    time.Duration
	Mode   Mode
	Jitter float64 // 0..1; overrides the global default when > 0
}

// Callback fires when a tracked key's lease elapses locally.
type Callback func(key string)

type tracked struct {
	expireAt time.Duration // monotonic-ish deadline stored as unix-nanos
	cb       Callback
}

// Manager resolves TTLs and schedules expiration callbacks.
type Manager struct {
	cfg config.TTL
	bus *events.Bus
	rec metrics.Recorder

	mu       sync.RWMutex
	perCache map[string]Policy
	watched  map[string]watchEntry

	now    func() time.Time
	stopCh chan struct{}
	doneCh chan struct{}
	once   sync.Once
}

type watchEntry struct {
	deadline time.Time
	cb       Callback
}

// New constructs a TTL Manager. bus and rec may be nil (a no-op recorder is
// substituted); the reaper is not started until Start is called.
func New(cfg config.TTL, bus *events.Bus, rec metrics.Recorder) *Manager {
	if rec == nil {
		rec = metrics.NewNoop()
	}
	return &Manager{
		cfg:      cfg,
		bus:      bus,
		rec:      rec,
		perCache: make(map[string]Policy),
		watched:  make(map[string]watchEntry),
		now:      time.Now,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// SetClock overrides the clock (tests). Not concurrency-safe.
func (m *Manager) SetClock(now func() time.Time) { m.now = now }

// Register sets the expiration policy for a named cache.
func (m *Manager) Register(cache string, p Policy) {
	if p.TTL <= 0 {
		p.TTL = m.cfg.Default
	}
	m.mu.Lock()
	m.perCache[cache] = p
	m.mu.Unlock()
}

// PolicyFor returns the policy registered for a cache, falling back to the
// module default (absolute, default TTL).
func (m *Manager) PolicyFor(cache string) Policy {
	m.mu.RLock()
	p, ok := m.perCache[cache]
	m.mu.RUnlock()
	if !ok {
		return Policy{TTL: m.cfg.Default, Mode: Absolute, Jitter: m.cfg.Jitter}
	}
	return p
}

// Resolve returns the effective TTL for a cache with jitter applied. A caller
// may pass an explicit override (> 0) which takes precedence over the policy.
func (m *Manager) Resolve(cache string, override time.Duration) time.Duration {
	p := m.PolicyFor(cache)
	base := p.TTL
	if override > 0 {
		base = override
	}
	jitter := p.Jitter
	if jitter <= 0 {
		jitter = m.cfg.Jitter
	}
	return m.Jitter(base, jitter, cache)
}

// IsSliding reports whether a cache uses sliding expiration.
func (m *Manager) IsSliding(cache string) bool { return m.PolicyFor(cache).Mode == Sliding }

// Jitter deterministically spreads a duration by up to ±fraction based on the
// seed, so many keys sharing a TTL do not all expire on the same millisecond.
// Determinism (vs. crypto randomness) keeps behavior reproducible in tests and
// avoids importing a disallowed time-seeded RNG.
func (m *Manager) Jitter(d time.Duration, fraction float64, seed string) time.Duration {
	if d <= 0 || fraction <= 0 {
		return d
	}
	if fraction > 1 {
		fraction = 1
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(seed))
	// Map hash to [-1, 1).
	norm := (float64(h.Sum32()%20000)/10000.0 - 1.0)
	delta := time.Duration(float64(d) * fraction * norm)
	res := d + delta
	if res <= 0 {
		res = d
	}
	return res
}

// Watch schedules cb to fire (and a TTLExpired event to publish) when ttl
// elapses for key. Re-watching an existing key resets its deadline (this is how
// sliding expiration is implemented at the scheduler level).
func (m *Manager) Watch(key string, ttl time.Duration, cb Callback) {
	if ttl <= 0 {
		return
	}
	m.mu.Lock()
	m.watched[key] = watchEntry{deadline: m.now().Add(ttl), cb: cb}
	m.mu.Unlock()
}

// Touch extends a watched key's deadline by ttl (sliding refresh). No-op if the
// key is not being watched.
func (m *Manager) Touch(key string, ttl time.Duration) {
	m.mu.Lock()
	if w, ok := m.watched[key]; ok {
		w.deadline = m.now().Add(ttl)
		m.watched[key] = w
	}
	m.mu.Unlock()
}

// Unwatch cancels scheduled expiration for key.
func (m *Manager) Unwatch(key string) {
	m.mu.Lock()
	delete(m.watched, key)
	m.mu.Unlock()
}

// WatchedCount returns the number of keys currently scheduled (test/introspection).
func (m *Manager) WatchedCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.watched)
}

// Start launches the cleanup reaper. It scans for elapsed deadlines every
// ReaperInterval, firing callbacks and emitting TTLExpired events. Safe to call
// once; subsequent calls are ignored.
func (m *Manager) Start(ctx context.Context) {
	m.once.Do(func() {
		go m.reap(ctx)
	})
}

func (m *Manager) reap(ctx context.Context) {
	defer close(m.doneCh)
	ticker := time.NewTicker(m.cfg.ReaperInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.sweep()
		}
	}
}

// Sweep runs a single cleanup pass immediately, firing callbacks and events for
// any keys whose deadline has elapsed. The background reaper calls this on each
// tick; callers may also invoke it to force an out-of-band cleanup.
func (m *Manager) Sweep() { m.sweep() }

// sweep fires callbacks for all keys whose deadline has passed. Callbacks run
// outside the lock so they may call back into the manager without deadlocking.
func (m *Manager) sweep() {
	now := m.now()
	var fired []watchEntryWithKey

	m.mu.Lock()
	for k, w := range m.watched {
		if !w.deadline.After(now) {
			fired = append(fired, watchEntryWithKey{key: k, entry: w})
			delete(m.watched, k)
		}
	}
	m.mu.Unlock()

	for _, f := range fired {
		m.rec.IncCounter(metrics.MetricTTLExpired, map[string]string{})
		if f.entry.cb != nil {
			m.rec.IncCounter(metrics.MetricTTLCallback, map[string]string{})
			f.entry.cb(f.key)
		}
		if m.bus != nil {
			m.bus.Emit(events.TTLExpired, "ttl", func(e *events.Event) { e.Key = f.key })
		}
	}
}

type watchEntryWithKey struct {
	key   string
	entry watchEntry
}

// Stop halts the reaper and waits for it to exit. Idempotent.
func (m *Manager) Stop() {
	select {
	case <-m.stopCh:
	default:
		close(m.stopCh)
	}
	select {
	case <-m.doneCh:
	case <-time.After(2 * time.Second):
	}
}
