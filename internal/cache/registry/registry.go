// Package registry maintains the catalog of caches known to the module: their
// strategy, TTL policy, live statistics, and health. It is the introspection
// surface behind admin endpoints and dashboards, and the single source of truth
// the Cache Manager consults to resolve per-cache behavior. All operations are
// concurrency-safe; counters use atomics on the hot path.
package registry

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"cpip/internal/cache/policies"
	"cpip/internal/cache/ttl"
	"cpip/internal/cache/types"
)

// Descriptor is the immutable registration metadata for a cache.
type Descriptor struct {
	Name         string            `json:"name"`
	Strategy     policies.Strategy `json:"strategy"`
	TTL          time.Duration     `json:"ttl"`
	Mode         ttl.Mode          `json:"mode"`
	Tags         []string          `json:"tags,omitempty"`
	RegisteredAt time.Time         `json:"registered_at"`
}

// counters holds atomic hot-path statistics for one cache.
type counters struct {
	hits          atomic.Int64
	misses        atomic.Int64
	sets          atomic.Int64
	deletes       atomic.Int64
	evictions     atomic.Int64
	errors        atomic.Int64
	invalidations atomic.Int64
}

// entry pairs a descriptor with its live counters and health.
type entry struct {
	desc     Descriptor
	counters counters
	health   atomic.Value // types.Health
}

// Registry is the concurrency-safe cache catalog.
type Registry struct {
	mu     sync.RWMutex
	caches map[string]*entry
	now    func() time.Time
}

// New constructs an empty registry.
func New() *Registry {
	return &Registry{caches: make(map[string]*entry), now: time.Now}
}

// Register adds or replaces a cache descriptor. Existing statistics are reset on
// re-registration so operators get a clean baseline.
func (r *Registry) Register(d Descriptor) {
	if d.RegisteredAt.IsZero() {
		d.RegisteredAt = r.now()
	}
	e := &entry{desc: d}
	e.health.Store(types.HealthUp)
	r.mu.Lock()
	r.caches[d.Name] = e
	r.mu.Unlock()
}

// IsRegistered reports whether a cache exists.
func (r *Registry) IsRegistered(name string) bool {
	r.mu.RLock()
	_, ok := r.caches[name]
	r.mu.RUnlock()
	return ok
}

// Descriptor returns a cache's registration metadata.
func (r *Registry) Descriptor(name string) (Descriptor, bool) {
	r.mu.RLock()
	e, ok := r.caches[name]
	r.mu.RUnlock()
	if !ok {
		return Descriptor{}, false
	}
	return e.desc, true
}

// Names returns all registered cache names, sorted.
func (r *Registry) Names() []string {
	r.mu.RLock()
	out := make([]string, 0, len(r.caches))
	for name := range r.caches {
		out = append(out, name)
	}
	r.mu.RUnlock()
	sort.Strings(out)
	return out
}

func (r *Registry) entry(name string) *entry {
	r.mu.RLock()
	e := r.caches[name]
	r.mu.RUnlock()
	return e
}

// --- Hot-path counter mutators (nil-safe: unknown caches are ignored) ---

func (r *Registry) RecordHit(name string) {
	if e := r.entry(name); e != nil {
		e.counters.hits.Add(1)
	}
}
func (r *Registry) RecordMiss(name string) {
	if e := r.entry(name); e != nil {
		e.counters.misses.Add(1)
	}
}
func (r *Registry) RecordSet(name string) {
	if e := r.entry(name); e != nil {
		e.counters.sets.Add(1)
	}
}
func (r *Registry) RecordDelete(name string) {
	if e := r.entry(name); e != nil {
		e.counters.deletes.Add(1)
	}
}
func (r *Registry) RecordEviction(name string) {
	if e := r.entry(name); e != nil {
		e.counters.evictions.Add(1)
	}
}
func (r *Registry) RecordError(name string) {
	if e := r.entry(name); e != nil {
		e.counters.errors.Add(1)
	}
}
func (r *Registry) RecordInvalidation(name string, n int64) {
	if e := r.entry(name); e != nil {
		e.counters.invalidations.Add(n)
	}
}

// SetHealth updates a cache's health classification.
func (r *Registry) SetHealth(name string, h types.Health) {
	if e := r.entry(name); e != nil {
		e.health.Store(h)
	}
}

// Health returns a cache's current health (HealthDown if unknown).
func (r *Registry) Health(name string) types.Health {
	e := r.entry(name)
	if e == nil {
		return types.HealthDown
	}
	if h, ok := e.health.Load().(types.Health); ok {
		return h
	}
	return types.HealthUp
}

// Stats returns a point-in-time snapshot for one cache, including the derived
// hit ratio.
func (r *Registry) Stats(name string) (types.Stats, bool) {
	e := r.entry(name)
	if e == nil {
		return types.Stats{}, false
	}
	hits := e.counters.hits.Load()
	misses := e.counters.misses.Load()
	s := types.Stats{
		Hits:          hits,
		Misses:        misses,
		Sets:          e.counters.sets.Load(),
		Deletes:       e.counters.deletes.Load(),
		Evictions:     e.counters.evictions.Load(),
		Errors:        e.counters.errors.Load(),
		Invalidations: e.counters.invalidations.Load(),
	}
	if total := hits + misses; total > 0 {
		s.HitRatio = float64(hits) / float64(total)
	}
	return s, true
}

// AllStats returns a snapshot for every registered cache.
func (r *Registry) AllStats() map[string]types.Stats {
	r.mu.RLock()
	names := make([]string, 0, len(r.caches))
	for n := range r.caches {
		names = append(names, n)
	}
	r.mu.RUnlock()
	out := make(map[string]types.Stats, len(names))
	for _, n := range names {
		if s, ok := r.Stats(n); ok {
			out[n] = s
		}
	}
	return out
}

// Unregister removes a cache from the catalog.
func (r *Registry) Unregister(name string) {
	r.mu.Lock()
	delete(r.caches, name)
	r.mu.Unlock()
}
