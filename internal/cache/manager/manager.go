// Package manager implements the Cache Manager: the composition root of the
// caching side of the module. It wires the Redis adapter, TTL manager, policy
// engine, registry, and invalidation manager behind the small Cache interface
// that business services consume. It is also the policies.Store implementation,
// so the policy engine drives caching strategy without ever importing Redis.
package manager

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cpip/internal/cache/config"
	"cpip/internal/cache/events"
	"cpip/internal/cache/invalidation"
	"cpip/internal/cache/keys"
	"cpip/internal/cache/logger"
	"cpip/internal/cache/metrics"
	"cpip/internal/cache/policies"
	"cpip/internal/cache/redis"
	"cpip/internal/cache/registry"
	"cpip/internal/cache/ttl"
	"cpip/internal/cache/types"
)

// Manager is the Cache Manager.
type Manager struct {
	cfg    config.Config
	client redis.Client
	kb     keys.Builder
	codec  types.Codec

	ttl    *ttl.Manager
	policy *policies.Engine
	reg    *registry.Registry
	inval  *invalidation.Manager

	bus *events.Bus
	rec metrics.Recorder
	log *logger.Logger

	mu     sync.RWMutex
	closed bool
}

// Params configures a Manager. Only Client is strictly required; the rest fall
// back to sensible defaults (no-op recorder, default slog, JSON codec, fresh bus).
type Params struct {
	Config  config.Config
	Client  redis.Client
	Codec   types.Codec
	Bus     *events.Bus
	Metrics metrics.Recorder
	Logger  *logger.Logger
	// Registry/TTL/Invalidation may be shared with other subsystems; if nil,
	// the manager creates its own.
	Registry     *registry.Registry
	TTL          *ttl.Manager
	Invalidation *invalidation.Manager
}

// New constructs and wires a Cache Manager.
func New(p Params) (*Manager, error) {
	if p.Client == nil {
		return nil, fmt.Errorf("%w: nil redis client", types.ErrConfig)
	}
	cfg, err := p.Config.Validate()
	if err != nil {
		return nil, err
	}

	rec := p.Metrics
	if rec == nil {
		rec = metrics.NewNoop()
	}
	bus := p.Bus
	if bus == nil {
		bus = events.NewBus()
	}
	codec := p.Codec
	if codec == nil {
		codec = types.JSONCodec{}
	}
	log := p.Logger
	if log == nil {
		log = logger.New(nil)
	}
	kb := keys.New(cfg.Redis.KeyPrefix)

	reg := p.Registry
	if reg == nil {
		reg = registry.New()
	}
	tm := p.TTL
	if tm == nil {
		tm = ttl.New(cfg.TTL, bus, rec)
	}
	inval := p.Invalidation
	if inval == nil {
		inval = invalidation.New(invalidation.Params{
			Client:  p.Client,
			Keys:    kb,
			NodeID:  cfg.Replication.NodeID,
			Bus:     bus,
			Metrics: rec,
		})
	}

	m := &Manager{
		cfg:    cfg,
		client: p.Client,
		kb:     kb,
		codec:  codec,
		ttl:    tm,
		reg:    reg,
		inval:  inval,
		bus:    bus,
		rec:    rec,
		log:    log,
	}
	// The policy engine talks to Redis exclusively through the manager (Store).
	m.policy = policies.NewEngine(m, cfg.Policy, bus, rec)
	return m, nil
}

// RegisterCache declares a cache. It must be called before Get/Set on that
// cache. Registration is idempotent-friendly: re-registering resets stats.
func (m *Manager) RegisterCache(spec CacheSpec) error {
	if spec.Name == "" {
		return fmt.Errorf("%w: empty cache name", types.ErrConfig)
	}
	strat := spec.Strategy
	if strat == "" {
		strat = policies.Strategy(m.cfg.Policy.Default)
	}
	ttlVal := spec.TTL
	if ttlVal <= 0 {
		ttlVal = m.cfg.TTL.Default
	}

	m.ttl.Register(spec.Name, ttl.Policy{TTL: ttlVal, Mode: spec.Mode})
	if err := m.policy.Register(spec.Name, policies.Registration{
		Strategy:          strat,
		Loader:            spec.Loader,
		Writer:            spec.Writer,
		FullTTL:           ttlVal,
		RefreshAheadRatio: spec.RefreshAheadRatio,
	}); err != nil {
		return err
	}
	m.reg.Register(registry.Descriptor{
		Name:     spec.Name,
		Strategy: strat,
		TTL:      ttlVal,
		Mode:     spec.Mode,
		Tags:     spec.DefaultTags,
	})
	return nil
}

// specTags returns the default tags for a cache (from its descriptor).
func (m *Manager) specTags(cache string) []string {
	if d, ok := m.reg.Descriptor(cache); ok {
		return d.Tags
	}
	return nil
}

func (m *Manager) ensureRegistered(cache string) error {
	if !m.reg.IsRegistered(cache) {
		return fmt.Errorf("%w: %q", types.ErrCacheNotRegistered, cache)
	}
	return nil
}

// --- Cache interface ---

// Get implements Cache.
func (m *Manager) Get(ctx context.Context, cache, key string, dst any) (bool, error) {
	if err := m.ensureRegistered(cache); err != nil {
		return false, err
	}
	start := time.Now()
	fullKey := m.kb.Cache(cache, key)
	resolved := m.ttl.Resolve(cache, 0)

	value, found, err := m.policy.Get(ctx, cache, fullKey, resolved)
	dur := time.Since(start)
	if err != nil {
		m.reg.RecordError(cache)
		m.rec.IncCounter(metrics.MetricCacheError, map[string]string{"cache": cache, "op": "get"})
		m.log.CacheOp(ctx, cache, "get", key, false, dur, err)
		return false, err
	}
	if !found {
		m.reg.RecordMiss(cache)
		m.rec.IncCounter(metrics.MetricCacheMiss, map[string]string{"cache": cache})
		m.bus.Emit(events.CacheMiss, "manager", func(e *events.Event) { e.Cache = cache; e.Key = key })
		m.log.CacheOp(ctx, cache, "get", key, false, dur, nil)
		return false, nil
	}

	m.reg.RecordHit(cache)
	m.rec.IncCounter(metrics.MetricCacheHit, map[string]string{"cache": cache})
	metrics.ObserveDuration(m.rec, metrics.MetricCacheGetDuration, start, map[string]string{"cache": cache})
	m.bus.Emit(events.CacheHit, "manager", func(e *events.Event) { e.Cache = cache; e.Key = key })
	m.log.CacheOp(ctx, cache, "get", key, true, dur, nil)

	// Sliding expiration: refresh the lease in Redis on access.
	if m.ttl.IsSliding(cache) {
		if _, err := m.client.Expire(ctx, fullKey, resolved); err == nil {
			m.ttl.Touch(fullKey, resolved)
		}
	}

	if dst != nil {
		if err := m.codec.Decode(value, dst); err != nil {
			m.reg.RecordError(cache)
			return true, err
		}
	}
	return true, nil
}

// GetItem implements Cache.
func (m *Manager) GetItem(ctx context.Context, cache, key string) (types.Item, error) {
	if err := m.ensureRegistered(cache); err != nil {
		return types.Item{}, err
	}
	fullKey := m.kb.Cache(cache, key)
	value, remaining, found, err := m.RawGet(ctx, fullKey)
	if err != nil {
		return types.Item{}, err
	}
	if !found {
		m.reg.RecordMiss(cache)
		return types.Item{Key: key, Found: false}, nil
	}
	m.reg.RecordHit(cache)
	return types.Item{Key: key, Value: value, TTL: remaining, Found: true}, nil
}

// Set implements Cache.
func (m *Manager) Set(ctx context.Context, cache, key string, value any, opts ...SetOption) error {
	if err := m.ensureRegistered(cache); err != nil {
		return err
	}
	o := setOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	start := time.Now()
	encoded, err := m.codec.Encode(value)
	if err != nil {
		m.reg.RecordError(cache)
		return err
	}
	fullKey := m.kb.Cache(cache, key)
	resolved := m.ttl.Resolve(cache, o.ttl)

	if err := m.policy.Set(ctx, cache, fullKey, key, encoded, resolved); err != nil {
		m.reg.RecordError(cache)
		m.rec.IncCounter(metrics.MetricCacheError, map[string]string{"cache": cache, "op": "set"})
		m.log.CacheOp(ctx, cache, "set", key, false, time.Since(start), err)
		return err
	}

	tags := append(append([]string(nil), m.specTags(cache)...), o.tags...)
	if len(tags) > 0 {
		if err := m.inval.IndexTags(ctx, fullKey, tags); err != nil {
			m.log.CacheOp(ctx, cache, "index_tags", key, false, time.Since(start), err)
		}
	}
	if d, ok := m.reg.Descriptor(cache); ok && d.Mode == ttl.Sliding {
		m.ttl.Watch(fullKey, resolved, nil)
	}

	m.reg.RecordSet(cache)
	m.rec.IncCounter(metrics.MetricCacheSet, map[string]string{"cache": cache})
	metrics.ObserveDuration(m.rec, metrics.MetricCacheSetDuration, start, map[string]string{"cache": cache})
	m.bus.Emit(events.CacheSet, "manager", func(e *events.Event) { e.Cache = cache; e.Key = key })
	m.log.CacheOp(ctx, cache, "set", key, false, time.Since(start), nil)
	return nil
}

// Delete implements Cache.
func (m *Manager) Delete(ctx context.Context, cache, key string) error {
	if err := m.ensureRegistered(cache); err != nil {
		return err
	}
	fullKey := m.kb.Cache(cache, key)
	if err := m.inval.InvalidateKey(ctx, fullKey); err != nil {
		m.reg.RecordError(cache)
		return err
	}
	m.ttl.Unwatch(fullKey)
	m.reg.RecordDelete(cache)
	m.rec.IncCounter(metrics.MetricCacheDelete, map[string]string{"cache": cache})
	m.bus.Emit(events.CacheDeleted, "manager", func(e *events.Event) { e.Cache = cache; e.Key = key })
	return nil
}

// Exists implements Cache.
func (m *Manager) Exists(ctx context.Context, cache, key string) (bool, error) {
	if err := m.ensureRegistered(cache); err != nil {
		return false, err
	}
	n, err := m.client.Exists(ctx, m.kb.Cache(cache, key))
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// GetMany implements Cache.
func (m *Manager) GetMany(ctx context.Context, cache string, keys []string) (map[string]types.Item, error) {
	if err := m.ensureRegistered(cache); err != nil {
		return nil, err
	}
	out := make(map[string]types.Item, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	full := make([]string, len(keys))
	for i, k := range keys {
		full[i] = m.kb.Cache(cache, k)
	}
	vals, err := m.client.MGet(ctx, full...)
	if err != nil {
		m.reg.RecordError(cache)
		return nil, err
	}
	for i, k := range keys {
		if vals[i] == nil {
			out[k] = types.Item{Key: k, Found: false}
			m.reg.RecordMiss(cache)
			continue
		}
		out[k] = types.Item{Key: k, Value: *vals[i], Found: true}
		m.reg.RecordHit(cache)
	}
	return out, nil
}

// SetMany implements Cache.
func (m *Manager) SetMany(ctx context.Context, cache string, values map[string]any, opts ...SetOption) error {
	if err := m.ensureRegistered(cache); err != nil {
		return err
	}
	o := setOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	resolved := m.ttl.Resolve(cache, o.ttl)
	pairs := make(map[string]string, len(values))
	for k, v := range values {
		enc, err := m.codec.Encode(v)
		if err != nil {
			m.reg.RecordError(cache)
			return err
		}
		pairs[m.kb.Cache(cache, k)] = enc
	}
	if err := m.client.SetMany(ctx, pairs, resolved); err != nil {
		m.reg.RecordError(cache)
		return err
	}
	for range values {
		m.reg.RecordSet(cache)
	}
	m.rec.AddCounter(metrics.MetricCacheSet, float64(len(values)), map[string]string{"cache": cache})
	return nil
}

// TTLOf implements Cache.
func (m *Manager) TTLOf(ctx context.Context, cache, key string) (time.Duration, error) {
	if err := m.ensureRegistered(cache); err != nil {
		return 0, err
	}
	return m.client.TTL(ctx, m.kb.Cache(cache, key))
}

// Stats implements Cache.
func (m *Manager) Stats(cache string) (types.Stats, bool) { return m.reg.Stats(cache) }

// --- Invalidation passthroughs ---

// InvalidateTag removes every entry tagged with tag across all caches.
func (m *Manager) InvalidateTag(ctx context.Context, tag string) (int, error) {
	return m.inval.InvalidateTag(ctx, tag)
}

// InvalidateCache removes every entry of a named cache.
func (m *Manager) InvalidateCache(ctx context.Context, cache string) (int, error) {
	n, err := m.inval.InvalidateCacheName(ctx, cache)
	if err == nil {
		m.reg.RecordInvalidation(cache, int64(n))
	}
	return n, err
}

// InvalidatePattern removes every full key matching a glob pattern.
func (m *Manager) InvalidatePattern(ctx context.Context, pattern string) (int, error) {
	return m.inval.InvalidatePattern(ctx, pattern)
}

// --- Accessors for wiring ---

// Registry exposes the cache registry (read-only introspection).
func (m *Manager) Registry() *registry.Registry { return m.reg }

// Invalidation exposes the invalidation manager.
func (m *Manager) Invalidation() *invalidation.Manager { return m.inval }

// Events exposes the event bus.
func (m *Manager) Events() *events.Bus { return m.bus }

// TTL exposes the TTL manager.
func (m *Manager) TTL() *ttl.Manager { return m.ttl }

// Policies exposes the policy engine (advanced wiring / graceful flush).
func (m *Manager) Policies() *policies.Engine { return m.policy }

// Start launches background workers (TTL reaper, invalidation subscriber).
func (m *Manager) Start(ctx context.Context) error {
	m.ttl.Start(ctx)
	return m.inval.Start(ctx)
}

// Close flushes write-behind buffers and stops background workers. It does not
// close the shared Redis client (the owner does).
func (m *Manager) Close(ctx context.Context) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	m.mu.Unlock()

	_ = m.policy.FlushWriteBehind(ctx)
	err := m.policy.Close()
	m.ttl.Stop()
	m.inval.Stop()
	return err
}
