// Package policies implements the cache policy engine. It decouples the caching
// STRATEGY (cache-aside, read-through, write-through, write-behind, refresh-ahead)
// from the mechanics of talking to Redis. The Cache Manager owns the raw Redis
// operations (exposed here as the Store interface) and delegates strategy
// decisions to an Engine.
//
// Strategy behavior:
//
//	CacheAside    Get returns the cached value or a miss; the caller loads and
//	              populates. Set writes the cache only. (Default, simplest.)
//	ReadThrough   Get transparently loads from the system of record on a miss,
//	              populates the cache, and returns the value.
//	WriteThrough  Set writes the cache and synchronously writes the system of
//	              record; both succeed or the call fails.
//	WriteBehind   Set writes the cache immediately and enqueues an asynchronous,
//	              coalesced write to the system of record (bounded buffer + workers).
//	RefreshAhead  Get serves the cached value but, once a configurable fraction
//	              of the TTL has elapsed, triggers a background reload so hot keys
//	              never expire under load.
package policies

import (
	"context"
	"fmt"
	"time"

	"cpip/internal/cache/config"
	"cpip/internal/cache/events"
	"cpip/internal/cache/metrics"
	"cpip/internal/cache/types"
)

// Strategy names a caching strategy.
type Strategy string

const (
	CacheAside   Strategy = "cache_aside"
	ReadThrough  Strategy = "read_through"
	WriteThrough Strategy = "write_through"
	WriteBehind  Strategy = "write_behind"
	RefreshAhead Strategy = "refresh_ahead"
)

// Valid reports whether s is a recognized strategy.
func (s Strategy) Valid() bool {
	switch s {
	case CacheAside, ReadThrough, WriteThrough, WriteBehind, RefreshAhead:
		return true
	}
	return false
}

// Loader loads a value from the system of record. found=false means the record
// does not exist (a genuine absence, cached as a negative lookup by the caller
// if desired). ttl is the caller-suggested cache lifetime (0 → use default).
type Loader func(ctx context.Context, key string) (value string, ttl time.Duration, found bool, err error)

// Writer persists a value to the system of record (PostgreSQL, object storage, …).
type Writer func(ctx context.Context, key, value string) error

// Store is the cache-side backend the engine manipulates. It is implemented by
// the Cache Manager over the Redis adapter; the engine never imports the manager.
type Store interface {
	// RawGet returns the cached value, its remaining TTL, and whether it existed.
	RawGet(ctx context.Context, fullKey string) (value string, remaining time.Duration, found bool, err error)
	// RawSet writes value with ttl (0 → no expiry).
	RawSet(ctx context.Context, fullKey, value string, ttl time.Duration) error
	// RawDelete removes the key.
	RawDelete(ctx context.Context, fullKey string) error
}

// Registration binds a cache name to a strategy and its collaborators.
type Registration struct {
	Strategy          Strategy
	Loader            Loader
	Writer            Writer
	FullTTL           time.Duration // canonical (pre-jitter) TTL, for refresh-ahead math
	RefreshAheadRatio float64       // 0 → engine default
}

func (r Registration) validate() error {
	if !r.Strategy.Valid() {
		return fmt.Errorf("%w: %q", types.ErrUnknownPolicy, r.Strategy)
	}
	switch r.Strategy {
	case ReadThrough, RefreshAhead:
		if r.Loader == nil {
			return fmt.Errorf("%w: strategy %q", types.ErrNoLoader, r.Strategy)
		}
	case WriteThrough, WriteBehind:
		if r.Writer == nil {
			return fmt.Errorf("%w: strategy %q", types.ErrNoWriter, r.Strategy)
		}
	}
	return nil
}

// Engine applies caching strategies against a Store.
type Engine struct {
	store Store
	cfg   config.Policy
	bus   *events.Bus
	rec   metrics.Recorder

	regs *registry
	wb   *writeBehind

	// singleflight-lite: dedupe concurrent refresh-ahead reloads per key.
	refreshing *keySet
}

// NewEngine constructs a policy engine. It starts the write-behind workers if
// any strategy needs them (lazily, on first WriteBehind registration).
func NewEngine(store Store, cfg config.Policy, bus *events.Bus, rec metrics.Recorder) *Engine {
	if rec == nil {
		rec = metrics.NewNoop()
	}
	e := &Engine{
		store:      store,
		cfg:        cfg,
		bus:        bus,
		rec:        rec,
		regs:       newRegistry(),
		refreshing: newKeySet(),
	}
	e.wb = newWriteBehind(cfg, rec, bus)
	return e
}

// Register validates and records a cache's strategy. Safe for concurrent use.
func (e *Engine) Register(cache string, r Registration) error {
	if err := r.validate(); err != nil {
		return err
	}
	if r.RefreshAheadRatio <= 0 {
		r.RefreshAheadRatio = e.cfg.RefreshAheadRatio
	}
	e.regs.set(cache, r)
	if r.Strategy == WriteBehind {
		e.wb.ensureStarted()
	}
	return nil
}

// StrategyFor returns the registered strategy for a cache, defaulting to the
// engine's configured default.
func (e *Engine) StrategyFor(cache string) Strategy {
	if r, ok := e.regs.get(cache); ok {
		return r.Strategy
	}
	if s := Strategy(e.cfg.Default); s.Valid() {
		return s
	}
	return CacheAside
}

// Get applies the read-side strategy. fullKey is the namespaced Redis key;
// cache is the logical cache name used to resolve the strategy.
func (e *Engine) Get(ctx context.Context, cache, fullKey string, ttl time.Duration) (string, bool, error) {
	reg, hasReg := e.regs.get(cache)
	value, remaining, found, err := e.store.RawGet(ctx, fullKey)
	if err != nil {
		return "", false, err
	}

	strat := e.StrategyFor(cache)
	switch strat {
	case ReadThrough:
		if !found {
			return e.readThroughLoad(ctx, cache, fullKey, reg, ttl)
		}
	case RefreshAhead:
		if found && hasReg {
			e.maybeRefresh(ctx, cache, fullKey, reg, remaining, ttl)
		} else if !found {
			// Cold key under refresh-ahead behaves like read-through on first miss.
			return e.readThroughLoad(ctx, cache, fullKey, reg, ttl)
		}
	}
	return value, found, nil
}

func (e *Engine) readThroughLoad(ctx context.Context, cache, fullKey string, reg Registration, ttl time.Duration) (string, bool, error) {
	if reg.Loader == nil {
		return "", false, fmt.Errorf("%w: cache %q", types.ErrNoLoader, cache)
	}
	start := time.Now()
	val, loadTTL, found, err := reg.Loader(ctx, fullKey)
	metrics.ObserveDuration(e.rec, metrics.MetricCacheLoadDuration, start, map[string]string{"cache": cache})
	if err != nil {
		e.rec.IncCounter(metrics.MetricCacheError, map[string]string{"cache": cache, "op": "load"})
		return "", false, err
	}
	if !found {
		return "", false, nil
	}
	useTTL := ttl
	if loadTTL > 0 {
		useTTL = loadTTL
	}
	if err := e.store.RawSet(ctx, fullKey, val, useTTL); err != nil {
		return "", false, err
	}
	return val, true, nil
}

// maybeRefresh triggers a background reload when a value has aged past the
// refresh-ahead threshold. Concurrent triggers for the same key are deduped.
func (e *Engine) maybeRefresh(ctx context.Context, cache, fullKey string, reg Registration, remaining, ttl time.Duration) {
	full := reg.FullTTL
	if full <= 0 {
		full = ttl
	}
	if full <= 0 || remaining <= 0 {
		return
	}
	threshold := time.Duration(float64(full) * (1 - reg.RefreshAheadRatio))
	if remaining > threshold {
		return
	}
	if !e.refreshing.add(fullKey) {
		return // a refresh is already in flight for this key
	}
	// Detach from the request context so the reload outlives the triggering Get.
	go func() {
		defer e.refreshing.remove(fullKey)
		rctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		start := time.Now()
		val, loadTTL, found, err := reg.Loader(rctx, fullKey)
		metrics.ObserveDuration(e.rec, metrics.MetricCacheLoadDuration, start, map[string]string{"cache": cache, "mode": "refresh_ahead"})
		if err != nil || !found {
			return
		}
		useTTL := ttl
		if loadTTL > 0 {
			useTTL = loadTTL
		}
		_ = e.store.RawSet(rctx, fullKey, val, useTTL)
	}()
}

// Set applies the write-side strategy.
func (e *Engine) Set(ctx context.Context, cache, fullKey, logicalKey, value string, ttl time.Duration) error {
	reg, hasReg := e.regs.get(cache)
	if err := e.store.RawSet(ctx, fullKey, value, ttl); err != nil {
		return err
	}
	if !hasReg {
		return nil
	}
	switch reg.Strategy {
	case WriteThrough:
		if reg.Writer == nil {
			return fmt.Errorf("%w: cache %q", types.ErrNoWriter, cache)
		}
		if err := reg.Writer(ctx, logicalKey, value); err != nil {
			e.rec.IncCounter(metrics.MetricCacheError, map[string]string{"cache": cache, "op": "write_through"})
			return fmt.Errorf("write-through failed: %w", err)
		}
	case WriteBehind:
		e.wb.enqueue(writeOp{cache: cache, key: logicalKey, value: value, writer: reg.Writer})
	}
	return nil
}

// Delete removes the cache entry and, for write strategies, records intent to
// delete downstream (the manager coordinates system-of-record deletes explicitly).
func (e *Engine) Delete(ctx context.Context, fullKey string) error {
	return e.store.RawDelete(ctx, fullKey)
}

// FlushWriteBehind synchronously drains the write-behind buffer (used on
// graceful shutdown so no async write is lost).
func (e *Engine) FlushWriteBehind(ctx context.Context) error { return e.wb.flushAll(ctx) }

// Close stops background workers, flushing pending write-behind operations.
func (e *Engine) Close() error { return e.wb.stop() }
