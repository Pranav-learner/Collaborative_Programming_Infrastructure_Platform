package manager

import (
	"context"
	"time"

	"cpip/internal/cache/policies"
	"cpip/internal/cache/ttl"
	"cpip/internal/cache/types"
)

// Cache is the minimal, backend-agnostic interface business services depend on.
// It is the public seam of the module's caching side: collaboration, execution,
// and sandbox services take a Cache, never a *redis.Client. A future gRPC or
// REST facade wraps this same interface.
type Cache interface {
	// Get decodes the cached value for (cache,key) into dst. Returns found=false
	// on a miss (dst is left untouched). Read-through caches populate on miss.
	Get(ctx context.Context, cache, key string, dst any) (found bool, err error)
	// GetItem returns the raw cached value plus metadata without decoding.
	GetItem(ctx context.Context, cache, key string) (types.Item, error)
	// Set encodes and stores value for (cache,key), applying the cache's write
	// strategy and TTL.
	Set(ctx context.Context, cache, key string, value any, opts ...SetOption) error
	// Delete removes a single entry.
	Delete(ctx context.Context, cache, key string) error
	// Exists reports whether a key is present without transferring its value.
	Exists(ctx context.Context, cache, key string) (bool, error)
	// GetMany bulk-reads keys, returning an Item per key (Found distinguishes
	// hits from misses).
	GetMany(ctx context.Context, cache string, keys []string) (map[string]types.Item, error)
	// SetMany bulk-writes values under one TTL.
	SetMany(ctx context.Context, cache string, values map[string]any, opts ...SetOption) error
	// TTLOf returns the remaining lifetime of a key.
	TTLOf(ctx context.Context, cache, key string) (time.Duration, error)
	// Stats returns the live statistics for a cache.
	Stats(cache string) (types.Stats, bool)
}

// SetOption customizes a single Set/SetMany call.
type SetOption func(*setOptions)

type setOptions struct {
	ttl  time.Duration
	tags []string
}

// WithTTL overrides the cache's default TTL for this write.
func WithTTL(d time.Duration) SetOption { return func(o *setOptions) { o.ttl = d } }

// WithTags attaches tags to the written key(s) so they can later be invalidated
// as a group via InvalidateTag.
func WithTags(tags ...string) SetOption {
	return func(o *setOptions) { o.tags = append(o.tags, tags...) }
}

// CacheSpec declares a cache and its behavior. Register it once at startup.
type CacheSpec struct {
	// Name is the logical cache identifier (namespaces keys).
	Name string
	// Strategy selects the caching policy (default cache-aside).
	Strategy policies.Strategy
	// TTL is the canonical lifetime for entries (0 → module default).
	TTL time.Duration
	// Mode selects absolute or sliding expiration.
	Mode ttl.Mode
	// Loader/Writer back read-through/write-through/write-behind/refresh-ahead.
	Loader policies.Loader
	Writer policies.Writer
	// RefreshAheadRatio (0..1) overrides the module default for refresh-ahead.
	RefreshAheadRatio float64
	// DefaultTags are applied to every entry written to this cache.
	DefaultTags []string
	// TrackExpiry enables local TTL callbacks/events for this cache's keys.
	TrackExpiry bool
}

var _ Cache = (*Manager)(nil)
