// Package registry is the routing and backend directory for the storage module.
// It answers three questions the rest of the module must never hard-code:
//
//   - Which physical bucket backs a logical bucket name? (logical → physical)
//   - Which logical bucket should an artifact of a given type land in? (type → bucket)
//   - Which ObjectStore backend serves a given bucket? (bucket → provider → store)
//
// This indirection is what lets operators remap buckets, split traffic across
// providers, or migrate MinIO → S3 → GCS by editing configuration alone. Business
// logic references logical names and artifact types; the registry resolves the
// concrete backend. It is the "Artifact Registry" of the architecture: the source
// of truth for type policy and bucket topology.
package registry

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"cpip/internal/storage/artifacts"
	"cpip/internal/storage/config"
	"cpip/internal/storage/sdk"
)

// Route is the fully-resolved destination for an operation: the backend store
// plus the physical bucket to address it against.
type Route struct {
	Store         sdk.ObjectStore
	Provider      config.Provider
	LogicalBucket string
	Bucket        string // physical
}

// Registry resolves buckets, types, and providers to concrete backends.
type Registry struct {
	mu sync.RWMutex

	defaultProvider config.Provider
	stores          map[config.Provider]sdk.ObjectStore

	logical       map[string]string          // logical bucket -> physical bucket
	bucketOwner   map[string]config.Provider // logical bucket -> provider serving it
	typeBucket    map[artifacts.Type]string  // artifact type -> logical bucket
	defaultBucket string
}

// Params configures a Registry. Stores maps each provider to its backend; the
// default provider must be present. Buckets/TypeBuckets/DefaultBucket come from
// config and define the routing topology.
type Params struct {
	DefaultProvider config.Provider
	Stores          map[config.Provider]sdk.ObjectStore
	Buckets         map[string]string         // logical -> physical
	TypeBuckets     map[artifacts.Type]string // type -> logical
	DefaultBucket   string
}

// New constructs a Registry. It fails fast on an inconsistent topology (missing
// default store, default bucket not declared, type routed to unknown bucket).
func New(p Params) (*Registry, error) {
	if len(p.Stores) == 0 {
		return nil, fmt.Errorf("%w: registry requires at least one store", artifacts.ErrConfig)
	}
	if _, ok := p.Stores[p.DefaultProvider]; !ok {
		return nil, fmt.Errorf("%w: default provider %q has no store", artifacts.ErrConfig, p.DefaultProvider)
	}
	if p.DefaultBucket == "" {
		return nil, fmt.Errorf("%w: default bucket is empty", artifacts.ErrConfig)
	}
	if _, ok := p.Buckets[p.DefaultBucket]; !ok {
		return nil, fmt.Errorf("%w: default bucket %q not in buckets map", artifacts.ErrConfig, p.DefaultBucket)
	}

	r := &Registry{
		defaultProvider: p.DefaultProvider,
		stores:          make(map[config.Provider]sdk.ObjectStore, len(p.Stores)),
		logical:         make(map[string]string, len(p.Buckets)),
		bucketOwner:     make(map[string]config.Provider, len(p.Buckets)),
		typeBucket:      make(map[artifacts.Type]string, len(p.TypeBuckets)),
		defaultBucket:   p.DefaultBucket,
	}
	for prov, st := range p.Stores {
		r.stores[prov] = st
	}
	for logical, physical := range p.Buckets {
		r.logical[logical] = physical
		// All buckets are served by the default provider in this stage; a future
		// multi-backend topology would carry per-bucket provider assignments.
		r.bucketOwner[logical] = p.DefaultProvider
	}
	for t, logical := range p.TypeBuckets {
		if _, ok := r.logical[logical]; !ok {
			return nil, fmt.Errorf("%w: type %q routed to unknown bucket %q", artifacts.ErrConfig, t, logical)
		}
		r.typeBucket[t] = logical
	}
	return r, nil
}

// BucketForType returns the logical bucket an artifact type routes to, falling
// back to the default bucket when the type has no explicit route.
func (r *Registry) BucketForType(t artifacts.Type) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if b, ok := r.typeBucket[t]; ok {
		return b
	}
	return r.defaultBucket
}

// DefaultBucket returns the fallback logical bucket.
func (r *Registry) DefaultBucket() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.defaultBucket
}

// PhysicalBucket resolves a logical bucket to its physical name.
func (r *Registry) PhysicalBucket(logical string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	phys, ok := r.logical[logical]
	if !ok {
		return "", fmt.Errorf("%w: unknown logical bucket %q", artifacts.ErrConfig, logical)
	}
	return phys, nil
}

// Resolve maps a logical bucket to a full Route (store + physical bucket).
func (r *Registry) Resolve(logical string) (Route, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	phys, ok := r.logical[logical]
	if !ok {
		return Route{}, fmt.Errorf("%w: unknown logical bucket %q", artifacts.ErrConfig, logical)
	}
	prov := r.bucketOwner[logical]
	st, ok := r.stores[prov]
	if !ok {
		return Route{}, fmt.Errorf("%w: no store for provider %q", artifacts.ErrConfig, prov)
	}
	return Route{Store: st, Provider: prov, LogicalBucket: logical, Bucket: phys}, nil
}

// ResolveForType is a convenience: route an artifact type directly.
func (r *Registry) ResolveForType(t artifacts.Type) (Route, error) {
	return r.Resolve(r.BucketForType(t))
}

// DefaultStore returns the backend for the default provider.
func (r *Registry) DefaultStore() sdk.ObjectStore {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stores[r.defaultProvider]
}

// Store returns the backend for a provider.
func (r *Registry) Store(p config.Provider) (sdk.ObjectStore, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	st, ok := r.stores[p]
	return st, ok
}

// LogicalBuckets returns all declared logical bucket names, sorted.
func (r *Registry) LogicalBuckets() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.logical))
	for l := range r.logical {
		out = append(out, l)
	}
	sort.Strings(out)
	return out
}

// EnsureBuckets creates every declared physical bucket on its backend. Safe to
// call repeatedly (EnsureBucket is idempotent) — invoked at startup.
func (r *Registry) EnsureBuckets(ctx context.Context) error {
	for _, logical := range r.LogicalBuckets() {
		route, err := r.Resolve(logical)
		if err != nil {
			return err
		}
		if err := route.Store.EnsureBucket(ctx, route.Bucket); err != nil {
			return fmt.Errorf("ensure bucket %q (%s): %w", route.Bucket, logical, err)
		}
	}
	return nil
}

// Close closes every distinct backend store exactly once.
func (r *Registry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := make(map[sdk.ObjectStore]struct{})
	var firstErr error
	for _, st := range r.stores {
		if _, done := seen[st]; done {
			continue
		}
		seen[st] = struct{}{}
		if err := st.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
