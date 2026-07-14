// Package registry provides a generic, concurrent-safe registry primitive shared
// by the metrics, trace, and logging registries. Centralizing the mechanism
// keeps every registry consistent (same conflict semantics, same iteration
// guarantees) and dependency-free — it imports nothing from the module.
//
// The registries the objectives call for (Metrics Registry, Trace Registry,
// Logging Registry) are each a thin, type-specialized wrapper over this
// primitive, so registration conflicts, lookups, and enumeration behave
// identically everywhere.
package registry

import (
	"errors"
	"sort"
	"sync"
)

var (
	// ErrConflict indicates a name is already registered.
	ErrConflict = errors.New("observability/registry: name already registered")
	// ErrNotFound indicates a name is not registered.
	ErrNotFound = errors.New("observability/registry: name not registered")
)

// Registry is a concurrent-safe name→value map with conflict-checked
// registration. The zero value is not usable; call New.
type Registry[T any] struct {
	mu    sync.RWMutex
	items map[string]T
}

// New constructs an empty Registry.
func New[T any]() *Registry[T] {
	return &Registry[T]{items: make(map[string]T)}
}

// Register stores value under name, returning ErrConflict if name is taken.
func (r *Registry[T]) Register(name string, value T) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.items[name]; ok {
		return ErrConflict
	}
	r.items[name] = value
	return nil
}

// GetOrCreate returns the existing value for name, or creates, stores, and
// returns a new one via create. It is the idempotent registration primitive the
// metrics meter uses so repeated Counter("x") calls return the same instrument.
func (r *Registry[T]) GetOrCreate(name string, create func() T) T {
	r.mu.RLock()
	if v, ok := r.items[name]; ok {
		r.mu.RUnlock()
		return v
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	if v, ok := r.items[name]; ok { // re-check under write lock
		return v
	}
	v := create()
	r.items[name] = v
	return v
}

// Get returns the value for name and whether it was present.
func (r *Registry[T]) Get(name string) (T, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.items[name]
	return v, ok
}

// Unregister removes name, returning ErrNotFound if it was absent.
func (r *Registry[T]) Unregister(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.items[name]; !ok {
		return ErrNotFound
	}
	delete(r.items, name)
	return nil
}

// Names returns all registered names, sorted for deterministic iteration.
func (r *Registry[T]) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.items))
	for k := range r.items {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// List returns all values (order matches Names).
func (r *Registry[T]) List() []T {
	names := r.Names()
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]T, 0, len(names))
	for _, n := range names {
		out = append(out, r.items[n])
	}
	return out
}

// ForEach invokes fn for each entry under a read lock. fn must not call back into
// the registry (it would deadlock). Return false from fn to stop early.
func (r *Registry[T]) ForEach(fn func(name string, value T) bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for k, v := range r.items {
		if !fn(k, v) {
			return
		}
	}
}

// Len returns the number of registered entries.
func (r *Registry[T]) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.items)
}
