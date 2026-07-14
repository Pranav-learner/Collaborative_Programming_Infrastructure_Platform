// Package registry implements the Node Registry: the concurrent-safe store of
// every node's record (identity, capabilities, status, load, heartbeat, health,
// metadata). It is write-through — the durable copy lives in the coordination
// backend (so it is visible cluster-wide), while a local cache serves reads
// without a round-trip.
//
// Consistency is monotonic: every cache commit is guarded by Node.Supersedes, so
// a stale write (older incarnation or timestamp) can never overwrite a fresher
// record, even when a background Refresh races a live update. This is what lets
// the registry stay correct under thousands of concurrent registrations and
// heartbeat updates.
package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"cpip/internal/coordination/backend"
	"cpip/internal/coordination/events"
	"cpip/internal/coordination/keys"
	"cpip/internal/coordination/logger"
	"cpip/internal/coordination/metrics"
	"cpip/internal/coordination/types"
)

// Registry is the Node Registry.
type Registry struct {
	backend backend.Backend
	kb      keys.Builder
	bus     *events.Bus
	rec     metrics.Recorder
	log     *logger.Logger
	now     func() time.Time

	mu    sync.RWMutex
	cache map[string]*types.Node
}

// Params configures a Registry.
type Params struct {
	Backend backend.Backend
	Keys    keys.Builder
	Events  *events.Bus
	Metrics metrics.Recorder
	Logger  *logger.Logger
}

// New constructs a Node Registry.
func New(p Params) *Registry {
	rec := p.Metrics
	if rec == nil {
		rec = metrics.NewNoop()
	}
	return &Registry{
		backend: p.Backend,
		kb:      p.Keys,
		bus:     p.Events,
		rec:     rec,
		log:     p.Logger.With("subsystem", "registry"),
		now:     time.Now,
		cache:   make(map[string]*types.Node),
	}
}

// Put writes a node through to the backend and commits it to the cache (guarded
// by Supersedes). It is the single mutation primitive used by higher layers.
func (r *Registry) Put(ctx context.Context, n *types.Node) error {
	if n == nil || n.ID == "" {
		return fmt.Errorf("%w: nil or id-less node", types.ErrInvalidNode)
	}
	cp := n.Clone()
	if cp.UpdatedAt.IsZero() {
		cp.UpdatedAt = r.now().UTC()
	}
	data, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("%w: encode node: %v", types.ErrInvalidNode, err)
	}
	if err := r.backend.Set(ctx, r.kb.NodeKey(cp.ID), string(data), 0); err != nil {
		r.rec.IncCounter(metrics.MetricBackendError, map[string]string{"op": "put_node"})
		return err
	}
	if _, err := r.backend.SAdd(ctx, r.kb.MembersSet(), cp.ID); err != nil {
		return err
	}
	r.commit(cp)
	return nil
}

// commit installs n into the cache only if it supersedes the current entry.
func (r *Registry) commit(n *types.Node) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cur, ok := r.cache[n.ID]; ok && !n.Supersedes(cur) {
		return
	}
	r.cache[n.ID] = n
	r.updateGaugesLocked()
}

// Get returns a node by ID, falling back to the backend on a cache miss.
func (r *Registry) Get(ctx context.Context, id string) (*types.Node, error) {
	r.mu.RLock()
	n, ok := r.cache[id]
	r.mu.RUnlock()
	if ok {
		return n.Clone(), nil
	}
	raw, found, err := r.backend.Get(ctx, r.kb.NodeKey(id))
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("%w: %s", types.ErrNodeNotFound, id)
	}
	node, derr := decode(raw)
	if derr != nil {
		return nil, derr
	}
	r.commit(node)
	return node.Clone(), nil
}

// Exists reports whether a node is known (cache or backend).
func (r *Registry) Exists(ctx context.Context, id string) (bool, error) {
	if _, err := r.Get(ctx, id); err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Remove deletes a node from the backend and cache.
func (r *Registry) Remove(ctx context.Context, id string) error {
	if _, err := r.backend.Delete(ctx, r.kb.NodeKey(id)); err != nil {
		return err
	}
	if _, err := r.backend.SRem(ctx, r.kb.MembersSet(), id); err != nil {
		return err
	}
	r.mu.Lock()
	delete(r.cache, id)
	r.updateGaugesLocked()
	r.mu.Unlock()
	return nil
}

// Mutate applies fn to a clone of the current node record and writes it through.
// fn runs OUTSIDE the cache lock, so backend I/O never serializes other readers.
// The commit is Supersedes-guarded, so concurrent mutators converge safely. fn
// should bump UpdatedAt-relevant fields; the registry stamps UpdatedAt.
func (r *Registry) Mutate(ctx context.Context, id string, fn func(*types.Node)) (*types.Node, error) {
	cur, err := r.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	fn(cur)
	cur.UpdatedAt = r.now().UTC()
	if err := r.Put(ctx, cur); err != nil {
		return nil, err
	}
	return cur, nil
}

// List returns a snapshot of all cached nodes.
func (r *Registry) List() []*types.Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*types.Node, 0, len(r.cache))
	for _, n := range r.cache {
		out = append(out, n.Clone())
	}
	types.SortNodesByID(out)
	return out
}

// Filter returns cached nodes matching pred.
func (r *Registry) Filter(pred func(*types.Node) bool) []*types.Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*types.Node
	for _, n := range r.cache {
		if pred(n) {
			out = append(out, n.Clone())
		}
	}
	types.SortNodesByID(out)
	return out
}

// Count returns the number of cached nodes.
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.cache)
}

// Refresh rebuilds the cache from the backend's authoritative membership set. It
// is the anti-entropy primitive the membership manager runs periodically to pull
// in nodes registered by other processes. Supersedes-guarded commits mean a
// concurrent local update is never clobbered by an older backend snapshot.
func (r *Registry) Refresh(ctx context.Context) error {
	ids, err := r.backend.SMembers(ctx, r.kb.MembersSet())
	if err != nil {
		return err
	}
	present := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		present[id] = struct{}{}
		raw, found, err := r.backend.Get(ctx, r.kb.NodeKey(id))
		if err != nil {
			return err
		}
		if !found {
			// Membership set references a vanished record; drop the reference.
			_, _ = r.backend.SRem(ctx, r.kb.MembersSet(), id)
			continue
		}
		node, derr := decode(raw)
		if derr != nil {
			continue
		}
		r.commit(node)
	}
	// Evict cache entries no longer in the authoritative set. A node registered by
	// another goroutine AFTER we snapshotted `present` would be wrongly evicted, so
	// each eviction candidate is re-checked against the backend (outside the cache
	// lock): if its record still exists it is refreshed instead of dropped. This
	// makes Refresh safe to run concurrently with live registrations.
	r.mu.RLock()
	var candidates []string
	for id := range r.cache {
		if _, ok := present[id]; !ok {
			candidates = append(candidates, id)
		}
	}
	r.mu.RUnlock()
	for _, id := range candidates {
		raw, found, err := r.backend.Get(ctx, r.kb.NodeKey(id))
		if err == nil && found {
			if node, derr := decode(raw); derr == nil {
				r.commit(node)
				continue
			}
		}
		r.mu.Lock()
		delete(r.cache, id)
		r.updateGaugesLocked()
		r.mu.Unlock()
	}
	return nil
}

func (r *Registry) updateGaugesLocked() {
	var active, schedulable int
	for _, n := range r.cache {
		if n.Status == types.StatusActive {
			active++
		}
		if n.IsSchedulable() {
			schedulable++
		}
	}
	r.rec.SetGauge(metrics.MetricNodesTotal, float64(len(r.cache)), nil)
	r.rec.SetGauge(metrics.MetricNodesActive, float64(active), nil)
	r.rec.SetGauge(metrics.MetricNodesSchedulable, float64(schedulable), nil)
}

func decode(raw string) (*types.Node, error) {
	var n types.Node
	if err := json.Unmarshal([]byte(raw), &n); err != nil {
		return nil, fmt.Errorf("%w: decode node: %v", types.ErrInvalidNode, err)
	}
	return &n, nil
}

func isNotFound(err error) bool {
	return errors.Is(err, types.ErrNodeNotFound)
}
