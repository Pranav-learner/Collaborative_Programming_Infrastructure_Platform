// Package leader implements a PLUGGABLE leader election framework. It provides
// lease-based leadership — a single winner holds a TTL'd lease it must renew —
// which is the coordination pattern behind Kubernetes Leases, Consul sessions,
// and etcd election. It is deliberately NOT a consensus protocol: there is one
// authoritative store (the coordination backend) and leadership is mutual
// exclusion over a key, not agreement among peers. A future Raft/etcd-native
// elector can be dropped in behind the Elector interface without changing the
// Election runtime or any business logic.
package leader

import (
	"context"
	"time"

	"cpip/internal/coordination/backend"
	"cpip/internal/coordination/keys"
)

// Elector is the pluggable election primitive. Implementations provide the
// mechanism (lease over Redis today; etcd/Consul/Raft tomorrow); the Election
// runtime provides the lifecycle (campaign loop, renewal, loss detection). Every
// method is a single, non-blocking attempt.
type Elector interface {
	// Campaign attempts to acquire (or confirm) leadership of scope for
	// candidateID for one lease. Returns true if candidateID is the leader.
	Campaign(ctx context.Context, scope, candidateID string, lease time.Duration) (bool, error)
	// Renew extends the lease iff candidateID still holds it. Returns false when
	// leadership was lost (someone else took over or the lease lapsed).
	Renew(ctx context.Context, scope, candidateID string, lease time.Duration) (bool, error)
	// Resign relinquishes leadership iff candidateID holds it.
	Resign(ctx context.Context, scope, candidateID string) error
	// Leader returns the current leader's id, or ("", false) if none.
	Leader(ctx context.Context, scope string) (string, bool, error)
	// Transfer atomically hands leadership from → to iff from currently holds it.
	Transfer(ctx context.Context, scope, from, to string, lease time.Duration) (bool, error)
}

// LeaseElector is the default Elector: leadership is ownership of a single TTL'd
// key. Acquisition is SET NX; renewal/resignation/transfer are token-fenced
// atomic compare-and-* ops, so a candidate can only ever renew, release, or hand
// off a lease it actually holds — the same fencing that makes the lock service
// safe.
type LeaseElector struct {
	backend backend.Backend
	kb      keys.Builder
}

// NewLeaseElector constructs a lease-based Elector over a coordination backend.
func NewLeaseElector(b backend.Backend, kb keys.Builder) *LeaseElector {
	return &LeaseElector{backend: b, kb: kb}
}

// Campaign acquires the lease, or confirms ownership if we already hold it (e.g.
// leadership was just transferred to us).
func (e *LeaseElector) Campaign(ctx context.Context, scope, candidateID string, lease time.Duration) (bool, error) {
	key := e.kb.LeaderKey(scope)
	ok, err := e.backend.SetNX(ctx, key, candidateID, lease)
	if err != nil {
		return false, err
	}
	if ok {
		return true, nil
	}
	// Key exists — we win only if it already names us.
	cur, found, err := e.backend.Get(ctx, key)
	if err != nil {
		return false, err
	}
	return found && cur == candidateID, nil
}

// Renew extends our lease iff we still hold the key.
func (e *LeaseElector) Renew(ctx context.Context, scope, candidateID string, lease time.Duration) (bool, error) {
	return e.backend.CompareAndExpire(ctx, e.kb.LeaderKey(scope), candidateID, lease)
}

// Resign deletes the key iff we hold it.
func (e *LeaseElector) Resign(ctx context.Context, scope, candidateID string) error {
	_, err := e.backend.CompareAndDelete(ctx, e.kb.LeaderKey(scope), candidateID)
	return err
}

// Leader returns the current leader id.
func (e *LeaseElector) Leader(ctx context.Context, scope string) (string, bool, error) {
	return e.backend.Get(ctx, e.kb.LeaderKey(scope))
}

// Transfer atomically replaces the leader value from → to iff from holds it. The
// new leader's Election loop confirms ownership on its next campaign tick.
func (e *LeaseElector) Transfer(ctx context.Context, scope, from, to string, lease time.Duration) (bool, error) {
	return e.backend.CompareAndSwap(ctx, e.kb.LeaderKey(scope), from, to, lease)
}

var _ Elector = (*LeaseElector)(nil)
