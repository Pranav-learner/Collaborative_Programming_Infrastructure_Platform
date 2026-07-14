// Package backend is the pluggable coordination substrate — the ONLY seam in the
// module that a concrete distributed store sits behind. Cluster services (registry,
// membership, leader election, locks, replication, discovery, heartbeat) depend
// solely on the Backend interface, never on Redis, etcd, or Consul directly. This
// is what realizes the module's decoupling mandate:
//
//	Cluster Services → backend.Backend → Redis (today) / etcd / Consul (future)
//
// Two implementations ship:
//
//   - Memory: a self-contained, thread-safe in-memory store (KV + TTL + atomic
//     compare-and-* + sets + prefix scan + in-process pub/sub). It has zero
//     external dependencies, powers the test suite, and is a valid single-node
//     backend.
//   - Redis:  a thin adapter over the platform's Redis client (internal/cache/redis),
//     providing real cross-node coordination for a multi-node deployment.
//
// The interface is deliberately narrow — exactly the primitives coordination
// needs — so a new backend (etcd's lease+txn, Consul's session+KV) is a single
// file with no ripple into business logic.
package backend

import (
	"context"
	"time"
)

// Subscription is a live pub/sub subscription. Payloads arrive on Messages until
// Close is called. It is the transport for the cluster event bus and state
// replication across nodes.
type Subscription interface {
	// Messages returns the receive-only stream of raw payloads.
	Messages() <-chan string
	// Close unsubscribes and releases resources. Idempotent.
	Close() error
}

// Backend is the contract every coordination substrate implements. All methods
// honor context cancellation. A missing key is reported via the found bool (Get)
// or a false result (compare-and-* ), never as an error, so callers branch on
// presence without unwrapping sentinels.
type Backend interface {
	// --- Key/value with TTL ---
	// Get returns (value, true, nil) when present, ("", false, nil) when absent.
	Get(ctx context.Context, key string) (value string, found bool, err error)
	// Set writes key=value with ttl (0 = no expiry).
	Set(ctx context.Context, key, value string, ttl time.Duration) error
	// SetNX writes key only if absent; returns true if stored. The primitive
	// behind lock acquisition and leader-lease claiming.
	SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	// Delete removes keys and returns the count removed.
	Delete(ctx context.Context, keys ...string) (int64, error)
	// Expire (re)sets a key's TTL; returns false if the key is absent.
	Expire(ctx context.Context, key string, ttl time.Duration) (bool, error)
	// TTL returns remaining lifetime: -2 if missing, -1 if no expiry, else the duration.
	TTL(ctx context.Context, key string) (time.Duration, error)

	// --- Atomic compare-and-* (owner fencing / optimistic concurrency) ---
	// CompareAndSwap sets key=newValue with ttl only if the current value equals
	// expected (or the key is absent and expected == ""). Returns true on success.
	CompareAndSwap(ctx context.Context, key, expected, newValue string, ttl time.Duration) (bool, error)
	// CompareAndDelete deletes key only if its value equals expected — the safe
	// lock/lease release primitive.
	CompareAndDelete(ctx context.Context, key, expected string) (bool, error)
	// CompareAndExpire refreshes key's TTL only if its value equals expected — the
	// safe lock/lease renew primitive.
	CompareAndExpire(ctx context.Context, key, expected string, ttl time.Duration) (bool, error)

	// --- Sets (membership rosters) ---
	SAdd(ctx context.Context, key string, members ...string) (int64, error)
	SRem(ctx context.Context, key string, members ...string) (int64, error)
	SMembers(ctx context.Context, key string) ([]string, error)

	// --- Enumeration (discovery / registry sweeps) ---
	// Scan returns all keys sharing prefix. On real Redis this is a cursor-based
	// SCAN; callers must treat the result as a point-in-time snapshot.
	Scan(ctx context.Context, prefix string) ([]string, error)

	// --- Pub/Sub (cross-node event bus & replication) ---
	Publish(ctx context.Context, channel, payload string) error
	Subscribe(ctx context.Context, channel string) (Subscription, error)

	// --- Lifecycle ---
	Ping(ctx context.Context) error
	Close() error
}
