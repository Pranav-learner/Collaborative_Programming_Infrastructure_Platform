// Package redis is the ONLY package in the module that talks to Redis. It
// defines a narrow Client interface covering exactly the operations the higher
// layers need, plus two implementations:
//
//   - Redis:    the production client backed by github.com/redis/go-redis/v9.
//   - Emulator: a faithful in-memory backend (strings, hashes, sets, TTL,
//     pub/sub, and atomic compare-and-* ops) that powers the test suite and
//     serves as a valid development backend with no external dependency.
//
// Business logic never imports this package directly; it goes through the
// Cache Manager and Distributed State Manager. Keeping the interface small
// keeps both backends in lock-step and makes a future Redis Cluster / Sentinel
// swap a one-file change.
package redis

import (
	"context"
	"time"
)

// TTL sentinels mirror Redis' TTL return contract.
const (
	// KeepTTL preserves an existing key's TTL on Set (Redis KEEPTTL).
	KeepTTL time.Duration = -1
	// NoExpiry stores a key without expiration.
	NoExpiry time.Duration = 0
)

// Message is a single pub/sub delivery.
type Message struct {
	Channel string
	Pattern string // non-empty for pattern subscriptions
	Payload string
}

// Subscription is a live pub/sub subscription. Messages arrive on Channel until
// Close is called or the context that created it is cancelled.
type Subscription interface {
	// Channel returns the receive-only stream of messages.
	Channel() <-chan Message
	// Close unsubscribes and releases resources. Idempotent.
	Close() error
}

// Client is the narrow contract every backend implements. All methods honor
// context cancellation. Missing keys are reported as types.ErrNil on the paths
// where that distinction matters (Get, HGet, CompareAnd*), and as zero values
// elsewhere (Exists, TTL) to mirror Redis semantics.
type Client interface {
	// --- Strings / key-value ---
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key, value string, ttl time.Duration) error
	// SetNX sets key only if it does not exist; returns true if stored. This is
	// the primitive behind lock acquisition.
	SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	Del(ctx context.Context, keys ...string) (int64, error)
	Exists(ctx context.Context, keys ...string) (int64, error)
	Expire(ctx context.Context, key string, ttl time.Duration) (bool, error)
	// TTL returns the remaining lifetime: -2 if the key is missing, -1 if it has
	// no expiry, otherwise the duration.
	TTL(ctx context.Context, key string) (time.Duration, error)
	Persist(ctx context.Context, key string) (bool, error)
	// Incr atomically increments an integer key and returns the new value.
	Incr(ctx context.Context, key string) (int64, error)

	// --- Atomic compare-and-* (Lua EVAL on real Redis) ---
	// CompareAndDelete deletes key only if its current value equals expected.
	// This is the safe lock-release primitive (owner fencing).
	CompareAndDelete(ctx context.Context, key, expected string) (bool, error)
	// CompareAndExtend refreshes key's TTL only if its value equals expected.
	// This is the safe lock-renew primitive.
	CompareAndExtend(ctx context.Context, key, expected string, ttl time.Duration) (bool, error)
	// CompareAndSet sets key to newValue with ttl only if its current value
	// equals expected (or the key is absent and expected==""). Backs distributed
	// state optimistic concurrency.
	CompareAndSet(ctx context.Context, key, expected, newValue string, ttl time.Duration) (bool, error)

	// --- Bulk ---
	MGet(ctx context.Context, keys ...string) ([]*string, error)
	// SetMany writes all pairs with the same ttl (pipelined on real Redis).
	SetMany(ctx context.Context, pairs map[string]string, ttl time.Duration) error
	// ScanKeys returns keys matching a glob pattern. On real Redis this is a
	// cursor-based SCAN (non-blocking); callers must treat results as a snapshot.
	ScanKeys(ctx context.Context, match string, count int64) ([]string, error)

	// --- Hashes (sessions, presence records) ---
	HSet(ctx context.Context, key string, fields map[string]string) error
	HGet(ctx context.Context, key, field string) (string, error)
	HGetAll(ctx context.Context, key string) (map[string]string, error)
	HDel(ctx context.Context, key string, fields ...string) (int64, error)

	// --- Sets (room membership, device sets) ---
	SAdd(ctx context.Context, key string, members ...string) (int64, error)
	SRem(ctx context.Context, key string, members ...string) (int64, error)
	SMembers(ctx context.Context, key string) ([]string, error)
	SIsMember(ctx context.Context, key, member string) (bool, error)

	// --- Pub/Sub ---
	Publish(ctx context.Context, channel, message string) (int64, error)
	Subscribe(ctx context.Context, channels ...string) (Subscription, error)
	PSubscribe(ctx context.Context, patterns ...string) (Subscription, error)

	// --- Health / lifecycle ---
	Ping(ctx context.Context) error
	Close() error
}
