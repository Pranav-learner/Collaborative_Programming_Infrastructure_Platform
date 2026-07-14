// Package types defines the leaf models, canonical error set, and shared
// value objects for the Redis distributed state & caching module (Stage 4
// Module 2). It has no dependencies on other cache packages so that every
// subsystem can import it without creating cycles.
package types

import "errors"

// The canonical cache/state error set. All subsystems wrap these sentinels so
// that callers can match with errors.Is regardless of the concrete backend.
var (
	// ErrNil indicates a key does not exist (a cache miss at the adapter layer).
	// It intentionally mirrors go-redis' redis.Nil so higher layers can treat a
	// miss uniformly.
	ErrNil = errors.New("cache: key does not exist")

	// ErrRedisUnavailable indicates the Redis backend could not be reached.
	ErrRedisUnavailable = errors.New("cache: redis unavailable")

	// ErrSerialization indicates a value could not be encoded to bytes.
	ErrSerialization = errors.New("cache: serialization failed")
	// ErrDeserialization indicates stored bytes could not be decoded.
	ErrDeserialization = errors.New("cache: deserialization failed")
	// ErrCorruption indicates a stored value failed an integrity check.
	ErrCorruption = errors.New("cache: value corruption detected")

	// ErrClosed indicates the component has been shut down.
	ErrClosed = errors.New("cache: component closed")
	// ErrConfig indicates an invalid configuration value.
	ErrConfig = errors.New("cache: invalid configuration")

	// --- Registry ---
	// ErrCacheNotRegistered indicates a named cache is unknown to the registry.
	ErrCacheNotRegistered = errors.New("cache: cache not registered")
	// ErrCacheAlreadyRegistered indicates a name collision on registration.
	ErrCacheAlreadyRegistered = errors.New("cache: cache already registered")

	// --- Policies ---
	// ErrNoLoader indicates a read-through/refresh-ahead policy was configured
	// without a loader function.
	ErrNoLoader = errors.New("cache: no loader configured for policy")
	// ErrNoWriter indicates a write-through/write-behind policy was configured
	// without a writer function.
	ErrNoWriter = errors.New("cache: no writer configured for policy")
	// ErrUnknownPolicy indicates an unrecognized cache policy was requested.
	ErrUnknownPolicy = errors.New("cache: unknown policy")

	// --- Locks ---
	// ErrLockNotAcquired indicates the lock could not be taken within the
	// acquisition deadline (contention or timeout).
	ErrLockNotAcquired = errors.New("cache: lock not acquired")
	// ErrLockNotHeld indicates release/renew was attempted on a lock the caller
	// does not own (lost lease, fencing mismatch, or already released).
	ErrLockNotHeld = errors.New("cache: lock not held by caller")
	// ErrLockExpired indicates the lease elapsed before the operation completed.
	ErrLockExpired = errors.New("cache: lock lease expired")

	// --- Sessions ---
	// ErrSessionNotFound indicates the requested session does not exist or expired.
	ErrSessionNotFound = errors.New("cache: session not found")
	// ErrSessionExpired indicates the session existed but its TTL elapsed.
	ErrSessionExpired = errors.New("cache: session expired")
	// ErrSessionConflict indicates a concurrent session update lost a CAS race.
	ErrSessionConflict = errors.New("cache: session update conflict")

	// --- Pub/Sub ---
	// ErrTopicNotRegistered indicates publish/subscribe on an unknown topic.
	ErrTopicNotRegistered = errors.New("cache: topic not registered")
	// ErrBackpressure indicates a subscriber's buffer overflowed and a message
	// was dropped (or the publish was rejected) to protect the process.
	ErrBackpressure = errors.New("cache: subscriber backpressure")
	// ErrPubSubClosed indicates the pub/sub manager was shut down.
	ErrPubSubClosed = errors.New("cache: pubsub closed")

	// --- State ---
	// ErrStateConflict indicates a distributed-state compare-and-set race was lost.
	ErrStateConflict = errors.New("cache: distributed state conflict")
	// ErrStateNotFound indicates a requested state key is absent.
	ErrStateNotFound = errors.New("cache: distributed state not found")
)
