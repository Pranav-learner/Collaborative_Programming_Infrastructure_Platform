// Package ratelimit defines the connection-admission rate-limiting hook used by
// the gateway. As with metrics, this module ships only the interface and a
// permissive default; a real limiter (token bucket per IP/principal, backed by
// Redis for cross-node correctness) is a later module and drops in via DI.
package ratelimit

// Limiter decides whether an action keyed by key (e.g. a client IP or user id)
// is currently permitted. Implementations MUST be safe for concurrent use.
type Limiter interface {
	Allow(key string) bool
}

// NoopLimiter permits everything. It is the default until the real limiter lands.
type NoopLimiter struct{}

// Allow always returns true.
func (NoopLimiter) Allow(string) bool { return true }

// Compile-time assurance.
var _ Limiter = NoopLimiter{}
