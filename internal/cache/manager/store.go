package manager

import (
	"context"
	"errors"
	"time"

	"cpip/internal/cache/policies"
	"cpip/internal/cache/redis"
	"cpip/internal/cache/types"
)

// RawGet implements policies.Store. It returns the stored value and its
// remaining TTL. The TTL is only fetched on a hit (an extra round trip that the
// refresh-ahead strategy needs); on a miss a single Redis call suffices.
func (m *Manager) RawGet(ctx context.Context, fullKey string) (string, time.Duration, bool, error) {
	value, err := m.client.Get(ctx, fullKey)
	if err != nil {
		if errors.Is(err, types.ErrNil) {
			return "", 0, false, nil
		}
		return "", 0, false, err
	}
	remaining, err := m.client.TTL(ctx, fullKey)
	if err != nil {
		// A TTL read failure must not fail the Get; treat as "no known expiry".
		remaining = -1 * time.Second
	}
	return value, remaining, true, nil
}

// RawSet implements policies.Store.
func (m *Manager) RawSet(ctx context.Context, fullKey, value string, ttl time.Duration) error {
	// ttl <= 0 means "no expiry" at the Redis layer.
	writeTTL := ttl
	if writeTTL < 0 {
		writeTTL = redis.NoExpiry
	}
	return m.client.Set(ctx, fullKey, value, writeTTL)
}

// RawDelete implements policies.Store.
func (m *Manager) RawDelete(ctx context.Context, fullKey string) error {
	_, err := m.client.Del(ctx, fullKey)
	return err
}

var _ policies.Store = (*Manager)(nil)
