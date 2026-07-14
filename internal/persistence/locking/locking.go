package locking

import (
	"context"
	"errors"
	"time"
)

// ErrOptimisticLockConflict is returned when an entity update fails because the version in the database is newer.
var ErrOptimisticLockConflict = errors.New("optimistic locking conflict detected")

// RetryConflict retries a database operation if it hits an optimistic locking conflict.
func RetryConflict(ctx context.Context, maxRetries int, delay time.Duration, fn func() error) error {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := fn()
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrOptimisticLockConflict) {
			return err
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return lastErr
}
