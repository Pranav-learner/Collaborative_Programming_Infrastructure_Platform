package sdk

import (
	"context"
	"errors"
	"time"

	"cpip/internal/reliability/backup"
	"cpip/internal/reliability/manager"
)

// SDK defines the developer-facing reliability orchestration boundary.
type SDK interface {
	Execute(ctx context.Context, policyName string, fn func() error) error
	Retry(ctx context.Context, policyName string, fn func() error) error
	Protect(ctx context.Context, policyName string, fn func() error) error
	Acquire(ctx context.Context, policyName string) (func(), error)
	Backup(ctx context.Context, components []backup.BackupComponent) (backup.BackupMetadata, error)
	Restore(ctx context.Context, meta backup.BackupMetadata) error
	Shutdown(ctx context.Context) error
}

type Client struct {
	mgr *manager.Manager
}

func NewClient(mgr *manager.Manager) *Client {
	return &Client{mgr: mgr}
}

// Execute runs the function wrapped in a circuit breaker.
func (c *Client) Execute(ctx context.Context, policyName string, fn func() error) error {
	cb, err := c.mgr.GetCircuitBreaker(policyName)
	if err != nil {
		return err
	}

	done, err := cb.Allow()
	if err != nil {
		return err
	}

	err = fn()
	done(err == nil)
	return err
}

// Retry executes the function wrapped in retry policy.
func (c *Client) Retry(ctx context.Context, policyName string, fn func() error) error {
	exec, err := c.mgr.GetRetry(policyName)
	if err != nil {
		return err
	}
	return exec.Execute(ctx, policyName, fn)
}

// Protect executes the function combining rate limit, backpressure, bulkhead, circuit breaker, and retry.
func (c *Client) Protect(ctx context.Context, policyName string, fn func() error) error {
	// 1. Rate Limit
	rl, err := c.mgr.GetRateLimiter(policyName)
	if err == nil {
		if !rl.Allow() {
			return errors.New("rate limit exceeded; execution rejected")
		}
	}

	// 2. Backpressure Check
	bp := c.mgr.Backpressure()
	if err := bp.Acquire(ctx, 1); err != nil { // Default PriorityNormal
		return err
	}
	start := time.Now()
	defer func() {
		bp.Release(time.Since(start))
	}()

	// 3. Bulkhead Acquisition
	bh, err := c.mgr.GetBulkhead(policyName)
	if err == nil {
		release, err := bh.Acquire(ctx)
		if err != nil {
			return err
		}
		defer release()
	}

	// 4. Circuit Breaker + Retry wrap
	cb, err := c.mgr.GetCircuitBreaker(policyName)
	if err != nil {
		// If no breaker, just retry
		return c.Retry(ctx, policyName, fn)
	}

	retryAction := func() error {
		done, err := cb.Allow()
		if err != nil {
			return err
		}

		err = fn()
		done(err == nil)
		return err
	}

	return c.Retry(ctx, policyName, retryAction)
}

// Acquire reserves tokens/slots in bulkheads or rate limiters.
func (c *Client) Acquire(ctx context.Context, policyName string) (func(), error) {
	// First check rate limiter
	rl, err := c.mgr.GetRateLimiter(policyName)
	if err == nil {
		if err := rl.Wait(ctx); err != nil {
			return nil, err
		}
	}

	// Then acquire bulkhead slot
	bh, err := c.mgr.GetBulkhead(policyName)
	if err != nil {
		// Just rate limiter release (no-op func)
		return func() {}, nil
	}

	return bh.Acquire(ctx)
}

func (c *Client) Backup(ctx context.Context, components []backup.BackupComponent) (backup.BackupMetadata, error) {
	return c.mgr.BackupManager().CreateBackup(ctx, components)
}

func (c *Client) Restore(ctx context.Context, meta backup.BackupMetadata) error {
	return c.mgr.BackupManager().RestoreBackup(ctx, meta)
}

func (c *Client) Shutdown(ctx context.Context) error {
	return c.mgr.ShutdownManager().Shutdown(ctx)
}
