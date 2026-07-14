package retry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cpip/internal/reliability/backoff"
	"cpip/internal/reliability/config"
	"cpip/internal/reliability/events"
	"cpip/internal/reliability/metrics"
)

// Classifier evaluates whether an error is transient and eligible for retries.
type Classifier func(error) bool

// RetryExecutor executes protected callbacks applying config rules.
type RetryExecutor struct {
	cfg        config.RetryConfig
	strategy   backoff.Strategy
	classifier Classifier
	bus        *events.Bus
	metrics    metrics.Recorder
}

// NewRetryExecutor constructs a RetryExecutor.
func NewRetryExecutor(
	cfg config.RetryConfig,
	strategy backoff.Strategy,
	classifier Classifier,
	bus *events.Bus,
	rec metrics.Recorder,
) *RetryExecutor {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 1
	}
	if strategy == nil {
		strategy = &backoff.FixedStrategy{}
	}
	return &RetryExecutor{
		cfg:        cfg,
		strategy:   strategy,
		classifier: classifier,
		bus:        bus,
		metrics:    rec,
	}
}

// Execute runs the operation. If it fails and is retryable, sleeps according to backoff strategy.
func (re *RetryExecutor) Execute(ctx context.Context, policyName string, fn func() error) error {
	var lastErr error
	var delay time.Duration

	for attempt := 1; attempt <= re.cfg.MaxAttempts; attempt++ {
		// Check context before executing
		if err := ctx.Err(); err != nil {
			return err
		}

		if re.metrics != nil {
			re.metrics.Inc(metrics.MetricRetryAttempts)
		}

		err := fn()
		if err == nil {
			return nil
		}

		lastErr = err

		// Check if we exhausted attempts
		if attempt >= re.cfg.MaxAttempts {
			break
		}

		// Check if the error is retryable (if classifier is defined)
		if re.classifier != nil && !re.classifier(err) {
			break
		}

		// Calculate backoff delay
		delay = re.strategy.NextDelay(attempt, re.cfg.InitialInterval, re.cfg.MaxInterval, delay)

		// Publish retry event
		if re.bus != nil {
			re.bus.Publish(events.Event{
				Type:      events.RetryExecuted,
				Timestamp: time.Now(),
				Policy:    policyName,
				Detail:    fmt.Sprintf("Attempt %d failed; retrying in %v. Error: %v", attempt, delay, err),
				Error:     err,
			})
		}

		// Wait for delay or context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}

	if re.metrics != nil {
		re.metrics.Inc(metrics.MetricRetryExhaustion)
	}

	return fmt.Errorf("retry execution exhausted after %d attempts: %w", re.cfg.MaxAttempts, lastErr)
}

// DefaultClassifier retries all non-nil errors except explicitly wrapped fatal errors.
func DefaultClassifier(err error) bool {
	if err == nil {
		return false
	}
	// We can check for a specific sentinel/wrapped "FatalError"
	var fatalErr *FatalError
	return !errors.As(err, &fatalErr)
}

// FatalError represents an error that cannot be retried.
type FatalError struct {
	Err error
}

func (e *FatalError) Error() string {
	return fmt.Sprintf("fatal error: %v", e.Err)
}

func (e *FatalError) Unwrap() error {
	return e.Err
}

// NewFatalError wraps an error as fatal.
func NewFatalError(err error) *FatalError {
	return &FatalError{Err: err}
}
