package config

import "time"

// CircuitBreakerConfig configures failure thresholds and state timeouts.
type CircuitBreakerConfig struct {
	FailureThreshold float64       `json:"failure_threshold"` // e.g. 0.5 (50% failure rate)
	RecoveryTimeout  time.Duration `json:"recovery_timeout"`  // time in Open state before Half-Open
	SuccessThreshold int           `json:"success_threshold"` // consecutive successes needed in Half-Open to Close
	FailureWindow    time.Duration `json:"failure_window"`    // sliding window to evaluate failures
	MinRequests      int           `json:"min_requests"`      // minimum requests in window before evaluating
}

// BackoffType determines the mathematical progression of retries.
type BackoffType string

const (
	BackoffFixed             BackoffType = "fixed"
	BackoffLinear            BackoffType = "linear"
	BackoffExponential       BackoffType = "exponential"
	BackoffExponentialJitter BackoffType = "exponential_jitter"
	BackoffDecorrelated      BackoffType = "decorrelated_jitter"
)

// RetryConfig specifies max attempts, backoff strategies, and types.
type RetryConfig struct {
	MaxAttempts     int         `json:"max_attempts"`
	InitialInterval time.Duration `json:"initial_interval"`
	MaxInterval     time.Duration `json:"max_interval"`
	BackoffType     BackoffType `json:"backoff_type"`
}

// BulkheadType defines execution limit pools.
type BulkheadType string

const (
	BulkheadPool      BulkheadType = "pool"
	BulkheadSemaphore BulkheadType = "semaphore"
)

// BulkheadConfig configures concurrent work limits.
type BulkheadConfig struct {
	Type          BulkheadType `json:"type"`
	MaxConcurrent int          `json:"max_concurrent"`
	QueueCapacity int          `json:"queue_capacity"` // Only used for TypePool
}

// RateLimitType defines algorithms.
type RateLimitType string

const (
	RateLimitTokenBucket  RateLimitType = "token_bucket"
	RateLimitLeakyBucket   RateLimitType = "leaky_bucket"
	RateLimitSlidingWindow RateLimitType = "sliding_window"
)

// RateLimitConfig defines capacity and refill rates.
type RateLimitConfig struct {
	Type     RateLimitType `json:"type"`
	Rate     float64       `json:"rate"` // Operations per second
	Burst    int           `json:"burst"`
	Interval time.Duration `json:"interval"` // For Sliding Window
}

// BackupPolicy configures retention and schedule strategies.
type BackupPolicy struct {
	Schedule       string        `json:"schedule"` // e.g. "@daily"
	RetentionLimit int           `json:"retention_limit"`
	ValidationEnabled bool       `json:"validation_enabled"`
}

// Policy wraps individual resilience configs.
type Policy struct {
	Retry          *RetryConfig          `json:"retry,omitempty"`
	CircuitBreaker *CircuitBreakerConfig `json:"circuit_breaker,omitempty"`
	Bulkhead       *BulkheadConfig       `json:"bulkhead,omitempty"`
	RateLimit      *RateLimitConfig      `json:"rate_limit,omitempty"`
}

// PlatformConfig holds all registered policies and timeouts.
type PlatformConfig struct {
	Policies        map[string]Policy `json:"policies"`
	ShutdownTimeout time.Duration     `json:"shutdown_timeout"`
	BackupPolicy    BackupPolicy      `json:"backup_policy"`
}

// DefaultPlatformConfig returns a sensible development/production config.
func DefaultPlatformConfig() PlatformConfig {
	return PlatformConfig{
		Policies: map[string]Policy{
			"default": {
				Retry: &RetryConfig{
					MaxAttempts:     3,
					InitialInterval: 100 * time.Millisecond,
					MaxInterval:     2 * time.Second,
					BackoffType:     BackoffExponentialJitter,
				},
				CircuitBreaker: &CircuitBreakerConfig{
					FailureThreshold: 0.5,
					RecoveryTimeout:  5 * time.Second,
					SuccessThreshold: 3,
					FailureWindow:    10 * time.Second,
					MinRequests:      5,
				},
				Bulkhead: &BulkheadConfig{
					Type:          BulkheadSemaphore,
					MaxConcurrent: 100,
				},
				RateLimit: &RateLimitConfig{
					Type:  RateLimitTokenBucket,
					Rate:  50.0,
					Burst: 10,
				},
			},
		},
		ShutdownTimeout: 30 * time.Second,
		BackupPolicy: BackupPolicy{
			Schedule:       "0 0 * * *",
			RetentionLimit: 7,
			ValidationEnabled: true,
		},
	}
}
