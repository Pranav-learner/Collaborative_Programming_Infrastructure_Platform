package manager

import (
	"fmt"
	"sync"
	"time"

	"cpip/internal/reliability/backoff"
	"cpip/internal/reliability/backpressure"
	"cpip/internal/reliability/backup"
	"cpip/internal/reliability/bulkhead"
	"cpip/internal/reliability/circuitbreaker"
	"cpip/internal/reliability/config"
	"cpip/internal/reliability/events"
	"cpip/internal/reliability/health"
	"cpip/internal/reliability/logger"
	"cpip/internal/reliability/metrics"
	"cpip/internal/reliability/ratelimit"
	"cpip/internal/reliability/recovery"
	"cpip/internal/reliability/retry"
	"cpip/internal/reliability/shutdown"
)

// Manager binds all resilience mechanisms under a unified registration and access engine.
type Manager struct {
	mu           sync.RWMutex
	cfg          config.PlatformConfig
	retryExecs   map[string]*retry.RetryExecutor
	breakers     map[string]*circuitbreaker.CircuitBreaker
	bulkheads    map[string]bulkhead.Bulkhead
	limiters     map[string]ratelimit.RateLimiter

	bus          *events.Bus
	metrics      metrics.Recorder
	logger       *logger.Logger

	backpressure *backpressure.BackpressureManager
	shutdown     *shutdown.Manager
	backup       *backup.BackupManager
	drPlanner    *recovery.DisasterRecoveryPlanner
	healthEngine *health.RecoveryEngine
}

// NewManager initializes the Manager, applying policies defined in PlatformConfig.
func NewManager(
	cfg config.PlatformConfig,
	bus *events.Bus,
	rec metrics.Recorder,
	log *logger.Logger,
	backupDir string,
) (*Manager, error) {
	bm, err := backup.NewBackupManager(backupDir, cfg.BackupPolicy, bus, rec)
	if err != nil {
		return nil, err
	}

	m := &Manager{
		cfg:          cfg,
		retryExecs:   make(map[string]*retry.RetryExecutor),
		breakers:     make(map[string]*circuitbreaker.CircuitBreaker),
		bulkheads:    make(map[string]bulkhead.Bulkhead),
		limiters:     make(map[string]ratelimit.RateLimiter),
		bus:          bus,
		metrics:      rec,
		logger:       log,
		backup:       bm,
		shutdown:     shutdown.NewManager(cfg.ShutdownTimeout, bus, log),
		drPlanner:    recovery.NewDisasterRecoveryPlanner(bus, rec),
		healthEngine: health.NewRecoveryEngine(bus, log),
	}

	// Dynamic backpressure with 200 tasks queue limit
	m.backpressure = backpressure.NewBackpressureManager(200, 200*time.Millisecond, 500*time.Millisecond, bus, rec)

	m.applyConfigPolicies(cfg)

	return m, nil
}

func (m *Manager) applyConfigPolicies(cfg config.PlatformConfig) {
	for name, pol := range cfg.Policies {
		if pol.Retry != nil {
			var bo backoff.Strategy
			switch pol.Retry.BackoffType {
			case config.BackoffFixed:
				bo = &backoff.FixedStrategy{}
			case config.BackoffLinear:
				bo = &backoff.LinearStrategy{}
			case config.BackoffExponential:
				bo = &backoff.ExponentialStrategy{}
			case config.BackoffExponentialJitter:
				bo = &backoff.ExponentialJitterStrategy{}
			case config.BackoffDecorrelated:
				bo = &backoff.DecorrelatedJitterStrategy{}
			default:
				bo = &backoff.FixedStrategy{}
			}
			m.retryExecs[name] = retry.NewRetryExecutor(*pol.Retry, bo, retry.DefaultClassifier, m.bus, m.metrics)
		}

		if pol.CircuitBreaker != nil {
			cb := circuitbreaker.NewCircuitBreaker(name, *pol.CircuitBreaker, m.bus, m.metrics)
			m.breakers[name] = cb
			m.healthEngine.RegisterBreaker(name, cb)
		}

		if pol.Bulkhead != nil {
			m.bulkheads[name] = bulkhead.Factory(name, *pol.Bulkhead, m.bus, m.metrics)
		}

		if pol.RateLimit != nil {
			m.limiters[name] = ratelimit.Factory(name, *pol.RateLimit, m.bus, m.metrics)
		}
	}
}

// GetRetry returns a registered RetryExecutor.
func (m *Manager) GetRetry(name string) (*retry.RetryExecutor, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ex, exists := m.retryExecs[name]
	if !exists {
		// Fallback to default
		ex, exists = m.retryExecs["default"]
		if !exists {
			return nil, fmt.Errorf("retry policy %q not found and default missing", name)
		}
	}
	return ex, nil
}

// GetCircuitBreaker returns a registered breaker.
func (m *Manager) GetCircuitBreaker(name string) (*circuitbreaker.CircuitBreaker, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cb, exists := m.breakers[name]
	if !exists {
		cb, exists = m.breakers["default"]
		if !exists {
			return nil, fmt.Errorf("circuit breaker policy %q not found and default missing", name)
		}
	}
	return cb, nil
}

// GetBulkhead returns a registered bulkhead.
func (m *Manager) GetBulkhead(name string) (bulkhead.Bulkhead, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	bh, exists := m.bulkheads[name]
	if !exists {
		bh, exists = m.bulkheads["default"]
		if !exists {
			return nil, fmt.Errorf("bulkhead policy %q not found and default missing", name)
		}
	}
	return bh, nil
}

// GetRateLimiter returns a registered ratelimiter.
func (m *Manager) GetRateLimiter(name string) (ratelimit.RateLimiter, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	rl, exists := m.limiters[name]
	if !exists {
		rl, exists = m.limiters["default"]
		if !exists {
			return nil, fmt.Errorf("ratelimiter policy %q not found and default missing", name)
		}
	}
	return rl, nil
}

// Accessors for managers
func (m *Manager) Backpressure() *backpressure.BackpressureManager {
	return m.backpressure
}

func (m *Manager) ShutdownManager() *shutdown.Manager {
	return m.shutdown
}

func (m *Manager) BackupManager() *backup.BackupManager {
	return m.backup
}

func (m *Manager) DisasterRecoveryPlanner() *recovery.DisasterRecoveryPlanner {
	return m.drPlanner
}

func (m *Manager) HealthEngine() *health.RecoveryEngine {
	return m.healthEngine
}

func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, bh := range m.bulkheads {
		bh.Close()
	}
}
