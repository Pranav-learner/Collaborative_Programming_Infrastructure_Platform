package health

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cpip/internal/reliability/events"
	"cpip/internal/reliability/logger"
)

// CircuitResetter defines control over circuit breaker states.
type CircuitResetter interface {
	Name() string
	Reset()
}

// RecoveryHook restart/reinitializes connections or processes.
type RecoveryHook func(context.Context) error

// HealthCheck verifies a service is operational.
type HealthCheck func(context.Context) error

type recoveryRegistration struct {
	hook  RecoveryHook
	check HealthCheck
}

// RecoveryEngine coordinates health corrections and resets.
type RecoveryEngine struct {
	mu           sync.RWMutex
	registrations map[string]recoveryRegistration
	breakers      map[string][]CircuitResetter
	bus          *events.Bus
	logger       *logger.Logger
}

func NewRecoveryEngine(bus *events.Bus, log *logger.Logger) *RecoveryEngine {
	return &RecoveryEngine{
		registrations: make(map[string]recoveryRegistration),
		breakers:      make(map[string][]CircuitResetter),
		bus:           bus,
		logger:        log,
	}
}

// Register adds service recovery handlers and post-recovery validations.
func (re *RecoveryEngine) Register(service string, hook RecoveryHook, check HealthCheck) {
	re.mu.Lock()
	defer re.mu.Unlock()
	re.registrations[service] = recoveryRegistration{
		hook:  hook,
		check: check,
	}
}

// RegisterBreaker binds a circuit breaker to a service for forced resets.
func (re *RecoveryEngine) RegisterBreaker(service string, cb CircuitResetter) {
	re.mu.Lock()
	defer re.mu.Unlock()
	re.breakers[service] = append(re.breakers[service], cb)
}

// AttemptRecovery executes recovery hooks, verifies health, and resets breakers.
func (re *RecoveryEngine) AttemptRecovery(ctx context.Context, service string) error {
	re.mu.RLock()
	reg, registered := re.registrations[service]
	breakersToReset := re.breakers[service]
	re.mu.RUnlock()

	if !registered {
		return fmt.Errorf("no recovery handler registered for service %q", service)
	}

	re.logger.Info("Starting auto-recovery attempt", "service", service)

	// 1. Run recovery hook (e.g. restart service, reconnect db)
	if err := reg.hook(ctx); err != nil {
		re.logger.Error("Recovery hook failed; escalating", "service", service, "error", err)
		return fmt.Errorf("recovery hook execution failed: %w", err)
	}

	// 2. Wait slightly for state stabilization
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(200 * time.Millisecond):
	}

	// 3. Verify health check passes
	if err := reg.check(ctx); err != nil {
		re.logger.Error("Post-recovery health check failed; escalating", "service", service, "error", err)
		return fmt.Errorf("post-recovery health check failed: %w", err)
	}

	// 4. Reset associated circuit breakers
	for _, cb := range breakersToReset {
		re.logger.Info("Resetting circuit breaker post-recovery", "breaker", cb.Name(), "service", service)
		cb.Reset()
	}

	re.logger.Info("Auto-recovery completed successfully", "service", service)
	return nil
}
