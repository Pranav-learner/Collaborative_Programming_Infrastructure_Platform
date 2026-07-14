package circuitbreaker

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"cpip/internal/reliability/config"
	"cpip/internal/reliability/events"
	"cpip/internal/reliability/metrics"
)

// State represents the current lifecycle phase of the circuit breaker.
type State int

const (
	StateClosed State = iota
	StateHalfOpen
	StateOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "CLOSED"
	case StateHalfOpen:
		return "HALF-OPEN"
	case StateOpen:
		return "OPEN"
	default:
		return "UNKNOWN"
	}
}

// ErrCircuitOpen is returned when the circuit breaker is in the Open state.
var ErrCircuitOpen = errors.New("circuit breaker is open; request blocked")

type requestRecord struct {
	timestamp time.Time
	success   bool
}

// CircuitBreaker protects calls to downstream services.
type CircuitBreaker struct {
	mu           sync.Mutex
	name         string
	cfg          config.CircuitBreakerConfig
	state        State
	records      []requestRecord
	openedAt     time.Time
	halfOpenSuccesses int

	bus          *events.Bus
	metrics      metrics.Recorder
}

// NewCircuitBreaker creates a CircuitBreaker.
func NewCircuitBreaker(
	name string,
	cfg config.CircuitBreakerConfig,
	bus *events.Bus,
	rec metrics.Recorder,
) *CircuitBreaker {
	if cfg.SuccessThreshold <= 0 {
		cfg.SuccessThreshold = 1
	}
	if cfg.FailureWindow <= 0 {
		cfg.FailureWindow = 10 * time.Second
	}
	cb := &CircuitBreaker{
		name:    name,
		cfg:     cfg,
		state:   StateClosed,
		records: make([]requestRecord, 0),
		bus:     bus,
		metrics: rec,
	}
	cb.updateMetrics()
	return cb
}

// Name returns the identifier of the breaker.
func (cb *CircuitBreaker) Name() string {
	return cb.name
}

// State returns the current State.
func (cb *CircuitBreaker) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.evaluateStateTransitions()
	return cb.state
}

// Allow checks if the request is permitted. If yes, returns a done function to record success/failure.
func (cb *CircuitBreaker) Allow() (func(success bool), error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.evaluateStateTransitions()

	if cb.state == StateOpen {
		return nil, ErrCircuitOpen
	}

	return func(success bool) {
		cb.mu.Lock()
		defer cb.mu.Unlock()

		cb.recordResult(success)
	}, nil
}

// Reset clears breaker states back to Closed.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.transitionTo(StateClosed)
}

func (cb *CircuitBreaker) evaluateStateTransitions() {
	now := time.Now()

	if cb.state == StateOpen {
		if now.Sub(cb.openedAt) >= cb.cfg.RecoveryTimeout {
			cb.transitionTo(StateHalfOpen)
		}
	}
}

func (cb *CircuitBreaker) recordResult(success bool) {
	now := time.Now()

	if cb.state == StateHalfOpen {
		if success {
			cb.halfOpenSuccesses++
			if cb.halfOpenSuccesses >= cb.cfg.SuccessThreshold {
				cb.transitionTo(StateClosed)
			}
		} else {
			cb.transitionTo(StateOpen)
		}
		return
	}

	// Track sliding window records for CLOSED state
	cb.records = append(cb.records, requestRecord{timestamp: now, success: success})
	cb.pruneOldRecords(now)

	if len(cb.records) >= cb.cfg.MinRequests {
		failures := 0
		for _, r := range cb.records {
			if !r.success {
				failures++
			}
		}
		rate := float64(failures) / float64(len(cb.records))
		if rate >= cb.cfg.FailureThreshold {
			cb.transitionTo(StateOpen)
		}
	}
}

func (cb *CircuitBreaker) pruneOldRecords(now time.Time) {
	cutoff := now.Add(-cb.cfg.FailureWindow)
	wIdx := 0
	for i, r := range cb.records {
		if r.timestamp.After(cutoff) {
			wIdx = i
			break
		}
	}
	if wIdx > 0 {
		cb.records = cb.records[wIdx:]
	}
}

func (cb *CircuitBreaker) transitionTo(s State) {
	oldState := cb.state
	cb.state = s

	if s == StateOpen {
		cb.openedAt = time.Now()
		cb.records = nil // Reset sliding window data
		if cb.metrics != nil {
			cb.metrics.Inc(metrics.MetricCircuitBreakerTrips)
		}
		if cb.bus != nil {
			cb.bus.Publish(events.Event{
				Type:      events.CircuitOpened,
				Timestamp: time.Now(),
				Policy:    cb.name,
				Detail:    fmt.Sprintf("Circuit breaker %q tripped. Transitioned from %s to OPEN", cb.name, oldState),
			})
		}
	} else if s == StateClosed {
		cb.halfOpenSuccesses = 0
		cb.records = nil
		if cb.bus != nil {
			cb.bus.Publish(events.Event{
				Type:      events.CircuitClosed,
				Timestamp: time.Now(),
				Policy:    cb.name,
				Detail:    fmt.Sprintf("Circuit breaker %q recovered. Transitioned to CLOSED", cb.name),
			})
		}
	} else if s == StateHalfOpen {
		cb.halfOpenSuccesses = 0
		if cb.bus != nil {
			cb.bus.Publish(events.Event{
				Type:      events.CircuitHalfOpened,
				Timestamp: time.Now(),
				Policy:    cb.name,
				Detail:    fmt.Sprintf("Circuit breaker %q test-ready. Transitioned to HALF-OPEN", cb.name),
			})
		}
	}

	cb.updateMetrics()
}

func (cb *CircuitBreaker) updateMetrics() {
	if cb.metrics == nil {
		return
	}
	var val float64
	switch cb.state {
	case StateClosed:
		val = 0
	case StateHalfOpen:
		val = 1
	case StateOpen:
		val = 2
	}
	cb.metrics.Set(metrics.MetricCircuitState+"."+cb.name, val)
}
