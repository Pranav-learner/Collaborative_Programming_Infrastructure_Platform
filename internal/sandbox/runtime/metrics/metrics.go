package metrics

import (
	"sync"
	"sync/atomic"
)

// RuntimeMetrics records operational event counters for tracing.
type RuntimeMetrics struct {
	mu                         sync.RWMutex
	Registrations              int64
	CompatibilityChecksPassed  int64
	CompatibilityChecksFailed  int64
	MigrationsAttempted        int64
	MigrationsSucceeded        int64
}

var globalMetrics RuntimeMetrics

// IncrementRegistrations increments the registration count.
func IncrementRegistrations() {
	atomic.AddInt64(&globalMetrics.Registrations, 1)
}

// IncrementCompatibilityChecks increments the check count based on result.
func IncrementCompatibilityChecks(passed bool) {
	if passed {
		atomic.AddInt64(&globalMetrics.CompatibilityChecksPassed, 1)
	} else {
		atomic.AddInt64(&globalMetrics.CompatibilityChecksFailed, 1)
	}
}

// IncrementMigrations increments migration attempts and success counters.
func IncrementMigrations(success bool) {
	atomic.AddInt64(&globalMetrics.MigrationsAttempted, 1)
	if success {
		atomic.AddInt64(&globalMetrics.MigrationsSucceeded, 1)
	}
}

// GetSnapshot returns a copy of active metrics.
func GetSnapshot() RuntimeMetrics {
	return RuntimeMetrics{
		Registrations:             atomic.LoadInt64(&globalMetrics.Registrations),
		CompatibilityChecksPassed:  atomic.LoadInt64(&globalMetrics.CompatibilityChecksPassed),
		CompatibilityChecksFailed:  atomic.LoadInt64(&globalMetrics.CompatibilityChecksFailed),
		MigrationsAttempted:       atomic.LoadInt64(&globalMetrics.MigrationsAttempted),
		MigrationsSucceeded:       atomic.LoadInt64(&globalMetrics.MigrationsSucceeded),
	}
}
