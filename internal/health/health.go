// Package health exposes liveness and readiness signals for the node.
//
//   - Liveness answers "is the process running?" — a failure means restart me.
//   - Readiness answers "should I receive traffic right now?" — it aggregates
//     dependency checks and the drain flag; a failure means take me out of the
//     load-balancer rotation without restarting.
//
// This module ships the mechanism; later modules register concrete checks
// (Redis, Postgres, worker pool) via Register. Checks MUST be cheap and
// non-mutating.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Check reports the health of one dependency. A nil error means healthy.
type Check func(ctx context.Context) error

// Checker holds named readiness checks and the drain flag.
type Checker struct {
	mu       sync.RWMutex
	checks   map[string]Check
	draining atomic.Bool
	timeout  time.Duration
}

// New builds a Checker. perCheckTimeout bounds each individual check.
func New(perCheckTimeout time.Duration) *Checker {
	if perCheckTimeout <= 0 {
		perCheckTimeout = 2 * time.Second
	}
	return &Checker{
		checks:  make(map[string]Check),
		timeout: perCheckTimeout,
	}
}

// Register adds (or replaces) a readiness check under name.
func (c *Checker) Register(name string, check Check) {
	c.mu.Lock()
	c.checks[name] = check
	c.mu.Unlock()
}

// SetDraining marks the node as draining; while draining, readiness fails even
// if all dependencies are healthy, so the load balancer stops sending new
// traffic during graceful shutdown.
func (c *Checker) SetDraining(v bool) { c.draining.Store(v) }

// LivenessHandler always returns 200 while the process is up.
func (c *Checker) LivenessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	}
}

// ReadinessHandler runs all checks (each bounded by the per-check timeout) and
// returns 200 only if the node is not draining and every check passes;
// otherwise 503 with a per-check breakdown.
func (c *Checker) ReadinessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if c.draining.Load() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"status": "draining",
			})
			return
		}

		c.mu.RLock()
		checks := make(map[string]Check, len(c.checks))
		for name, ck := range c.checks {
			checks[name] = ck
		}
		c.mu.RUnlock()

		results := make(map[string]string, len(checks))
		healthy := true
		for name, ck := range checks {
			ctx, cancel := context.WithTimeout(r.Context(), c.timeout)
			err := ck(ctx)
			cancel()
			if err != nil {
				healthy = false
				results[name] = "error: " + err.Error()
			} else {
				results[name] = "ok"
			}
		}

		status := http.StatusOK
		state := "ready"
		if !healthy {
			status = http.StatusServiceUnavailable
			state = "not_ready"
		}
		writeJSON(w, status, map[string]any{"status": state, "checks": results})
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
