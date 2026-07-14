package shutdown

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"cpip/internal/reliability/events"
	"cpip/internal/reliability/logger"
)

// Hook defines a prioritized task executed during shutdown.
type Hook struct {
	Name     string
	Priority int // Lower numbers run first
	Action   func(context.Context) error
}

// Manager orchestrates connection draining, worker shutdown, and cleanup hooks.
type Manager struct {
	mu      sync.Mutex
	hooks   []Hook
	timeout time.Duration
	bus     *events.Bus
	logger  *logger.Logger
}

func NewManager(timeout time.Duration, bus *events.Bus, log *logger.Logger) *Manager {
	return &Manager{
		hooks:   make([]Hook, 0),
		timeout: timeout,
		bus:     bus,
		logger:  log,
	}
}

// Register adds a callback to the shutdown hooks list.
func (m *Manager) Register(name string, priority int, action func(context.Context) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hooks = append(m.hooks, Hook{
		Name:     name,
		Priority: priority,
		Action:   action,
	})
}

// Shutdown triggers the shutdown sequence, executing registered hooks sequentially sorted by priority.
func (m *Manager) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	// Sort hooks by priority (ascending)
	sort.Slice(m.hooks, func(i, j int) bool {
		return m.hooks[i].Priority < m.hooks[j].Priority
	})
	hooksToRun := append([]Hook(nil), m.hooks...)
	m.mu.Unlock()

	if m.bus != nil {
		m.bus.Publish(events.Event{
			Type:      events.ShutdownStarted,
			Timestamp: time.Now(),
			Detail:    fmt.Sprintf("Shutdown started with %d registered hooks", len(hooksToRun)),
		})
	}

	shutdownCtx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()

	var firstErr error
	for _, hook := range hooksToRun {
		m.logger.Info("Executing shutdown hook", "name", hook.Name, "priority", hook.Priority)

		hookErrChan := make(chan error, 1)
		go func() {
			hookErrChan <- hook.Action(shutdownCtx)
		}()

		select {
		case <-shutdownCtx.Done():
			err := fmt.Errorf("shutdown hook %q timed out or context cancelled: %w", hook.Name, shutdownCtx.Err())
			m.logger.Error("Shutdown hook failed", "name", hook.Name, "error", err)
			if firstErr == nil {
				firstErr = err
			}
			return firstErr
		case err := <-hookErrChan:
			if err != nil {
				m.logger.Error("Shutdown hook returned error", "name", hook.Name, "error", err)
				if firstErr == nil {
					firstErr = err
				}
			} else {
				m.logger.Info("Shutdown hook completed successfully", "name", hook.Name)
			}
		}
	}

	if m.bus != nil {
		m.bus.Publish(events.Event{
			Type:      events.ShutdownCompleted,
			Timestamp: time.Now(),
			Detail:    "Shutdown sequence completed",
		})
	}

	return firstErr
}

// ListenToSignals traps interrupts (SIGINT, SIGTERM) and initiates shutdown.
func (m *Manager) ListenToSignals(cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		m.logger.Warn("System caught signal, initiating graceful shutdown", "signal", sig.String())
		cancel()

		ctx, shutdownCancel := context.WithTimeout(context.Background(), m.timeout+5*time.Second)
		defer shutdownCancel()

		if err := m.Shutdown(ctx); err != nil {
			m.logger.Error("Graceful shutdown finished with errors", "error", err)
			os.Exit(1)
		}
		os.Exit(0)
	}()
}
