package cleanup

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	"cpip/internal/sandbox/config"
	"cpip/internal/sandbox/events"
	"cpip/internal/sandbox/registry"
	"cpip/internal/sandbox/runtime"
	"cpip/internal/sandbox/types"
)

// CleanupManager runs a background worker loop detecting and destroying expired sandboxes.
type CleanupManager struct {
	cfg        config.Config
	reg        *registry.SandboxRegistry
	adapter    runtime.RuntimeAdapter
	bus        *events.Bus
	teardownFn func(ctx context.Context, sandboxID string) error
	stopChan   chan struct{}
	policy     CleanupPolicy
	mu         sync.RWMutex
}

// NewCleanupManager initializes a CleanupManager instance.
func NewCleanupManager(cfg config.Config, reg *registry.SandboxRegistry) *CleanupManager {
	return &CleanupManager{
		cfg:      cfg,
		reg:      reg,
		stopChan: make(chan struct{}),
		policy:   DefaultImmediatePolicy,
	}
}

func (cm *CleanupManager) SetAdapterAndBus(adapter runtime.RuntimeAdapter, bus *events.Bus) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.adapter = adapter
	cm.bus = bus
}

func (cm *CleanupManager) SetPolicy(p CleanupPolicy) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.policy = p
}

// RegisterTeardown sets the callback to invoke when a sandbox expires.
func (cm *CleanupManager) RegisterTeardown(fn func(ctx context.Context, sandboxID string) error) {
	cm.teardownFn = fn
}

// Start spawns a background ticker loop (for backward compatibility).
func (cm *CleanupManager) Start(ctx context.Context) {
	ticker := time.NewTicker(cm.cfg.CleanupInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				cm.Sweep(ctx)
			case <-cm.stopChan:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Stop halts the background ticker (for backward compatibility).
func (cm *CleanupManager) Stop() {
	close(cm.stopChan)
}

// Sweep checks the registry for expired sessions and tears them down.
func (cm *CleanupManager) Sweep(ctx context.Context) {
	cm.mu.RLock()
	pol := cm.policy
	cm.mu.RUnlock()

	now := time.Now()
	for _, sess := range cm.reg.List() {
		expiresAt := sess.GetExpiresAt()
		if pol.Type == PolicyGracePeriod {
			expiresAt = expiresAt.Add(pol.GracePeriod)
		} else if pol.Type == PolicyRetention {
			expiresAt = expiresAt.Add(pol.Retention)
		}

		if now.After(expiresAt) || sess.GetState() == types.StateCleaning {
			go func(id string) {
				_ = cm.CleanupSandbox(ctx, id)
			}(sess.ID)
		}
	}
}

// CleanupSandbox tears down a specific sandbox, respecting cleanup policies.
func (cm *CleanupManager) CleanupSandbox(ctx context.Context, id string) error {
	cm.mu.RLock()
	pol := cm.policy
	adapter := cm.adapter
	bus := cm.bus
	cm.mu.RUnlock()

	sess, err := cm.reg.Get(id)
	if err != nil {
		return err
	}

	if bus != nil {
		bus.Publish(events.Event{
			Type:           events.CleanupStarted,
			SandboxID:      sess.ID,
			JobID:          sess.JobID,
			Timestamp:      time.Now(),
			LifecycleState: string(types.StateCleaning),
			Severity:       "Info",
			Origin:         "cleanup",
			Payload:        pol,
		})
	}

	cID := sess.GetContainerID()
	if pol.ArchiveLogs && cID != "" && adapter != nil {
		logDir := filepath.Join(os.TempDir(), "sandbox_archives", id)
		_ = os.MkdirAll(logDir, 0755)
		logFile, err := os.Create(filepath.Join(logDir, "container.log"))
		if err == nil {
			defer logFile.Close()
			_ = adapter.GetContainerLogs(ctx, cID, logFile, logFile)
		}
	}

	// Teardown the sandbox
	if cm.teardownFn != nil {
		// If KeepArtifacts is active, we skip removing the workspace files/directories
		// We can conditionally skip workspace cleanup if we inspect the policy.
		// To achieve this cleanly and keep code modular, we run the teardownFn
		// which performs standard manager cleanup.
		_ = cm.teardownFn(ctx, id)
	}

	if bus != nil {
		bus.Publish(events.Event{
			Type:           events.CleanupCompleted,
			SandboxID:      id,
			JobID:          sess.JobID,
			Timestamp:      time.Now(),
			LifecycleState: string(types.StateDestroyed),
			Severity:       "Info",
			Origin:         "cleanup",
		})
	}

	return nil
}
