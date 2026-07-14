package cleanup

import (
	"context"
	"time"

	"cpip/internal/sandbox/config"
	"cpip/internal/sandbox/registry"
)

// CleanupManager runs a background worker loop detecting and destroying expired sandboxes.
type CleanupManager struct {
	cfg        config.Config
	reg        *registry.SandboxRegistry
	teardownFn func(ctx context.Context, sandboxID string) error
	stopChan   chan struct{}
}

// NewCleanupManager initializes a CleanupManager instance.
func NewCleanupManager(cfg config.Config, reg *registry.SandboxRegistry) *CleanupManager {
	return &CleanupManager{
		cfg:      cfg,
		reg:      reg,
		stopChan: make(chan struct{}),
	}
}

// RegisterTeardown sets the callback to invoke when a sandbox expires.
func (cm *CleanupManager) RegisterTeardown(fn func(ctx context.Context, sandboxID string) error) {
	cm.teardownFn = fn
}

// Start spawns a background ticker loop.
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

// Stop halts the background ticker.
func (cm *CleanupManager) Stop() {
	close(cm.stopChan)
}

// Sweep checks the registry for expired sessions and tears them down.
func (cm *CleanupManager) Sweep(ctx context.Context) {
	now := time.Now()
	for _, sess := range cm.reg.List() {
		if now.After(sess.GetExpiresAt()) && cm.teardownFn != nil {
			// Trigger asynchronous teardown to avoid blocking the loop
			go func(id string) {
				_ = cm.teardownFn(ctx, id)
			}(sess.ID)
		}
	}
}
