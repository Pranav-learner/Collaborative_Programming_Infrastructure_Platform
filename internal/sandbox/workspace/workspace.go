package workspace

import (
	"fmt"
	"os"
	"path/filepath"

	"cpip/internal/sandbox/config"
)

// WorkspaceManager controls the directory structures mapped into container workspaces.
type WorkspaceManager struct {
	cfg config.Config
}

// NewWorkspaceManager initializes a WorkspaceManager instance.
func NewWorkspaceManager(cfg config.Config) *WorkspaceManager {
	return &WorkspaceManager{cfg: cfg}
}

// Prepare Workspace sets up a new local workspace directory path on the host.
func (wm *WorkspaceManager) PrepareWorkspace(sandboxID string) (string, error) {
	// Create the root directory if it does not exist
	rootPath, err := filepath.Abs(wm.cfg.WorkspaceRoot)
	if err != nil {
		return "", fmt.Errorf("failed to get absolute path of workspace root: %w", err)
	}

	if err := os.MkdirAll(rootPath, 0755); err != nil {
		return "", fmt.Errorf("failed to create workspace root %s: %w", rootPath, err)
	}

	path := filepath.Join(rootPath, sandboxID)
	if err := os.MkdirAll(path, 0755); err != nil {
		return "", fmt.Errorf("failed to create sandbox workspace %s: %w", path, err)
	}

	return path, nil
}

// CleanupWorkspace removes the physical directory on the host.
func (wm *WorkspaceManager) CleanupWorkspace(path string) error {
	if path == "" {
		return nil
	}
	// Basic guard to make sure we don't accidentally remove "/", "/var", "/tmp" or similar roots
	absPath, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	absRoot, err := filepath.Abs(wm.cfg.WorkspaceRoot)
	if err != nil {
		return err
	}

	// Must be a child of workspace root
	if len(absPath) <= len(absRoot) || absPath[:len(absRoot)] != absRoot {
		return fmt.Errorf("invalid path for cleanup: %s (must reside in %s)", absPath, absRoot)
	}

	if err := os.RemoveAll(absPath); err != nil {
		return fmt.Errorf("failed to remove workspace path %s: %w", absPath, err)
	}
	return nil
}
