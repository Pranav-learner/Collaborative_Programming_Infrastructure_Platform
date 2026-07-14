package sdk

import (
	"context"
	"io"
	"time"

	"cpip/internal/sandbox/types"
)

// SandboxSDK is the public interface for managing isolated containers.
// The execution engine must interact with sandboxes ONLY through this interface.
type SandboxSDK interface {
	// CreateSandbox registers, prepares directory workspace, pulls image and creates the container.
	CreateSandbox(ctx context.Context, jobID, language string, expiration time.Duration) (*types.SandboxSession, error)

	// DestroySandbox stops the container, cleans up mounts/volumes, deletes the workspace and removes the container.
	DestroySandbox(ctx context.Context, sandboxID string) error

	// Start transitions the sandbox container into the running state.
	Start(ctx context.Context, sandboxID string) error

	// Stop stops the sandbox container gracefully.
	Stop(ctx context.Context, sandboxID string, timeout time.Duration) error

	// CopyFiles copies scripts, source files or inputs directly into the sandbox workspace.
	CopyFiles(ctx context.Context, sandboxID string, files map[string]string) error

	// CollectLogs streams standard stdout and stderr logs from the running sandbox container.
	CollectLogs(ctx context.Context, sandboxID string, stdout, stderr io.Writer) error

	// Inspect retrieves active sandbox session metadata and runtime statuses.
	Inspect(ctx context.Context, sandboxID string) (*types.SandboxSession, error)

	// Statistics fetches CPU and Memory utilization statistics for the container.
	Statistics(ctx context.Context, sandboxID string) (types.Stats, error)
}
