package sdk

import (
	"context"
	"io"
	"time"

	"cpip/internal/sandbox/runtime"
	"cpip/internal/sandbox/types"
)

// ExecutionAPI specifies the runtime operations necessary for executing user code in sandboxes.
type ExecutionAPI interface {
	CreateSandbox(ctx context.Context, sandboxID string, cfg runtime.ContainerConfig) (string, error)
	StartSandbox(ctx context.Context, sandboxID string) error
	StopSandbox(ctx context.Context, sandboxID string, timeout time.Duration) error
	DestroySandbox(ctx context.Context, sandboxID string) error
	PrepareWorkspace(sandboxID string) (string, error)
	CopyFiles(ctx context.Context, sandboxID string, files map[string]string) error
	CollectLogs(ctx context.Context, sandboxID string, stdout, stderr io.Writer) error
	Cleanup(sandboxID string) error
}

// AdministrativeAPI specifies control plane operations for monitoring, benchmarking, and querying runtimes.
type AdministrativeAPI interface {
	Health(ctx context.Context, sandboxID string) (string, error)
	Capabilities(runtimeID string) (runtime.RuntimeDescriptor, error)
	Statistics(ctx context.Context, sandboxID string) (types.Stats, error)
	Version(runtimeID string) (string, error)
	Benchmark(ctx context.Context, runtimeID string) (map[string]any, error)
	Metadata(runtimeID string) (map[string]string, error)
	Configuration(runtimeID string) (map[string]any, error)
}
