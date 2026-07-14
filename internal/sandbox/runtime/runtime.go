package runtime

import (
	"context"
	"io"
	"time"

	"cpip/internal/sandbox/types"
)

// ContainerInfo carries basic inspect information returned by a container engine.
type ContainerInfo struct {
	ID      string
	State   string
	IP      string
	Running bool
}

// RuntimeAdapter abstracts the container engine operations.
type RuntimeAdapter interface {
	CreateContainer(ctx context.Context, image string, cmd []string, env []string, binds []string, network string, name string) (string, error)
	StartContainer(ctx context.Context, containerID string) error
	StopContainer(ctx context.Context, containerID string, timeout time.Duration) error
	RemoveContainer(ctx context.Context, containerID string) error
	InspectContainer(ctx context.Context, containerID string) (ContainerInfo, error)
	PullImage(ctx context.Context, image string) error
	ImageExists(ctx context.Context, image string) (bool, error)
	GetContainerLogs(ctx context.Context, containerID string, stdout, stderr io.Writer) error
	GetContainerStats(ctx context.Context, containerID string) (types.Stats, error)
}
