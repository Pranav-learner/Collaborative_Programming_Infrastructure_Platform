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

// ResourceLimits specifies generic resource constraints.
type ResourceLimits struct {
	CPUShares        int64
	CPUQuotaUs       int64
	MemoryBytes      int64
	SwapBytes        int64
	ProcessLimit     int64
	OpenFileLimit    int64
	TempStorageBytes int64
}

// SecuritySettings specifies generic security restrictions.
type SecuritySettings struct {
	ReadOnlyRoot      bool
	WritableWorkspace bool
	NetworkMode       string
	DropCapabilities  []string
	AllowCapabilities []string
	RunAsNonRoot      bool
	UID               int
	GID               int
}

// ContainerConfig aggregates all options to create a container.
type ContainerConfig struct {
	Image     string
	Cmd       []string
	Env       []string
	Binds     []string
	Network   string
	Name      string
	Resources ResourceLimits
	Security  SecuritySettings
}

// RuntimeAdapter abstracts the container engine operations.
type RuntimeAdapter interface {
	CreateContainer(ctx context.Context, cfg ContainerConfig) (string, error)
	StartContainer(ctx context.Context, containerID string) error
	StopContainer(ctx context.Context, containerID string, timeout time.Duration) error
	RemoveContainer(ctx context.Context, containerID string) error
	InspectContainer(ctx context.Context, containerID string) (ContainerInfo, error)
	PullImage(ctx context.Context, image string) error
	ImageExists(ctx context.Context, image string) (bool, error)
	GetContainerLogs(ctx context.Context, containerID string, stdout, stderr io.Writer) error
	GetContainerStats(ctx context.Context, containerID string) (types.Stats, error)
}
