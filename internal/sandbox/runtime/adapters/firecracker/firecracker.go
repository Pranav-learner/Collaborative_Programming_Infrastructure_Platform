package firecracker

import (
	"context"
	"errors"
	"io"
	"time"

	"cpip/internal/sandbox/runtime"
	sandboxTypes "cpip/internal/sandbox/types"
)

var ErrNotImplemented = errors.New("firecracker runtime adapter is not implemented in this stage")

type FirecrackerRuntimeAdapter struct{}

func NewFirecrackerRuntimeAdapter() *FirecrackerRuntimeAdapter {
	return &FirecrackerRuntimeAdapter{}
}

func (a *FirecrackerRuntimeAdapter) CreateContainer(ctx context.Context, cfg runtime.ContainerConfig) (string, error) {
	return "", ErrNotImplemented
}

func (a *FirecrackerRuntimeAdapter) StartContainer(ctx context.Context, containerID string) error {
	return ErrNotImplemented
}

func (a *FirecrackerRuntimeAdapter) StopContainer(ctx context.Context, containerID string, timeout time.Duration) error {
	return ErrNotImplemented
}

func (a *FirecrackerRuntimeAdapter) RemoveContainer(ctx context.Context, containerID string) error {
	return ErrNotImplemented
}

func (a *FirecrackerRuntimeAdapter) InspectContainer(ctx context.Context, containerID string) (runtime.ContainerInfo, error) {
	return runtime.ContainerInfo{}, ErrNotImplemented
}

func (a *FirecrackerRuntimeAdapter) PullImage(ctx context.Context, img string) error {
	return ErrNotImplemented
}

func (a *FirecrackerRuntimeAdapter) ImageExists(ctx context.Context, img string) (bool, error) {
	return false, ErrNotImplemented
}

func (a *FirecrackerRuntimeAdapter) GetContainerLogs(ctx context.Context, containerID string, stdout, stderr io.Writer) error {
	return ErrNotImplemented
}

func (a *FirecrackerRuntimeAdapter) GetContainerStats(ctx context.Context, containerID string) (sandboxTypes.Stats, error) {
	return sandboxTypes.Stats{}, ErrNotImplemented
}
