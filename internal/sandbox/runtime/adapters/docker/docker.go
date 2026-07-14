package docker

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"

	"cpip/internal/sandbox/runtime"
	sandboxTypes "cpip/internal/sandbox/types"
)

// DockerRuntimeAdapter implements runtime.RuntimeAdapter using the official Docker Go SDK.
type DockerRuntimeAdapter struct {
	cli *client.Client
}

// NewDockerRuntimeAdapter instantiates a new DockerRuntimeAdapter from the local environment.
func NewDockerRuntimeAdapter() (*DockerRuntimeAdapter, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Docker client: %w", err)
	}
	return &DockerRuntimeAdapter{cli: cli}, nil
}

// CreateContainer invokes Docker to create a container.
func (a *DockerRuntimeAdapter) CreateContainer(ctx context.Context, cfg runtime.ContainerConfig) (string, error) {
	config := &container.Config{
		Image:        cfg.Image,
		Cmd:          cfg.Cmd,
		Env:          cfg.Env,
		AttachStdout: true,
		AttachStderr: true,
		Tty:          false,
	}

	if cfg.Security.RunAsNonRoot || cfg.Security.UID > 0 {
		config.User = fmt.Sprintf("%d:%d", cfg.Security.UID, cfg.Security.GID)
	}

	var pidsLimit *int64
	if cfg.Resources.ProcessLimit > 0 {
		val := cfg.Resources.ProcessLimit
		pidsLimit = &val
	}

	hostConfig := &container.HostConfig{
		Binds: cfg.Binds,
		Resources: container.Resources{
			CPUShares: cfg.Resources.CPUShares,
			CPUQuota:  cfg.Resources.CPUQuotaUs,
			Memory:    cfg.Resources.MemoryBytes,
			PidsLimit: pidsLimit,
		},
		ReadonlyRootfs: cfg.Security.ReadOnlyRoot,
		CapDrop:        cfg.Security.DropCapabilities,
		CapAdd:         cfg.Security.AllowCapabilities,
	}

	if cfg.Resources.MemoryBytes > 0 && cfg.Resources.SwapBytes >= 0 {
		hostConfig.Resources.MemorySwap = cfg.Resources.MemoryBytes + cfg.Resources.SwapBytes
	}

	if cfg.Security.NetworkMode != "" {
		hostConfig.NetworkMode = container.NetworkMode(cfg.Security.NetworkMode)
	}

	resp, err := a.cli.ContainerCreate(ctx, config, hostConfig, nil, nil, cfg.Name)
	if err != nil {
		return "", fmt.Errorf("docker container creation failed: %w", err)
	}

	// If a custom network is configured and network mode is not none, attach it.
	if cfg.Network != "" && cfg.Security.NetworkMode != "none" && cfg.Security.NetworkMode != "isolated" {
		err = a.cli.NetworkConnect(ctx, cfg.Network, resp.ID, nil)
		if err != nil {
			// Clean up container on network attach failure
			_ = a.RemoveContainer(ctx, resp.ID)
			return "", fmt.Errorf("failed to connect container to network %s: %w", cfg.Network, err)
		}
	}

	return resp.ID, nil
}

// StartContainer launches the container.
func (a *DockerRuntimeAdapter) StartContainer(ctx context.Context, containerID string) error {
	err := a.cli.ContainerStart(ctx, containerID, container.StartOptions{})
	if err != nil {
		return fmt.Errorf("failed to start container: %w", err)
	}
	return nil
}

// StopContainer gracefully halts the container.
func (a *DockerRuntimeAdapter) StopContainer(ctx context.Context, containerID string, timeout time.Duration) error {
	seconds := int(timeout.Seconds())
	stopOptions := container.StopOptions{
		Timeout: &seconds,
	}
	err := a.cli.ContainerStop(ctx, containerID, stopOptions)
	if err != nil {
		return fmt.Errorf("failed to stop container: %w", err)
	}
	return nil
}

// RemoveContainer deletes the container.
func (a *DockerRuntimeAdapter) RemoveContainer(ctx context.Context, containerID string) error {
	err := a.cli.ContainerRemove(ctx, containerID, container.RemoveOptions{
		Force:         true,
		RemoveVolumes: true,
	})
	if err != nil {
		return fmt.Errorf("failed to remove container: %w", err)
	}
	return nil
}

// InspectContainer gets state, running status, IP address etc.
func (a *DockerRuntimeAdapter) InspectContainer(ctx context.Context, containerID string) (runtime.ContainerInfo, error) {
	resp, err := a.cli.ContainerInspect(ctx, containerID)
	if err != nil {
		return runtime.ContainerInfo{}, fmt.Errorf("failed to inspect container: %w", err)
	}

	ip := ""
	if resp.NetworkSettings != nil {
		ip = resp.NetworkSettings.IPAddress
		for _, net := range resp.NetworkSettings.Networks {
			if net.IPAddress != "" {
				ip = net.IPAddress
				break
			}
		}
	}

	return runtime.ContainerInfo{
		ID:      resp.ID,
		State:   resp.State.Status,
		IP:      ip,
		Running: resp.State.Running,
	}, nil
}

// PullImage pulls the docker image from remote registry.
func (a *DockerRuntimeAdapter) PullImage(ctx context.Context, img string) error {
	reader, err := a.cli.ImagePull(ctx, img, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("failed to pull image: %w", err)
	}
	defer reader.Close()

	// Drain output to ensure it pulls completely
	_, _ = io.Copy(io.Discard, reader)
	return nil
}

// ImageExists checks image existence locally.
func (a *DockerRuntimeAdapter) ImageExists(ctx context.Context, img string) (bool, error) {
	_, _, err := a.cli.ImageInspectWithRaw(ctx, img)
	if err != nil {
		if client.IsErrNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// GetContainerLogs returns logs.
func (a *DockerRuntimeAdapter) GetContainerLogs(ctx context.Context, containerID string, stdout, stderr io.Writer) error {
	opts := container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     false,
	}

	reader, err := a.cli.ContainerLogs(ctx, containerID, opts)
	if err != nil {
		return fmt.Errorf("failed to get container logs: %w", err)
	}
	defer reader.Close()

	_, err = stdcopy.StdCopy(stdout, stderr, reader)
	if err != nil {
		return fmt.Errorf("failed to copy container logs: %w", err)
	}
	return nil
}

// GetContainerStats gets execution metrics.
func (a *DockerRuntimeAdapter) GetContainerStats(ctx context.Context, containerID string) (sandboxTypes.Stats, error) {
	resp, err := a.cli.ContainerStats(ctx, containerID, false)
	if err != nil {
		return sandboxTypes.Stats{}, err
	}
	defer resp.Body.Close()

	// Parse stats if needed, or return generic stats to satisfy mock/unimplemented behavior.
	return sandboxTypes.Stats{
		CPUPercentage:    0.0,
		MemoryUsageBytes: 0,
	}, nil
}
