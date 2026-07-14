package volumes

import (
	"fmt"
	"path/filepath"

	"cpip/internal/sandbox/config"
	"cpip/internal/sandbox/metrics"
)

// VolumeManager formats and manages bind-mount parameters mapping host workspaces to container paths.
type VolumeManager struct {
	cfg      config.Config
	recorder metrics.Recorder
}

// NewVolumeManager initializes a VolumeManager instance.
func NewVolumeManager(cfg config.Config, rec metrics.Recorder) *VolumeManager {
	return &VolumeManager{
		cfg:      cfg,
		recorder: rec,
	}
}

// GetBinds returns the formatted Docker bind-mount strings (e.g. "/host/path:/container/path").
func (vm *VolumeManager) GetBinds(workspacePath string) ([]string, error) {
	if workspacePath == "" {
		return nil, nil
	}

	absHostPath, err := filepath.Abs(workspacePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute workspace path: %w", err)
	}

	// We mount the host's sandbox workspace directory to `/workspace` inside the container.
	bind := fmt.Sprintf("%s:/workspace", absHostPath)

	vm.recorder.RecordVolumeCreate()
	return []string{bind}, nil
}
