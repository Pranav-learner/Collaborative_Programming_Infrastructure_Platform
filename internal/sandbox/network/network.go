package network

import (
	"context"

	"cpip/internal/sandbox/config"
	"cpip/internal/sandbox/metrics"
)

// NetworkManager prepares networks for running containers.
type NetworkManager struct {
	cfg      config.Config
	recorder metrics.Recorder
}

// NewNetworkManager initializes a NetworkManager instance.
func NewNetworkManager(cfg config.Config, rec metrics.Recorder) *NetworkManager {
	return &NetworkManager{
		cfg:      cfg,
		recorder: rec,
	}
}

// GetNetworkName returns the network name to assign to the sandbox container.
func (nm *NetworkManager) GetNetworkName(ctx context.Context) (string, error) {
	// For now, we return the configured network name.
	// In the future, we can dynamically build Docker custom networks.
	nm.recorder.RecordNetworkCreate()
	return nm.cfg.NetworkName, nil
}
