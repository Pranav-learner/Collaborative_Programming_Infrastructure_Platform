package services

import (
	"cpip/internal/deployment/probes"
	"cpip/internal/deployment/resources"
)

// ServiceType classifies services to determine network policies or deployment templates.
type ServiceType string

const (
	TypeAPI      ServiceType = "api"
	TypeGateway  ServiceType = "gateway"
	TypeWorker   ServiceType = "worker"
	TypeDatabase ServiceType = "database"
	TypeStorage  ServiceType = "storage"
	TypeMonitor  ServiceType = "monitoring"
)

// PortConfig maps container ports to external ports.
type PortConfig struct {
	Name          string `json:"name"`
	ContainerPort int    `json:"container_port"`
	ServicePort   int    `json:"service_port"`
	Protocol      string `json:"protocol"` // "TCP" or "UDP"
}

// VolumeType represents storage backends.
type VolumeType string

const (
	VolumePVC      VolumeType = "PersistentVolumeClaim"
	VolumeHostPath VolumeType = "HostPath"
	VolumeEmptyDir VolumeType = "EmptyDir"
	VolumeConfigMap VolumeType = "ConfigMap"
	VolumeSecretMap VolumeType = "Secret"
)

// VolumeConfig configures service mounts.
type VolumeConfig struct {
	Name      string     `json:"name"`
	MountPath string     `json:"mount_path"`
	SubPath   string     `json:"sub_path,omitempty"`
	ReadOnly  bool       `json:"read_only"`
	Type      VolumeType `json:"type"`
	Size      string     `json:"size,omitempty"`       // e.g. "10Gi" (for PVC)
	HostPath  string     `json:"host_path,omitempty"`  // For HostPath mount
}

// Service represents a deployable application or component in CPIP.
type Service struct {
	Name         string                    `json:"name"`
	Type         ServiceType               `json:"type"`
	Image        string                    `json:"image"`
	Version      string                    `json:"version"`
	Replicas     int                       `json:"replicas"`
	Resources    resources.ResourceConfig  `json:"resources"`
	Ports        []PortConfig              `json:"ports,omitempty"`
	Env          map[string]string         `json:"env,omitempty"`
	Secrets      map[string]string         `json:"secrets,omitempty"` // Env name -> secret key reference
	Volumes      []VolumeConfig            `json:"volumes,omitempty"`
	Health       probes.FullHealthConfig   `json:"health,omitempty"`
	Dependencies []string                  `json:"dependencies,omitempty"`
}

// GetEnvList returns sorted environment variables as Key-Value strings (useful for generators).
func (s *Service) GetEnvList() []struct{ Key, Value string } {
	res := make([]struct{ Key, Value string }, 0, len(s.Env))
	for k, v := range s.Env {
		res = append(res, struct{ Key, Value string }{Key: k, Value: v})
	}
	return res
}
