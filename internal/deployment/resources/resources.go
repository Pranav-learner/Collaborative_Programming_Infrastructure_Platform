package resources

// ResourceConfig specifies resource limits and requests for a container.
type ResourceConfig struct {
	CPURequest      string `json:"cpu_request"`      // e.g., "100m", "0.5"
	CPULimit        string `json:"cpu_limit"`        // e.g., "200m", "1"
	MemoryRequest   string `json:"memory_request"`   // e.g., "128Mi", "1Gi"
	MemoryLimit     string `json:"memory_limit"`     // e.g., "256Mi", "2Gi"
	Storage         string `json:"storage"`          // e.g., "10Gi" (for persistent volumes)
	EphemeralStorage string `json:"ephemeral_storage"` // e.g., "2Gi" (for emptyDir or local scratch space)
	GPUCount        int    `json:"gpu_count"`        // Future GPU support count
	GPUType         string `json:"gpu_type"`         // Future GPU model/type, e.g. "nvidia-t4"
}

// DefaultResourceConfig returns sensible defaults for standard microservices.
func DefaultResourceConfig() ResourceConfig {
	return ResourceConfig{
		CPURequest:      "100m",
		CPULimit:        "500m",
		MemoryRequest:   "128Mi",
		MemoryLimit:     "512Mi",
		EphemeralStorage: "1Gi",
	}
}
