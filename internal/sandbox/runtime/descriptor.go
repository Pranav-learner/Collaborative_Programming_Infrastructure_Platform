package runtime

import (
	"cpip/internal/sandbox/runtime/features"
	"cpip/internal/sandbox/runtime/version"
)

// RuntimeDescriptor represents the metadata and capability description of a runtime.
type RuntimeDescriptor struct {
	RuntimeID         string
	DisplayName       string
	Vendor            string
	Version           string
	Status            version.Status
	Priority          int
	DefaultRuntime    bool
	Experimental      bool
	Deprecated        bool
	Capabilities      map[features.Feature]bool
	SupportedProfiles []string
	HealthStatus      string
	Statistics        map[string]any
	Metadata          map[string]string

	// Future extension fields
	SupportsGPU               bool
	SupportsSnapshots         bool
	SupportsRootless          bool
	SupportsMicroVM           bool
	SupportsCheckpointRestore bool
}

// HasFeature returns whether the descriptor specifies a feature as supported.
func (d *RuntimeDescriptor) HasFeature(feature features.Feature) bool {
	if d.Capabilities == nil {
		return false
	}
	return d.Capabilities[feature]
}
