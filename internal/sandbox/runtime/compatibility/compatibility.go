package compatibility

import (
	"fmt"

	"cpip/internal/sandbox/runtime"
	"cpip/internal/sandbox/runtime/features"
)

// CompatibilityLayer performs deep compatibility audits for runtimes.
type CompatibilityLayer struct{}

// NewCompatibilityLayer instantiates a CompatibilityLayer.
func NewCompatibilityLayer() *CompatibilityLayer {
	return &CompatibilityLayer{}
}

// ValidateProfileCompatibility checks if a combination of profiles is compatible with a runtime descriptor.
func (c *CompatibilityLayer) ValidateProfileCompatibility(desc runtime.RuntimeDescriptor, lang string, secProfile string, resProfile string, image string) error {
	// Validate Language Compatibility
	// Stub/placeholder rule: gvisor does not support GPU-bound profiles or legacy compilers if flagged
	if lang == "cuda" && !desc.SupportsGPU {
		return fmt.Errorf("language 'cuda' requires GPU acceleration which runtime %s does not support", desc.RuntimeID)
	}

	// Validate Security Profile Compatibility
	if secProfile == "HostAccess" && desc.RuntimeID == "gvisor" {
		return fmt.Errorf("security profile 'HostAccess' is incompatible with sandboxed gVisor runtime %s", desc.RuntimeID)
	}

	// Validate Resource limits / ReadOnly constraints
	if secProfile == "ReadOnly" && !desc.HasFeature(features.SupportsReadOnlyRootFS) {
		return fmt.Errorf("security profile 'ReadOnly' requires feature 'SupportsReadOnlyRootFS' which runtime %s does not support", desc.RuntimeID)
	}

	// Validate Image compatibility
	if image == "" {
		return fmt.Errorf("image configuration cannot be empty")
	}

	return nil
}
