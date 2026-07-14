package version

import (
	"fmt"
	"strconv"
	"strings"
)

// Status represents the support status of a runtime version.
type Status string

const (
	StatusSupported    Status = "supported"
	StatusDeprecated   Status = "deprecated"
	StatusExperimental Status = "experimental"
	StatusUnsupported  Status = "unsupported"
)

// VersionPolicy defines constraints and rules for validating runtime versions.
type VersionPolicy struct {
	MinimumSupportedVersion string
	MaximumSupportedVersion string
	CompatibilityVersion    string
	AllowedStatuses        []Status
}

// DefaultPolicy provides a base policy constraint.
var DefaultPolicy = VersionPolicy{
	MinimumSupportedVersion: "1.0.0",
	MaximumSupportedVersion: "4.0.0",
	CompatibilityVersion:    "1.0.0",
	AllowedStatuses: []Status{
		StatusSupported,
		StatusDeprecated,
		StatusExperimental,
	},
}

// Validate checks whether a version satisfies the VersionPolicy constraints.
func (p *VersionPolicy) Validate(versionStr string, status Status) error {
	// Check status
	allowed := false
	for _, s := range p.AllowedStatuses {
		if s == status {
			allowed = true
			break
		}
	}
	if !allowed {
		return fmt.Errorf("status %q is not allowed by version policy", status)
	}

	// Simple semver compare helpers
	if err := compareSemVerGE(versionStr, p.MinimumSupportedVersion); err != nil {
		return fmt.Errorf("version %s does not meet minimum supported version %s: %w", versionStr, p.MinimumSupportedVersion, err)
	}

	if p.MaximumSupportedVersion != "" {
		if err := compareSemVerLE(versionStr, p.MaximumSupportedVersion); err != nil {
			return fmt.Errorf("version %s exceeds maximum supported version %s: %w", versionStr, p.MaximumSupportedVersion, err)
		}
	}

	return nil
}

// GetMigrationRecommendation returns recommendations when upgrading/migrating between versions.
func (p *VersionPolicy) GetMigrationRecommendation(from, to string) string {
	if from == to {
		return "No migration needed: versions are identical."
	}
	return fmt.Sprintf("Recommended: Validate image compatibility and capability mappings before migrating from %s to %s.", from, to)
}

// Helper to check version >= min
func compareSemVerGE(v1, v2 string) error {
	p1 := parseVer(v1)
	p2 := parseVer(v2)
	for i := 0; i < 3; i++ {
		if p1[i] < p2[i] {
			return fmt.Errorf("version component mismatch")
		} else if p1[i] > p2[i] {
			return nil
		}
	}
	return nil
}

// Helper to check version <= max
func compareSemVerLE(v1, v2 string) error {
	p1 := parseVer(v1)
	p2 := parseVer(v2)
	for i := 0; i < 3; i++ {
		if p1[i] > p2[i] {
			return fmt.Errorf("version component mismatch")
		} else if p1[i] < p2[i] {
			return nil
		}
	}
	return nil
}

func parseVer(v string) [3]int {
	v = strings.TrimPrefix(v, "v")
	parts := strings.Split(v, ".")
	var res [3]int
	for i := 0; i < 3 && i < len(parts); i++ {
		val, _ := strconv.Atoi(parts[i])
		res[i] = val
	}
	return res
}
