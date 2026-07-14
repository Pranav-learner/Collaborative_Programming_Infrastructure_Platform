package policies

import (
	"fmt"
	"time"
)

type SystemBounds struct {
	MinMemoryBytes int64
	MaxMemoryBytes int64
	MinCPUShares   int64
	MaxCPUShares   int64
	MaxTimeout     time.Duration
	MaxPIDs        int64
	MaxFDs         int64
}

var DefaultBounds = SystemBounds{
	MinMemoryBytes: 4 * 1024 * 1024,        // 4MB
	MaxMemoryBytes: 16 * 1024 * 1024 * 1024, // 16GB
	MinCPUShares:   2,
	MaxCPUShares:   4096,
	MaxTimeout:     10 * time.Minute,
	MaxPIDs:        500,
	MaxFDs:         65536,
}

type PolicyValidator struct {
	bounds SystemBounds
}

func NewPolicyValidator(bounds SystemBounds) *PolicyValidator {
	return &PolicyValidator{bounds: bounds}
}

func (v *PolicyValidator) ValidateSecurityPolicy(policy SecurityPolicy) error {
	if policy.ID == "" {
		return fmt.Errorf("security policy ID cannot be empty")
	}

	netMode := policy.Profile.Network.Mode
	if netMode != "none" && netMode != "bridge" && netMode != "isolated" {
		return fmt.Errorf("invalid network mode: %s", netMode)
	}

	if policy.Profile.User.RunAsNonRoot && policy.Profile.User.UID == 0 {
		return fmt.Errorf("security policy requires RunAsNonRoot but UID is 0 (root)")
	}

	return nil
}

func (v *PolicyValidator) ValidateResourcePolicy(policy ResourcePolicy) error {
	if policy.ID == "" {
		return fmt.Errorf("resource policy ID cannot be empty")
	}

	p := policy.Profile

	if p.MemoryLimitBytes < v.bounds.MinMemoryBytes {
		return fmt.Errorf("memory limit %d is below minimum allowed (%d)", p.MemoryLimitBytes, v.bounds.MinMemoryBytes)
	}
	if p.MemoryLimitBytes > v.bounds.MaxMemoryBytes {
		return fmt.Errorf("memory limit %d exceeds maximum allowed (%d)", p.MemoryLimitBytes, v.bounds.MaxMemoryBytes)
	}

	if p.CPULimitShares < v.bounds.MinCPUShares {
		return fmt.Errorf("CPU shares %d is below minimum allowed (%d)", p.CPULimitShares, v.bounds.MinCPUShares)
	}
	if p.CPULimitShares > v.bounds.MaxCPUShares {
		return fmt.Errorf("CPU shares %d exceeds maximum allowed (%d)", p.CPULimitShares, v.bounds.MaxCPUShares)
	}

	if p.ExecutionTimeout <= 0 {
		return fmt.Errorf("execution timeout must be positive")
	}
	if p.ExecutionTimeout > v.bounds.MaxTimeout {
		return fmt.Errorf("execution timeout %v exceeds maximum allowed (%v)", p.ExecutionTimeout, v.bounds.MaxTimeout)
	}

	if p.ProcessLimit <= 0 || p.ProcessLimit > v.bounds.MaxPIDs {
		return fmt.Errorf("process limit %d is invalid or exceeds bounds (max %d)", p.ProcessLimit, v.bounds.MaxPIDs)
	}

	if p.OpenFileLimit <= 0 || p.OpenFileLimit > v.bounds.MaxFDs {
		return fmt.Errorf("open file limit %d is invalid or exceeds bounds (max %d)", p.OpenFileLimit, v.bounds.MaxFDs)
	}

	return nil
}
