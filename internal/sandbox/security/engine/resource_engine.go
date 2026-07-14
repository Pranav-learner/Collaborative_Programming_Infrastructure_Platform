package engine

import (
	"cpip/internal/sandbox/runtime"
	"cpip/internal/sandbox/security/policies"
)

type ResourcePolicyEngine struct{}

func NewResourcePolicyEngine() *ResourcePolicyEngine {
	return &ResourcePolicyEngine{}
}

func (e *ResourcePolicyEngine) CreateResourceLimits(policy policies.ResourcePolicy) runtime.ResourceLimits {
	p := policy.Profile
	return runtime.ResourceLimits{
		CPUShares:        p.CPULimitShares,
		CPUQuotaUs:       p.CPUQuotaMicroseconds,
		MemoryBytes:      p.MemoryLimitBytes,
		SwapBytes:        p.SwapLimitBytes,
		ProcessLimit:     p.ProcessLimit,
		OpenFileLimit:    p.OpenFileLimit,
		TempStorageBytes: p.TempStorageLimitBytes,
	}
}
