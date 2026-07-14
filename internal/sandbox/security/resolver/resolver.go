package resolver

import (
	"fmt"

	"cpip/internal/sandbox/security/policies"
	"cpip/internal/sandbox/security/profiles"
)

type PolicyResolver struct {
	reg policies.Registry
	val policies.Validator
}

func NewPolicyResolver(reg policies.Registry, val policies.Validator) *PolicyResolver {
	return &PolicyResolver{
		reg: reg,
		val: val,
	}
}

// ResolveSecurityPolicy resolves a security profile by ID, merges custom adjustments, and validates the output.
func (pr *PolicyResolver) ResolveSecurityPolicy(id string, custom map[string]any) (policies.SecurityPolicy, error) {
	var baseProfile profiles.SecurityProfile

	if id == "" {
		baseProfile = profiles.GetDefaultSecurityProfile(profiles.ProfileDefault)
	} else {
		regPolicy, err := pr.reg.GetLatestSecurityPolicy(id)
		if err == nil {
			baseProfile = regPolicy.Profile
		} else {
			baseProfile = profiles.GetDefaultSecurityProfile(profiles.SecurityProfileID(id))
		}
	}

	policyID := id
	if policyID == "" {
		policyID = string(baseProfile.ID)
	}
	resolved := policies.SecurityPolicy{
		ID:          policyID,
		Version:     1,
		Profile:     baseProfile,
		CustomRules: custom,
	}

	if custom != nil {
		if val, ok := custom["read_only_root"]; ok {
			if ro, isBool := val.(bool); isBool {
				resolved.Profile.Filesystem.ReadOnlyRoot = ro
			}
		}
		if val, ok := custom["network_mode"]; ok {
			if mode, isStr := val.(string); isStr {
				resolved.Profile.Network.Mode = mode
			}
		}
	}

	if err := pr.val.ValidateSecurityPolicy(resolved); err != nil {
		return policies.SecurityPolicy{}, fmt.Errorf("security policy resolution failed validation: %w", err)
	}

	return resolved, nil
}

// ResolveResourcePolicy resolves a resource profile by ID, merges custom overrides, and validates the output.
func (pr *PolicyResolver) ResolveResourcePolicy(id string, custom map[string]any) (policies.ResourcePolicy, error) {
	var baseProfile profiles.ResourceProfile

	if id == "" {
		baseProfile = profiles.GetDefaultResourceProfile(profiles.ProfileSmall)
	} else {
		regPolicy, err := pr.reg.GetLatestResourcePolicy(id)
		if err == nil {
			baseProfile = regPolicy.Profile
		} else {
			baseProfile = profiles.GetDefaultResourceProfile(profiles.ResourceProfileID(id))
		}
	}

	policyID := id
	if policyID == "" {
		policyID = string(baseProfile.ID)
	}
	resolved := policies.ResourcePolicy{
		ID:          policyID,
		Version:     1,
		Profile:     baseProfile,
		CustomRules: custom,
	}

	if custom != nil {
		if val, ok := custom["memory_limit_bytes"]; ok {
			if mem, isInt := val.(int64); isInt {
				resolved.Profile.MemoryLimitBytes = mem
			} else if memF, isFloat := val.(float64); isFloat {
				resolved.Profile.MemoryLimitBytes = int64(memF)
			}
		}
		if val, ok := custom["cpu_limit_shares"]; ok {
			if shares, isInt := val.(int64); isInt {
				resolved.Profile.CPULimitShares = shares
			} else if sharesF, isFloat := val.(float64); isFloat {
				resolved.Profile.CPULimitShares = int64(sharesF)
			}
		}
		if val, ok := custom["process_limit"]; ok {
			if limit, isInt := val.(int64); isInt {
				resolved.Profile.ProcessLimit = limit
			} else if limitF, isFloat := val.(float64); isFloat {
				resolved.Profile.ProcessLimit = int64(limitF)
			}
		}
	}

	if err := pr.val.ValidateResourcePolicy(resolved); err != nil {
		return policies.ResourcePolicy{}, fmt.Errorf("resource policy resolution failed validation: %w", err)
	}

	return resolved, nil
}
