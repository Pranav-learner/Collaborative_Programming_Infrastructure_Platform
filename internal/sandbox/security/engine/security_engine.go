package engine

import (
	"strings"

	"cpip/internal/sandbox/runtime"
	"cpip/internal/sandbox/security/policies"
)

type SecurityPolicyEngine struct{}

func NewSecurityPolicyEngine() *SecurityPolicyEngine {
	return &SecurityPolicyEngine{}
}

// CreateSecuritySettings returns security settings and sanitizes environment variables.
func (e *SecurityPolicyEngine) CreateSecuritySettings(policy policies.SecurityPolicy, env []string) (runtime.SecuritySettings, []string) {
	p := policy.Profile
	settings := runtime.SecuritySettings{
		ReadOnlyRoot:      p.Filesystem.ReadOnlyRoot,
		WritableWorkspace: p.Filesystem.WritableWorkspace,
		NetworkMode:       p.Network.Mode,
		DropCapabilities:  p.Capabilities.DropCapabilities,
		AllowCapabilities: p.Capabilities.AllowCapabilities,
		RunAsNonRoot:      p.User.RunAsNonRoot,
		UID:               p.User.UID,
		GID:               p.User.GID,
	}

	sanitizedEnv := e.SanitizeEnvironment(p.Environment.BlockedVariables, p.Environment.AllowedVariables, env)

	return settings, sanitizedEnv
}

// SanitizeEnvironment filters out blocked variables.
func (e *SecurityPolicyEngine) SanitizeEnvironment(blocked []string, allowed []string, env []string) []string {
	var result []string

	blockAll := false
	for _, b := range blocked {
		if b == "*" {
			blockAll = true
			break
		}
	}

	for _, item := range env {
		parts := strings.SplitN(item, "=", 2)
		if len(parts) < 1 {
			continue
		}
		key := parts[0]

		isAllowed := false
		for _, a := range allowed {
			if a == key || strings.HasPrefix(key, a) {
				isAllowed = true
				break
			}
		}

		if isAllowed {
			result = append(result, item)
			continue
		}

		if blockAll {
			continue
		}

		isBlocked := false
		for _, b := range blocked {
			if b == key || strings.HasPrefix(key, b) {
				isBlocked = true
				break
			}
		}

		if !isBlocked {
			result = append(result, item)
		}
	}

	return result
}
