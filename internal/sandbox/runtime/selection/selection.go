package selection

import (
	"fmt"

	"cpip/internal/sandbox/runtime/registry"
)

// SelectionPolicy defines mapping rules for selecting runtimes.
type SelectionPolicy struct {
	DefaultRuntime string
	Rules          map[string]string // e.g. "HighSecurity" -> "gvisor"
}

// SelectionEngine evaluates selection policies.
type SelectionEngine struct {
	reg    *registry.RuntimeRegistry
	policy SelectionPolicy
}

// NewSelectionEngine instantiates a SelectionEngine.
func NewSelectionEngine(reg *registry.RuntimeRegistry, policy SelectionPolicy) *SelectionEngine {
	if policy.Rules == nil {
		policy.Rules = make(map[string]string)
	}
	return &SelectionEngine{
		reg:    reg,
		policy: policy,
	}
}

// Select determines the optimal runtime based on security profile or requirement tags.
func (e *SelectionEngine) Select(secProfile string) (string, error) {
	// Match rule
	if rtID, ok := e.policy.Rules[secProfile]; ok {
		// Verify registered
		if _, err := e.reg.Get(rtID); err == nil {
			return rtID, nil
		}
	}

	// HighSecurity fallback rule
	if secProfile == "HighSecurity" {
		if _, err := e.reg.Get("gvisor"); err == nil {
			return "gvisor", nil
		}
	}

	// Fallback to default
	if e.policy.DefaultRuntime != "" {
		if _, err := e.reg.Get(e.policy.DefaultRuntime); err == nil {
			return e.policy.DefaultRuntime, nil
		}
	}

	def, err := e.reg.GetDefault()
	if err != nil {
		return "", fmt.Errorf("failed to select runtime: %w", err)
	}

	return def.RuntimeID, nil
}
