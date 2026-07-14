package negotiation

import (
	"fmt"

	"cpip/internal/sandbox/runtime/features"
	"cpip/internal/sandbox/runtime/registry"
)

// ExecutionRequirements defines the capabilities requested for a specific task.
type ExecutionRequirements struct {
	RequiredFeatures []features.Feature
	Language         string
	SecurityLevel    string // "HighSecurity", "Default", etc.
	ResourceProfile  string
}

// NegotiationReport details which runtimes were tested, compatible, or rejected.
type NegotiationReport struct {
	CompatibleRuntimes []string
	SelectedRuntime    string
	RejectionReasons   map[string]string
}

// NegotiationManager handles capability matching against registered runtimes.
type NegotiationManager struct {
	reg *registry.RuntimeRegistry
}

// NewNegotiationManager creates a new NegotiationManager.
func NewNegotiationManager(reg *registry.RuntimeRegistry) *NegotiationManager {
	return &NegotiationManager{reg: reg}
}

// Negotiate matches requirements against registry runtimes to select the optimal runtime.
func (n *NegotiationManager) Negotiate(req ExecutionRequirements) (*NegotiationReport, error) {
	runtimes := n.reg.List()
	report := &NegotiationReport{
		CompatibleRuntimes: make([]string, 0),
		RejectionReasons:   make(map[string]string),
	}

	for _, rt := range runtimes {
		rejected := false
		for _, feat := range req.RequiredFeatures {
			if !rt.HasFeature(feat) {
				report.RejectionReasons[rt.RuntimeID] = fmt.Sprintf("Missing required feature capability: %s", feat)
				rejected = true
				break
			}
		}

		if !rejected {
			// Check security mapping rules: HighSecurity requires gvisor
			if req.SecurityLevel == "HighSecurity" && rt.RuntimeID != "gvisor" {
				report.RejectionReasons[rt.RuntimeID] = "Security level 'HighSecurity' requires gvisor isolation."
				rejected = true
			}
		}

		if !rejected {
			report.CompatibleRuntimes = append(report.CompatibleRuntimes, rt.RuntimeID)
		}
	}

	if len(report.CompatibleRuntimes) == 0 {
		return report, fmt.Errorf("no compatible runtimes found for execution requirements")
	}

	// Select optimal runtime by priority, falling back to default or first compatible.
	var bestID string
	maxPriority := -1

	for _, rtID := range report.CompatibleRuntimes {
		rt, _ := n.reg.Get(rtID)
		if rt.Priority > maxPriority {
			maxPriority = rt.Priority
			bestID = rtID
		}
	}

	report.SelectedRuntime = bestID
	return report, nil
}
