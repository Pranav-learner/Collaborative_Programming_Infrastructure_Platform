package validation

import (
	"fmt"
	"regexp"
	"strings"

	"cpip/internal/deployment/services"
)

// ValidationResult accumulates validation warnings and errors.
type ValidationResult struct {
	IsValid  bool     `json:"is_valid"`
	Errors   []string `json:"errors,omitempty"`
	Warnings []string `json:"warnings,omitempty"`
}

// Validator runs semantic and structural validation checks on a service graph.
type Validator struct{}

// NewValidator creates a Validator instance.
func NewValidator() *Validator {
	return &Validator{}
}

// Validate checks resource limits, port conflicts, volume configurations, and dependency graphs.
func (v *Validator) Validate(servicesList []services.Service) (ValidationResult, error) {
	res := ValidationResult{IsValid: true}

	if len(servicesList) == 0 {
		res.IsValid = false
		res.Errors = append(res.Errors, "deployment must contain at least one service")
		return res, nil
	}

	serviceMap := make(map[string]services.Service)
	portMap := make(map[int]string) // port -> serviceName
	
	// Precompile resource checkers
	resPattern := regexp.MustCompile(`^[0-9]+(\.[0-9]+)?(m|Gi|Mi|Ki|G|M|K)?$`)

	for _, s := range servicesList {
		if strings.TrimSpace(s.Name) == "" {
			res.Errors = append(res.Errors, "service name cannot be empty")
			continue
		}
		if _, exists := serviceMap[s.Name]; exists {
			res.Errors = append(res.Errors, fmt.Sprintf("duplicate service name %q", s.Name))
		}
		serviceMap[s.Name] = s

		// Validate Replicas
		if s.Replicas < 0 {
			res.Errors = append(res.Errors, fmt.Sprintf("service %q has negative replica count: %d", s.Name, s.Replicas))
		}

		// Validate Ports
		for _, port := range s.Ports {
			if port.ContainerPort <= 0 || port.ContainerPort > 65535 {
				res.Errors = append(res.Errors, fmt.Sprintf("service %q has invalid container port: %d", s.Name, port.ContainerPort))
			}
			if port.ServicePort <= 0 || port.ServicePort > 65535 {
				res.Errors = append(res.Errors, fmt.Sprintf("service %q has invalid service port: %d", s.Name, port.ServicePort))
			}
			if conflictSvc, exists := portMap[port.ServicePort]; exists {
				res.Errors = append(res.Errors, fmt.Sprintf("port conflict: service port %d is declared in both %q and %q", port.ServicePort, conflictSvc, s.Name))
			}
			portMap[port.ServicePort] = s.Name
		}

		// Validate CPU/Memory syntax
		if s.Resources.CPURequest != "" && !resPattern.MatchString(s.Resources.CPURequest) {
			res.Errors = append(res.Errors, fmt.Sprintf("service %q has malformed CPU request: %q", s.Name, s.Resources.CPURequest))
		}
		if s.Resources.CPULimit != "" && !resPattern.MatchString(s.Resources.CPULimit) {
			res.Errors = append(res.Errors, fmt.Sprintf("service %q has malformed CPU limit: %q", s.Name, s.Resources.CPULimit))
		}
		if s.Resources.MemoryRequest != "" && !resPattern.MatchString(s.Resources.MemoryRequest) {
			res.Errors = append(res.Errors, fmt.Sprintf("service %q has malformed memory request: %q", s.Name, s.Resources.MemoryRequest))
		}
		if s.Resources.MemoryLimit != "" && !resPattern.MatchString(s.Resources.MemoryLimit) {
			res.Errors = append(res.Errors, fmt.Sprintf("service %q has malformed memory limit: %q", s.Name, s.Resources.MemoryLimit))
		}

		// Validate Volumes
		for _, vol := range s.Volumes {
			if vol.MountPath == "" {
				res.Errors = append(res.Errors, fmt.Sprintf("service %q has volume %q with empty mount path", s.Name, vol.Name))
			}
			if vol.Type == services.VolumePVC && vol.Size == "" {
				res.Errors = append(res.Errors, fmt.Sprintf("service %q volume %q is of type PVC but has empty size", s.Name, vol.Name))
			}
		}
	}

	// Validate Dependencies & Circular dependencies
	if len(res.Errors) == 0 {
		for name, s := range serviceMap {
			for _, dep := range s.Dependencies {
				if _, exists := serviceMap[dep]; !exists {
					res.Errors = append(res.Errors, fmt.Sprintf("service %q depends on non-existent service %q", name, dep))
				}
			}
		}
		// Check for circular dependency loops using DFS
		visited := make(map[string]bool)
		visiting := make(map[string]bool)
		for name := range serviceMap {
			if !visited[name] {
				if hasCycle(name, serviceMap, visited, visiting) {
					res.Errors = append(res.Errors, fmt.Sprintf("circular dependency detected starting at service %q", name))
					break
				}
			}
		}
	}

	if len(res.Errors) > 0 {
		res.IsValid = false
	}
	return res, nil
}

func hasCycle(name string, m map[string]services.Service, visited, visiting map[string]bool) bool {
	visiting[name] = true
	s := m[name]
	for _, dep := range s.Dependencies {
		if visiting[dep] {
			return true
		}
		if !visited[dep] {
			if hasCycle(dep, m, visited, visiting) {
				return true
			}
		}
	}
	visiting[name] = false
	visited[name] = true
	return false
}
