package compose

import (
	"context"
	"fmt"
	"strings"

	"cpip/internal/deployment/probes"
	"cpip/internal/deployment/services"
)

// Provider implements the deployment.Provider interface for Docker Compose.
type Provider struct {
	name string
}

// NewProvider constructs a Compose Provider.
func NewProvider() *Provider {
	return &Provider{name: "compose"}
}

// Name returns the provider name.
func (p *Provider) Name() string {
	return p.name
}

// Generate generates a standard Docker Compose YAML file content.
func (p *Provider) Generate(_ context.Context, profile string, svcs []services.Service) (string, error) {
	var sb strings.Builder

	sb.WriteString("version: '3.8'\n\n")

	// Global Networks
	sb.WriteString("networks:\n")
	sb.WriteString("  cpip-network:\n")
	sb.WriteString("    driver: bridge\n\n")

	// Global Volumes (only PVC types mapping to named volumes)
	hasVolumes := false
	for _, s := range svcs {
		for _, v := range s.Volumes {
			if v.Type == services.VolumePVC {
				if !hasVolumes {
					sb.WriteString("volumes:\n")
					hasVolumes = true
				}
				sb.WriteString(fmt.Sprintf("  %s:\n", v.Name))
			}
		}
	}
	if hasVolumes {
		sb.WriteString("\n")
	}

	sb.WriteString("services:\n")

	for _, s := range svcs {
		sb.WriteString(fmt.Sprintf("  %s:\n", s.Name))
		sb.WriteString(fmt.Sprintf("    image: %s:%s\n", s.Image, s.Version))
		
		if s.Replicas > 1 {
			sb.WriteString("    deploy:\n")
			sb.WriteString(fmt.Sprintf("      replicas: %d\n", s.Replicas))
		}

		// Ports
		if len(s.Ports) > 0 {
			sb.WriteString("    ports:\n")
			for _, port := range s.Ports {
				sb.WriteString(fmt.Sprintf("      - \"%d:%d\"\n", port.ServicePort, port.ContainerPort))
			}
		}

		// Environment Variables
		if len(s.Env) > 0 || len(s.Secrets) > 0 {
			sb.WriteString("    environment:\n")
			for k, v := range s.Env {
				sb.WriteString(fmt.Sprintf("      - %s=%s\n", k, v))
			}
			for k, ref := range s.Secrets {
				// Translate secrets into environment variables for Compose simplicity
				sb.WriteString(fmt.Sprintf("      - %s=%s\n", k, fmt.Sprintf("${SECRET_%s:-dummy-secret-value}", strings.ToUpper(strings.ReplaceAll(ref, ".", "_")))))
			}
		}

		// Volumes
		if len(s.Volumes) > 0 {
			sb.WriteString("    volumes:\n")
			for _, vol := range s.Volumes {
				if vol.Type == services.VolumePVC {
					sb.WriteString(fmt.Sprintf("      - %s:%s\n", vol.Name, vol.MountPath))
				} else if vol.Type == services.VolumeHostPath {
					sb.WriteString(fmt.Sprintf("      - %s:%s\n", vol.HostPath, vol.MountPath))
				} else {
					sb.WriteString(fmt.Sprintf("      - cpip-vol-%s:%s\n", vol.Name, vol.MountPath))
				}
			}
		}

		// Healthchecks (Translated from Readiness / Liveness HTTP actions)
		var check *probes.ProbeConfig
		if s.Health.Readiness != nil {
			check = s.Health.Readiness
		} else if s.Health.Liveness != nil {
			check = s.Health.Liveness
		}

		if check != nil {
			sb.WriteString("    healthcheck:\n")
			if check.Action.Type == probes.ActionHTTP {
				sb.WriteString(fmt.Sprintf("      test: [\"CMD\", \"curl\", \"-f\", \"http://localhost:%d%s\"]\n", check.Action.Port, check.Action.Path))
			} else if check.Action.Type == probes.ActionTCP {
				sb.WriteString(fmt.Sprintf("      test: [\"CMD\", \"nc\", \"-z\", \"localhost\", \"%d\"]\n", check.Action.Port))
			} else if check.Action.Type == probes.ActionExec && len(check.Action.Command) > 0 {
				sb.WriteString("      test: [\"CMD\"")
				for _, cmd := range check.Action.Command {
					sb.WriteString(fmt.Sprintf(", %q", cmd))
				}
				sb.WriteString("]\n")
			}
			sb.WriteString(fmt.Sprintf("      interval: %ds\n", check.PeriodSeconds))
			sb.WriteString(fmt.Sprintf("      timeout: %ds\n", check.TimeoutSeconds))
			sb.WriteString(fmt.Sprintf("      retries: %d\n", check.FailureThreshold))
			sb.WriteString(fmt.Sprintf("      start_period: %ds\n", check.InitialDelaySeconds))
		}

		// Dependencies
		if len(s.Dependencies) > 0 {
			sb.WriteString("    depends_on:\n")
			for _, dep := range s.Dependencies {
				sb.WriteString(fmt.Sprintf("      - %s\n", dep))
			}
		}

		// Networks
		sb.WriteString("    networks:\n")
		sb.WriteString("      - cpip-network\n\n")
	}

	return sb.String(), nil
}
