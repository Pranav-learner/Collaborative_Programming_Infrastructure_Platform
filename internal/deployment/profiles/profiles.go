package profiles

import (
	"fmt"

	"cpip/internal/deployment/config"
	"cpip/internal/deployment/resources"
	"cpip/internal/deployment/services"
)

// OverrideRules specifies parameter overrides applied to a service within a profile.
type OverrideRules struct {
	Replicas  *int                      `json:"replicas,omitempty"`
	Resources *resources.ResourceConfig `json:"resources,omitempty"`
	Env       map[string]string         `json:"env,omitempty"`
}

// ProfileDefinition defines the target properties and rules for an environment profile.
type ProfileDefinition struct {
	Name      config.Profile           `json:"name"`
	GlobalEnv map[string]string        `json:"global_env,omitempty"`
	Overrides map[string]OverrideRules `json:"overrides,omitempty"` // serviceName -> overrides
}

// ProfileManager manages environment configurations.
type ProfileManager struct {
	profiles map[config.Profile]ProfileDefinition
}

// NewProfileManager initializes default environment profiles.
func NewProfileManager() *ProfileManager {
	pm := &ProfileManager{
		profiles: make(map[config.Profile]ProfileDefinition),
	}
	pm.registerDefaults()
	return pm
}

// RegisterProfile adds or updates a profile definition.
func (pm *ProfileManager) RegisterProfile(pd ProfileDefinition) {
	pm.profiles[pd.Name] = pd
}

// ApplyProfile transforms a slice of services by applying environment overrides.
func (pm *ProfileManager) ApplyProfile(prof config.Profile, svcs []services.Service) ([]services.Service, error) {
	def, ok := pm.profiles[prof]
	if !ok {
		return nil, fmt.Errorf("profile %q is not registered", prof)
	}

	result := make([]services.Service, len(svcs))
	for i, s := range svcs {
		// Deep copy to prevent mutating the original templates
		copied := s
		copied.Env = copyMap(s.Env)
		copied.Secrets = copyMap(s.Secrets)
		copied.Ports = append([]services.PortConfig(nil), s.Ports...)
		copied.Volumes = append([]services.VolumeConfig(nil), s.Volumes...)
		copied.Dependencies = append([]string(nil), s.Dependencies...)

		// Apply global profile env variables
		for k, v := range def.GlobalEnv {
			copied.Env[k] = v
		}

		// Apply service-specific overrides
		if rules, match := def.Overrides[s.Name]; match {
			if rules.Replicas != nil {
				copied.Replicas = *rules.Replicas
			}
			if rules.Resources != nil {
				copied.Resources = *rules.Resources
			}
			for k, v := range rules.Env {
				copied.Env[k] = v
			}
		}

		result[i] = copied
	}

	return result, nil
}

func (pm *ProfileManager) registerDefaults() {
	// 1. Local Profile
	localReplicas := 1
	pm.RegisterProfile(ProfileDefinition{
		Name: config.ProfileLocal,
		GlobalEnv: map[string]string{
			"CPIP_ENV": "local",
		},
		Overrides: map[string]OverrideRules{
			"api": {
				Replicas: &localReplicas,
				Resources: &resources.ResourceConfig{
					CPURequest:    "50m",
					CPULimit:      "200m",
					MemoryRequest: "64Mi",
					MemoryLimit:   "256Mi",
				},
			},
			"websocket-gateway": {
				Replicas: &localReplicas,
				Resources: &resources.ResourceConfig{
					CPURequest:    "50m",
					CPULimit:      "200m",
					MemoryRequest: "64Mi",
					MemoryLimit:   "256Mi",
				},
			},
		},
	})

	// 2. Development Profile
	devReplicas := 1
	pm.RegisterProfile(ProfileDefinition{
		Name: config.ProfileDevelopment,
		GlobalEnv: map[string]string{
			"CPIP_ENV": "development",
		},
		Overrides: map[string]OverrideRules{
			"api": {
				Replicas: &devReplicas,
				Resources: &resources.ResourceConfig{
					CPURequest:    "100m",
					CPULimit:      "300m",
					MemoryRequest: "128Mi",
					MemoryLimit:   "512Mi",
				},
			},
		},
	})

	// 2b. Testing Profile
	testingReplicas := 1
	pm.RegisterProfile(ProfileDefinition{
		Name: config.ProfileTesting,
		GlobalEnv: map[string]string{
			"CPIP_ENV": "testing",
		},
		Overrides: map[string]OverrideRules{
			"api": {
				Replicas: &testingReplicas,
				Resources: &resources.ResourceConfig{
					CPURequest:    "100m",
					CPULimit:      "300m",
					MemoryRequest: "128Mi",
					MemoryLimit:   "512Mi",
				},
			},
		},
	})

	// 3. Staging Profile
	stagingReplicas := 2
	pm.RegisterProfile(ProfileDefinition{
		Name: config.ProfileStaging,
		GlobalEnv: map[string]string{
			"CPIP_ENV": "staging",
		},
		Overrides: map[string]OverrideRules{
			"api": {
				Replicas: &stagingReplicas,
				Resources: &resources.ResourceConfig{
					CPURequest:    "200m",
					CPULimit:      "1000m",
					MemoryRequest: "256Mi",
					MemoryLimit:   "1Gi",
				},
			},
		},
	})

	// 4. Production Profile
	prodReplicas := 3
	pm.RegisterProfile(ProfileDefinition{
		Name: config.ProfileProduction,
		GlobalEnv: map[string]string{
			"CPIP_ENV": "production",
		},
		Overrides: map[string]OverrideRules{
			"api": {
				Replicas: &prodReplicas,
				Resources: &resources.ResourceConfig{
					CPURequest:    "500m",
					CPULimit:      "2",
					MemoryRequest: "512Mi",
					MemoryLimit:   "2Gi",
				},
			},
			"websocket-gateway": {
				Replicas: &prodReplicas,
				Resources: &resources.ResourceConfig{
					CPURequest:    "500m",
					CPULimit:      "2",
					MemoryRequest: "512Mi",
					MemoryLimit:   "2Gi",
				},
			},
		},
	})
}

func copyMap(src map[string]string) map[string]string {
	if src == nil {
		return make(map[string]string)
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
