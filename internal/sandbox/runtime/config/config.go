package config

// RuntimeConfig wraps runtime definitions for composition roots.
type RuntimeConfig struct {
	DefaultRuntime       string            `json:"default_runtime"`
	PreferredRuntime     string            `json:"preferred_runtime"`
	SecurityRuntimeRules map[string]string `json:"security_runtime_rules"`
	BenchmarkIterations  int               `json:"benchmark_iterations"`
	MigrationDryRun      bool              `json:"migration_dry_run"`
}

// DefaultRuntimeConfig returns a standard initialization block.
func DefaultRuntimeConfig() RuntimeConfig {
	return RuntimeConfig{
		DefaultRuntime:   "docker",
		PreferredRuntime: "docker",
		SecurityRuntimeRules: map[string]string{
			"HighSecurity": "gvisor",
		},
		BenchmarkIterations: 5,
		MigrationDryRun:     false,
	}
}
