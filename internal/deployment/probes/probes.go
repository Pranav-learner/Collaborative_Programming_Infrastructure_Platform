package probes

import "time"

// ActionType represents the diagnostic check mechanism.
type ActionType string

const (
	ActionHTTP ActionType = "HTTP"
	ActionTCP  ActionType = "TCP"
	ActionExec ActionType = "Exec"
)

// ProbeAction defines the method to probe.
type ProbeAction struct {
	Type    ActionType `json:"type"`
	Path    string     `json:"path,omitempty"` // For HTTP
	Port    int        `json:"port"`           // For HTTP/TCP
	Command []string   `json:"command,omitempty"` // For Exec
}

// ProbeConfig configures startup, liveness, and readiness diagnostics.
type ProbeConfig struct {
	Action              ProbeAction   `json:"action"`
	InitialDelaySeconds int           `json:"initial_delay_seconds"`
	PeriodSeconds       int           `json:"period_seconds"`
	TimeoutSeconds      int           `json:"timeout_seconds"`
	SuccessThreshold    int           `json:"success_threshold"`
	FailureThreshold    int           `json:"failure_threshold"`
	GracePeriodSeconds  time.Duration `json:"grace_period_seconds"`
}

// FullHealthConfig specifies startup, readiness, and liveness configurations.
type FullHealthConfig struct {
	Startup   *ProbeConfig `json:"startup,omitempty"`
	Readiness *ProbeConfig `json:"readiness,omitempty"`
	Liveness  *ProbeConfig `json:"liveness,omitempty"`
}

// DefaultHTTPProbe returns a standard HTTP probe configuration.
func DefaultHTTPProbe(path string, port int) *ProbeConfig {
	return &ProbeConfig{
		Action: ProbeAction{
			Type: ActionHTTP,
			Path: path,
			Port: port,
		},
		InitialDelaySeconds: 5,
		PeriodSeconds:       10,
		TimeoutSeconds:      2,
		SuccessThreshold:    1,
		FailureThreshold:    3,
	}
}

// DefaultTCPProbe returns a standard TCP port check configuration.
func DefaultTCPProbe(port int) *ProbeConfig {
	return &ProbeConfig{
		Action: ProbeAction{
			Type: ActionTCP,
			Port: port,
		},
		InitialDelaySeconds: 2,
		PeriodSeconds:       10,
		TimeoutSeconds:      2,
		SuccessThreshold:    1,
		FailureThreshold:    3,
	}
}
