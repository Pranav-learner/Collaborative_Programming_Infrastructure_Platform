package runtime

import (
	"io"
	"time"
)

// RunInput carries parameters for running an executable.
type RunInput struct {
	SessionID      string            `json:"session_id"`
	ExecutablePath string            `json:"executable_path"`
	Source         string            `json:"source"`
	Stdin          string            `json:"stdin,omitempty"`
	Args           []string          `json:"args,omitempty"`
	Env            map[string]string `json:"env,omitempty"`
	Stdout         io.Writer         `json:"-"`
	Stderr         io.Writer         `json:"-"`
	ShutdownGrace  time.Duration     `json:"shutdown_grace"`
	OnStart        func(pid int)     `json:"-"`
}

// RunResult describes the execution outcome.
type RunResult struct {
	ExitCode int           `json:"exit_code"`
	Duration time.Duration `json:"duration"`
	PID      int           `json:"pid"`
	Error    string        `json:"error,omitempty"`
}
