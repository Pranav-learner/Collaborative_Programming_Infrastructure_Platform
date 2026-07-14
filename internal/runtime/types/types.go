package types

import (
	"context"
	"errors"
	"time"
)

// SessionState represents the state of a single execution runtime session.
type SessionState string

const (
	StateCreated           SessionState = "created"
	StateCompiling         SessionState = "compiling"
	StateCompilationFailed SessionState = "compilation_failed"
	StateRunning           SessionState = "running"
	StateCompleted         SessionState = "completed"
	StateFailed            SessionState = "failed"
	StateTimedOut          SessionState = "timed_out"
	StateCancelled         SessionState = "cancelled"
	StateCleaningUp        SessionState = "cleaning_up"
	StateFinished          SessionState = "finished"
)

// ResourceProfile describes the resource constraints assigned to the execution.
type ResourceProfile struct {
	Tier          string
	MemoryBytes   int64
	CPUMillicores int
	PidsLimit     int
	TmpfsBytes    int64
	WallTimeout   time.Duration
}

// SessionStats holds execution statistics.
type SessionStats struct {
	CompileTime       time.Duration `json:"compile_time"`
	ExecutionTime     time.Duration `json:"execution_time"`
	TotalTime         time.Duration `json:"total_time"`
	CpuTime           time.Duration `json:"cpu_time"`
	PeakMemoryBytes   int64         `json:"peak_memory_bytes"`
	BytesStdoutStream int64         `json:"bytes_stdout_stream"`
	BytesStderrStream int64         `json:"bytes_stderr_stream"`
}

// CompilerState represents compiler metrics and outcome.
type CompilerState struct {
	Compiled      bool          `json:"compiled"`
	CompilerCmd   string        `json:"compiler_cmd"`
	Duration      time.Duration `json:"duration"`
	OutputSummary string        `json:"output_summary"`
	Error         string        `json:"error,omitempty"`
}

// RunnerState represents process runner details.
type RunnerState struct {
	PID           int           `json:"pid"`
	ExitCode      int           `json:"exit_code"`
	Duration      time.Duration `json:"duration"`
	Error         string        `json:"error,omitempty"`
}

// Session represents a single active execution runtime session.
type Session struct {
	ID            string          `json:"id"`
	JobID         string          `json:"job_id"`
	WorkerID      string          `json:"worker_id"`
	CorrelationID string          `json:"correlation_id"`
	Language      string          `json:"language"`
	State         SessionState    `json:"state"`
	Resource      ResourceProfile `json:"resource"`

	Compiler CompilerState `json:"compiler"`
	Runner   RunnerState   `json:"runner"`
	Stats    SessionStats  `json:"stats"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

	// Context and cancellation signal propagation.
	Context context.Context    `json:"-"`
	Cancel  context.CancelFunc `json:"-"`

	// Future extension markers for Stage 3 integration.
	ContainerID string `json:"container_id,omitempty"`
	SandboxID   string `json:"sandbox_id,omitempty"`
}

// Standard errors returned by the runtime subsystem.
var (
	ErrSessionNotFound     = errors.New("runtime: session not found")
	ErrSessionAlreadyExist = errors.New("runtime: session already exists")
	ErrInvalidLanguage     = errors.New("runtime: invalid or unsupported language")
	ErrCompilationFailed   = errors.New("runtime: compilation failed")
	ErrExecutionFailed     = errors.New("runtime: execution failed")
	ErrOutputLimitExceeded = errors.New("runtime: output buffer limit exceeded")
	ErrTimeout             = errors.New("runtime: execution timed out")
	ErrCancelled           = errors.New("runtime: execution cancelled")
)
