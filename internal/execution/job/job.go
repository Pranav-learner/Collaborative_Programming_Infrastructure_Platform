package job

import "time"

// Priority orders jobs for scheduling; higher values are more urgent. The valid
// range is bounded by configuration (see config.Config).
type Priority int8

const (
	// PriorityLow is background/batch work.
	PriorityLow Priority = 0
	// PriorityNormal is the default interactive priority.
	PriorityNormal Priority = 1
	// PriorityHigh is latency-sensitive interactive work.
	PriorityHigh Priority = 2
	// PriorityCritical is reserved for operational/system executions.
	PriorityCritical Priority = 3
)

// String returns the lowercase name of the priority.
func (p Priority) String() string {
	switch p {
	case PriorityLow:
		return "low"
	case PriorityNormal:
		return "normal"
	case PriorityHigh:
		return "high"
	case PriorityCritical:
		return "critical"
	default:
		return "unknown"
	}
}

// ResourceProfile describes the resource envelope requested (or defaulted) for a
// job. It is advisory metadata at this stage: the sandbox/runtime modules will
// later enforce it. A zero value means "use the language default".
type ResourceProfile struct {
	// Tier is a human label for the profile (e.g. "small", "standard", "large").
	Tier string `json:"tier"`
	// MemoryBytes is the memory ceiling.
	MemoryBytes int64 `json:"memory_bytes"`
	// CPUMillicores is the CPU allowance in thousandths of a core.
	CPUMillicores int `json:"cpu_millicores"`
	// PidsLimit caps the number of processes/threads.
	PidsLimit int `json:"pids_limit"`
	// TmpfsBytes is the writable scratch space ceiling.
	TmpfsBytes int64 `json:"tmpfs_bytes"`
	// WallTimeout is the wall-clock execution deadline for this profile.
	WallTimeout time.Duration `json:"wall_timeout"`
}

// IsZero reports whether the profile is the zero value.
func (r ResourceProfile) IsZero() bool { return r == ResourceProfile{} }

// ExecutionOptions carry runtime toggles that the future runtime manager will
// honor. They are transported and validated here but not acted upon.
type ExecutionOptions struct {
	// Args are additional program arguments.
	Args []string `json:"args,omitempty"`
	// Env are additional environment variables (key=value).
	Env map[string]string `json:"env,omitempty"`
	// NetworkAccess requests egress network (default deny).
	NetworkAccess bool `json:"network_access"`
}

// Request is the raw execution submission accepted by the orchestrator, before
// a Job is created. It carries the authentication/authorization context needed
// by the validation pipeline.
type Request struct {
	// Identity/correlation. Any of these may be empty; the orchestrator assigns
	// missing IDs deterministically.
	RequestID     string
	CorrelationID string
	UserID        string
	SessionID     string
	RoomID        string

	// Payload.
	Language         string
	Source           string
	Stdin            string
	CompilerOptions  []string
	ExecutionOptions ExecutionOptions

	// Controls.
	Priority  Priority
	Timeout   time.Duration
	Resources *ResourceProfile // optional override of the language default
	Metadata  map[string]string

	// Security context supplied by the gateway/auth layer.
	Authenticated bool
	Roles         []string
}

// Job is the runtime entity tracked through the execution lifecycle. Job values
// are plain data: the registry stores the authoritative copy under its lock and
// hands out value copies, so a Job obtained from a query is a safe, immutable
// snapshot. Live cancellation lives in the execution context, not here.
type Job struct {
	ID            string `json:"id"`
	RequestID     string `json:"request_id"`
	CorrelationID string `json:"correlation_id"`
	UserID        string `json:"user_id"`
	SessionID     string `json:"session_id"`
	RoomID        string `json:"room_id"`

	Language         string           `json:"language"`
	Source           string           `json:"source"`
	Stdin            string           `json:"stdin"`
	CompilerOptions  []string         `json:"compiler_options,omitempty"`
	ExecutionOptions ExecutionOptions `json:"execution_options"`

	Priority Priority `json:"priority"`
	State    State    `json:"state"`
	Outcome  Outcome  `json:"outcome"`

	RetryCount int `json:"retry_count"`
	MaxRetries int `json:"max_retries"`

	CreatedAt   time.Time `json:"created_at"`
	ScheduledAt time.Time `json:"scheduled_at"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`

	Timeout   time.Duration   `json:"timeout"`
	Resources ResourceProfile `json:"resources"`

	// CancelRequested records that a cancellation was requested for this job; the
	// live cancellation signal is delivered via the execution context.
	CancelRequested bool `json:"cancel_requested"`

	Metadata map[string]string `json:"metadata,omitempty"`

	// Assignments populated by future modules (queue/worker/sandbox). Empty here.
	WorkerID    string `json:"worker_id,omitempty"`
	SandboxID   string `json:"sandbox_id,omitempty"`
	ContainerID string `json:"container_id,omitempty"`

	// Transitions counts lifecycle state changes, for statistics.
	Transitions int `json:"transitions"`
}

// Defaults supplies the values the orchestrator injects when constructing a Job
// from a Request: identifiers, timing, and resolved resource/timeout defaults.
type Defaults struct {
	ID            string
	RequestID     string
	CorrelationID string
	Now           time.Time
	Timeout       time.Duration
	MaxRetries    int
	Resources     ResourceProfile
}

// New constructs a Job in StatePending from a validated request and the supplied
// defaults. Missing timeout/resource fields on the request fall back to the
// defaults. The returned Job is not yet registered.
func New(req Request, d Defaults) Job {
	timeout := req.Timeout
	if timeout <= 0 {
		timeout = d.Timeout
	}
	resources := d.Resources
	if req.Resources != nil && !req.Resources.IsZero() {
		resources = *req.Resources
	}
	if resources.WallTimeout <= 0 {
		resources.WallTimeout = timeout
	}

	// Defensive copies so post-construction mutation of the request cannot alter
	// the registered job.
	var meta map[string]string
	if len(req.Metadata) > 0 {
		meta = make(map[string]string, len(req.Metadata))
		for k, v := range req.Metadata {
			meta[k] = v
		}
	}
	var compiler []string
	if len(req.CompilerOptions) > 0 {
		compiler = append(compiler, req.CompilerOptions...)
	}

	return Job{
		ID:               d.ID,
		RequestID:        d.RequestID,
		CorrelationID:    d.CorrelationID,
		UserID:           req.UserID,
		SessionID:        req.SessionID,
		RoomID:           req.RoomID,
		Language:         req.Language,
		Source:           req.Source,
		Stdin:            req.Stdin,
		CompilerOptions:  compiler,
		ExecutionOptions: req.ExecutionOptions,
		Priority:         req.Priority,
		State:            StatePending,
		Outcome:          OutcomeNone,
		MaxRetries:       d.MaxRetries,
		CreatedAt:        d.Now,
		Timeout:          timeout,
		Resources:        resources,
		Metadata:         meta,
	}
}

// Clone returns a deep copy of the job, safe to hand to callers.
func (j Job) Clone() Job {
	cp := j
	if j.Metadata != nil {
		cp.Metadata = make(map[string]string, len(j.Metadata))
		for k, v := range j.Metadata {
			cp.Metadata[k] = v
		}
	}
	if j.CompilerOptions != nil {
		cp.CompilerOptions = append([]string(nil), j.CompilerOptions...)
	}
	if j.ExecutionOptions.Args != nil {
		cp.ExecutionOptions.Args = append([]string(nil), j.ExecutionOptions.Args...)
	}
	if j.ExecutionOptions.Env != nil {
		cp.ExecutionOptions.Env = make(map[string]string, len(j.ExecutionOptions.Env))
		for k, v := range j.ExecutionOptions.Env {
			cp.ExecutionOptions.Env[k] = v
		}
	}
	return cp
}

// Statistics is an immutable snapshot of a job's derived timing and counters.
type Statistics struct {
	State       State         `json:"state"`
	Outcome     Outcome       `json:"outcome"`
	RetryCount  int           `json:"retry_count"`
	Transitions int           `json:"transitions"`
	QueueWait   time.Duration `json:"queue_wait"`
	ExecTime    time.Duration `json:"exec_time"`
	TotalTime   time.Duration `json:"total_time"`
}

// Statistics derives timing statistics from the job's timestamps. Durations that
// cannot yet be computed (because the relevant timestamp is unset) are zero.
func (j Job) Statistics() Statistics {
	s := Statistics{
		State:       j.State,
		Outcome:     j.Outcome,
		RetryCount:  j.RetryCount,
		Transitions: j.Transitions,
	}
	if !j.ScheduledAt.IsZero() && !j.StartedAt.IsZero() {
		s.QueueWait = j.StartedAt.Sub(j.ScheduledAt)
	}
	if !j.StartedAt.IsZero() && !j.CompletedAt.IsZero() {
		s.ExecTime = j.CompletedAt.Sub(j.StartedAt)
	}
	if !j.CreatedAt.IsZero() && !j.CompletedAt.IsZero() {
		s.TotalTime = j.CompletedAt.Sub(j.CreatedAt)
	}
	return s
}
