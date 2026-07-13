// Package metrics defines the metrics-recording seam for the execution
// orchestrator. The orchestrator depends only on the Recorder interface; a
// concrete Prometheus/OpenTelemetry adapter is injected in production and a Noop
// is used in tests. No metrics-vendor dependency leaks into the engine.
package metrics

// Recorder abstracts the collection of execution-orchestrator telemetry. All
// methods must be safe for concurrent use and cheap (non-blocking).
type Recorder interface {
	// --- Intake / validation ---
	ExecutionRequested()
	ExecutionValidated(durationMs int64)
	ExecutionRejected(reason string)
	ValidationFailed(validator string)

	// --- Job lifecycle ---
	JobCreated()
	JobQueued()
	JobDispatched()
	JobStarted()
	JobCompleted(durationMs int64)
	JobFailed()
	JobTimedOut()
	JobCancelled()
	JobRetried()
	JobArchived()

	// --- State machine ---
	StateTransition(from, to string)
	IllegalTransition()

	// --- Gauges ---
	ActiveJobs(count int)
	QueueDepth(count int)

	// --- Scheduler ---
	ScheduleFailed()
}

// Noop is a Recorder that does nothing. It is the default when no metrics backend
// is injected.
type Noop struct{}

// NewNoop constructs a Noop recorder.
func NewNoop() *Noop { return &Noop{} }

func (n *Noop) ExecutionRequested()            {}
func (n *Noop) ExecutionValidated(int64)       {}
func (n *Noop) ExecutionRejected(string)       {}
func (n *Noop) ValidationFailed(string)        {}
func (n *Noop) JobCreated()                    {}
func (n *Noop) JobQueued()                     {}
func (n *Noop) JobDispatched()                 {}
func (n *Noop) JobStarted()                    {}
func (n *Noop) JobCompleted(int64)             {}
func (n *Noop) JobFailed()                     {}
func (n *Noop) JobTimedOut()                   {}
func (n *Noop) JobCancelled()                  {}
func (n *Noop) JobRetried()                    {}
func (n *Noop) JobArchived()                   {}
func (n *Noop) StateTransition(string, string) {}
func (n *Noop) IllegalTransition()             {}
func (n *Noop) ActiveJobs(int)                 {}
func (n *Noop) QueueDepth(int)                 {}
func (n *Noop) ScheduleFailed()                {}
