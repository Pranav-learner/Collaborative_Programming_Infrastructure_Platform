// Package metrics defines the metrics-recording seam for the queue subsystem.
// The engine depends only on the Recorder interface; a concrete
// Prometheus/OpenTelemetry adapter is injected in production and a Noop is used
// in tests.
package metrics

// Recorder abstracts the collection of queue telemetry. All methods must be safe
// for concurrent use and cheap (non-blocking).
type Recorder interface {
	// --- Producer ---
	MessagePublished(priority string)
	PublishFailed()

	// --- Consumer ---
	MessageClaimed(count int)
	MessageAcknowledged()
	AckFailed()
	ConsumerStarted()
	ConsumerStopped()
	PendingReclaimed(count int)

	// --- Dispatcher ---
	JobDispatched()
	DispatchRejected(reason string)

	// --- Retry / DLQ ---
	RetryScheduled(attempt int)
	RetryFailed()
	MovedToDeadLetter(reason string)

	// --- Workers ---
	WorkerRegistered()
	WorkerOffline()
	WorkerRecovered()
	HeartbeatReceived()
	HeartbeatTimeout()
	JobProcessed(durationMs int64, success bool)

	// --- Gauges ---
	QueueDepth(stream string, depth int64)
	PendingDepth(depth int64)
	ActiveWorkers(count int)
	IdleWorkers(count int)
	DeadLetterDepth(depth int64)
}

// Noop is a Recorder that does nothing.
type Noop struct{}

// NewNoop constructs a Noop recorder.
func NewNoop() *Noop { return &Noop{} }

func (n *Noop) MessagePublished(string)        {}
func (n *Noop) PublishFailed()                 {}
func (n *Noop) MessageClaimed(int)             {}
func (n *Noop) MessageAcknowledged()           {}
func (n *Noop) AckFailed()                     {}
func (n *Noop) ConsumerStarted()               {}
func (n *Noop) ConsumerStopped()               {}
func (n *Noop) PendingReclaimed(int)           {}
func (n *Noop) JobDispatched()                 {}
func (n *Noop) DispatchRejected(string)        {}
func (n *Noop) RetryScheduled(int)             {}
func (n *Noop) RetryFailed()                   {}
func (n *Noop) MovedToDeadLetter(string)       {}
func (n *Noop) WorkerRegistered()              {}
func (n *Noop) WorkerOffline()                 {}
func (n *Noop) WorkerRecovered()               {}
func (n *Noop) HeartbeatReceived()             {}
func (n *Noop) HeartbeatTimeout()              {}
func (n *Noop) JobProcessed(int64, bool)       {}
func (n *Noop) QueueDepth(string, int64)       {}
func (n *Noop) PendingDepth(int64)             {}
func (n *Noop) ActiveWorkers(int)              {}
func (n *Noop) IdleWorkers(int)                {}
func (n *Noop) DeadLetterDepth(int64)          {}
