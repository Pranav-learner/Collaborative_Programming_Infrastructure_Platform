// Package metrics defines the observability extension point for the gateway.
//
// This module intentionally ships only an interface and a no-op implementation.
// A later observability module will provide a Prometheus-backed Recorder without
// touching any calling code: the hot-path components (connection, manager,
// gateway) depend only on the Recorder interface and are handed one via
// dependency injection.
//
// Recorder implementations MUST be safe for concurrent use and MUST be cheap and
// non-blocking — they are called on the connection I/O path.
package metrics

// Recorder receives lifecycle and traffic events from the gateway. Every method
// corresponds to a countable/measurable event that a backend (Prometheus, OTel,
// StatsD, ...) can turn into counters, gauges, and histograms.
type Recorder interface {
	// Connection lifecycle.
	ConnectionOpened()                // a connection became active
	ConnectionClosed(reason string)   // a connection terminated; reason is a low-cardinality label
	ConnectionRejected(reason string) // an inbound connection was refused before/at upgrade
	SetActiveConnections(n int)       // gauge: current live connections on this node

	// Traffic.
	MessageReceived(bytes int) // an inbound data frame was accepted
	MessageSent(bytes int)     // an outbound data frame was written
	SendDropped()              // an outbound frame could not be queued (slow consumer)

	// Heartbeat.
	PingSent()         // the server sent a ping
	PongReceived()     // the server received a pong
	HeartbeatTimeout() // a connection was reaped for missing pongs
}

// Noop is a Recorder that ignores everything. It is the default until the
// Prometheus module is wired in, and it is used throughout tests.
type Noop struct{}

// NewNoop returns a no-op Recorder.
func NewNoop() Noop { return Noop{} }

func (Noop) ConnectionOpened()         {}
func (Noop) ConnectionClosed(string)   {}
func (Noop) ConnectionRejected(string) {}
func (Noop) SetActiveConnections(int)  {}
func (Noop) MessageReceived(int)       {}
func (Noop) MessageSent(int)           {}
func (Noop) SendDropped()              {}
func (Noop) PingSent()                 {}
func (Noop) PongReceived()             {}
func (Noop) HeartbeatTimeout()         {}

// Compile-time assurance that Noop satisfies Recorder.
var _ Recorder = Noop{}
