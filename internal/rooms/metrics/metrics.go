// Package metrics defines the observability extension point for the room
// subsystem. Like the connection-layer metrics package, it ships only an
// interface and a no-op implementation: a later observability module supplies a
// Prometheus/OTel-backed Recorder by dependency injection without touching any
// calling code.
//
// This is intentionally distinct from cpip/internal/metrics (which counts
// connection/traffic events). Rooms have their own vocabulary — lifecycle
// transitions, membership churn, recovery outcomes, event-bus health — and
// keeping the two interfaces separate avoids bloating the hot-path connection
// recorder with room concerns.
//
// Recorder implementations MUST be safe for concurrent use and MUST be cheap and
// non-blocking: they are called while room and membership locks may be held.
package metrics

// Recorder receives room lifecycle, membership, recovery, and event-bus signals.
// All labels are low-cardinality (reason/state names, never room or user IDs) so
// they map cleanly onto counters and gauges.
type Recorder interface {
	// Room lifecycle.
	RoomCreated()              // a room was created and registered
	RoomClosed(reason string)  // a room transitioned to Closed; reason is a label
	RoomDestroyed()            // a room was removed from the registry
	SetActiveRooms(n int)      // gauge: rooms currently registered on this node
	StateTransition(to string) // a room entered lifecycle state `to`

	// Membership.
	ParticipantJoined()            // a participant joined a room (first time)
	ParticipantLeft(reason string) // a participant left; reason is a label
	SetParticipants(n int)         // gauge: total participants across all rooms on this node

	// Session recovery.
	RecoveryStarted()   // a disconnected participant entered its recovery window
	RecoveryCompleted() // a participant reconnected within its window
	RecoveryExpired()   // a recovery window elapsed; the participant was evicted

	// Event bus health.
	EventPublished() // an event was published to the bus
	EventDropped()   // an event was dropped for a slow subscriber (buffer full)

	// Persistence.
	PersistenceError() // a best-effort persistence write failed
}

// Noop is a Recorder that ignores everything. It is the default until a real
// observability backend is wired in, and it is used throughout tests.
type Noop struct{}

// NewNoop returns a no-op Recorder.
func NewNoop() Noop { return Noop{} }

func (Noop) RoomCreated()           {}
func (Noop) RoomClosed(string)      {}
func (Noop) RoomDestroyed()         {}
func (Noop) SetActiveRooms(int)     {}
func (Noop) StateTransition(string) {}
func (Noop) ParticipantJoined()     {}
func (Noop) ParticipantLeft(string) {}
func (Noop) SetParticipants(int)    {}
func (Noop) RecoveryStarted()       {}
func (Noop) RecoveryCompleted()     {}
func (Noop) RecoveryExpired()       {}
func (Noop) EventPublished()        {}
func (Noop) EventDropped()          {}
func (Noop) PersistenceError()      {}

// Compile-time assurance that Noop satisfies Recorder.
var _ Recorder = Noop{}
