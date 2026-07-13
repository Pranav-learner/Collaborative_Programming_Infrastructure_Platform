// Package metrics defines the metrics-recording seam for the collaboration
// engine. The engine depends only on the Recorder interface; a concrete
// Prometheus/OpenTelemetry adapter is injected in production and a Noop is used
// in tests. This keeps the engine free of any metrics-vendor dependency.
package metrics

// Recorder abstracts the collection of collaboration-engine telemetry. All
// methods must be safe for concurrent use and cheap (non-blocking).
type Recorder interface {
	// --- Document lifecycle ---
	DocumentCreated()
	DocumentLoaded()
	DocumentSaved()
	DocumentArchived()
	DocumentDestroyed()
	DocumentRecovered()
	ActiveDocuments(count int)

	// --- Updates ---
	UpdateApplied(size int)
	UpdateGenerated(size int)
	UpdateRejected(reason string)

	// --- Synchronization ---
	SyncStarted()
	SyncCompleted(durationMs int64)
	SyncFailed()
	LateJoinSync()
	BatchSync(count int)

	// --- Snapshots ---
	SnapshotCreated(durationMs int64)
	SnapshotFull()
	SnapshotIncremental()
	SnapshotFailed()

	// --- Recovery ---
	RecoveryStarted()
	RecoveryCompleted(durationMs int64)
	RecoveryFailed()
	MissedUpdatesReplayed(count int)

	// --- Participants ---
	ParticipantJoined()
	ParticipantLeft()
	ParticipantSynchronized()

	// --- Event bus ---
	SyncHandshakeCompleted()
	EventPublished()
	EventDropped()
}

// Noop is a Recorder that does nothing. It is the default when no metrics
// backend is injected.
type Noop struct{}

// NewNoop constructs a Noop metrics recorder.
func NewNoop() *Noop { return &Noop{} }

func (n *Noop) DocumentCreated()               {}
func (n *Noop) DocumentLoaded()                {}
func (n *Noop) DocumentSaved()                 {}
func (n *Noop) DocumentArchived()              {}
func (n *Noop) DocumentDestroyed()             {}
func (n *Noop) DocumentRecovered()             {}
func (n *Noop) ActiveDocuments(count int)      {}
func (n *Noop) UpdateApplied(size int)         {}
func (n *Noop) UpdateGenerated(size int)       {}
func (n *Noop) UpdateRejected(reason string)   {}
func (n *Noop) SyncStarted()                   {}
func (n *Noop) SyncCompleted(durationMs int64) {}
func (n *Noop) SyncFailed()                    {}
func (n *Noop) LateJoinSync()                  {}
func (n *Noop) BatchSync(count int)            {}
func (n *Noop) SnapshotCreated(dMs int64)      {}
func (n *Noop) SnapshotFull()                  {}
func (n *Noop) SnapshotIncremental()           {}
func (n *Noop) SnapshotFailed()                {}
func (n *Noop) RecoveryStarted()               {}
func (n *Noop) RecoveryCompleted(dMs int64)    {}
func (n *Noop) RecoveryFailed()                {}
func (n *Noop) MissedUpdatesReplayed(c int)    {}
func (n *Noop) ParticipantJoined()             {}
func (n *Noop) ParticipantLeft()               {}
func (n *Noop) ParticipantSynchronized()       {}
func (n *Noop) SyncHandshakeCompleted()        {}
func (n *Noop) EventPublished()                {}
func (n *Noop) EventDropped()                  {}
