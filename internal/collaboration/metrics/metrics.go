package metrics

// Recorder defines the interface for collecting collaboration engine performance and business metrics.
type Recorder interface {
	DocumentCreated()
	DocumentSaved()
	DocumentArchived()
	DocumentDestroyed()
	UpdateApplied(size int)
	SnapshotCreated(durationMs int64)
	SyncHandshakeCompleted()
	EventPublished()
	EventDropped()
}

// Noop is a metrics recorder that does nothing.
type Noop struct{}

// NewNoop constructs a Noop metrics recorder.
func NewNoop() *Noop {
	return &Noop{}
}

func (n *Noop) DocumentCreated()                 {}
func (n *Noop) DocumentSaved()                   {}
func (n *Noop) DocumentArchived()                {}
func (n *Noop) DocumentDestroyed()               {}
func (n *Noop) UpdateApplied(size int)           {}
func (n *Noop) SnapshotCreated(durationMs int64) {}
func (n *Noop) SyncHandshakeCompleted()          {}
func (n *Noop) EventPublished()                  {}
func (n *Noop) EventDropped()                    {}
