package artifacts

// State is the lifecycle state of an artifact. The state machine guards illegal
// transitions (e.g. downloading a half-uploaded object, resurrecting a purged one).
type State string

const (
	// Pending: metadata reserved, bytes not yet uploaded.
	Pending State = "pending"
	// Uploading: the upload pipeline is streaming bytes to the backend.
	Uploading State = "uploading"
	// Available: bytes stored, integrity verified, ready to serve.
	Available State = "available"
	// Archived: moved to cold retention (still restorable).
	Archived State = "archived"
	// Expired: retention elapsed; eligible for cleanup but bytes may still exist.
	Expired State = "expired"
	// Deleting: cleanup/delete in progress.
	Deleting State = "deleting"
	// Deleted: soft-deleted; metadata retained, bytes removed.
	Deleted State = "deleted"
	// Corrupted: integrity validation failed; quarantined from serving.
	Corrupted State = "corrupted"
)

// transitions encodes the allowed state machine edges.
var transitions = map[State]map[State]bool{
	Pending:   {Uploading: true, Deleting: true, Deleted: true},
	Uploading: {Available: true, Corrupted: true, Deleting: true, Deleted: true},
	Available: {Archived: true, Expired: true, Deleting: true, Corrupted: true},
	Archived:  {Available: true, Expired: true, Deleting: true},
	Expired:   {Deleting: true, Archived: true, Available: true}, // Available = restore
	Deleting:  {Deleted: true},
	Deleted:   {Available: true},                                // Available = restore from soft-delete
	Corrupted: {Deleting: true, Deleted: true, Uploading: true}, // Uploading = re-upload/repair
}

// CanTransition reports whether from → to is a legal lifecycle transition.
func CanTransition(from, to State) bool {
	if from == to {
		return true // idempotent no-op transitions are allowed
	}
	return transitions[from][to]
}

// Terminal reports whether an artifact in this state serves no bytes.
func (s State) Terminal() bool { return s == Deleted }

// Serveable reports whether an artifact in this state may be downloaded.
func (s State) Serveable() bool { return s == Available || s == Archived }
