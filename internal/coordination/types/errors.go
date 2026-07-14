package types

import "errors"

// The canonical coordination error set. Every subsystem wraps these sentinels so
// callers match with errors.Is regardless of the concrete backend (Redis today,
// etcd/Consul tomorrow).
var (
	// --- Backend ---
	// ErrBackendUnavailable indicates the coordination backend could not be reached.
	ErrBackendUnavailable = errors.New("coordination: backend unavailable")
	// ErrKeyNotFound indicates a backend key does not exist.
	ErrKeyNotFound = errors.New("coordination: key not found")
	// ErrClosed indicates the component has been shut down.
	ErrClosed = errors.New("coordination: component closed")
	// ErrConfig indicates an invalid configuration value.
	ErrConfig = errors.New("coordination: invalid configuration")

	// --- Cluster / registry / membership ---
	// ErrNodeNotFound indicates a node is unknown to the registry.
	ErrNodeNotFound = errors.New("coordination: node not found")
	// ErrNodeExists indicates a registration collided with a live node of the
	// same ID at an equal-or-higher incarnation (split registration).
	ErrNodeExists = errors.New("coordination: node already registered")
	// ErrInvalidNode indicates a structurally invalid node record.
	ErrInvalidNode = errors.New("coordination: invalid node")
	// ErrMembershipConflict indicates two views of the cluster disagree and could
	// not be reconciled automatically.
	ErrMembershipConflict = errors.New("coordination: membership conflict")
	// ErrNotMember indicates an operation referenced a node that is not a member.
	ErrNotMember = errors.New("coordination: not a cluster member")

	// --- Leader election ---
	// ErrNotLeader indicates a leadership operation was attempted by a non-leader.
	ErrNotLeader = errors.New("coordination: not the leader")
	// ErrLeadershipLost indicates the leader lease lapsed or was taken over.
	ErrLeadershipLost = errors.New("coordination: leadership lost")
	// ErrElectionInProgress indicates a campaign is already running.
	ErrElectionInProgress = errors.New("coordination: election already in progress")
	// ErrNoLeader indicates there is currently no elected leader.
	ErrNoLeader = errors.New("coordination: no leader elected")

	// --- Locks ---
	// ErrLockNotAcquired indicates the lock could not be taken within the deadline.
	ErrLockNotAcquired = errors.New("coordination: lock not acquired")
	// ErrLockNotHeld indicates release/renew on a lock the caller does not own.
	ErrLockNotHeld = errors.New("coordination: lock not held by caller")
	// ErrLockExpired indicates the lease elapsed before the operation completed.
	ErrLockExpired = errors.New("coordination: lock lease expired")

	// --- Heartbeat ---
	// ErrHeartbeatTimeout indicates a node missed its heartbeat deadline.
	ErrHeartbeatTimeout = errors.New("coordination: heartbeat timeout")

	// --- Replication ---
	// ErrReplicationFailed indicates a state update could not be broadcast/applied.
	ErrReplicationFailed = errors.New("coordination: replication failed")
	// ErrUnknownDomain indicates replication for an unregistered state domain.
	ErrUnknownDomain = errors.New("coordination: unknown replication domain")

	// --- Discovery ---
	// ErrNoCandidates indicates service discovery found no node matching a query.
	ErrNoCandidates = errors.New("coordination: no matching nodes")
)
