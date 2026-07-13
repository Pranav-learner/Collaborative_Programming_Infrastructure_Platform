package types

import "errors"

// The canonical queue/worker error set. Callers compare with errors.Is.
var (
	// ErrInvalidMessage indicates a structurally invalid message could not be encoded.
	ErrInvalidMessage = errors.New("queue: invalid message")
	// ErrDeserialize indicates a stream entry could not be decoded into a message.
	ErrDeserialize = errors.New("queue: message deserialization failed")
	// ErrDuplicateMessage indicates a message ID already exists in the queue.
	ErrDuplicateMessage = errors.New("queue: duplicate message")
	// ErrMessageNotFound indicates the requested message does not exist.
	ErrMessageNotFound = errors.New("queue: message not found")
	// ErrIllegalMessageTransition indicates an illegal message state transition.
	ErrIllegalMessageTransition = errors.New("queue: illegal message state transition")

	// ErrRedisUnavailable indicates the Redis backend could not be reached.
	ErrRedisUnavailable = errors.New("queue: redis unavailable")
	// ErrAckFailed indicates an acknowledgement could not be recorded.
	ErrAckFailed = errors.New("queue: acknowledgement failed")
	// ErrClaimTimeout indicates a claim/read operation exceeded its deadline.
	ErrClaimTimeout = errors.New("queue: claim timeout")
	// ErrGroupExists is a benign signal that a consumer group already exists.
	ErrGroupExists = errors.New("queue: consumer group already exists")

	// ErrRetryOverflow indicates a message exceeded its maximum retry count.
	ErrRetryOverflow = errors.New("queue: retry overflow")
	// ErrDeadLetter indicates a message was routed to the dead-letter queue.
	ErrDeadLetter = errors.New("queue: dead letter")

	// ErrWorkerNotFound indicates the requested worker does not exist.
	ErrWorkerNotFound = errors.New("queue: worker not found")
	// ErrDuplicateWorker indicates a worker ID already exists.
	ErrDuplicateWorker = errors.New("queue: duplicate worker")
	// ErrIllegalWorkerTransition indicates an illegal worker state transition.
	ErrIllegalWorkerTransition = errors.New("queue: illegal worker state transition")
	// ErrHeartbeatTimeout indicates a worker missed its heartbeat deadline.
	ErrHeartbeatTimeout = errors.New("queue: worker heartbeat timeout")

	// ErrQueueClosed indicates the queue/pool has been shut down.
	ErrQueueClosed = errors.New("queue: closed")
	// ErrNoCapacity indicates no worker capacity is currently available (backpressure).
	ErrNoCapacity = errors.New("queue: no worker capacity")
	// ErrConfig indicates an invalid configuration value.
	ErrConfig = errors.New("queue: invalid configuration")
)
