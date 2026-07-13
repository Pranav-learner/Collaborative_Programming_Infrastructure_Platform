package job

import "errors"

// The canonical execution-orchestrator error set. Callers should compare with
// errors.Is. Validation failures wrap the specific sentinel below so that a
// caller can distinguish, e.g., an unsupported language from an oversized body.
var (
	// ErrInvalidRequest indicates a structurally invalid execution request.
	ErrInvalidRequest = errors.New("execution: invalid request")
	// ErrValidationFailed indicates the request failed the validation pipeline.
	ErrValidationFailed = errors.New("execution: validation failed")
	// ErrUnauthenticated indicates the request carried no valid authentication.
	ErrUnauthenticated = errors.New("execution: unauthenticated")
	// ErrUnauthorized indicates the principal may not perform the execution.
	ErrUnauthorized = errors.New("execution: unauthorized")
	// ErrUnsupportedLanguage indicates the requested language is unknown or disabled.
	ErrUnsupportedLanguage = errors.New("execution: unsupported language")
	// ErrCodeTooLarge indicates the source code exceeds the configured limit.
	ErrCodeTooLarge = errors.New("execution: source code exceeds maximum size")
	// ErrStdinTooLarge indicates the standard input exceeds the configured limit.
	ErrStdinTooLarge = errors.New("execution: standard input exceeds maximum size")
	// ErrInvalidTimeout indicates the requested timeout is non-positive or over the limit.
	ErrInvalidTimeout = errors.New("execution: invalid timeout")
	// ErrInvalidResourceProfile indicates a requested resource profile is out of bounds.
	ErrInvalidResourceProfile = errors.New("execution: invalid resource profile")
	// ErrInvalidPriority indicates a priority outside the configured range.
	ErrInvalidPriority = errors.New("execution: invalid priority")
	// ErrInvalidMetadata indicates request metadata violates the configured limits.
	ErrInvalidMetadata = errors.New("execution: invalid metadata")

	// ErrJobNotFound indicates the requested job does not exist.
	ErrJobNotFound = errors.New("execution: job not found")
	// ErrDuplicateJob indicates a job with the same ID already exists.
	ErrDuplicateJob = errors.New("execution: duplicate job id")
	// ErrIllegalTransition indicates an attempt to make an illegal state transition.
	ErrIllegalTransition = errors.New("execution: illegal state transition")
	// ErrCancellationConflict indicates a cancel of an already-finished job.
	ErrCancellationConflict = errors.New("execution: cancellation conflict")
	// ErrRetryConflict indicates a retry of a job that is not retryable.
	ErrRetryConflict = errors.New("execution: retry conflict")
	// ErrRetriesExhausted indicates the job has reached its maximum retry count.
	ErrRetriesExhausted = errors.New("execution: retries exhausted")
	// ErrSchedulerUnavailable indicates the scheduler could not accept the job.
	ErrSchedulerUnavailable = errors.New("execution: scheduler unavailable")
)
