package artifacts

import "errors"

// The canonical storage/artifact error set. Callers match with errors.Is
// regardless of the concrete backend or subsystem.
var (
	// ErrNotFound indicates an artifact or object does not exist.
	ErrNotFound = errors.New("storage: artifact not found")
	// ErrObjectNotFound indicates the backing object is missing from storage.
	ErrObjectNotFound = errors.New("storage: object not found")
	// ErrAlreadyExists indicates an artifact ID or object key collision.
	ErrAlreadyExists = errors.New("storage: artifact already exists")

	// ErrIntegrityMismatch indicates a content-hash validation failure.
	ErrIntegrityMismatch = errors.New("storage: integrity hash mismatch")
	// ErrCorrupted indicates stored bytes failed validation.
	ErrCorrupted = errors.New("storage: object corrupted")

	// ErrUploadFailed indicates the upload pipeline could not complete.
	ErrUploadFailed = errors.New("storage: upload failed")
	// ErrDownloadFailed indicates the download pipeline could not complete.
	ErrDownloadFailed = errors.New("storage: download failed")
	// ErrCompressionFailed indicates a (de)compression error.
	ErrCompressionFailed = errors.New("storage: compression failed")

	// ErrObjectTooLarge indicates the object exceeds the configured max size.
	ErrObjectTooLarge = errors.New("storage: object exceeds maximum size")
	// ErrInvalidArtifact indicates a structurally invalid artifact/upload request.
	ErrInvalidArtifact = errors.New("storage: invalid artifact")

	// ErrIllegalTransition indicates an illegal lifecycle state transition.
	ErrIllegalTransition = errors.New("storage: illegal lifecycle transition")

	// ErrRetentionViolation indicates an operation blocked by retention/legal hold.
	ErrRetentionViolation = errors.New("storage: retention policy violation")
	// ErrLegalHold indicates deletion blocked by an active legal hold.
	ErrLegalHold = errors.New("storage: object under legal hold")

	// ErrVersionNotFound indicates a requested version does not exist.
	ErrVersionNotFound = errors.New("storage: version not found")

	// ErrUnauthorized indicates the caller may not access the artifact.
	ErrUnauthorized = errors.New("storage: unauthorized")

	// ErrBackendUnavailable indicates the storage backend could not be reached.
	ErrBackendUnavailable = errors.New("storage: backend unavailable")
	// ErrMetadataInconsistent indicates metadata and object storage disagree.
	ErrMetadataInconsistent = errors.New("storage: metadata inconsistency")

	// ErrConfig indicates an invalid configuration value.
	ErrConfig = errors.New("storage: invalid configuration")
	// ErrClosed indicates the component has been shut down.
	ErrClosed = errors.New("storage: component closed")
	// ErrNotImplemented indicates an operation the active backend does not support.
	ErrNotImplemented = errors.New("storage: operation not implemented")
)
