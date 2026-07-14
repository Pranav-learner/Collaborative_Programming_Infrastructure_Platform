// Package sdk defines the Storage SDK: the vendor-neutral contract every object
// storage backend implements. It is THE decoupling seam of the module. Business
// logic (via the Artifact Manager) depends only on the ObjectStore interface, so
// MinIO, AWS S3, GCS, Azure Blob, or a local filesystem are interchangeable
// without touching a single line of business code.
//
// The interface is deliberately narrow and blob-oriented: it knows about
// buckets, keys, bytes, and streams — never about artifacts, versions, or
// retention (those are higher-level concerns layered on top). Keeping it minimal
// keeps every adapter honest and swappable.
package sdk

import (
	"context"
	"io"
	"time"
)

// ObjectRef uniquely identifies an object within a backend.
type ObjectRef struct {
	Bucket string
	Key    string
}

// Object is the metadata describing a stored object (the result of Stat/List).
type Object struct {
	Bucket       string
	Key          string
	Size         int64
	ETag         string // backend entity tag (often the MD5/hash of stored bytes)
	ContentType  string
	LastModified time.Time
	Metadata     map[string]string // user metadata (x-amz-meta-*)
}

// PutInput describes a single object write.
type PutInput struct {
	Bucket string
	Key    string
	// Body streams the object bytes. The adapter reads exactly Size bytes.
	Body io.Reader
	// Size is the exact byte length of Body. Must be >= 0 (this module buffers
	// and measures in the upload pipeline before calling Put). Multipart upload
	// for unknown-length streams is a future stage.
	Size int64
	// ContentType is the MIME type stored with the object.
	ContentType string
	// Metadata is user metadata persisted alongside the object.
	Metadata map[string]string
}

// PutResult reports the outcome of a write.
type PutResult struct {
	ETag      string
	Size      int64
	VersionID string // backend version id, when the bucket is versioned
}

// GetOptions customizes a read.
type GetOptions struct {
	// Range, when non-nil, requests a byte range [Start, End] (inclusive).
	Range *ByteRange
}

// ByteRange is an inclusive byte range for ranged reads.
type ByteRange struct {
	Start int64
	End   int64
}

// GetOutput is the result of a read. Body MUST be closed by the caller.
type GetOutput struct {
	Body   io.ReadCloser
	Object Object
}

// ListOptions customizes a listing.
type ListOptions struct {
	Prefix     string
	Recursive  bool
	MaxKeys    int
	StartAfter string
}

// ListResult is a page of objects.
type ListResult struct {
	Objects     []Object
	IsTruncated bool
	NextMarker  string
}

// SignedURLMethod is the HTTP method a presigned URL authorizes.
type SignedURLMethod string

const (
	SignedGet SignedURLMethod = "GET"
	SignedPut SignedURLMethod = "PUT"
)

// SignedURLOptions customizes presigned URL generation.
type SignedURLOptions struct {
	Method SignedURLMethod
	Expiry time.Duration
}

// ObjectStore is the contract every storage backend implements. All methods
// honor context cancellation and return errors wrapping the canonical
// artifacts.Err* sentinels (notably ErrObjectNotFound and ErrBackendUnavailable).
type ObjectStore interface {
	// Name identifies the backend implementation ("minio", "s3", "filesystem").
	Name() string

	// --- Bucket lifecycle ---
	EnsureBucket(ctx context.Context, bucket string) error
	BucketExists(ctx context.Context, bucket string) (bool, error)
	RemoveBucket(ctx context.Context, bucket string) error
	ListBuckets(ctx context.Context) ([]string, error)

	// --- Object operations (the Storage SDK surface) ---
	Upload(ctx context.Context, in PutInput) (PutResult, error)
	Download(ctx context.Context, ref ObjectRef, opts GetOptions) (*GetOutput, error)
	Delete(ctx context.Context, ref ObjectRef) error
	Exists(ctx context.Context, ref ObjectRef) (bool, error)
	Stat(ctx context.Context, ref ObjectRef) (Object, error)
	List(ctx context.Context, bucket string, opts ListOptions) (ListResult, error)
	Copy(ctx context.Context, src, dst ObjectRef) error
	Move(ctx context.Context, src, dst ObjectRef) error
	GenerateSignedURL(ctx context.Context, ref ObjectRef, opts SignedURLOptions) (string, error)

	// Validate checks backend reachability and credential validity (preflight /
	// health probe). Cheap and side-effect-free.
	Validate(ctx context.Context) error

	// Close releases backend resources (connection pools, file handles).
	Close() error
}

// Multipart-capable backends may additionally implement this interface. It is
// declared for forward-compatibility; no adapter implements it in this stage.
type MultipartUploader interface {
	InitiateMultipart(ctx context.Context, ref ObjectRef, contentType string) (uploadID string, err error)
	UploadPart(ctx context.Context, ref ObjectRef, uploadID string, partNumber int, body io.Reader, size int64) (etag string, err error)
	CompleteMultipart(ctx context.Context, ref ObjectRef, uploadID string, parts []CompletedPart) (PutResult, error)
	AbortMultipart(ctx context.Context, ref ObjectRef, uploadID string) error
}

// CompletedPart identifies a finished multipart part (future multipart support).
type CompletedPart struct {
	PartNumber int
	ETag       string
}
