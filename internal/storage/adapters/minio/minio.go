// Package minio provides the MinIO object storage adapter — the platform's
// DEFAULT backend. MinIO speaks the S3 protocol, so this adapter is a thin
// configuration layer over the stdlib-only S3 adapter: it forces path-style
// addressing (required for MinIO and localhost deployments) and reports its name
// as "minio" for observability. Because it reuses the S3 SigV4 signer, swapping
// MinIO for AWS S3, GCS interop, or any S3-compatible store is a one-line
// provider change with no business-logic impact.
package minio

import (
	"net/http"
	"time"

	"cpip/internal/storage/adapters/s3"
	"cpip/internal/storage/sdk"
)

// Options configures the MinIO adapter.
type Options struct {
	// Endpoint is the MinIO server host[:port], e.g. "localhost:9000".
	Endpoint       string
	Region         string
	AccessKey      string
	SecretKey      string
	UseSSL         bool
	RequestTimeout time.Duration
	HTTPClient     *http.Client
}

// New constructs a MinIO-backed ObjectStore. It is a *s3.Store configured for
// MinIO; callers depend only on the sdk.ObjectStore interface.
func New(opts Options) (sdk.ObjectStore, error) {
	region := opts.Region
	if region == "" {
		region = "us-east-1" // MinIO accepts any region; keep SigV4 happy.
	}
	return s3.NewNamed(s3.Options{
		Endpoint:       opts.Endpoint,
		Region:         region,
		AccessKey:      opts.AccessKey,
		SecretKey:      opts.SecretKey,
		UseSSL:         opts.UseSSL,
		PathStyle:      true, // MinIO requires path-style addressing
		RequestTimeout: opts.RequestTimeout,
		HTTPClient:     opts.HTTPClient,
	}, "minio")
}
