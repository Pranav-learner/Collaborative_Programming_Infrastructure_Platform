// Package config defines the configuration surface for the object storage &
// artifact management module. Configuration is injected at construction time;
// there is no global state. Each subsystem receives only the sub-struct it needs.
package config

import (
	"time"

	"cpip/internal/storage/artifacts"
)

// Provider identifies the default object storage backend.
type Provider string

const (
	ProviderMinIO      Provider = "minio"
	ProviderS3         Provider = "s3"
	ProviderFilesystem Provider = "filesystem"
	// ProviderGCS / ProviderAzure are reserved for future adapters.
	ProviderGCS   Provider = "gcs"
	ProviderAzure Provider = "azure"
)

// Backend holds connection parameters shared by S3-compatible providers
// (MinIO, AWS S3, GCS interop) and the filesystem provider.
type Backend struct {
	// Endpoint is the host[:port] of an S3-compatible service. For AWS S3 leave
	// empty to derive from Region; for MinIO set e.g. "localhost:9000".
	Endpoint string `json:"endpoint"`
	// Region is the S3 region (e.g. "us-east-1"). MinIO accepts any value.
	Region string `json:"region"`
	// AccessKey / SecretKey authenticate SigV4 requests.
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	// UseSSL selects https vs http for the endpoint.
	UseSSL bool `json:"use_ssl"`
	// PathStyle forces path-style addressing (bucket in the path, not the host).
	// Required for MinIO and localhost; AWS uses virtual-host style.
	PathStyle bool `json:"path_style"`
	// FilesystemRoot is the root directory for the filesystem provider.
	FilesystemRoot string `json:"filesystem_root"`
	// RequestTimeout bounds a single backend HTTP request.
	RequestTimeout time.Duration `json:"request_timeout"`
	// MaxRetries is the per-operation retry budget for transient failures.
	MaxRetries int `json:"max_retries"`
	// RetryBackoff is the base backoff between retries.
	RetryBackoff time.Duration `json:"retry_backoff"`
}

// Compression configures automatic compression in the upload pipeline.
type Compression struct {
	// Enabled toggles automatic compression.
	Enabled bool `json:"enabled"`
	// Algorithm is the default algorithm (currently gzip).
	Algorithm artifacts.Algorithm `json:"algorithm"`
	// Level is the gzip level (1..9); 0 → default.
	Level int `json:"level"`
	// MinSize skips compression for objects smaller than this (overhead not worth it).
	MinSize int64 `json:"min_size"`
	// MinRatio requires at least this fractional saving to keep the compressed
	// form; otherwise the original is stored (avoids "compressed" JPEGs growing).
	MinRatio float64 `json:"min_ratio"`
}

// Retention configures default retention behavior.
type Retention struct {
	// DefaultMode is applied when an upload specifies none.
	DefaultMode artifacts.RetentionMode `json:"default_mode"`
	// DefaultTTL is used with RetainUntil when no explicit expiry is given.
	DefaultTTL time.Duration `json:"default_ttl"`
	// MaxVersions is the default lineage cap for RetainVersions.
	MaxVersions int `json:"max_versions"`
	// PerType overrides the default TTL by artifact type.
	PerType map[artifacts.Type]time.Duration `json:"per_type,omitempty"`
}

// Cleanup configures the background cleanup/reaper.
type Cleanup struct {
	// Enabled toggles the scheduled cleanup loop.
	Enabled bool `json:"enabled"`
	// Interval is how often the reaper scans for expired/orphaned artifacts.
	Interval time.Duration `json:"interval"`
	// BatchSize bounds artifacts processed per scan.
	BatchSize int `json:"batch_size"`
	// OrphanGrace delays orphan deletion so in-flight uploads aren't reaped.
	OrphanGrace time.Duration `json:"orphan_grace"`
	// DryRun logs what would be deleted without deleting (safe rollout).
	DryRun bool `json:"dry_run"`
}

// Config is the composition root of module configuration.
type Config struct {
	// Provider selects the default backend.
	Provider Provider `json:"provider"`
	Backend  Backend  `json:"backend"`

	// Buckets maps a logical bucket name to a physical bucket. Callers reference
	// logical names; the registry resolves the physical bucket + provider.
	Buckets map[string]string `json:"buckets"`
	// TypeBuckets routes each artifact type to a logical bucket.
	TypeBuckets map[artifacts.Type]string `json:"type_buckets,omitempty"`
	// DefaultBucket is used when no type-specific routing matches.
	DefaultBucket string `json:"default_bucket"`

	Compression Compression `json:"compression"`
	Retention   Retention   `json:"retention"`
	Cleanup     Cleanup     `json:"cleanup"`

	// MaxObjectSize rejects uploads larger than this (bytes). 0 → unlimited.
	MaxObjectSize int64 `json:"max_object_size"`
	// MultipartThreshold is the size above which multipart upload SHOULD be used.
	// Multipart itself is a future stage; the threshold is recorded for readiness.
	MultipartThreshold int64 `json:"multipart_threshold"`
	// SignedURLTTL is the default lifetime of a presigned URL.
	SignedURLTTL time.Duration `json:"signed_url_ttl"`
}

// Default returns a production-sensible configuration targeting a local MinIO.
func Default() Config {
	return Config{
		Provider: ProviderMinIO,
		Backend: Backend{
			Endpoint:       "localhost:9000",
			Region:         "us-east-1",
			AccessKey:      "minioadmin",
			SecretKey:      "minioadmin",
			UseSSL:         false,
			PathStyle:      true,
			FilesystemRoot: "/var/lib/cpip/artifacts",
			RequestTimeout: 30 * time.Second,
			MaxRetries:     3,
			RetryBackoff:   200 * time.Millisecond,
		},
		Buckets: map[string]string{
			"artifacts": "cpip-artifacts",
			"logs":      "cpip-logs",
			"snapshots": "cpip-snapshots",
			"archives":  "cpip-archives",
		},
		TypeBuckets: map[artifacts.Type]string{
			artifacts.ExecutionLog:          "logs",
			artifacts.RuntimeLog:            "logs",
			artifacts.ExecutionOutput:       "logs",
			artifacts.CollaborationSnapshot: "snapshots",
			artifacts.WorkspaceArchive:      "archives",
			artifacts.SourceArchive:         "archives",
		},
		DefaultBucket: "artifacts",
		Compression: Compression{
			Enabled:   true,
			Algorithm: artifacts.Gzip,
			Level:     0,
			MinSize:   1024,
			MinRatio:  0.05,
		},
		Retention: Retention{
			DefaultMode: artifacts.RetainForever,
			DefaultTTL:  720 * time.Hour, // 30 days
			MaxVersions: 10,
		},
		Cleanup: Cleanup{
			Enabled:     true,
			Interval:    15 * time.Minute,
			BatchSize:   500,
			OrphanGrace: 1 * time.Hour,
			DryRun:      false,
		},
		MaxObjectSize:      5 << 30,  // 5 GiB
		MultipartThreshold: 64 << 20, // 64 MiB
		SignedURLTTL:       15 * time.Minute,
	}
}

// Validate normalizes zero-valued fields to defaults and rejects nonsensical
// values, returning a normalized copy.
func (c Config) Validate() (Config, error) {
	d := Default()

	if c.Provider == "" {
		c.Provider = d.Provider
	}
	switch c.Provider {
	case ProviderMinIO, ProviderS3, ProviderFilesystem, ProviderGCS, ProviderAzure:
	default:
		return Config{}, wrap("unknown provider: " + string(c.Provider))
	}

	if c.Backend.Region == "" {
		c.Backend.Region = d.Backend.Region
	}
	if c.Backend.RequestTimeout <= 0 {
		c.Backend.RequestTimeout = d.Backend.RequestTimeout
	}
	if c.Backend.MaxRetries < 0 {
		c.Backend.MaxRetries = d.Backend.MaxRetries
	}
	if c.Backend.RetryBackoff <= 0 {
		c.Backend.RetryBackoff = d.Backend.RetryBackoff
	}
	if c.Provider == ProviderFilesystem && c.Backend.FilesystemRoot == "" {
		c.Backend.FilesystemRoot = d.Backend.FilesystemRoot
	}

	if len(c.Buckets) == 0 {
		c.Buckets = d.Buckets
	}
	if c.DefaultBucket == "" {
		c.DefaultBucket = d.DefaultBucket
	}
	if _, ok := c.Buckets[c.DefaultBucket]; !ok {
		return Config{}, wrap("default_bucket " + c.DefaultBucket + " is not in buckets map")
	}

	if c.Compression.Algorithm == "" {
		c.Compression.Algorithm = d.Compression.Algorithm
	}
	if c.Compression.Level < 0 || c.Compression.Level > 9 {
		return Config{}, wrap("compression.level must be in [0,9]")
	}
	if c.Compression.MinSize < 0 {
		c.Compression.MinSize = d.Compression.MinSize
	}
	if c.Compression.MinRatio < 0 || c.Compression.MinRatio > 1 {
		return Config{}, wrap("compression.min_ratio must be in [0,1]")
	}

	if c.Retention.DefaultMode == "" {
		c.Retention.DefaultMode = d.Retention.DefaultMode
	}
	if c.Retention.DefaultTTL <= 0 {
		c.Retention.DefaultTTL = d.Retention.DefaultTTL
	}
	if c.Retention.MaxVersions <= 0 {
		c.Retention.MaxVersions = d.Retention.MaxVersions
	}

	if c.Cleanup.Interval <= 0 {
		c.Cleanup.Interval = d.Cleanup.Interval
	}
	if c.Cleanup.BatchSize <= 0 {
		c.Cleanup.BatchSize = d.Cleanup.BatchSize
	}
	if c.Cleanup.OrphanGrace < 0 {
		c.Cleanup.OrphanGrace = d.Cleanup.OrphanGrace
	}

	if c.MaxObjectSize < 0 {
		return Config{}, wrap("max_object_size must be >= 0")
	}
	if c.MaxObjectSize == 0 {
		c.MaxObjectSize = d.MaxObjectSize
	}
	if c.MultipartThreshold <= 0 {
		c.MultipartThreshold = d.MultipartThreshold
	}
	if c.SignedURLTTL <= 0 {
		c.SignedURLTTL = d.SignedURLTTL
	}
	return c, nil
}

func wrap(msg string) error { return &configError{msg: msg} }

type configError struct{ msg string }

func (e *configError) Error() string { return "storage/config: " + e.msg }

// Is lets callers match config errors against artifacts.ErrConfig.
func (e *configError) Is(target error) bool { return target == artifacts.ErrConfig }
