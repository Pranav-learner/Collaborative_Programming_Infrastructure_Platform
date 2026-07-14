package registry

import (
	"fmt"

	"cpip/internal/storage/adapters/filesystem"
	"cpip/internal/storage/adapters/minio"
	"cpip/internal/storage/adapters/s3"
	"cpip/internal/storage/artifacts"
	"cpip/internal/storage/config"
	"cpip/internal/storage/sdk"
)

// BuildStore constructs the ObjectStore adapter for a provider from module
// configuration. It is the ONLY place in the module that references a concrete
// adapter package — every other package depends solely on sdk.ObjectStore. To
// add GCS/Azure later, implement the adapter and extend this switch; nothing else
// changes.
func BuildStore(cfg config.Config) (sdk.ObjectStore, error) {
	b := cfg.Backend
	switch cfg.Provider {
	case config.ProviderMinIO:
		return minio.New(minio.Options{
			Endpoint:       b.Endpoint,
			Region:         b.Region,
			AccessKey:      b.AccessKey,
			SecretKey:      b.SecretKey,
			UseSSL:         b.UseSSL,
			RequestTimeout: b.RequestTimeout,
		})
	case config.ProviderS3:
		return s3.New(s3.Options{
			Endpoint:       b.Endpoint,
			Region:         b.Region,
			AccessKey:      b.AccessKey,
			SecretKey:      b.SecretKey,
			UseSSL:         b.UseSSL,
			PathStyle:      b.PathStyle,
			RequestTimeout: b.RequestTimeout,
		})
	case config.ProviderFilesystem:
		return filesystem.New(b.FilesystemRoot)
	case config.ProviderGCS, config.ProviderAzure:
		return nil, fmt.Errorf("%w: provider %q adapter is a future stage", artifacts.ErrNotImplemented, cfg.Provider)
	default:
		return nil, fmt.Errorf("%w: unknown provider %q", artifacts.ErrNotImplemented, cfg.Provider)
	}
}

// FromConfig builds the default backend and wires a Registry from configuration.
// This is the convenience the composition root uses. For dependency injection of
// a custom/mock store, use New directly.
func FromConfig(cfg config.Config) (*Registry, error) {
	cfg, err := cfg.Validate()
	if err != nil {
		return nil, err
	}
	store, err := BuildStore(cfg)
	if err != nil {
		return nil, err
	}
	return New(Params{
		DefaultProvider: cfg.Provider,
		Stores:          map[config.Provider]sdk.ObjectStore{cfg.Provider: store},
		Buckets:         cfg.Buckets,
		TypeBuckets:     cfg.TypeBuckets,
		DefaultBucket:   cfg.DefaultBucket,
	})
}
