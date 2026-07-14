// Package objectstore implements the Object Storage Manager: the module's
// blob-level control plane. It sits directly above the Storage SDK and the
// registry, translating logical bucket names into concrete backend routes and
// owning the concerns the raw adapter does not: bucket provisioning, object
// lookup, integrity validation, storage health, and storage statistics.
//
// The upload and download pipelines talk to THIS manager, never to a vendor SDK.
// Because every call is expressed in logical buckets, the physical backend can be
// remapped or replaced without touching a caller.
package objectstore

import (
	"context"
	"errors"
	"io"
	"time"

	"cpip/internal/storage/artifacts"
	"cpip/internal/storage/content"
	"cpip/internal/storage/events"
	"cpip/internal/storage/logger"
	"cpip/internal/storage/metrics"
	"cpip/internal/storage/registry"
	"cpip/internal/storage/sdk"
)

// Manager is the Object Storage Manager.
type Manager struct {
	reg *registry.Registry
	rec metrics.Recorder
	log *logger.Logger
	bus *events.Bus
}

// Params configures a Manager. Only Registry is required.
type Params struct {
	Registry *registry.Registry
	Metrics  metrics.Recorder
	Logger   *logger.Logger
	Events   *events.Bus
}

// New constructs an Object Storage Manager.
func New(p Params) *Manager {
	rec := p.Metrics
	if rec == nil {
		rec = metrics.NewNoop()
	}
	return &Manager{
		reg: p.Registry,
		rec: rec,
		log: p.Logger.With("subsystem", "objectstore"),
		bus: p.Events,
	}
}

// Registry exposes the underlying routing registry.
func (m *Manager) Registry() *registry.Registry { return m.reg }

// EnsureBuckets provisions every configured bucket. Called once at startup.
func (m *Manager) EnsureBuckets(ctx context.Context) error {
	if err := m.reg.EnsureBuckets(ctx); err != nil {
		return err
	}
	m.bus.Emit(events.BucketCreated, "objectstore", nil)
	return nil
}

// Put writes bytes to (logicalBucket, key). Size must be the exact byte length.
func (m *Manager) Put(ctx context.Context, logicalBucket, key string, body io.Reader, size int64, contentType string, meta map[string]string) (sdk.PutResult, error) {
	route, err := m.reg.Resolve(logicalBucket)
	if err != nil {
		return sdk.PutResult{}, err
	}
	res, err := route.Store.Upload(ctx, sdk.PutInput{
		Bucket:      route.Bucket,
		Key:         key,
		Body:        body,
		Size:        size,
		ContentType: contentType,
		Metadata:    meta,
	})
	if err != nil {
		m.rec.IncCounter(metrics.MetricBackendError, map[string]string{"op": "put"})
		return sdk.PutResult{}, err
	}
	return res, nil
}

// Get opens a read stream for (logicalBucket, key). The caller MUST close Body.
func (m *Manager) Get(ctx context.Context, logicalBucket, key string, opts sdk.GetOptions) (*sdk.GetOutput, error) {
	route, err := m.reg.Resolve(logicalBucket)
	if err != nil {
		return nil, err
	}
	out, err := route.Store.Download(ctx, sdk.ObjectRef{Bucket: route.Bucket, Key: key}, opts)
	if err != nil {
		if !errors.Is(err, artifacts.ErrObjectNotFound) {
			m.rec.IncCounter(metrics.MetricBackendError, map[string]string{"op": "get"})
		}
		return nil, err
	}
	return out, nil
}

// Exists reports whether (logicalBucket, key) is present.
func (m *Manager) Exists(ctx context.Context, logicalBucket, key string) (bool, error) {
	route, err := m.reg.Resolve(logicalBucket)
	if err != nil {
		return false, err
	}
	return route.Store.Exists(ctx, sdk.ObjectRef{Bucket: route.Bucket, Key: key})
}

// Stat returns object metadata for (logicalBucket, key).
func (m *Manager) Stat(ctx context.Context, logicalBucket, key string) (sdk.Object, error) {
	route, err := m.reg.Resolve(logicalBucket)
	if err != nil {
		return sdk.Object{}, err
	}
	return route.Store.Stat(ctx, sdk.ObjectRef{Bucket: route.Bucket, Key: key})
}

// Delete removes (logicalBucket, key). Idempotent at the backend.
func (m *Manager) Delete(ctx context.Context, logicalBucket, key string) error {
	route, err := m.reg.Resolve(logicalBucket)
	if err != nil {
		return err
	}
	return route.Store.Delete(ctx, sdk.ObjectRef{Bucket: route.Bucket, Key: key})
}

// List enumerates objects in a logical bucket.
func (m *Manager) List(ctx context.Context, logicalBucket string, opts sdk.ListOptions) (sdk.ListResult, error) {
	route, err := m.reg.Resolve(logicalBucket)
	if err != nil {
		return sdk.ListResult{}, err
	}
	return route.Store.List(ctx, route.Bucket, opts)
}

// Copy duplicates an object within/between logical buckets on the same provider.
func (m *Manager) Copy(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string) error {
	src, err := m.reg.Resolve(srcBucket)
	if err != nil {
		return err
	}
	dst, err := m.reg.Resolve(dstBucket)
	if err != nil {
		return err
	}
	if src.Provider != dst.Provider {
		// Cross-provider copy is a stream-through (future optimization: server-side).
		return m.streamCopy(ctx, src, srcKey, dst, dstKey)
	}
	return src.Store.Copy(ctx,
		sdk.ObjectRef{Bucket: src.Bucket, Key: srcKey},
		sdk.ObjectRef{Bucket: dst.Bucket, Key: dstKey})
}

func (m *Manager) streamCopy(ctx context.Context, src registry.Route, srcKey string, dst registry.Route, dstKey string) error {
	out, err := src.Store.Download(ctx, sdk.ObjectRef{Bucket: src.Bucket, Key: srcKey}, sdk.GetOptions{})
	if err != nil {
		return err
	}
	defer out.Body.Close()
	_, err = dst.Store.Upload(ctx, sdk.PutInput{
		Bucket: dst.Bucket, Key: dstKey, Body: out.Body, Size: out.Object.Size,
		ContentType: out.Object.ContentType, Metadata: out.Object.Metadata,
	})
	return err
}

// SignedURL mints a presigned URL for (logicalBucket, key).
func (m *Manager) SignedURL(ctx context.Context, logicalBucket, key string, opts sdk.SignedURLOptions) (string, error) {
	route, err := m.reg.Resolve(logicalBucket)
	if err != nil {
		return "", err
	}
	url, err := route.Store.GenerateSignedURL(ctx, sdk.ObjectRef{Bucket: route.Bucket, Key: key}, opts)
	if err != nil {
		return "", err
	}
	m.rec.IncCounter(metrics.MetricSignedURL, map[string]string{"method": string(opts.Method)})
	return url, nil
}

// ValidateIntegrity streams an object and confirms its SHA-256 matches expected.
// It is the on-demand integrity check the download pipeline and cleanup reaper
// use to detect silent corruption.
func (m *Manager) ValidateIntegrity(ctx context.Context, logicalBucket, key string, expected content.Digest) error {
	out, err := m.Get(ctx, logicalBucket, key, sdk.GetOptions{})
	if err != nil {
		return err
	}
	defer out.Body.Close()
	if err := content.VerifyReader(out.Body, expected); err != nil {
		m.rec.IncCounter(metrics.MetricIntegrityMismatch, nil)
		m.bus.Emit(events.IntegrityFailed, "objectstore", func(e *events.Event) {
			e.Bucket = logicalBucket
			e.Key = key
		})
		return err
	}
	m.rec.IncCounter(metrics.MetricIntegrityOK, nil)
	m.bus.Emit(events.IntegrityValidated, "objectstore", func(e *events.Event) {
		e.Bucket = logicalBucket
		e.Key = key
	})
	return nil
}

// --- Health & statistics ---

// BackendHealth reports reachability of each distinct backend.
type BackendHealth struct {
	Provider  string
	Name      string
	Healthy   bool
	Error     string
	CheckedAt time.Time
}

// Health probes every backend and returns per-provider status. It records the
// storage.backend.up gauge for dashboards.
func (m *Manager) Health(ctx context.Context) []BackendHealth {
	var out []BackendHealth
	seen := map[string]struct{}{}
	for _, logical := range m.reg.LogicalBuckets() {
		route, err := m.reg.Resolve(logical)
		if err != nil {
			continue
		}
		if _, done := seen[string(route.Provider)]; done {
			continue
		}
		seen[string(route.Provider)] = struct{}{}
		h := BackendHealth{Provider: string(route.Provider), Name: route.Store.Name(), CheckedAt: time.Now().UTC()}
		if err := route.Store.Validate(ctx); err != nil {
			h.Healthy = false
			h.Error = err.Error()
			m.rec.SetGauge(metrics.MetricBackendUp, 0, map[string]string{"provider": h.Provider})
			m.log.Backend(ctx, h.Provider, "unhealthy", err)
		} else {
			h.Healthy = true
			m.rec.SetGauge(metrics.MetricBackendUp, 1, map[string]string{"provider": h.Provider})
		}
		out = append(out, h)
	}
	return out
}

// Healthy reports whether every backend is reachable.
func (m *Manager) Healthy(ctx context.Context) bool {
	for _, h := range m.Health(ctx) {
		if !h.Healthy {
			return false
		}
	}
	return true
}

// BucketStats summarizes one logical bucket.
type BucketStats struct {
	LogicalBucket string
	Bucket        string
	ObjectCount   int64
	TotalBytes    int64
	Truncated     bool // true when the listing was capped by sampleLimit
}

// StorageStats aggregates statistics across all buckets. Because object stores
// have no O(1) size API, it lists objects up to sampleLimit per bucket (0 =
// unbounded). For authoritative counts at scale, prefer the metadata store's
// Count — this is an operational snapshot of the physical backend.
func (m *Manager) StorageStats(ctx context.Context, sampleLimit int) ([]BucketStats, error) {
	var out []BucketStats
	for _, logical := range m.reg.LogicalBuckets() {
		route, err := m.reg.Resolve(logical)
		if err != nil {
			return nil, err
		}
		bs := BucketStats{LogicalBucket: logical, Bucket: route.Bucket}
		marker := ""
		for {
			res, err := route.Store.List(ctx, route.Bucket, sdk.ListOptions{Recursive: true, MaxKeys: 1000, StartAfter: marker})
			if err != nil {
				return nil, err
			}
			for _, o := range res.Objects {
				bs.ObjectCount++
				bs.TotalBytes += o.Size
				marker = o.Key
			}
			if sampleLimit > 0 && bs.ObjectCount >= int64(sampleLimit) {
				bs.Truncated = true
				break
			}
			if !res.IsTruncated || len(res.Objects) == 0 {
				break
			}
		}
		out = append(out, bs)
	}
	return out, nil
}
