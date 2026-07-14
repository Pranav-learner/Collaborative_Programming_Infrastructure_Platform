// Package upload implements the Upload Pipeline: the ordered set of stages every
// byte payload passes through on its way into the platform. The stages are
//
//	Validate → Materialize → Hash → Deduplicate → Compress → Store → Verify → Register → Audit
//
// Each stage is a method, so the pipeline reads as a linear story and individual
// stages are unit-testable. The pipeline is the ONLY sanctioned way to create an
// artifact: it guarantees that a persisted metadata record always corresponds to
// verified, content-addressed bytes in object storage.
//
// Content addressing means identical bytes map to one physical object regardless
// of how many artifacts reference them: the dedup stage reuses an existing
// object (and its compression representation) instead of re-uploading.
package upload

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"cpip/internal/id"
	"cpip/internal/storage/artifacts"
	"cpip/internal/storage/compression"
	"cpip/internal/storage/config"
	"cpip/internal/storage/content"
	"cpip/internal/storage/events"
	"cpip/internal/storage/logger"
	"cpip/internal/storage/metadata"
	"cpip/internal/storage/metrics"
	"cpip/internal/storage/objectstore"
	"cpip/internal/storage/retention"
	"cpip/internal/storage/sdk"
	"cpip/internal/storage/versioning"
)

// Request describes an artifact to create. Provide bytes via exactly one of Data
// or Reader; when using Reader, Size should be set when known (it bounds the read).
type Request struct {
	// Content source (exactly one).
	Data   []byte
	Reader io.Reader
	Size   int64 // hint/limit for Reader; ignored for Data

	// Classification & ownership.
	Type        artifacts.Type
	ContentType string
	Owner       string
	JobID       string
	RoomID      string
	DocumentID  string
	Language    string
	Metadata    map[string]string

	// LineageID appends a new version to an existing lineage; empty starts one.
	LineageID string

	// Bucket overrides type-based routing (logical bucket name).
	Bucket string

	// Retention overrides the default policy for this artifact.
	Retention artifacts.RetentionPolicy

	// ForceAlgorithm overrides compression policy when non-nil (use artifacts.None
	// to force storing verbatim).
	ForceAlgorithm *artifacts.Algorithm

	// DisableDedup skips the content-addressed deduplication stage.
	DisableDedup bool

	// VerifyReadback re-downloads and re-hashes the stored object after upload for
	// end-to-end integrity assurance (slower; off by default).
	VerifyReadback bool
}

// Result reports the outcome of a successful upload.
type Result struct {
	Artifact     *artifacts.Artifact
	Digest       content.Digest
	Deduplicated bool  // true when bytes already existed and upload was skipped
	BytesStored  int64 // bytes actually written to the backend (0 when deduplicated)
}

// Pipeline is the Upload Pipeline. It is safe for concurrent use — it holds no
// per-request state; every Upload call runs independently.
type Pipeline struct {
	objects *objectstore.Manager
	store   metadata.Store
	comp    *compression.Manager
	ret     *retention.Manager
	ver     *versioning.Manager
	cfg     config.Config
	bus     *events.Bus
	rec     metrics.Recorder
	log     *logger.Logger
	now     func() time.Time
}

// Params configures a Pipeline.
type Params struct {
	Objects     *objectstore.Manager
	Store       metadata.Store
	Compression *compression.Manager
	Retention   *retention.Manager
	Versioning  *versioning.Manager
	Config      config.Config
	Events      *events.Bus
	Metrics     metrics.Recorder
	Logger      *logger.Logger
}

// New constructs an Upload Pipeline.
func New(p Params) *Pipeline {
	rec := p.Metrics
	if rec == nil {
		rec = metrics.NewNoop()
	}
	return &Pipeline{
		objects: p.Objects,
		store:   p.Store,
		comp:    p.Compression,
		ret:     p.Retention,
		ver:     p.Versioning,
		cfg:     p.Config,
		bus:     p.Events,
		rec:     rec,
		log:     p.Logger.With("subsystem", "upload"),
		now:     time.Now,
	}
}

// Upload runs the full pipeline and returns the registered artifact.
func (p *Pipeline) Upload(ctx context.Context, req Request) (*Result, error) {
	start := p.now()
	p.rec.IncCounter(metrics.MetricUploadStarted, map[string]string{"type": string(req.Type)})

	logical, data, err := p.validateAndMaterialize(req)
	if err != nil {
		p.fail(ctx, req, err)
		return nil, err
	}

	// --- Hash (content addressing over the ORIGINAL bytes) ---
	digest := content.HashBytes(data)
	p.rec.IncCounter(metrics.MetricHashComputed, nil)
	origSize := int64(len(data))

	// --- Deduplicate ---
	var (
		objectKey   string
		comp        artifacts.Compression
		deduped     bool
		bytesStored int64
	)
	if !req.DisableDedup {
		if existing, derr := p.store.FindByContentHash(ctx, logical, digest.String()); derr == nil && existing != nil {
			// Reuse the physical object and its stored representation verbatim.
			objectKey = existing.ObjectKey
			comp = existing.Compression
			deduped = true
			p.rec.IncCounter(metrics.MetricUploadDeduped, nil)
		} else if derr != nil && !errors.Is(derr, artifacts.ErrNotFound) {
			p.fail(ctx, req, derr)
			return nil, derr
		}
	}

	// --- Compress + Store (only when not deduplicated) ---
	if !deduped {
		stored, cmeta, cerr := p.compress(req, data)
		if cerr != nil {
			p.fail(ctx, req, cerr)
			return nil, cerr
		}
		comp = cmeta
		objectKey = content.ObjectKey(digest, extFor(cmeta.Algorithm, req.ContentType))
		if serr := p.storeBytes(ctx, logical, objectKey, stored, req.ContentType, digest); serr != nil {
			p.fail(ctx, req, serr)
			return nil, serr
		}
		bytesStored = int64(len(stored))

		// --- Verify (optional end-to-end readback) ---
		if req.VerifyReadback {
			if verr := p.verify(ctx, logical, objectKey, comp.Algorithm, digest); verr != nil {
				// Roll back the just-written object; leave no orphan.
				_ = p.objects.Delete(ctx, logical, objectKey)
				p.fail(ctx, req, verr)
				return nil, verr
			}
		}
		if comp.Algorithm != artifacts.None {
			p.bus.Emit(events.CompressionCompleted, "upload", func(e *events.Event) {
				e.Bucket = logical
				e.Key = objectKey
				e.Payload = comp
			})
		}
	}

	// --- Register (build record + commit version) ---
	art := p.buildArtifact(req, logical, objectKey, digest, origSize, comp, start)
	if err := p.ver.Commit(ctx, art); err != nil {
		// The object bytes are content-addressed and may be shared; do NOT delete
		// them on a registration failure — an orphan reaper handles unreferenced
		// objects safely. Report the failure.
		p.fail(ctx, req, err)
		return nil, err
	}

	// --- Audit ---
	dur := metrics.ObserveDuration(p.rec, metrics.MetricUploadDuration, start, nil)
	p.rec.IncCounter(metrics.MetricUploadCompleted, map[string]string{"type": string(req.Type)})
	p.rec.AddCounter(metrics.MetricUploadBytes, float64(bytesStored), nil)
	p.rec.IncCounter(metrics.MetricArtifactCreated, nil)
	p.log.Upload(ctx, art.ID, art.Bucket, art.ObjectKey, art.Size, dur, nil)
	p.emitCreatedUploaded(art, deduped)

	return &Result{Artifact: art, Digest: digest, Deduplicated: deduped, BytesStored: bytesStored}, nil
}

// --- stages ---

func (p *Pipeline) validateAndMaterialize(req Request) (string, []byte, error) {
	if !req.Type.Valid() {
		return "", nil, fmt.Errorf("%w: unknown type %q", artifacts.ErrInvalidArtifact, req.Type)
	}
	logical := req.Bucket
	if logical == "" {
		logical = p.objects.Registry().BucketForType(req.Type)
	}
	if _, err := p.objects.Registry().PhysicalBucket(logical); err != nil {
		return "", nil, err
	}

	data, err := p.materialize(req)
	if err != nil {
		return "", nil, err
	}
	if max := p.cfg.MaxObjectSize; max > 0 && int64(len(data)) > max {
		return "", nil, fmt.Errorf("%w: %d > %d", artifacts.ErrObjectTooLarge, len(data), max)
	}
	if len(data) == 0 {
		return "", nil, fmt.Errorf("%w: empty content", artifacts.ErrInvalidArtifact)
	}
	return logical, data, nil
}

// materialize collects the request bytes into memory. Streaming/multipart for
// very large objects is a future stage; the read is bounded by MaxObjectSize so a
// hostile Reader cannot exhaust memory.
func (p *Pipeline) materialize(req Request) ([]byte, error) {
	if req.Data != nil {
		return req.Data, nil
	}
	if req.Reader == nil {
		return nil, fmt.Errorf("%w: no content (Data and Reader both nil)", artifacts.ErrInvalidArtifact)
	}
	limit := p.cfg.MaxObjectSize
	if limit <= 0 {
		limit = 5 << 30
	}
	// Read one extra byte to detect overflow past the limit.
	data, err := io.ReadAll(io.LimitReader(req.Reader, limit+1))
	if err != nil {
		return nil, fmt.Errorf("%w: read body: %v", artifacts.ErrUploadFailed, err)
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("%w: exceeds %d", artifacts.ErrObjectTooLarge, limit)
	}
	return data, nil
}

func (p *Pipeline) compress(req Request, data []byte) ([]byte, artifacts.Compression, error) {
	// Honor an explicit override.
	if req.ForceAlgorithm != nil {
		alg := *req.ForceAlgorithm
		if alg == artifacts.None {
			p.rec.IncCounter(metrics.MetricCompressionSkipped, nil)
			return data, artifacts.Compression{Algorithm: artifacts.None, OriginalSize: int64(len(data)), CompressedSize: int64(len(data)), Ratio: 1}, nil
		}
	}
	res, err := p.comp.Compress(req.Type, data)
	if err != nil {
		return nil, artifacts.Compression{}, err
	}
	cmeta := artifacts.Compression{
		Algorithm:      res.Algorithm,
		OriginalSize:   res.OriginalSize,
		CompressedSize: res.StoredSize,
		Ratio:          res.Ratio(),
	}
	if res.Applied {
		p.rec.IncCounter(metrics.MetricCompressionApplied, map[string]string{"algorithm": string(res.Algorithm)})
		p.rec.ObserveHistogram(metrics.MetricCompressionRatio, res.Ratio(), nil)
		p.rec.AddCounter(metrics.MetricCompressionSaved, float64(res.OriginalSize-res.StoredSize), nil)
	} else {
		p.rec.IncCounter(metrics.MetricCompressionSkipped, nil)
	}
	return res.Data, cmeta, nil
}

func (p *Pipeline) storeBytes(ctx context.Context, logical, key string, data []byte, contentType string, digest content.Digest) error {
	meta := map[string]string{"content-sha256": digest.Hex()}
	_, err := p.objects.Put(ctx, logical, key, bytes.NewReader(data), int64(len(data)), contentType, meta)
	if err != nil {
		return fmt.Errorf("%w: %v", artifacts.ErrUploadFailed, err)
	}
	return nil
}

// verify re-reads the stored object, decompresses if needed, and confirms the
// SHA-256 matches the original digest.
func (p *Pipeline) verify(ctx context.Context, logical, key string, alg artifacts.Algorithm, digest content.Digest) error {
	out, err := p.objects.Get(ctx, logical, key, sdk.GetOptions{})
	if err != nil {
		return err
	}
	defer out.Body.Close()
	rc, err := p.comp.DecompressStream(alg, out.Body)
	if err != nil {
		return err
	}
	defer rc.Close()
	if err := content.VerifyReader(rc, digest); err != nil {
		p.rec.IncCounter(metrics.MetricIntegrityMismatch, nil)
		return err
	}
	p.rec.IncCounter(metrics.MetricIntegrityOK, nil)
	return nil
}

func (p *Pipeline) buildArtifact(req Request, logical, objectKey string, digest content.Digest, origSize int64, comp artifacts.Compression, start time.Time) *artifacts.Artifact {
	now := p.now().UTC()
	lineage := req.LineageID
	if lineage == "" {
		lineage = versioning.NewLineageID()
	}
	return &artifacts.Artifact{
		ID:          id.NewWithPrefix("art"),
		ObjectKey:   objectKey,
		Bucket:      logical,
		ContentHash: digest.String(),
		Size:        origSize,
		ContentType: req.ContentType,
		Type:        req.Type,
		Owner:       req.Owner,
		JobID:       req.JobID,
		RoomID:      req.RoomID,
		DocumentID:  req.DocumentID,
		Language:    req.Language,
		LineageID:   lineage,
		Compression: comp,
		Retention:   p.ret.Resolve(req.Type, req.Retention),
		State:       artifacts.Available,
		CreatedAt:   now,
		UpdatedAt:   now,
		Metadata:    req.Metadata,
		Statistics:  artifacts.Statistics{UploadDurationMs: p.now().Sub(start).Milliseconds()},
	}
}

func (p *Pipeline) emitCreatedUploaded(a *artifacts.Artifact, deduped bool) {
	fill := func(e *events.Event) {
		e.ArtifactID = a.ID
		e.LineageID = a.LineageID
		e.Bucket = a.Bucket
		e.Key = a.ObjectKey
		e.Owner = a.Owner
		e.JobID = a.JobID
		e.RoomID = a.RoomID
	}
	p.bus.Emit(events.ArtifactCreated, "upload", fill)
	p.bus.Emit(events.ArtifactUploaded, "upload", func(e *events.Event) {
		fill(e)
		e.Payload = map[string]any{"deduplicated": deduped, "size": a.Size}
	})
}

func (p *Pipeline) fail(ctx context.Context, req Request, err error) {
	p.rec.IncCounter(metrics.MetricUploadFailed, map[string]string{"type": string(req.Type)})
	p.log.Upload(ctx, "", req.Bucket, "", 0, 0, err)
}

// extFor derives a friendly extension for the content-addressed key. It appends
// the compression suffix so a stored object's encoding is visible in the key.
func extFor(alg artifacts.Algorithm, _ string) string {
	switch alg {
	case artifacts.Gzip:
		return "gz"
	case artifacts.Zstd:
		return "zst"
	case artifacts.LZ4:
		return "lz4"
	default:
		return ""
	}
}
