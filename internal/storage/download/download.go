// Package download implements the Download Pipeline: the ordered stages every
// read passes through on the way out of the platform. The stages are
//
//	Authorize → Lookup → Resolve object → (Integrity) → Fetch → Decompress → Stream → Audit
//
// The pipeline hands back the ORIGINAL bytes: decompression is transparent, so a
// caller never needs to know how an object was stored. Reads deliberately do NOT
// mutate the metadata system-of-record on the hot path (no write amplification);
// access statistics are surfaced through the metrics recorder and an
// ArtifactDownloaded event that a subscriber may fold into the record.
package download

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"cpip/internal/storage/artifacts"
	"cpip/internal/storage/compression"
	"cpip/internal/storage/content"
	"cpip/internal/storage/events"
	"cpip/internal/storage/logger"
	"cpip/internal/storage/metadata"
	"cpip/internal/storage/metrics"
	"cpip/internal/storage/objectstore"
	"cpip/internal/storage/sdk"
)

// Authorizer decides whether a caller may read an artifact. Returning a non-nil
// error (typically wrapping artifacts.ErrUnauthorized) blocks the download. A nil
// Authorizer allows all reads (authorization is enforced at a higher layer).
type Authorizer func(ctx context.Context, a *artifacts.Artifact) error

// Request selects the artifact to download: by ArtifactID, or by LineageID with
// an optional Version (0 = latest).
type Request struct {
	ArtifactID string
	LineageID  string
	Version    int64

	// Range requests a byte range. Only valid for uncompressed objects; a ranged
	// read of a compressed object returns ErrNotImplemented (decompress-then-slice
	// is a future optimization).
	Range *sdk.ByteRange

	// Verify buffers the object and validates its SHA-256 against the recorded
	// content hash BEFORE returning the body — strong integrity at the cost of
	// buffering. When false, the body streams and callers may verify at EOF via
	// Output.Verify.
	Verify bool
}

// Output is the result of a download. Body MUST be closed by the caller.
type Output struct {
	Artifact    *artifacts.Artifact
	Body        io.ReadCloser
	Size        int64
	ContentType string
	Digest      content.Digest

	verifyFn func() error
}

// Verify returns integrity status after the body has been fully read (only
// meaningful when the request did not pre-verify). It reports nil when no
// streaming verifier was attached.
func (o *Output) Verify() error {
	if o.verifyFn == nil {
		return nil
	}
	return o.verifyFn()
}

// Pipeline is the Download Pipeline. It is safe for concurrent use.
type Pipeline struct {
	objects *objectstore.Manager
	store   metadata.Store
	comp    *compression.Manager
	authz   Authorizer
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
	Authorizer  Authorizer
	Events      *events.Bus
	Metrics     metrics.Recorder
	Logger      *logger.Logger
}

// New constructs a Download Pipeline.
func New(p Params) *Pipeline {
	rec := p.Metrics
	if rec == nil {
		rec = metrics.NewNoop()
	}
	return &Pipeline{
		objects: p.Objects,
		store:   p.Store,
		comp:    p.Compression,
		authz:   p.Authorizer,
		bus:     p.Events,
		rec:     rec,
		log:     p.Logger.With("subsystem", "download"),
		now:     time.Now,
	}
}

// Download runs the full pipeline and returns a stream of the original bytes.
func (p *Pipeline) Download(ctx context.Context, req Request) (*Output, error) {
	start := p.now()
	p.rec.IncCounter(metrics.MetricDownloadStarted, nil)

	// --- Lookup ---
	art, err := p.lookup(ctx, req)
	if err != nil {
		p.fail(ctx, "", err)
		return nil, err
	}

	// --- Authorize ---
	if p.authz != nil {
		if err := p.authz(ctx, art); err != nil {
			p.fail(ctx, art.ID, err)
			return nil, err
		}
	}

	// --- Serveability ---
	if !art.State.Serveable() {
		err := fmt.Errorf("%w: artifact %s is %s", artifacts.ErrDownloadFailed, art.ID, art.State)
		p.fail(ctx, art.ID, err)
		return nil, err
	}

	digest := content.Digest(art.ContentHash)
	compressed := art.Compression.Algorithm != "" && art.Compression.Algorithm != artifacts.None

	if req.Range != nil && compressed {
		err := fmt.Errorf("%w: ranged read of compressed object", artifacts.ErrNotImplemented)
		p.fail(ctx, art.ID, err)
		return nil, err
	}

	// --- Strong (buffered) integrity path ---
	if req.Verify {
		return p.downloadVerified(ctx, art, digest, start)
	}

	// --- Fetch ---
	getOpts := sdk.GetOptions{}
	if req.Range != nil {
		getOpts.Range = req.Range
	}
	out, err := p.objects.Get(ctx, art.Bucket, art.ObjectKey, getOpts)
	if err != nil {
		p.fail(ctx, art.ID, err)
		return nil, err
	}

	// --- Decompress (transparent) ---
	rc, err := p.comp.DecompressStream(art.Compression.Algorithm, out.Body)
	if err != nil {
		_ = out.Body.Close()
		p.fail(ctx, art.ID, err)
		return nil, err
	}

	// Attach a streaming verifier so callers can confirm integrity at EOF, unless
	// this is a partial (ranged) read where a whole-object hash is meaningless.
	body := &chainCloser{Reader: rc, closers: []io.Closer{rc, out.Body}}
	result := &Output{
		Artifact:    art,
		Body:        body,
		Size:        art.Size,
		ContentType: art.ContentType,
		Digest:      digest,
	}
	if req.Range == nil {
		tee, hasher := content.TeeHasher(body.Reader)
		body.Reader = tee
		result.verifyFn = func() error { return content.Verify(digest, hasher.Digest()) }
	}

	p.audit(ctx, art, start)
	return result, nil
}

// downloadVerified buffers the whole object, validates it, then returns an
// in-memory reader — the object is proven intact before a byte reaches the caller.
func (p *Pipeline) downloadVerified(ctx context.Context, art *artifacts.Artifact, digest content.Digest, start time.Time) (*Output, error) {
	out, err := p.objects.Get(ctx, art.Bucket, art.ObjectKey, sdk.GetOptions{})
	if err != nil {
		p.fail(ctx, art.ID, err)
		return nil, err
	}
	defer out.Body.Close()
	raw, err := p.comp.DecompressStream(art.Compression.Algorithm, out.Body)
	if err != nil {
		p.fail(ctx, art.ID, err)
		return nil, err
	}
	data, err := io.ReadAll(raw)
	_ = raw.Close()
	if err != nil {
		err = fmt.Errorf("%w: %v", artifacts.ErrDownloadFailed, err)
		p.fail(ctx, art.ID, err)
		return nil, err
	}
	if actual := content.HashBytes(data); actual != digest {
		err := content.Verify(digest, actual)
		p.rec.IncCounter(metrics.MetricIntegrityMismatch, nil)
		p.bus.Emit(events.IntegrityFailed, "download", func(e *events.Event) { e.ArtifactID = art.ID })
		p.fail(ctx, art.ID, err)
		return nil, err
	}
	p.rec.IncCounter(metrics.MetricIntegrityOK, nil)
	p.audit(ctx, art, start)
	return &Output{
		Artifact:    art,
		Body:        io.NopCloser(bytes.NewReader(data)),
		Size:        int64(len(data)),
		ContentType: art.ContentType,
		Digest:      digest,
	}, nil
}

func (p *Pipeline) lookup(ctx context.Context, req Request) (*artifacts.Artifact, error) {
	switch {
	case req.ArtifactID != "":
		return p.store.Get(ctx, req.ArtifactID)
	case req.LineageID != "" && req.Version > 0:
		return p.store.GetVersion(ctx, req.LineageID, req.Version)
	case req.LineageID != "":
		return p.store.GetLatest(ctx, req.LineageID)
	default:
		return nil, fmt.Errorf("%w: download request selects no artifact", artifacts.ErrInvalidArtifact)
	}
}

func (p *Pipeline) audit(ctx context.Context, art *artifacts.Artifact, start time.Time) {
	dur := metrics.ObserveDuration(p.rec, metrics.MetricDownloadDuration, start, nil)
	p.rec.IncCounter(metrics.MetricDownloadCompleted, nil)
	p.rec.AddCounter(metrics.MetricDownloadBytes, float64(art.Size), nil)
	p.log.Download(ctx, art.ID, art.Bucket, art.ObjectKey, art.Size, dur, nil)
	p.bus.Emit(events.ArtifactDownloaded, "download", func(e *events.Event) {
		e.ArtifactID = art.ID
		e.LineageID = art.LineageID
		e.Bucket = art.Bucket
		e.Key = art.ObjectKey
		e.Owner = art.Owner
	})
}

func (p *Pipeline) fail(ctx context.Context, artifactID string, err error) {
	p.rec.IncCounter(metrics.MetricDownloadFailed, nil)
	p.log.Download(ctx, artifactID, "", "", 0, 0, err)
}

// chainCloser wraps a reader with an ordered list of closers (decompressor then
// backend body) so both are released exactly once.
type chainCloser struct {
	io.Reader
	closers []io.Closer
	closed  bool
}

func (c *chainCloser) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	var firstErr error
	for _, cl := range c.closers {
		if err := cl.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
