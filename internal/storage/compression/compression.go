// Package compression provides the Compression Manager: pluggable compression
// codecs plus the policy engine that decides whether a given object should be
// compressed at all. It is invoked by the upload pipeline (compress on the way
// in) and the download pipeline (decompress on the way out).
//
// The design is codec-agnostic: the Codec interface hides the algorithm, and a
// registry maps artifacts.Algorithm → Codec. Only gzip (stdlib, portable) ships
// in this stage; zstd/lz4 are reserved. Business logic never references a
// concrete codec — it asks the Manager, which honors configuration and
// per-type policy.
package compression

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"cpip/internal/storage/artifacts"
	"cpip/internal/storage/config"
)

// Codec compresses and decompresses a byte stream under one algorithm.
type Codec interface {
	Algorithm() artifacts.Algorithm
	// Compress returns the compressed form of src.
	Compress(src []byte, level int) ([]byte, error)
	// Decompress returns the original form of src.
	Decompress(src []byte) ([]byte, error)
	// NewReader wraps a compressed stream for streaming decompression.
	NewReader(r io.Reader) (io.ReadCloser, error)
}

// Result captures the outcome of a compression decision made by the Manager.
type Result struct {
	// Algorithm actually applied (artifacts.None when compression was skipped).
	Algorithm artifacts.Algorithm
	// Data is the bytes to store (compressed when Applied, original otherwise).
	Data []byte
	// OriginalSize / StoredSize are logical vs on-backend byte counts.
	OriginalSize int64
	StoredSize   int64
	// Applied reports whether compression was kept (true) or reverted (false).
	Applied bool
}

// Ratio returns StoredSize/OriginalSize (1.0 means no gain).
func (r Result) Ratio() float64 {
	if r.OriginalSize == 0 {
		return 1
	}
	return float64(r.StoredSize) / float64(r.OriginalSize)
}

// Manager owns the codec registry and the compression policy.
type Manager struct {
	cfg    config.Compression
	codecs map[artifacts.Algorithm]Codec
}

// New constructs a Manager from configuration, registering all shipped codecs.
func New(cfg config.Compression) *Manager {
	m := &Manager{
		cfg:    cfg,
		codecs: make(map[artifacts.Algorithm]Codec),
	}
	m.register(gzipCodec{})
	return m
}

func (m *Manager) register(c Codec) { m.codecs[c.Algorithm()] = c }

// Codec returns the codec for an algorithm, or false if unsupported.
func (m *Manager) Codec(a artifacts.Algorithm) (Codec, bool) {
	c, ok := m.codecs[a]
	return c, ok
}

// ShouldCompress applies the policy: global toggle, per-type compressibility,
// minimum size threshold. The actual gain check happens in Compress (a codec may
// still revert if the ratio is poor).
func (m *Manager) ShouldCompress(t artifacts.Type, size int64) bool {
	if !m.cfg.Enabled {
		return false
	}
	if m.cfg.Algorithm == "" || m.cfg.Algorithm == artifacts.None {
		return false
	}
	if size < m.cfg.MinSize {
		return false
	}
	return t.Compressible()
}

// Compress runs the policy and, when compression is warranted, compresses src
// with the configured algorithm. If the achieved saving is below MinRatio the
// original bytes are kept (Applied=false, Algorithm=None) — this prevents storing
// a "compressed" JPEG that is actually larger than the source.
func (m *Manager) Compress(t artifacts.Type, src []byte) (Result, error) {
	orig := int64(len(src))
	base := Result{Algorithm: artifacts.None, Data: src, OriginalSize: orig, StoredSize: orig, Applied: false}

	if !m.ShouldCompress(t, orig) {
		return base, nil
	}
	codec, ok := m.codecs[m.cfg.Algorithm]
	if !ok {
		// Misconfigured algorithm should not fail an upload; store verbatim.
		return base, nil
	}
	compressed, err := codec.Compress(src, m.cfg.Level)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %s: %v", artifacts.ErrCompressionFailed, codec.Algorithm(), err)
	}
	stored := int64(len(compressed))
	saving := 1 - (float64(stored) / float64(max64(orig, 1)))
	if saving < m.cfg.MinRatio {
		// Not worth it — keep the original.
		return base, nil
	}
	return Result{
		Algorithm:    codec.Algorithm(),
		Data:         compressed,
		OriginalSize: orig,
		StoredSize:   stored,
		Applied:      true,
	}, nil
}

// Decompress reverses a stored object back to its original bytes given the
// algorithm recorded in the artifact's compression metadata.
func (m *Manager) Decompress(a artifacts.Algorithm, src []byte) ([]byte, error) {
	if a == "" || a == artifacts.None {
		return src, nil
	}
	codec, ok := m.codecs[a]
	if !ok {
		return nil, fmt.Errorf("%w: unknown algorithm %q", artifacts.ErrCompressionFailed, a)
	}
	out, err := codec.Decompress(src)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %v", artifacts.ErrCompressionFailed, a, err)
	}
	return out, nil
}

// DecompressStream wraps a compressed reader for streaming decompression, so the
// download pipeline never buffers a whole object in memory. For artifacts.None
// the reader is returned unchanged.
func (m *Manager) DecompressStream(a artifacts.Algorithm, r io.Reader) (io.ReadCloser, error) {
	if a == "" || a == artifacts.None {
		return io.NopCloser(r), nil
	}
	codec, ok := m.codecs[a]
	if !ok {
		return nil, fmt.Errorf("%w: unknown algorithm %q", artifacts.ErrCompressionFailed, a)
	}
	return codec.NewReader(r)
}

// --- gzip codec ---

type gzipCodec struct{}

func (gzipCodec) Algorithm() artifacts.Algorithm { return artifacts.Gzip }

func (gzipCodec) Compress(src []byte, level int) ([]byte, error) {
	if level <= 0 || level > gzip.BestCompression {
		level = gzip.DefaultCompression
	}
	var buf bytes.Buffer
	zw, err := gzip.NewWriterLevel(&buf, level)
	if err != nil {
		return nil, err
	}
	if _, err := zw.Write(src); err != nil {
		_ = zw.Close()
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (gzipCodec) Decompress(src []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	return io.ReadAll(zr)
}

func (gzipCodec) NewReader(r io.Reader) (io.ReadCloser, error) {
	return gzip.NewReader(r)
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

var _ Codec = gzipCodec{}
