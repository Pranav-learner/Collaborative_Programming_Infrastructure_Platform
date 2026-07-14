// Package content implements content addressing: SHA-256 hashing, immutable
// content-addressed object keys, streaming hash verification, and the primitives
// for content deduplication. It is the integrity backbone of the module — every
// object's identity is derived from its bytes, so identical content maps to one
// stored object regardless of how many artifacts reference it.
//
// The package is dependency-light (it imports only the artifacts leaf for the
// canonical error set) so it can be reused by the upload pipeline, download
// pipeline, integrity checks, and any future content-addressable-store (CAS)
// optimization.
package content

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"path"
	"strings"

	"cpip/internal/storage/artifacts"
)

// HashPrefix is the algorithm tag prepended to every digest ("sha256:<hex>").
// Storing the algorithm inline future-proofs the format for a later CAS upgrade
// (e.g. blake3) without ambiguity.
const HashPrefix = "sha256:"

// Digest is a canonical content hash string of the form "sha256:<64 hex chars>".
type Digest string

// String returns the digest as a plain string.
func (d Digest) String() string { return string(d) }

// Hex returns the hex portion of the digest without the algorithm prefix.
func (d Digest) Hex() string { return strings.TrimPrefix(string(d), HashPrefix) }

// Valid reports whether the digest is well-formed (correct prefix and 64 hex).
func (d Digest) Valid() bool {
	h := d.Hex()
	if len(h) != 64 || !strings.HasPrefix(string(d), HashPrefix) {
		return false
	}
	_, err := hex.DecodeString(h)
	return err == nil
}

// NewDigest wraps a raw hex hash into a canonical Digest.
func NewDigest(hexHash string) Digest { return Digest(HashPrefix + hexHash) }

// Hasher is a streaming SHA-256 accumulator. It implements io.Writer so it can
// be composed into an io.MultiWriter during a single-pass upload — the bytes are
// hashed as they stream to the backend, never buffered solely to be hashed.
type Hasher struct {
	h    hash.Hash
	size int64
}

// NewHasher returns a fresh streaming Hasher.
func NewHasher() *Hasher { return &Hasher{h: sha256.New()} }

// Write feeds bytes into the running hash and accumulates the byte count.
func (w *Hasher) Write(p []byte) (int, error) {
	n, err := w.h.Write(p)
	w.size += int64(n)
	return n, err
}

// Digest returns the canonical digest of everything written so far.
func (w *Hasher) Digest() Digest {
	return NewDigest(hex.EncodeToString(w.h.Sum(nil)))
}

// Size returns the number of bytes hashed so far.
func (w *Hasher) Size() int64 { return w.size }

// HashBytes computes the digest of an in-memory buffer.
func HashBytes(b []byte) Digest {
	sum := sha256.Sum256(b)
	return NewDigest(hex.EncodeToString(sum[:]))
}

// HashReader streams r through SHA-256, returning the digest and byte count. It
// reads r to EOF; the caller retains ownership of closing r.
func HashReader(r io.Reader) (Digest, int64, error) {
	w := NewHasher()
	n, err := io.Copy(w, r)
	if err != nil {
		return "", n, fmt.Errorf("%w: hash stream: %v", artifacts.ErrUploadFailed, err)
	}
	return w.Digest(), n, nil
}

// TeeHasher wraps r so that every byte read from the returned reader is fed into
// h. This lets the download/verify path hash bytes as they are consumed without
// a second pass. Callers read the returned reader to EOF, then compare h.Digest()
// against the expected digest.
func TeeHasher(r io.Reader) (io.Reader, *Hasher) {
	h := NewHasher()
	return io.TeeReader(r, h), h
}

// Verify compares an actual digest against an expected one, returning a wrapped
// ErrIntegrityMismatch when they differ.
func Verify(expected, actual Digest) error {
	if expected == actual {
		return nil
	}
	return fmt.Errorf("%w: expected %s got %s", artifacts.ErrIntegrityMismatch, expected, actual)
}

// VerifyReader hashes r and verifies it against expected. It reads r to EOF.
func VerifyReader(r io.Reader, expected Digest) error {
	actual, _, err := HashReader(r)
	if err != nil {
		return err
	}
	return Verify(expected, actual)
}

// ObjectKey derives the immutable, content-addressed storage key for a digest.
// The two-level fan-out (aa/bb/<hash>) keeps directory/prefix cardinality low on
// filesystem and object backends alike, which matters at millions of objects.
// An optional extension is appended for content-type friendliness (harmless to
// the CAS property since the hash still uniquely identifies the bytes).
//
//	sha256:deadbeef...  ->  cas/de/ad/deadbeef...[.ext]
func ObjectKey(d Digest, ext string) string {
	h := d.Hex()
	if len(h) < 4 {
		// Defensive: never index out of range on a malformed digest.
		return path.Join("cas", h) + normalizeExt(ext)
	}
	return path.Join("cas", h[0:2], h[2:4], h) + normalizeExt(ext)
}

func normalizeExt(ext string) string {
	if ext == "" {
		return ""
	}
	if !strings.HasPrefix(ext, ".") {
		ext = "." + ext
	}
	// Guard against path separators smuggled through an extension.
	ext = strings.ReplaceAll(ext, "/", "")
	ext = strings.ReplaceAll(ext, "\\", "")
	return ext
}

// IsContentAddressed reports whether key was produced by ObjectKey.
func IsContentAddressed(key string) bool {
	return strings.HasPrefix(key, "cas/")
}
