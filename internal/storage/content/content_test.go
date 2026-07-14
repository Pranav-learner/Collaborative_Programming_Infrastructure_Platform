package content

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"cpip/internal/storage/artifacts"
)

func TestHashDeterministicAndDigestFormat(t *testing.T) {
	d1 := HashBytes([]byte("hello world"))
	d2 := HashBytes([]byte("hello world"))
	if d1 != d2 {
		t.Fatalf("hash not deterministic: %s != %s", d1, d2)
	}
	if !d1.Valid() {
		t.Fatalf("digest %q reported invalid", d1)
	}
	if !strings.HasPrefix(d1.String(), HashPrefix) {
		t.Fatalf("missing prefix: %s", d1)
	}
	if len(d1.Hex()) != 64 {
		t.Fatalf("expected 64 hex chars, got %d", len(d1.Hex()))
	}
	// Known SHA-256 of "hello world".
	const want = "sha256:b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
	if d1.String() != want {
		t.Fatalf("unexpected digest\n got %s\nwant %s", d1, want)
	}
}

func TestHashReaderMatchesHashBytes(t *testing.T) {
	data := bytes.Repeat([]byte("cpip"), 4096)
	streamed, n, err := HashReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(data)) {
		t.Fatalf("byte count: got %d want %d", n, len(data))
	}
	if streamed != HashBytes(data) {
		t.Fatalf("streamed digest mismatch")
	}
}

func TestTeeHasher(t *testing.T) {
	data := []byte("integrity-stream")
	r, h := TeeHasher(bytes.NewReader(data))
	out := bytes.NewBuffer(nil)
	if _, err := out.ReadFrom(r); err != nil {
		t.Fatal(err)
	}
	if h.Digest() != HashBytes(data) {
		t.Fatalf("tee digest mismatch")
	}
}

func TestVerifyMismatch(t *testing.T) {
	err := Verify(HashBytes([]byte("a")), HashBytes([]byte("b")))
	if !errors.Is(err, artifacts.ErrIntegrityMismatch) {
		t.Fatalf("expected integrity mismatch, got %v", err)
	}
	if err := Verify(HashBytes([]byte("a")), HashBytes([]byte("a"))); err != nil {
		t.Fatalf("expected match, got %v", err)
	}
}

func TestObjectKeyFanOutAndDeterminism(t *testing.T) {
	d := HashBytes([]byte("some content"))
	k1 := ObjectKey(d, "gz")
	k2 := ObjectKey(d, "gz")
	if k1 != k2 {
		t.Fatalf("object key not deterministic")
	}
	if !IsContentAddressed(k1) {
		t.Fatalf("key %q not recognized as content-addressed", k1)
	}
	hexPart := d.Hex()
	wantPrefix := "cas/" + hexPart[0:2] + "/" + hexPart[2:4] + "/"
	if !strings.HasPrefix(k1, wantPrefix) {
		t.Fatalf("key %q missing fan-out prefix %q", k1, wantPrefix)
	}
	if !strings.HasSuffix(k1, ".gz") {
		t.Fatalf("key %q missing extension", k1)
	}
	// Path separators smuggled through an extension must be stripped so the key
	// cannot escape its bucket namespace.
	sneaky := ObjectKey(d, "a/b/c")
	if strings.Count(sneaky, "/") != strings.Count(k1, "/") {
		t.Fatalf("extension separators not sanitized: %q", sneaky)
	}
}
