package compression

import (
	"bytes"
	"io"
	"math/rand"
	"testing"

	"cpip/internal/storage/artifacts"
	"cpip/internal/storage/config"
)

func defaultMgr() *Manager {
	return New(config.Default().Compression)
}

func TestCompressRoundTrip(t *testing.T) {
	m := defaultMgr()
	// Highly compressible payload well above MinSize.
	src := bytes.Repeat([]byte("the quick brown fox jumps "), 1000)
	res, err := m.Compress(artifacts.ExecutionLog, src)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Applied {
		t.Fatalf("expected compression to be applied")
	}
	if res.Algorithm != artifacts.Gzip {
		t.Fatalf("expected gzip, got %s", res.Algorithm)
	}
	if res.StoredSize >= res.OriginalSize {
		t.Fatalf("compression did not shrink: %d >= %d", res.StoredSize, res.OriginalSize)
	}
	back, err := m.Decompress(res.Algorithm, res.Data)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(back, src) {
		t.Fatalf("round-trip mismatch")
	}
}

func TestCompressSkippedBelowMinSize(t *testing.T) {
	m := defaultMgr()
	res, err := m.Compress(artifacts.ExecutionLog, []byte("tiny"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied {
		t.Fatalf("tiny payload should not be compressed")
	}
	if res.Algorithm != artifacts.None {
		t.Fatalf("expected None, got %s", res.Algorithm)
	}
}

func TestCompressSkippedForIncompressibleType(t *testing.T) {
	m := defaultMgr()
	src := bytes.Repeat([]byte("x"), 8192)
	res, err := m.Compress(artifacts.CompiledBinary, src)
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied {
		t.Fatalf("compiled binaries should skip compression by policy")
	}
}

func TestCompressRevertsWhenNoGain(t *testing.T) {
	// Incompressible (high-entropy) data of a compressible type should revert to
	// the original because the achieved ratio is below MinRatio. A fixed seed
	// keeps the test deterministic.
	m := defaultMgr()
	src := make([]byte, 8192)
	rng := rand.New(rand.NewSource(1))
	_, _ = rng.Read(src)
	res, err := m.Compress(artifacts.ExecutionOutput, src)
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied {
		t.Fatalf("expected revert to original for incompressible data")
	}
	if int64(len(res.Data)) != res.OriginalSize {
		t.Fatalf("reverted data should equal original")
	}
}

func TestDecompressStream(t *testing.T) {
	m := defaultMgr()
	src := bytes.Repeat([]byte("stream me "), 500)
	res, err := m.Compress(artifacts.RuntimeLog, src)
	if err != nil {
		t.Fatal(err)
	}
	rc, err := m.DecompressStream(res.Algorithm, bytes.NewReader(res.Data))
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	back, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(back, src) {
		t.Fatalf("stream round-trip mismatch")
	}
}

func TestDecompressNonePassthrough(t *testing.T) {
	m := defaultMgr()
	src := []byte("verbatim")
	back, err := m.Decompress(artifacts.None, src)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(back, src) {
		t.Fatalf("None decompress should pass through")
	}
}
