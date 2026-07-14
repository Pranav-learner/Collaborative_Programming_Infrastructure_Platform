package types

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
)

// Codec encodes and decodes cache values to and from the string form stored in
// Redis. Business code deals in typed Go values; the codec is the single place
// where wire representation is decided, so the format can evolve (JSON → msgpack
// → protobuf) without touching any caller.
type Codec interface {
	// Name identifies the codec (recorded in metrics/logs).
	Name() string
	// Encode serializes v into a storable string.
	Encode(v any) (string, error)
	// Decode deserializes data into the value pointed to by dst.
	Decode(data string, dst any) error
}

// JSONCodec is the default codec. It is dependency-free, human-readable in
// redis-cli, and adequate for the metadata-sized values this module caches.
type JSONCodec struct{}

// Name implements Codec.
func (JSONCodec) Name() string { return "json" }

// Encode implements Codec.
func (JSONCodec) Encode(v any) (string, error) {
	// A raw string is stored verbatim to avoid double-quoting overhead on the
	// hot path (the manager caches many string values directly).
	if s, ok := v.(string); ok {
		return s, nil
	}
	if b, ok := v.([]byte); ok {
		return string(b), nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrSerialization, err)
	}
	return string(b), nil
}

// Decode implements Codec.
func (JSONCodec) Decode(data string, dst any) error {
	switch d := dst.(type) {
	case *string:
		*d = data
		return nil
	case *[]byte:
		*d = []byte(data)
		return nil
	}
	if err := json.Unmarshal([]byte(data), dst); err != nil {
		return fmt.Errorf("%w: %v", ErrDeserialization, err)
	}
	return nil
}

// ChecksummedCodec wraps another codec, prepending a CRC32 integrity header so
// that silent value corruption (partial writes, encoding drift) is detected on
// read rather than surfacing as a confusing downstream decode failure.
type ChecksummedCodec struct {
	Inner Codec
}

const checksumPrefix = "c1:" // versioned magic so the format can evolve

// Name implements Codec.
func (c ChecksummedCodec) Name() string { return "crc32+" + c.Inner.Name() }

// Encode implements Codec.
func (c ChecksummedCodec) Encode(v any) (string, error) {
	payload, err := c.Inner.Encode(v)
	if err != nil {
		return "", err
	}
	sum := crc32.ChecksumIEEE([]byte(payload))
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], sum)
	var b bytes.Buffer
	b.WriteString(checksumPrefix)
	b.Write(hdr[:])
	b.WriteString(payload)
	return b.String(), nil
}

// Decode implements Codec.
func (c ChecksummedCodec) Decode(data string, dst any) error {
	if len(data) < len(checksumPrefix)+4 || data[:len(checksumPrefix)] != checksumPrefix {
		return fmt.Errorf("%w: missing checksum header", ErrCorruption)
	}
	body := data[len(checksumPrefix):]
	want := binary.BigEndian.Uint32([]byte(body[:4]))
	payload := body[4:]
	if got := crc32.ChecksumIEEE([]byte(payload)); got != want {
		return fmt.Errorf("%w: crc mismatch want=%08x got=%08x", ErrCorruption, want, got)
	}
	return c.Inner.Decode(payload, dst)
}

// Fingerprint returns a short stable hash of a value's encoded form. It is used
// for optimistic concurrency (compare-and-set) on distributed state without
// storing an explicit version column.
func Fingerprint(data string) string {
	sum := sha256.Sum256([]byte(data))
	return fmt.Sprintf("%x", sum[:8])
}
