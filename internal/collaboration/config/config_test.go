package config

import (
	"errors"
	"testing"
	"time"
)

func TestValidateNormalizesZeros(t *testing.T) {
	got, err := Config{}.Validate()
	if err != nil {
		t.Fatalf("validate empty: %v", err)
	}
	d := Default()
	if got.SnapshotInterval != d.SnapshotInterval {
		t.Errorf("SnapshotInterval = %v, want default %v", got.SnapshotInterval, d.SnapshotInterval)
	}
	if got.MaxUpdateSize != d.MaxUpdateSize || got.RetentionCount != d.RetentionCount {
		t.Errorf("limits not normalized: %+v", got)
	}
}

func TestValidatePreservesExplicitValues(t *testing.T) {
	in := Default()
	in.SnapshotInterval = 42 * time.Second
	in.RetentionCount = 9
	got, err := in.Validate()
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if got.SnapshotInterval != 42*time.Second || got.RetentionCount != 9 {
		t.Fatalf("explicit values overwritten: %+v", got)
	}
}

func TestValidateRejectsBadValues(t *testing.T) {
	bad := Default()
	bad.IncrementalSnapshotThreshold = -1
	if _, err := bad.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("negative incremental threshold err = %v", err)
	}

	bad2 := Config{MaxDocumentSize: 10, MaxUpdateSize: 100}
	if _, err := bad2.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("update>doc err = %v", err)
	}
}
