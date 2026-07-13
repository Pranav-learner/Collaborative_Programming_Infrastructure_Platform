package config

import (
	"errors"
	"testing"
	"time"

	"cpip/internal/execution/job"
)

func TestValidateNormalizesZeros(t *testing.T) {
	got, err := Config{}.Validate()
	if err != nil {
		t.Fatalf("validate empty: %v", err)
	}
	d := Default()
	if got.MaxCodeSize != d.MaxCodeSize || got.DefaultTimeout != d.DefaultTimeout || got.MaxRetries != d.MaxRetries {
		t.Fatalf("zeros not normalized: %+v", got)
	}
}

func TestValidatePreservesExplicit(t *testing.T) {
	in := Default()
	in.MaxTimeout = 90 * time.Second
	in.MaxRetries = 7
	got, err := in.Validate()
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if got.MaxTimeout != 90*time.Second || got.MaxRetries != 7 {
		t.Fatalf("explicit values overwritten: %+v", got)
	}
}

func TestValidateRejectsBad(t *testing.T) {
	bad := Default()
	bad.DefaultTimeout = 2 * time.Minute
	bad.MaxTimeout = time.Second
	if _, err := bad.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("default>max err = %v", err)
	}

	bad2 := Default()
	bad2.MinPriority = job.PriorityCritical
	bad2.MaxPriority = job.PriorityLow
	if _, err := bad2.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("min>max priority err = %v", err)
	}

	bad3 := Default()
	bad3.MaxRetries = -1
	if _, err := bad3.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("negative retries err = %v", err)
	}
}
