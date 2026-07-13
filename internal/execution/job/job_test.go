package job

import (
	"testing"
	"time"
)

func TestNewAppliesDefaults(t *testing.T) {
	now := time.Now()
	req := Request{Language: "go", Source: "package main", Priority: PriorityNormal}
	d := Defaults{
		ID: "job-1", RequestID: "req-1", CorrelationID: "corr-1", Now: now,
		Timeout: 5 * time.Second, MaxRetries: 3,
		Resources: ResourceProfile{Tier: "standard", MemoryBytes: 256, WallTimeout: 5 * time.Second},
	}
	j := New(req, d)

	if j.ID != "job-1" || j.State != StatePending || j.Outcome != OutcomeNone {
		t.Fatalf("unexpected base fields: %+v", j)
	}
	if j.Timeout != 5*time.Second {
		t.Errorf("timeout = %v, want 5s", j.Timeout)
	}
	if j.Resources.MemoryBytes != 256 {
		t.Errorf("resources not defaulted from language: %+v", j.Resources)
	}
	if j.MaxRetries != 3 {
		t.Errorf("max retries = %d", j.MaxRetries)
	}
}

func TestNewRequestOverridesResources(t *testing.T) {
	override := ResourceProfile{Tier: "large", MemoryBytes: 1024, WallTimeout: 9 * time.Second}
	req := Request{Language: "go", Source: "x", Timeout: 8 * time.Second, Resources: &override}
	j := New(req, Defaults{ID: "j", Now: time.Now(), Timeout: 5 * time.Second})

	if j.Timeout != 8*time.Second {
		t.Errorf("request timeout not honored: %v", j.Timeout)
	}
	if j.Resources.MemoryBytes != 1024 {
		t.Errorf("request resource override not honored: %+v", j.Resources)
	}
}

func TestCloneIsDeep(t *testing.T) {
	j := Job{
		ID:              "j",
		Metadata:        map[string]string{"k": "v"},
		CompilerOptions: []string{"-O2"},
		ExecutionOptions: ExecutionOptions{
			Args: []string{"a"}, Env: map[string]string{"E": "1"},
		},
	}
	cp := j.Clone()
	cp.Metadata["k"] = "mutated"
	cp.CompilerOptions[0] = "-O0"
	cp.ExecutionOptions.Args[0] = "b"
	cp.ExecutionOptions.Env["E"] = "2"

	if j.Metadata["k"] != "v" || j.CompilerOptions[0] != "-O2" ||
		j.ExecutionOptions.Args[0] != "a" || j.ExecutionOptions.Env["E"] != "1" {
		t.Fatal("Clone did not deep-copy; mutation leaked into original")
	}
}

func TestStatistics(t *testing.T) {
	base := time.Now()
	j := Job{
		State:       StateCompleted,
		Outcome:     OutcomeSuccess,
		RetryCount:  1,
		CreatedAt:   base,
		ScheduledAt: base.Add(1 * time.Second),
		StartedAt:   base.Add(2 * time.Second),
		CompletedAt: base.Add(5 * time.Second),
	}
	s := j.Statistics()
	if s.QueueWait != 1*time.Second {
		t.Errorf("queue wait = %v, want 1s", s.QueueWait)
	}
	if s.ExecTime != 3*time.Second {
		t.Errorf("exec time = %v, want 3s", s.ExecTime)
	}
	if s.TotalTime != 5*time.Second {
		t.Errorf("total time = %v, want 5s", s.TotalTime)
	}
}

func TestPriorityString(t *testing.T) {
	if PriorityCritical.String() != "critical" || PriorityLow.String() != "low" {
		t.Error("priority strings changed")
	}
}
