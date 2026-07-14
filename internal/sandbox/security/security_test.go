package security_test

import (
	"context"
	"io"
	"path/filepath"
	"testing"
	"time"

	"cpip/internal/sandbox/config"
	"cpip/internal/sandbox/events"
	"cpip/internal/sandbox/manager"
	"cpip/internal/sandbox/metrics"
	"cpip/internal/sandbox/runtime"
	"cpip/internal/sandbox/security/engine"
	"cpip/internal/sandbox/security/policies"
	"cpip/internal/sandbox/security/profiles"
	"cpip/internal/sandbox/types"
)

type MockSecRuntimeAdapter struct {
	Stats       types.Stats
	StatsErr    error
	CreatedCfgs []runtime.ContainerConfig
	StopCalled  bool
}

func (m *MockSecRuntimeAdapter) CreateContainer(ctx context.Context, cfg runtime.ContainerConfig) (string, error) {
	m.CreatedCfgs = append(m.CreatedCfgs, cfg)
	return "mock-container-id", nil
}

func (m *MockSecRuntimeAdapter) StartContainer(ctx context.Context, containerID string) error {
	return nil
}

func (m *MockSecRuntimeAdapter) StopContainer(ctx context.Context, containerID string, timeout time.Duration) error {
	m.StopCalled = true
	return nil
}

func (m *MockSecRuntimeAdapter) RemoveContainer(ctx context.Context, containerID string) error {
	return nil
}

func (m *MockSecRuntimeAdapter) InspectContainer(ctx context.Context, containerID string) (runtime.ContainerInfo, error) {
	return runtime.ContainerInfo{ID: containerID, State: "running", IP: "127.0.0.1", Running: true}, nil
}

func (m *MockSecRuntimeAdapter) PullImage(ctx context.Context, image string) error {
	return nil
}

func (m *MockSecRuntimeAdapter) ImageExists(ctx context.Context, image string) (bool, error) {
	return true, nil
}

func (m *MockSecRuntimeAdapter) GetContainerLogs(ctx context.Context, containerID string, stdout, stderr io.Writer) error {
	return nil
}

func (m *MockSecRuntimeAdapter) GetContainerStats(ctx context.Context, containerID string) (types.Stats, error) {
	return m.Stats, m.StatsErr
}

func testConfig() config.Config {
	tmpDir, _ := filepath.Abs("./tmp_sandbox_workspaces_sec")
	return config.Config{
		WorkspaceRoot:      tmpDir,
		ImageRegistry:      "",
		ContainerNamingPat: "test-sec-%s",
		CleanupInterval:    50 * time.Millisecond,
		ImageCacheEnabled:  true,
		ContainerTimeout:   1 * time.Second,
		NetworkName:        "test-network",
		LanguageImages: map[string]string{
			"python3": "python:3.12-alpine",
		},
	}
}

func TestSecurityProfiles(t *testing.T) {
	// 1. Verify default security profile mappings
	defProf := profiles.GetDefaultSecurityProfile(profiles.ProfileDefault)
	if defProf.ID != profiles.ProfileDefault {
		t.Errorf("Expected ProfileDefault ID, got %s", defProf.ID)
	}
	if !defProf.Filesystem.ReadOnlyRoot {
		t.Errorf("Expected Default profile to have read-only root")
	}

	roProf := profiles.GetDefaultSecurityProfile(profiles.ProfileReadOnly)
	if roProf.Filesystem.WritableWorkspace {
		t.Errorf("Expected ReadOnly profile to have non-writable workspace")
	}

	// 2. Verify environment sanitization
	secEngine := engine.NewSecurityPolicyEngine()
	env := []string{"PATH=/usr/bin", "SECRET_KEY=supersecret", "CUSTOM_VAR=value"}

	policy := policies.SecurityPolicy{
		ID:      "test-env-policy",
		Version: 1,
		Profile: profiles.GetDefaultSecurityProfile(profiles.ProfileDefault),
	}

	_, sanitized := secEngine.CreateSecuritySettings(policy, env)

	hasSecret := false
	hasPath := false
	for _, item := range sanitized {
		if item == "SECRET_KEY=supersecret" {
			hasSecret = true
		}
		if item == "PATH=/usr/bin" {
			hasPath = true
		}
	}

	if hasSecret {
		t.Errorf("Secret env variable was not filtered out")
	}
	if !hasPath {
		t.Errorf("Allowed PATH variable was filtered out")
	}
}

func TestPolicyRegistryAndValidation(t *testing.T) {
	reg := policies.NewMemRegistry()
	val := policies.NewPolicyValidator(policies.DefaultBounds)

	// 1. Valid policy registration
	pol := policies.ResourcePolicy{
		ID:      "custom-limits",
		Version: 1,
		Profile: profiles.GetDefaultResourceProfile(profiles.ProfileSmall),
	}

	err := reg.RegisterResourcePolicy(pol)
	if err != nil {
		t.Fatalf("Failed to register resource policy: %v", err)
	}

	// Try registering duplicate version
	err = reg.RegisterResourcePolicy(pol)
	if err == nil {
		t.Errorf("Expected error registering duplicate version")
	}

	// 2. Invalid policy validation (out of bounds)
	invalidPol := policies.ResourcePolicy{
		ID:      "oversized",
		Version: 1,
		Profile: profiles.ResourceProfile{
			ID:               "oversized",
			MemoryLimitBytes: 100 * 1024 * 1024 * 1024, // 100GB (exceeds 16GB max)
			CPULimitShares:   1024,
			ExecutionTimeout: 5 * time.Second,
			ProcessLimit:     20,
			OpenFileLimit:    64,
		},
	}

	err = val.ValidateResourcePolicy(invalidPol)
	if err == nil {
		t.Errorf("Expected validation failure for out-of-bounds memory limit")
	}
}

func TestSandboxSecurityEnforcement(t *testing.T) {
	cfg := testConfig()
	rec := metrics.NewInMemRecorder()
	adapter := &MockSecRuntimeAdapter{}

	mgr := manager.NewSandboxManager(cfg, adapter, rec)

	ctx := context.Background()
	sess, err := mgr.CreateSandbox(ctx, "job-sec-1", "python3", 10*time.Second, "Default", "Tiny", nil)
	if err != nil {
		t.Fatalf("Failed to create sandbox: %v", err)
	}

	if sess.MemoryLimitBytes != 64*1024*1024 {
		t.Errorf("Expected 64MB memory limit, got %d", sess.MemoryLimitBytes)
	}

	// Verify CreateContainer call has the security and resource configuration applied
	if len(adapter.CreatedCfgs) == 0 {
		t.Fatalf("No container was created")
	}

	appliedCfg := adapter.CreatedCfgs[0]
	if appliedCfg.Resources.MemoryBytes != 64*1024*1024 {
		t.Errorf("Expected memory limit in container config, got %d", appliedCfg.Resources.MemoryBytes)
	}
	if !appliedCfg.Security.ReadOnlyRoot {
		t.Errorf("Expected ReadOnlyRoot security configuration to be true")
	}

	// Verify policy registration retrieval
	audLogger := mgr.GetAuditLogger()
	entries := audLogger.ListEntries()

	foundApplied := false
	for _, entry := range entries {
		if entry.Action == "policy_applied" {
			foundApplied = true
			if entry.Metadata["security_profile"] != "Default" {
				t.Errorf("Expected security profile Default, got %v", entry.Metadata["security_profile"])
			}
		}
	}

	if !foundApplied {
		t.Errorf("Audit log did not record policy application")
	}
}

func TestResourceMonitorViolation(t *testing.T) {
	cfg := testConfig()
	rec := metrics.NewInMemRecorder()
	adapter := &MockSecRuntimeAdapter{}

	// Set Stats to simulate limit violation (exceeds Tiny profile's 64MB memory limit)
	adapter.Stats = types.Stats{
		CPUPercentage:    5.0,
		MemoryUsageBytes: 70 * 1024 * 1024, // 70MB
	}

	mgr := manager.NewSandboxManager(cfg, adapter, rec)

	eventCh := mgr.EventBus().Subscribe(100)
	defer mgr.EventBus().Unsubscribe(eventCh)

	ctx := context.Background()
	sess, err := mgr.CreateSandbox(ctx, "job-sec-2", "python3", 10*time.Second, "Default", "Tiny", nil)
	if err != nil {
		t.Fatalf("Failed to create sandbox: %v", err)
	}

	// Transition to executing to start monitoring
	err = mgr.Start(ctx, sess.ID)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for the monitor poller to run and catch the violation
	time.Sleep(250 * time.Millisecond)

	// Verify container stop was called because of memory limit violation
	if !adapter.StopCalled {
		t.Errorf("Expected container to be stopped on memory violation")
	}

	// Verify events were published
	hasLimitExceeded := false
	hasViolation := false
	for len(eventCh) > 0 {
		e := <-eventCh
		if e.Type == events.LimitExceeded {
			hasLimitExceeded = true
		}
		if e.Type == events.ResourceViolation {
			hasViolation = true
		}
	}

	if !hasLimitExceeded {
		t.Errorf("Expected LimitExceeded event")
	}
	if !hasViolation {
		t.Errorf("Expected ResourceViolation event")
	}
}
