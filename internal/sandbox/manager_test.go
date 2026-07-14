package sandbox_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"cpip/internal/sandbox/cleanup"
	"cpip/internal/sandbox/config"
	"cpip/internal/sandbox/events"
	"cpip/internal/sandbox/manager"
	"cpip/internal/sandbox/metrics"
	"cpip/internal/sandbox/runtime"
	"cpip/internal/sandbox/types"
)

// MockRuntimeAdapter implements runtime.RuntimeAdapter for clean unit testing.
type MockRuntimeAdapter struct {
	mu            sync.Mutex
	Containers    map[string]runtime.ContainerInfo
	Logs          map[string][]byte
	ShouldFail    bool
	ImagePulls    int
	ExistsCheck   bool
}

func NewMockRuntimeAdapter() *MockRuntimeAdapter {
	return &MockRuntimeAdapter{
		Containers: make(map[string]runtime.ContainerInfo),
		Logs:       make(map[string][]byte),
	}
}

func (m *MockRuntimeAdapter) CreateContainer(ctx context.Context, cfg runtime.ContainerConfig) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ShouldFail {
		return "", errors.New("container creation forced failure")
	}

	cID := "container-" + cfg.Name
	m.Containers[cID] = runtime.ContainerInfo{
		ID:      cID,
		State:   "created",
		IP:      "172.18.0.2",
		Running: false,
	}
	return cID, nil
}

func (m *MockRuntimeAdapter) StartContainer(ctx context.Context, containerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	info, ok := m.Containers[containerID]
	if !ok {
		return errors.New("container not found")
	}

	info.Running = true
	info.State = "running"
	m.Containers[containerID] = info
	return nil
}

func (m *MockRuntimeAdapter) StopContainer(ctx context.Context, containerID string, timeout time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	info, ok := m.Containers[containerID]
	if !ok {
		return errors.New("container not found")
	}

	info.Running = false
	info.State = "exited"
	m.Containers[containerID] = info
	return nil
}

func (m *MockRuntimeAdapter) RemoveContainer(ctx context.Context, containerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.Containers[containerID]
	if !ok {
		return errors.New("container not found")
	}

	delete(m.Containers, containerID)
	return nil
}

func (m *MockRuntimeAdapter) InspectContainer(ctx context.Context, containerID string) (runtime.ContainerInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	info, ok := m.Containers[containerID]
	if !ok {
		return runtime.ContainerInfo{}, errors.New("container not found")
	}
	return info, nil
}

func (m *MockRuntimeAdapter) PullImage(ctx context.Context, image string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ImagePulls++
	return nil
}

func (m *MockRuntimeAdapter) ImageExists(ctx context.Context, image string) (bool, error) {
	return m.ExistsCheck, nil
}

func (m *MockRuntimeAdapter) GetContainerLogs(ctx context.Context, containerID string, stdout, stderr io.Writer) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	logBytes, ok := m.Logs[containerID]
	if !ok {
		logBytes = []byte("mock output stdout\nmock output stderr\n")
	}

	_, _ = stdout.Write(logBytes)
	return nil
}

func (m *MockRuntimeAdapter) GetContainerStats(ctx context.Context, containerID string) (types.Stats, error) {
	return types.Stats{
		CPUPercentage:    12.5,
		MemoryUsageBytes: 45 * 1024 * 1024,
	}, nil
}

func testConfig() config.Config {
	tmpDir, _ := filepath.Abs("./tmp_sandbox_workspaces")
	return config.Config{
		WorkspaceRoot:      tmpDir,
		ImageRegistry:      "",
		ContainerNamingPat: "test-sandbox-%s",
		CleanupInterval:    50 * time.Millisecond,
		ImageCacheEnabled:  true,
		ContainerTimeout:   1 * time.Second,
		NetworkName:        "test-network",
		LanguageImages: map[string]string{
			"python3": "python:3.12-alpine",
		},
	}
}

func TestSandboxLifecycle(t *testing.T) {
	cfg := testConfig()
	rec := metrics.NewInMemRecorder()
	adapter := NewMockRuntimeAdapter()

	mgr := manager.NewSandboxManager(cfg, adapter, rec)

	// 1. Subscribe to events
	eventCh := mgr.EventBus().Subscribe(100)
	defer mgr.EventBus().Unsubscribe(eventCh)

	// 2. Create Sandbox
	ctx := context.Background()
	sess, err := mgr.CreateSandbox(ctx, "job-1", "python3", 10*time.Second, "", "", nil)
	if err != nil {
		t.Fatalf("CreateSandbox failed: %v", err)
	}

	if sess.GetState() != types.StateReady {
		t.Errorf("Expected state Ready, got %s", sess.GetState())
	}

	// Verify workspace exists on host
	if _, err := os.Stat(sess.GetWorkspacePath()); os.IsNotExist(err) {
		t.Errorf("Workspace directory does not exist: %s", sess.GetWorkspacePath())
	}

	// 3. Start Sandbox (transitions to Executing)
	err = mgr.Start(ctx, sess.ID)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if sess.GetState() != types.StateExecuting {
		t.Errorf("Expected state Executing, got %s", sess.GetState())
	}

	// 4. Copy Files
	files := map[string]string{
		"main.py":   "print('hello from sandbox')",
		"input.txt": "some test data",
	}
	err = mgr.CopyFiles(ctx, sess.ID, files)
	if err != nil {
		t.Fatalf("CopyFiles failed: %v", err)
	}

	// Read and verify files exist in the workspace on host
	content, err := os.ReadFile(filepath.Join(sess.GetWorkspacePath(), "main.py"))
	if err != nil || string(content) != "print('hello from sandbox')" {
		t.Errorf("File main.py was not copied correctly: %v", err)
	}

	// Path traversal check
	badFiles := map[string]string{
		"../traversal.py": "malicious",
	}
	err = mgr.CopyFiles(ctx, sess.ID, badFiles)
	if err == nil {
		t.Error("Expected CopyFiles to block path traversal")
	}

	// 5. Collect Logs
	var stdout, stderr bytes.Buffer
	err = mgr.CollectLogs(ctx, sess.ID, &stdout, &stderr)
	if err != nil {
		t.Fatalf("CollectLogs failed: %v", err)
	}
	if !bytes.Contains(stdout.Bytes(), []byte("mock output stdout")) {
		t.Errorf("Logs not captured properly: %s", stdout.String())
	}

	// 6. Stats & Inspect
	stats, err := mgr.Statistics(ctx, sess.ID)
	if err != nil || stats.CPUPercentage != 12.5 {
		t.Errorf("Stats not collected correctly: %+v, err: %v", stats, err)
	}

	inspected, err := mgr.Inspect(ctx, sess.ID)
	if err != nil || inspected.GetStatus() != "running" {
		t.Errorf("Inspect failed or container not marked running: %v", err)
	}

	// 7. Stop Sandbox
	err = mgr.Stop(ctx, sess.ID, 1*time.Second)
	if err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	if sess.GetState() != types.StateReady {
		t.Errorf("Expected state Ready after Stop, got %s", sess.GetState())
	}

	// 8. Destroy Sandbox
	workspacePath := sess.GetWorkspacePath()
	err = mgr.DestroySandbox(ctx, sess.ID)
	if err != nil {
		t.Fatalf("DestroySandbox failed: %v", err)
	}

	// Verify workspace directory was deleted on host
	if _, err := os.Stat(workspacePath); !os.IsNotExist(err) {
		t.Errorf("Workspace directory was not cleaned up: %s", workspacePath)
	}

	// Verify events received
	receivedTypes := make(map[events.Type]bool)
	for len(eventCh) > 0 {
		e := <-eventCh
		receivedTypes[e.Type] = true
	}

	expectedEvents := []events.Type{
		events.SandboxCreated,
		events.WorkspacePrepared,
		events.ContainerCreated,
		events.ContainerStarted,
		events.SandboxReady,
		events.ExecutionAttached,
		events.CleanupStarted,
		events.SandboxDestroyed,
	}

	for _, exp := range expectedEvents {
		if !receivedTypes[exp] {
			t.Errorf("Expected event type %s was not received", exp)
		}
	}
}

func TestCleanupSweep(t *testing.T) {
	cfg := testConfig()
	rec := metrics.NewInMemRecorder()
	adapter := NewMockRuntimeAdapter()
	mgr := manager.NewSandboxManager(cfg, adapter, rec)

	cleanupMgr := cleanup.NewCleanupManager(cfg, mgr.Registry())
	cleanupMgr.RegisterTeardown(func(ctx context.Context, id string) error {
		return mgr.DestroySandbox(ctx, id)
	})

	ctx := context.Background()
	cleanupMgr.Start(ctx)
	defer cleanupMgr.Stop()

	// Create a sandbox with immediate expiration (1 millisecond)
	sess, err := mgr.CreateSandbox(ctx, "job-expired", "python3", 1*time.Millisecond, "", "", nil)
	if err != nil {
		t.Fatalf("Failed to create sandbox: %v", err)
	}

	// Wait for cleanup sweep to trigger and destroy it
	time.Sleep(150 * time.Millisecond)

	_, err = mgr.Registry().Get(sess.ID)
	if err == nil {
		t.Error("Expected sandbox to be automatically swept and unregistered")
	}
}

func TestConcurrentSandboxes(t *testing.T) {
	cfg := testConfig()
	rec := metrics.NewInMemRecorder()
	adapter := NewMockRuntimeAdapter()
	mgr := manager.NewSandboxManager(cfg, adapter, rec)

	var wg sync.WaitGroup
	count := 50

	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx := context.Background()

			sess, err := mgr.CreateSandbox(ctx, "job-concurrent", "python3", 10*time.Second, "", "", nil)
			if err != nil {
				t.Errorf("CreateSandbox failed in worker %d: %v", id, err)
				return
			}

			_ = mgr.Start(ctx, sess.ID)
			_ = mgr.Stop(ctx, sess.ID, 1*time.Second)
			_ = mgr.DestroySandbox(ctx, sess.ID)
		}(i)
	}

	wg.Wait()

	// All should be unregistered
	if len(mgr.Registry().List()) != 0 {
		t.Errorf("Expected registry to be empty after concurrent teardown, found %d active sandboxes", len(mgr.Registry().List()))
	}
}
