package languages_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"cpip/internal/languages/compiler"
	"cpip/internal/languages/config"
	"cpip/internal/languages/events"
	"cpip/internal/languages/manager"
	"cpip/internal/languages/metrics"
	"cpip/internal/languages/plugins"
	"cpip/internal/languages/runtime"
	"cpip/internal/languages/templates"
	"cpip/internal/languages/types"
)

// MockPlugin implements sdk.PluginSDK for testing.
type MockPlugin struct {
	ID            string
	PluginVer     string
	Status        string
	ShouldFail    bool
	CompileDelay  time.Duration
	ExecuteDelay  time.Duration
}

func (m *MockPlugin) Initialize(ctx context.Context, cfg config.PluginConfig) error {
	if m.ShouldFail {
		return errors.New("initialization failed")
	}
	return nil
}

func (m *MockPlugin) Validate(ctx context.Context, source string) error {
	if source == "invalid" {
		return errors.New("invalid syntax")
	}
	return nil
}

func (m *MockPlugin) Compile(ctx context.Context, req compiler.CompileRequest) (compiler.CompileResult, error) {
	time.Sleep(m.CompileDelay)
	if m.ShouldFail {
		return compiler.CompileResult{Success: false, Output: "compiler crash"}, nil
	}
	return compiler.CompileResult{
		Success:        true,
		ExecutablePath: "/tmp/mock-binary",
		Output:         "compiled ok",
	}, nil
}

func (m *MockPlugin) Run(ctx context.Context, input runtime.RunInput) (runtime.RunResult, error) {
	time.Sleep(m.ExecuteDelay)
	if m.ShouldFail {
		return runtime.RunResult{ExitCode: 1, Error: "execution crash"}, nil
	}
	return runtime.RunResult{
		ExitCode: 0,
		PID:      12345,
	}, nil
}

func (m *MockPlugin) Cleanup(ctx context.Context, sessionID string) error {
	return nil
}

func (m *MockPlugin) Capabilities() []string {
	return []string{"concurrency", "networking"}
}

func (m *MockPlugin) Metadata() types.LanguageMetadata {
	return types.LanguageMetadata{
		ID:               m.ID,
		DisplayName:      m.ID + " Language",
		Version:          "1.0.0",
		Compiler:         "mockc",
		Runtime:          "mockrt",
		Extension:        ".mock",
		CompileRequired:  true,
		ExecutionProfile: "default",
		ResourceProfile:  "small",
		Capabilities:     m.Capabilities(),
		Status:           m.Status,
		PluginVersion:    m.PluginVer,
	}
}

func (m *MockPlugin) Health(ctx context.Context) error {
	return nil
}

func (m *MockPlugin) Version() string {
	return m.PluginVer
}

func testConfig() config.Config {
	return config.Config{
		PluginDirs:        []string{"/tmp/plugins"},
		ValidationEnabled: true,
		VersionPolicy:     "strict",
		ProfileDefaults: config.ProfileDefaultConfig{
			Timeout:     10 * time.Second,
			MemoryLimit: 256 * 1024 * 1024,
			CPULimit:    1000,
			FileLimit:   10 * 1024 * 1024,
			OutputLimit: 1 * 1024 * 1024,
		},
		ResourceDefaults: config.ResourceDefaultConfig{
			Small: config.ResourceLimits{
				CPUMillicores: 1000,
				MemoryBytes:   256 * 1024 * 1024,
				PidsLimit:     64,
				TmpfsBytes:    64 * 1024 * 1024,
				WallTimeout:   10 * time.Second,
			},
		},
	}
}

func TestPluginLifecycle(t *testing.T) {
	cfg := testConfig()
	rec := metrics.NewInMemRecorder()
	mgr := manager.NewManager(cfg, rec)

	pluginSDK := &MockPlugin{
		ID:        "mock-lang",
		PluginVer: "1.0.0",
		Status:    "stable",
	}

	// 1. Subscribe to events
	bus := mgr.EventBus()
	eventCh := bus.Subscribe(10)
	defer bus.Unsubscribe(eventCh)

	// 2. Register Plugin
	err := mgr.RegisterPlugin(pluginSDK)
	if err != nil {
		t.Fatalf("RegisterPlugin failed: %v", err)
	}

	p, err := mgr.GetPlugin("mock-lang")
	if err != nil {
		t.Fatalf("GetPlugin failed: %v", err)
	}

	if p.State() != plugins.StateValidated {
		t.Errorf("Expected state validated, got %s", p.State())
	}

	// 3. Load & Initialize Plugin
	ctx := context.Background()
	pCfg := config.PluginConfig{
		WorkspaceDir: "/tmp/workspace",
	}
	err = mgr.InitializePlugin(ctx, "mock-lang", pCfg)
	if err != nil {
		t.Fatalf("InitializePlugin failed: %v", err)
	}

	if p.State() != plugins.StateReady {
		t.Errorf("Expected state ready, got %s", p.State())
	}

	// Verify events
	var eventsReceived []events.Type
	for len(eventCh) > 0 {
		e := <-eventCh
		eventsReceived = append(eventsReceived, e.Type)
	}

	expectedEvents := []events.Type{
		events.PluginRegistered,
		events.PluginValidated,
		events.PluginLoaded,
		events.PluginInitialized,
		events.PluginReady,
	}

	for _, exp := range expectedEvents {
		found := false
		for _, rec := range eventsReceived {
			if rec == exp {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected event %s not received", exp)
		}
	}

	// 4. Validate, Compile and Run
	err = p.Validate(ctx, "valid code")
	if err != nil {
		t.Errorf("Validation failed: %v", err)
	}

	compileRes, err := p.Compile(ctx, compiler.CompileRequest{
		SessionID: "sess-1",
		Source:    "valid code",
	})
	if err != nil || !compileRes.Success {
		t.Errorf("Compilation failed: %v, res: %+v", err, compileRes)
	}

	runRes, err := p.Run(ctx, runtime.RunInput{
		SessionID:      "sess-1",
		ExecutablePath: compileRes.ExecutablePath,
	})
	if err != nil || runRes.ExitCode != 0 {
		t.Errorf("Run failed: %v, res: %+v", err, runRes)
	}

	// 5. Check metrics and stats
	stats := p.Stats()
	if stats.TotalExecutions != 1 || stats.SuccessfulExecutions != 1 {
		t.Errorf("Incorrect stats: %+v", stats)
	}

	if rec.Registers["mock-lang"] != 1 || rec.Loads["mock-lang"] != 1 {
		t.Errorf("Incorrect recorder metrics: %+v", rec)
	}

	// 6. Unload
	err = mgr.UnloadPlugin("mock-lang")
	if err != nil {
		t.Errorf("UnloadPlugin failed: %v", err)
	}
	if p.State() != plugins.StateUnloaded {
		t.Errorf("Expected state unloaded, got %s", p.State())
	}

	// 7. Remove
	err = mgr.RemovePlugin("mock-lang")
	if err != nil {
		t.Errorf("RemovePlugin failed: %v", err)
	}

	_, err = mgr.GetPlugin("mock-lang")
	if !errors.Is(err, manager.ErrPluginNotFound) {
		t.Errorf("Expected ErrPluginNotFound, got %v", err)
	}
}

func TestValidationVersionPolicy(t *testing.T) {
	cfg := testConfig()
	cfg.VersionPolicy = "strict"
	mgr := manager.NewManager(cfg, nil)

	// Strict SemVer validation fails on non-SemVer
	badPlugin := &MockPlugin{
		ID:        "strict-lang",
		PluginVer: "1.0", // Invalid SemVer (needs patch version)
		Status:    "stable",
	}

	err := mgr.RegisterPlugin(badPlugin)
	if err == nil {
		t.Error("Expected registration to fail under strict SemVer policy")
	}

	// Loose SemVer validation passes
	cfg.VersionPolicy = "loose"
	mgrLoose := manager.NewManager(cfg, nil)
	err = mgrLoose.RegisterPlugin(badPlugin)
	if err != nil {
		t.Errorf("Expected registration to succeed under loose SemVer policy: %v", err)
	}
}

func TestExecutionAndResourceProfiles(t *testing.T) {
	cfg := testConfig()
	mgr := manager.NewManager(cfg, nil)

	// Get execution profile
	ep, err := mgr.Profiles().GetExecution("cpu_intensive")
	if err != nil {
		t.Fatalf("Failed to get cpu_intensive profile: %v", err)
	}
	if ep.CPULimit != 4000 {
		t.Errorf("Expected CPU limit 4000, got %d", ep.CPULimit)
	}

	// Get resource profile
	rp, err := mgr.Profiles().GetResource("small")
	if err != nil {
		t.Fatalf("Failed to get small resource profile: %v", err)
	}
	if rp.CPUMillicores != 1000 {
		t.Errorf("Expected CPUMillicores 1000, got %d", rp.CPUMillicores)
	}
}

func TestTemplates(t *testing.T) {
	cfg := testConfig()
	mgr := manager.NewManager(cfg, nil)

	code, err := mgr.Templates().GetTemplate("python3", templates.TemplateHelloWorld)
	if err != nil {
		t.Fatalf("Failed to retrieve python3 HelloWorld template: %v", err)
	}
	if code != `print("Hello, World!")` {
		t.Errorf("Expected print hello world, got: %s", code)
	}
}

func TestConcurrencyAndRace(t *testing.T) {
	cfg := testConfig()
	mgr := manager.NewManager(cfg, nil)

	pluginSDK := &MockPlugin{
		ID:           "concurrent-lang",
		PluginVer:    "1.0.0",
		Status:       "stable",
		CompileDelay: 10 * time.Millisecond,
		ExecuteDelay: 10 * time.Millisecond,
	}

	err := mgr.RegisterPlugin(pluginSDK)
	if err != nil {
		t.Fatalf("RegisterPlugin failed: %v", err)
	}

	p, err := mgr.GetPlugin("concurrent-lang")
	if err != nil {
		t.Fatalf("GetPlugin failed: %v", err)
	}

	err = mgr.InitializePlugin(context.Background(), "concurrent-lang", config.PluginConfig{})
	if err != nil {
		t.Fatalf("InitializePlugin failed: %v", err)
	}

	var wg sync.WaitGroup
	workers := 20
	wg.Add(workers)

	// Run concurrent validation, compilation and executions
	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			ctx := context.Background()

			_ = p.Validate(ctx, "valid code")
			compileRes, _ := p.Compile(ctx, compiler.CompileRequest{
				SessionID: "sess-concurrent",
				Source:    "valid code",
			})
			_, _ = p.Run(ctx, runtime.RunInput{
				SessionID:      "sess-concurrent",
				ExecutablePath: compileRes.ExecutablePath,
			})
		}(i)
	}

	wg.Wait()

	stats := p.Stats()
	if stats.TotalExecutions != int64(workers) {
		t.Errorf("Expected %d executions, got %d", workers, stats.TotalExecutions)
	}
}
