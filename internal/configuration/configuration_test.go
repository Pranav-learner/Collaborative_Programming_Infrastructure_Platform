package configuration_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"cpip/internal/configuration/config"
	"cpip/internal/configuration/events"
	"cpip/internal/configuration/featureflags"
	"cpip/internal/configuration/logger"
	"cpip/internal/configuration/manager"
	"cpip/internal/configuration/metrics"
	"cpip/internal/configuration/profiles"
	"cpip/internal/configuration/providers"
	"cpip/internal/configuration/registry"
	"cpip/internal/configuration/runtime"
	"cpip/internal/configuration/sdk"
	"cpip/internal/configuration/secrets"
	"cpip/internal/configuration/validation"
	"cpip/internal/configuration/versioning"
	"cpip/internal/configuration/watcher"
)

func TestConfigurationLoadingAndOverriding(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "config.yaml")
	jsonPath := filepath.Join(tmpDir, "config.json")

	// 1. Setup YAML file config
	yamlContent := `
common:
  app.name: "CPIP-YAML"
  server.port: "8080"
  database.host: "localhost"
development:
  database.host: "dev-db"
`
	if err := os.WriteFile(yamlPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("Failed to write yaml: %v", err)
	}

	// 2. Setup JSON file config
	jsonContent := `
{
	"common": {
		"server.port": "9090",
		"database.user": "json-user"
	},
	"production": {
		"database.host": "prod-db"
	}
}
`
	if err := os.WriteFile(jsonPath, []byte(jsonContent), 0644); err != nil {
		t.Fatalf("Failed to write json: %v", err)
	}

	// Setup Environment Variable
	os.Setenv("TEST_APP_NAME", "CPIP-ENV")
	defer os.Unsetenv("TEST_APP_NAME")

	// Initialize Configuration Orchestrator Components
	cfg := config.DefaultPlatformConfig()
	cfg.ActiveProfile = config.ProfileDevelopment

	reg := registry.NewRegistry()
	yamlProv := providers.NewYAMLProvider(yamlPath, 20)
	jsonProv := providers.NewJSONProvider(jsonPath, 30)
	envProv := providers.NewEnvProvider("TEST_", 10) // Highest precedence (lowest number)

	if err := reg.Register(yamlProv); err != nil {
		t.Fatalf("Register YAML failed: %v", err)
	}
	if err := reg.Register(jsonProv); err != nil {
		t.Fatalf("Register JSON failed: %v", err)
	}
	if err := reg.Register(envProv); err != nil {
		t.Fatalf("Register ENV failed: %v", err)
	}

	profMgr := profiles.NewProfileManager(cfg.ActiveProfile)
	validator := validation.NewValidator()
	verMgr := versioning.NewVersionManager(cfg.MaxVersions)
	watch := watcher.NewWatcher(cfg.WatchInterval, nil)
	runEngine := runtime.NewEngine(metrics.NewInMemoryRecorder(), events.NewBus())

	secLog := logger.New(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	secBus := events.NewBus()
	secRec := metrics.NewInMemoryRecorder()
	secMgr := secrets.NewSecretManager(cfg.SecretMaskChar, secLog, secRec, secBus)

	ffPlatform := featureflags.NewPlatform(secRec, secBus)
	bus := events.NewBus()
	rec := metrics.NewInMemoryRecorder()
	log := logger.New(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))

	mgr := manager.NewManager(cfg, reg, profMgr, validator, verMgr, watch, runEngine, secMgr, ffPlatform, bus, rec, log)
	cSdk := sdk.NewSDK(mgr)

	ctx := context.Background()

	// Initial load
	snap, err := mgr.Load(ctx)
	if err != nil {
		t.Fatalf("Load config failed: %v", err)
	}

	// Verify merged and active profile configs
	// ENV provider: TEST_APP_NAME -> app.name (stripped prefix and lowercase normalized)
	// Value should be "CPIP-ENV" from env variable, overriding yaml "CPIP-YAML" since env priority is 10 (higher precedence than yaml 20)
	if appName, ok := cSdk.Get("app.name"); !ok || appName != "CPIP-ENV" {
		t.Errorf("Expected app.name to be 'CPIP-ENV' (env override), got %q (ok=%t)", appName, ok)
	}

	// JSON provider: "common.server.port": "9090" should override YAML "common.server.port": "8080" because JSON priority is 30... Wait!
	// In registry, lower priority number = higher precedence.
	// Priority list:
	// Env: 10 (highest precedence)
	// YAML: 20 (middle precedence)
	// JSON: 30 (lowest precedence)
	// So YAML port "8080" should override JSON port "9090"
	if port, ok := cSdk.GetInt("server.port"); !ok || port != 8080 {
		t.Errorf("Expected server.port to be 8080 (YAML override), got %d (ok=%t)", port, ok)
	}

	// Profile verification: ActiveProfile=development
	// YAML has: common.database.host: localhost, development.database.host: dev-db
	// Expected resolved database.host to be "dev-db"
	if dbHost, ok := cSdk.Get("database.host"); !ok || dbHost != "dev-db" {
		t.Errorf("Expected database.host to be 'dev-db' (development profile override), got %q (ok=%t)", dbHost, ok)
	}

	// JSON has common.database.user: json-user
	if dbUser, ok := cSdk.Get("database.user"); !ok || dbUser != "json-user" {
		t.Errorf("Expected database.user to be 'json-user', got %q", dbUser)
	}

	// Verify current snapshot values
	if snap.Version != 1 {
		t.Errorf("Expected initial version 1, got %d", snap.Version)
	}
}

func TestEnvironmentProfileInheritance(t *testing.T) {
	raw := map[string]string{
		"common.port":      "80",
		"common.host":      "0.0.0.0",
		"development.host": "localhost",
		"local.host":       "127.0.0.1",
		"production.host":  "prod.server",
	}

	// Test Local Profile: chain is Local -> Development -> Common
	pmLocal := profiles.NewProfileManager(config.ProfileLocal)
	resLocal := pmLocal.ResolveConfig(raw)

	if resLocal["port"] != "80" {
		t.Errorf("Local profile: expected port '80' from common, got %q", resLocal["port"])
	}
	if resLocal["host"] != "127.0.0.1" {
		t.Errorf("Local profile: expected host '127.0.0.1' from local override, got %q", resLocal["host"])
	}

	// Test Development Profile: chain is Development -> Common (Local is ignored)
	pmDev := profiles.NewProfileManager(config.ProfileDevelopment)
	resDev := pmDev.ResolveConfig(raw)

	if resDev["host"] != "localhost" {
		t.Errorf("Development profile: expected host 'localhost' from development, got %q", resDev["host"])
	}

	// Test Production Profile: chain is Production -> Common
	pmProd := profiles.NewProfileManager(config.ProfileProduction)
	resProd := pmProd.ResolveConfig(raw)

	if resProd["host"] != "prod.server" {
		t.Errorf("Production profile: expected host 'prod.server' from production override, got %q", resProd["host"])
	}
}

func TestConfigurationValidation(t *testing.T) {
	val := validation.NewValidator()
	val.AddRule(validation.Rule{
		Key:      "port",
		Required: true,
		Type:     "int",
		MinInt:   intPtr(1),
		MaxInt:   intPtr(65535),
	})
	val.AddRule(validation.Rule{
		Key:        "env",
		AllowedSet: []string{"dev", "prod", "test"},
	})
	val.AddRule(validation.Rule{
		Key:       "db.pass",
		DependsOn: "db.user",
	})

	// Valid configuration
	goodData := map[string]string{
		"port":    "8080",
		"env":     "dev",
		"db.user": "admin",
		"db.pass": "secret",
	}
	if err := val.Validate(goodData); err != nil {
		t.Errorf("Expected validation success, got: %v", err)
	}

	// Missing required key
	badData1 := map[string]string{
		"env": "dev",
	}
	if err := val.Validate(badData1); err == nil {
		t.Error("Expected failure due to missing required port, got nil")
	}

	// Port type / range violation
	badData2 := map[string]string{
		"port": "invalid",
	}
	if err := val.Validate(badData2); err == nil {
		t.Error("Expected failure due to port type mismatch, got nil")
	}

	badData3 := map[string]string{
		"port": "70000",
	}
	if err := val.Validate(badData3); err == nil {
		t.Error("Expected failure due to port range violation, got nil")
	}

	// Allowed set violation
	badData4 := map[string]string{
		"port": "8080",
		"env":  "staging",
	}
	if err := val.Validate(badData4); err == nil {
		t.Error("Expected failure due to environment not in allowed set, got nil")
	}

	// Dependency violation: db.pass is set but db.user is missing
	badData5 := map[string]string{
		"port":    "8080",
		"db.pass": "secret",
	}
	if err := val.Validate(badData5); err == nil {
		t.Error("Expected failure due to missing dependency db.user, got nil")
	}
}

func TestHotReloadAndSnapshots(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "config.yaml")

	initialYaml := `
common:
  app.name: "initial-name"
`
	if err := os.WriteFile(yamlPath, []byte(initialYaml), 0644); err != nil {
		t.Fatalf("Failed to write yaml: %v", err)
	}

	cfg := config.DefaultPlatformConfig()
	reg := registry.NewRegistry()
	yamlProv := providers.NewYAMLProvider(yamlPath, 10)
	if err := reg.Register(yamlProv); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	profMgr := profiles.NewProfileManager(cfg.ActiveProfile)
	validator := validation.NewValidator()
	verMgr := versioning.NewVersionManager(cfg.MaxVersions)
	watch := watcher.NewWatcher(cfg.WatchInterval, nil)
	runEngine := runtime.NewEngine(metrics.NewInMemoryRecorder(), events.NewBus())

	secMgr := secrets.NewSecretManager(cfg.SecretMaskChar, nil, metrics.NewInMemoryRecorder(), events.NewBus())
	ffPlatform := featureflags.NewPlatform(metrics.NewInMemoryRecorder(), events.NewBus())
	bus := events.NewBus()
	rec := metrics.NewInMemoryRecorder()

	mgr := manager.NewManager(cfg, reg, profMgr, validator, verMgr, watch, runEngine, secMgr, ffPlatform, bus, rec, nil)
	cSdk := sdk.NewSDK(mgr)

	ctx := context.Background()

	// Initial Load
	_, err := mgr.Load(ctx)
	if err != nil {
		t.Fatalf("Initial load failed: %v", err)
	}

	if val, ok := cSdk.Get("app.name"); !ok || val != "initial-name" {
		t.Errorf("Expected 'initial-name', got %q", val)
	}

	// Update yaml file content
	updatedYaml := `
common:
  app.name: "updated-name"
`
	if err := os.WriteFile(yamlPath, []byte(updatedYaml), 0644); err != nil {
		t.Fatalf("Failed to write updated yaml: %v", err)
	}

	// Trigger hot reload
	snap, err := mgr.Reload(ctx)
	if err != nil {
		t.Fatalf("Reload failed: %v", err)
	}

	if val, ok := cSdk.Get("app.name"); !ok || val != "updated-name" {
		t.Errorf("Expected config change to 'updated-name' after reload, got %q", val)
	}

	if snap.Version != 2 {
		t.Errorf("Expected reload to create version 2, got %d", snap.Version)
	}
}

func TestVersionRollback(t *testing.T) {
	cfg := config.DefaultPlatformConfig()
	reg := registry.NewRegistry()
	memProv := providers.NewMemoryProvider("mem", 10)
	reg.Register(memProv)

	profMgr := profiles.NewProfileManager(cfg.ActiveProfile)
	validator := validation.NewValidator()
	verMgr := versioning.NewVersionManager(cfg.MaxVersions)
	runEngine := runtime.NewEngine(metrics.NewInMemoryRecorder(), events.NewBus())
	secMgr := secrets.NewSecretManager(cfg.SecretMaskChar, nil, metrics.NewInMemoryRecorder(), events.NewBus())
	ffPlatform := featureflags.NewPlatform(metrics.NewInMemoryRecorder(), events.NewBus())
	bus := events.NewBus()
	rec := metrics.NewInMemoryRecorder()

	mgr := manager.NewManager(cfg, reg, profMgr, validator, verMgr, watcher.NewWatcher(time.Second, nil), runEngine, secMgr, ffPlatform, bus, rec, nil)
	cSdk := sdk.NewSDK(mgr)

	ctx := context.Background()

	// Version 1
	memProv.Set(ctx, "app.name", "v1")
	mgr.Load(ctx)

	// Version 2
	memProv.Set(ctx, "app.name", "v2")
	mgr.Reload(ctx)

	// Verify state is v2
	if val, _ := cSdk.Get("app.name"); val != "v2" {
		t.Errorf("Expected 'v2', got %q", val)
	}

	// Rollback to version 1
	snap, err := mgr.Rollback(ctx, 1)
	if err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	// Verify state is restored to v1
	if val, _ := cSdk.Get("app.name"); val != "v1" {
		t.Errorf("Expected rollback state 'v1', got %q", val)
	}

	// Rollback recording generates a new snapshot version (3)
	if snap.Version != 3 {
		t.Errorf("Expected snapshot version 3 for rollback transaction, got %d", snap.Version)
	}
}

func TestSecretManagerAndRotation(t *testing.T) {
	ctx := context.Background()
	cfg := config.DefaultPlatformConfig()

	secLog := logger.New(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	secRec := metrics.NewInMemoryRecorder()
	secBus := events.NewBus()
	sm := secrets.NewSecretManager(cfg.SecretMaskChar, secLog, secRec, secBus)

	// 1. Setup Memory Secret Provider
	memSec := secrets.NewMemorySecretProvider(10)
	memSec.SetSecret("database.password", "super-secret-pass")
	sm.RegisterProvider(memSec)

	// 2. Setup Env Secret Provider
	os.Setenv("SECRET_API_KEY", "env-api-key-12345")
	defer os.Unsetenv("SECRET_API_KEY")
	envSec := secrets.NewEnvSecretProvider("SECRET_", 20)
	sm.RegisterProvider(envSec)

	// Test Memory Retrieval
	secVal, err := sm.Get(ctx, "database.password")
	if err != nil {
		t.Fatalf("Failed to retrieve database.password secret: %v", err)
	}
	if secVal != "super-secret-pass" {
		t.Errorf("Expected 'super-secret-pass', got %q", secVal)
	}

	// Test Env Retrieval
	apiKeyVal, err := sm.Get(ctx, "api.key")
	if err != nil {
		t.Fatalf("Failed to retrieve api.key: %v", err)
	}
	if apiKeyVal != "env-api-key-12345" {
		t.Errorf("Expected 'env-api-key-12345', got %q", apiKeyVal)
	}

	// Test Masking Function
	masked := sm.Mask(secVal)
	expectedMask := "••••••••ass"
	if masked != expectedMask {
		t.Errorf("Expected mask %q, got %q", expectedMask, masked)
	}

	// Test Secret Rotation
	rotatedVal, err := sm.Rotate(ctx, "database.password")
	if err != nil {
		t.Fatalf("Failed to rotate secret: %v", err)
	}
	if rotatedVal == "super-secret-pass" {
		t.Error("Secret rotation returned original value instead of a new one")
	}

	// Re-get secret to verify rotation persisted in provider
	valAfterRotate, _ := sm.Get(ctx, "database.password")
	if valAfterRotate != rotatedVal {
		t.Errorf("Expected retrieved secret to be the newly rotated value %q, got %q", rotatedVal, valAfterRotate)
	}
}

func TestEncryptedFileSecretProvider(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "secrets.enc")

	keyHex, err := secrets.GenerateRandomKey(32)
	if err != nil {
		t.Fatalf("GenerateRandomKey failed: %v", err)
	}

	prov, err := secrets.NewEncryptedFileSecretProvider(filePath, keyHex, 10)
	if err != nil {
		t.Fatalf("NewEncryptedFileSecretProvider failed: %v", err)
	}

	// Write secret -> should encrypt and save to file
	err = prov.WriteSecret("redis.password", "redis-secure")
	if err != nil {
		t.Fatalf("WriteSecret failed: %v", err)
	}

	// Verify file was created
	if _, err := os.Stat(filePath); err != nil {
		t.Fatalf("Encrypted file not created: %v", err)
	}

	// Load secrets using a new provider instance with the same key
	prov2, err := secrets.NewEncryptedFileSecretProvider(filePath, keyHex, 10)
	if err != nil {
		t.Fatalf("NewEncryptedFileSecretProvider 2 failed: %v", err)
	}

	decrypted, err := prov2.GetSecret(ctx, "redis.password")
	if err != nil {
		t.Fatalf("Failed to get secret: %v", err)
	}
	if decrypted != "redis-secure" {
		t.Errorf("Expected 'redis-secure', got %q", decrypted)
	}

	// Validate decryption fails with a wrong key
	wrongKey, _ := secrets.GenerateRandomKey(32)
	provWrong, _ := secrets.NewEncryptedFileSecretProvider(filePath, wrongKey, 10)
	_, err = provWrong.GetSecret(ctx, "redis.password")
	if err == nil {
		t.Error("Expected decryption failure with incorrect key, got nil")
	}
}

func TestFeatureFlagPlatform(t *testing.T) {
	rec := metrics.NewInMemoryRecorder()
	bus := events.NewBus()
	p := featureflags.NewPlatform(rec, bus)

	// Flag 1: Standard boolean flag
	p.RegisterFlag(featureflags.FeatureFlag{
		Key:     "enable-v2-api",
		Enabled: true,
	})

	// Flag 2: Targeting specific users
	p.RegisterFlag(featureflags.FeatureFlag{
		Key:          "beta-ui",
		Enabled:      true,
		AllowedUsers: []string{"user-alpha", "user-beta"},
	})

	// Flag 3: Percentage rollout (25%)
	p.RegisterFlag(featureflags.FeatureFlag{
		Key:            "new-crdt-engine",
		Enabled:        true,
		RolloutPercent: 25,
	})

	// Flag 4: Kill switch active
	p.RegisterFlag(featureflags.FeatureFlag{
		Key:          "broken-feature",
		Enabled:      true,
		IsKillSwitch: true,
	})

	ctxBetaUser := featureflags.TargetContext{UserID: "user-beta"}
	ctxNormalUser := featureflags.TargetContext{UserID: "user-gamma"}

	// Eval Boolean flag
	if !p.Evaluate("enable-v2-api", ctxNormalUser) {
		t.Error("enable-v2-api flag should be globally enabled")
	}

	// Eval Targeting
	if !p.Evaluate("beta-ui", ctxBetaUser) {
		t.Error("beta-ui should be enabled for user-beta")
	}
	if p.Evaluate("beta-ui", ctxNormalUser) {
		t.Error("beta-ui should be disabled for user-gamma (not in allowed list)")
	}

	// Eval Kill Switch
	if p.Evaluate("broken-feature", ctxBetaUser) {
		t.Error("broken-feature should be disabled globally due to active kill switch")
	}

	// Eval Rollout Percent consistency
	var enabledCount int
	for i := 0; i < 100; i++ {
		userCtx := featureflags.TargetContext{UserID: fmt.Sprintf("user-%d", i)}
		if p.Evaluate("new-crdt-engine", userCtx) {
			enabledCount++
		}
	}
	// With 25% rollout, we expect approximately 25 users enabled (exact count depends on hash distribution)
	if enabledCount < 10 || enabledCount > 40 {
		t.Errorf("Percentage rollout out of expected boundary [10-40], got %d", enabledCount)
	}
}

func TestConcurrencyAndRaceConditions(t *testing.T) {
	cfg := config.DefaultPlatformConfig()
	reg := registry.NewRegistry()
	memProv := providers.NewMemoryProvider("mem", 10)
	reg.Register(memProv)

	profMgr := profiles.NewProfileManager(cfg.ActiveProfile)
	validator := validation.NewValidator()
	verMgr := versioning.NewVersionManager(cfg.MaxVersions)
	runEngine := runtime.NewEngine(metrics.NewInMemoryRecorder(), events.NewBus())

	secLog := logger.New(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	secRec := metrics.NewInMemoryRecorder()
	secBus := events.NewBus()
	secMgr := secrets.NewSecretManager(cfg.SecretMaskChar, secLog, secRec, secBus)

	ffPlatform := featureflags.NewPlatform(secRec, secBus)
	bus := events.NewBus()
	rec := metrics.NewInMemoryRecorder()
	log := logger.New(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))

	mgr := manager.NewManager(cfg, reg, profMgr, validator, verMgr, watcher.NewWatcher(time.Second, nil), runEngine, secMgr, ffPlatform, bus, rec, log)
	cSdk := sdk.NewSDK(mgr)

	ctx := context.Background()
	memProv.Set(ctx, "counter", "0")
	mgr.Load(ctx)

	ffPlatform.RegisterFlag(featureflags.FeatureFlag{
		Key:     "test-flag",
		Enabled: true,
	})

	var wg sync.WaitGroup
	workers := 20
	iterations := 100

	wg.Add(workers * 3)

	// Goroutine group 1: Concurrent configuration readers
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_, _ = cSdk.Get("counter")
			}
		}()
	}

	// Goroutine group 2: Concurrent runtime override updates
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				cSdk.SetOverride(ctx, "counter", fmt.Sprintf("%d-%d", workerID, j))
			}
		}(i)
	}

	// Goroutine group 3: Concurrent feature flag evaluators
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			defer wg.Done()
			userCtx := featureflags.TargetContext{UserID: fmt.Sprintf("user-%d", workerID)}
			for j := 0; j < iterations; j++ {
				_ = cSdk.EvaluateFlag("test-flag", userCtx)
			}
		}(i)
	}

	wg.Wait()
}

func intPtr(i int64) *int64 {
	return &i
}
