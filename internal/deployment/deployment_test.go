package deployment_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"cpip/internal/deployment/compose"
	"cpip/internal/deployment/config"
	"cpip/internal/deployment/events"
	"cpip/internal/deployment/helm"
	"cpip/internal/deployment/kubernetes"
	"cpip/internal/deployment/logger"
	"cpip/internal/deployment/manager"
	"cpip/internal/deployment/metrics"
	"cpip/internal/deployment/middleware"
	"cpip/internal/deployment/profiles"
	"cpip/internal/deployment/probes"
	"cpip/internal/deployment/resources"
	"cpip/internal/deployment/rollback"
	"cpip/internal/deployment/sdk"
	"cpip/internal/deployment/services"
	"cpip/internal/deployment/validation"
)

func TestValidationEngine(t *testing.T) {
	val := validation.NewValidator()

	// 1. Validate empty services
	res, err := val.Validate(nil)
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
	if res.IsValid {
		t.Error("expected validation to fail for empty services list")
	}

	// 2. Validate valid services graph
	validSvcs := []services.Service{
		{
			Name:    "redis",
			Type:    services.TypeDatabase,
			Image:   "redis",
			Version: "7.0",
			Ports: []services.PortConfig{
				{Name: "redis", ContainerPort: 6379, ServicePort: 6379, Protocol: "TCP"},
			},
		},
		{
			Name:    "api",
			Type:    services.TypeAPI,
			Image:   "cpip-api",
			Version: "1.0",
			Ports: []services.PortConfig{
				{Name: "http", ContainerPort: 8080, ServicePort: 80, Protocol: "TCP"},
			},
			Dependencies: []string{"redis"},
		},
	}
	res, err = val.Validate(validSvcs)
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
	if !res.IsValid {
		t.Errorf("expected valid service graph to pass, got errors: %v", res.Errors)
	}

	// 3. Port conflict validation
	conflictSvcs := []services.Service{
		{
			Name: "service-a",
			Ports: []services.PortConfig{
				{ContainerPort: 8080, ServicePort: 8080, Protocol: "TCP"},
			},
		},
		{
			Name: "service-b",
			Ports: []services.PortConfig{
				{ContainerPort: 9000, ServicePort: 8080, Protocol: "TCP"},
			},
		},
	}
	res, _ = val.Validate(conflictSvcs)
	if res.IsValid {
		t.Error("expected port conflict to fail validation")
	}

	// 4. Circular dependency detection
	circularSvcs := []services.Service{
		{
			Name:         "service-a",
			Dependencies: []string{"service-b"},
		},
		{
			Name:         "service-b",
			Dependencies: []string{"service-a"},
		},
	}
	res, _ = val.Validate(circularSvcs)
	if res.IsValid {
		t.Error("expected circular dependency to fail validation")
	}
}

func TestProfileOverrides(t *testing.T) {
	pm := profiles.NewProfileManager()
	svcs := []services.Service{
		{
			Name:     "api",
			Replicas: 1,
			Resources: resources.ResourceConfig{
				CPURequest: "100m",
			},
		},
	}

	// Apply Production Profile
	resolved, err := pm.ApplyProfile(config.ProfileProduction, svcs)
	if err != nil {
		t.Fatalf("failed to apply profile: %v", err)
	}

	if len(resolved) != 1 {
		t.Fatalf("expected 1 resolved service, got %d", len(resolved))
	}

	if resolved[0].Replicas != 3 {
		t.Errorf("expected 3 replicas for Production profile, got %d", resolved[0].Replicas)
	}

	if resolved[0].Resources.CPURequest != "500m" {
		t.Errorf("expected CPU request override to be 500m, got %s", resolved[0].Resources.CPURequest)
	}
}

func TestComposeGenerator(t *testing.T) {
	gen := compose.NewProvider()
	svcs := []services.Service{
		{
			Name:     "api",
			Image:    "cpip-api",
			Version:  "latest",
			Replicas: 2,
			Ports: []services.PortConfig{
				{ContainerPort: 8080, ServicePort: 80, Protocol: "TCP"},
			},
			Env: map[string]string{
				"DEBUG": "true",
			},
			Health: probes.FullHealthConfig{
				Readiness: probes.DefaultHTTPProbe("/healthz", 8080),
			},
		},
	}

	yaml, err := gen.Generate(context.Background(), "development", svcs)
	if err != nil {
		t.Fatalf("failed to generate compose yaml: %v", err)
	}

	if !strings.Contains(yaml, "version: '3.8'") {
		t.Error("compose file missing version tag")
	}
	if !strings.Contains(yaml, "cpip-api:latest") {
		t.Error("compose file missing correct image:tag")
	}
	if !strings.Contains(yaml, "DEBUG=true") {
		t.Error("compose file missing environment variables")
	}
}

func TestKubernetesGenerator(t *testing.T) {
	gen := kubernetes.NewProvider()
	svcs := []services.Service{
		{
			Name:     "api",
			Type:     services.TypeAPI,
			Image:    "cpip-api",
			Version:  "v1.0.0",
			Replicas: 3,
			Ports: []services.PortConfig{
				{Name: "http", ContainerPort: 8080, ServicePort: 80, Protocol: "TCP"},
			},
			Health: probes.FullHealthConfig{
				Liveness: probes.DefaultHTTPProbe("/live", 8080),
			},
		},
	}

	manifest, err := gen.Generate(context.Background(), "cpip-system", "production", svcs)
	if err != nil {
		t.Fatalf("failed to generate k8s manifests: %v", err)
	}

	if !strings.Contains(manifest, "kind: Namespace") {
		t.Error("manifest missing Namespace declaration")
	}
	if !strings.Contains(manifest, "kind: Deployment") {
		t.Error("manifest missing Deployment declaration")
	}
	if !strings.Contains(manifest, "kind: Service") {
		t.Error("manifest missing Service declaration")
	}
	if !strings.Contains(manifest, "kind: Ingress") {
		t.Error("manifest missing Ingress declaration")
	}
}

func TestHelmGenerator(t *testing.T) {
	gen := helm.NewGenerator()
	svcs := []services.Service{
		{
			Name:     "api",
			Image:    "cpip-api",
			Version:  "v1.0",
			Replicas: 2,
		},
	}

	chart, err := gen.Generate(context.Background(), "cpip", "0.1.0", svcs)
	if err != nil {
		t.Fatalf("failed to generate Helm chart: %v", err)
	}

	if chart.Name != "cpip" {
		t.Errorf("expected chart name 'cpip', got %s", chart.Name)
	}

	if _, exists := chart.Files["Chart.yaml"]; !exists {
		t.Error("chart missing Chart.yaml file")
	}
	if _, exists := chart.Files["values.yaml"]; !exists {
		t.Error("chart missing values.yaml file")
	}
}

func TestRollbackRegistry(t *testing.T) {
	reg := rollback.NewRegistry(3)
	svcs := []services.Service{{Name: "api"}}

	// 1. Record snapshots
	reg.RecordSnapshot("staging", svcs, "deploy 1", rollback.StatusSuccess)
	reg.RecordSnapshot("staging", svcs, "deploy 2", rollback.StatusSuccess)
	reg.RecordSnapshot("staging", svcs, "deploy 3", rollback.StatusFailed)

	history := reg.History("staging")
	if len(history) != 3 {
		t.Fatalf("expected 3 history records, got %d", len(history))
	}

	// Limit checks
	reg.RecordSnapshot("staging", svcs, "deploy 4", rollback.StatusSuccess)
	history = reg.History("staging")
	if len(history) > 3 {
		t.Errorf("registry history exceeded max limits: %d", len(history))
	}

	// 2. Fetch by version
	snap, err := reg.GetByVersion("staging", 2)
	if err != nil {
		t.Fatalf("failed to get revision 2: %v", err)
	}
	if snap.Description != "deploy 2" {
		t.Errorf("expected 'deploy 2', got %s", snap.Description)
	}
}

func TestObservabilityMiddlewareAndOrchestration(t *testing.T) {
	// Initialize subsystems
	cfg := config.DefaultPlatformConfig()
	cfg.DefaultProvider = "kubernetes"
	cfg.ActiveProfile = config.ProfileTesting

	profMgr := profiles.NewProfileManager()
	val := validation.NewValidator()
	reg := rollback.NewRegistry(10)
	bus := events.NewBus()
	rec := metrics.NewInMemoryRecorder()
	log := logger.New(nil)

	// Subscribe to events
	var eventMu sync.Mutex
	receivedEvents := make([]events.EventType, 0)
	bus.Subscribe(func(ev events.Event) {
		eventMu.Lock()
		defer eventMu.Unlock()
		receivedEvents = append(receivedEvents, ev.Type)
	})

	mgr := manager.NewManager(cfg, profMgr, val, reg, bus, rec, log)

	// Decorate and register Kubernetes provider
	k8sAdapter := kubernetes.NewProviderAdapter("cpip-test")
	instrumentedK8s := middleware.NewObservabilityProvider(k8sAdapter, rec, log)
	mgr.RegisterProvider(instrumentedK8s)

	// Register Docker Compose provider
	composeAdapter := compose.NewProviderAdapter()
	instrumentedCompose := middleware.NewObservabilityProvider(composeAdapter, rec, log)
	mgr.RegisterProvider(instrumentedCompose)

	// Initialize SDK
	client := sdk.NewClient(mgr)

	svcs := []services.Service{
		{
			Name:    "gateway",
			Type:    services.TypeGateway,
			Image:   "cpip-gateway",
			Version: "1.0",
			Ports: []services.PortConfig{
				{ContainerPort: 8081, ServicePort: 81},
			},
		},
	}

	// 1. Deploy via SDK
	res, err := client.Deploy(context.Background(), svcs)
	if err != nil {
		t.Fatalf("SDK deployment failed: %v", err)
	}
	if !res.Success {
		t.Error("expected successful deployment")
	}

	// Give async events time to distribute
	time.Sleep(100 * time.Millisecond)

	// Check metrics recorder
	attempts := rec.Get(metrics.MetricDeployAttempts)
	successes := rec.Get(metrics.MetricDeploySuccesses)
	if attempts != 1 || successes != 1 {
		t.Errorf("metrics not correctly updated. attempts: %f, successes: %f", attempts, successes)
	}

	// Check events distribution
	eventMu.Lock()
	hasStarted := false
	hasSucceeded := false
	for _, et := range receivedEvents {
		if et == events.DeploymentStarted {
			hasStarted = true
		}
		if et == events.DeploymentSucceeded {
			hasSucceeded = true
		}
	}
	eventMu.Unlock()

	if !hasStarted || !hasSucceeded {
		t.Errorf("did not receive correct lifecycle events: %v", receivedEvents)
	}

	// 2. Rollback via SDK
	rollbackRes, err := client.Rollback(context.Background(), 1)
	if err != nil {
		t.Fatalf("rollback failed: %v", err)
	}
	if !rollbackRes.Success {
		t.Error("expected successful rollback execution")
	}

	// Verify forward rollback transaction recorded
	hist := reg.History("testing")
	if len(hist) < 2 {
		t.Fatalf("expected at least 2 entries in rollback history, got %d", len(hist))
	}
}

func TestConcurrentConcurrencySafety(t *testing.T) {
	cfg := config.DefaultPlatformConfig()
	profMgr := profiles.NewProfileManager()
	val := validation.NewValidator()
	reg := rollback.NewRegistry(100)
	bus := events.NewBus()
	rec := metrics.NewInMemoryRecorder()
	log := logger.New(nil)

	mgr := manager.NewManager(cfg, profMgr, val, reg, bus, rec, log)
	k8sAdapter := kubernetes.NewProviderAdapter("cpip-concurrent")
	mgr.RegisterProvider(k8sAdapter)

	svcs := []services.Service{
		{
			Name:  "web",
			Image: "nginx",
		},
	}

	var wg sync.WaitGroup
	workers := 10

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = mgr.Deploy(context.Background(), svcs)
			_, _ = mgr.Status(context.Background())
			_ = mgr.Config()
		}()
	}

	wg.Wait()
}
