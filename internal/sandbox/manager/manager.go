package manager

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"time"

	"cpip/internal/sandbox/config"
	"cpip/internal/sandbox/events"
	"cpip/internal/sandbox/filesystem"
	"cpip/internal/sandbox/images"
	"cpip/internal/sandbox/lifecycle"
	"cpip/internal/sandbox/metrics"
	"cpip/internal/sandbox/network"
	"cpip/internal/sandbox/registry"
	"cpip/internal/sandbox/runtime"
	"cpip/internal/sandbox/security/audit"
	"cpip/internal/sandbox/security/engine"
	"cpip/internal/sandbox/security/monitor"
	"cpip/internal/sandbox/security/policies"
	"cpip/internal/sandbox/security/resolver"
	"cpip/internal/sandbox/types"
	"cpip/internal/sandbox/volumes"
	"cpip/internal/sandbox/workspace"
)

// SandboxManager orchestrates the lifecycle, workspaces, filesystem operations, and container runtimes.
type SandboxManager struct {
	cfg         config.Config
	adapter     runtime.RuntimeAdapter
	reg         *registry.SandboxRegistry
	bus         *events.Bus
	recorder    metrics.Recorder
	lifecycle   *lifecycle.LifecycleManager
	images      *images.ImageManager
	workspace   *workspace.WorkspaceManager
	filesystem  *filesystem.FilesystemManager
	network     *network.NetworkManager
	volumes     *volumes.VolumeManager
	policyReg   policies.Registry
	policyVal   policies.Validator
	resolver    *resolver.PolicyResolver
	resEngine   *engine.ResourcePolicyEngine
	secEngine   *engine.SecurityPolicyEngine
	monitor     *monitor.ResourceMonitor
	auditLogger *audit.AuditLogger
}

// NewSandboxManager initializes a SandboxManager composition root.
func NewSandboxManager(cfg config.Config, adapter runtime.RuntimeAdapter, rec metrics.Recorder) *SandboxManager {
	if rec == nil {
		rec = metrics.NewInMemRecorder()
	}
	bus := events.NewBus()
	reg := registry.NewSandboxRegistry()

	polReg := policies.NewMemRegistry()
	polVal := policies.NewPolicyValidator(policies.DefaultBounds)
	resv := resolver.NewPolicyResolver(polReg, polVal)
	resEng := engine.NewResourcePolicyEngine()
	secEng := engine.NewSecurityPolicyEngine()
	audLog := audit.NewAuditLogger(bus, nil)
	mon := monitor.NewResourceMonitor(reg, adapter, bus, rec, 100*time.Millisecond)

	mgr := &SandboxManager{
		cfg:         cfg,
		adapter:     adapter,
		reg:         reg,
		bus:         bus,
		recorder:    rec,
		lifecycle:   lifecycle.NewLifecycleManager(bus, rec),
		images:      images.NewImageManager(cfg, adapter, bus),
		workspace:   workspace.NewWorkspaceManager(cfg),
		filesystem:  filesystem.NewFilesystemManager(),
		network:     network.NewNetworkManager(cfg, rec),
		volumes:     volumes.NewVolumeManager(cfg, rec),
		policyReg:   polReg,
		policyVal:   polVal,
		resolver:    resv,
		resEngine:   resEng,
		secEngine:   secEng,
		monitor:     mon,
		auditLogger: audLog,
	}

	mon.RegisterViolationHandler(func(ctx context.Context, id string, reason string) {
		_ = mgr.Stop(ctx, id, 1*time.Second)
		audLog.Record("violation_detected", id, "", reason, nil)
	})

	mon.Start(context.Background())

	return mgr
}

// CreateSandbox registers, prepares directory workspace, pulls image and creates the container.
func (sm *SandboxManager) CreateSandbox(ctx context.Context, jobID, language string, expiration time.Duration, secProfile string, resProfile string, custom map[string]any) (*types.SandboxSession, error) {
	// 1. Generate Sandbox ID early to associate with logs/audit
	sbID := generateID()

	// 2. Resolve policies and validate
	resPolicy, err := sm.resolver.ResolveResourcePolicy(resProfile, custom)
	if err != nil {
		sm.auditLogger.Record("execution_denied", sbID, jobID, fmt.Sprintf("failed to resolve resource policy: %v", err), nil)
		return nil, fmt.Errorf("resource policy resolve: %w", err)
	}

	secPolicy, err := sm.resolver.ResolveSecurityPolicy(secProfile, custom)
	if err != nil {
		sm.auditLogger.Record("execution_denied", sbID, jobID, fmt.Sprintf("failed to resolve security policy: %v", err), nil)
		return nil, fmt.Errorf("security policy resolve: %w", err)
	}

	// Publish PolicyResolved and Profile Applied events
	sm.bus.Publish(events.Event{
		Type:      events.PolicyResolved,
		SandboxID: sbID,
		JobID:     jobID,
		Timestamp: time.Now(),
		Payload:   map[string]any{"security": secPolicy.ID, "resource": resPolicy.ID},
	})

	sm.bus.Publish(events.Event{
		Type:      events.SecurityProfileApplied,
		SandboxID: sbID,
		JobID:     jobID,
		Timestamp: time.Now(),
		Payload:   secPolicy,
	})

	sm.bus.Publish(events.Event{
		Type:      events.ResourceProfileApplied,
		SandboxID: sbID,
		JobID:     jobID,
		Timestamp: time.Now(),
		Payload:   resPolicy,
	})

	// Map to limits/settings
	resLimits := sm.resEngine.CreateResourceLimits(resPolicy)
	secSettings, sanitizedEnv := sm.secEngine.CreateSecuritySettings(secPolicy, nil)

	// Map language to image
	img, err := sm.images.GetImageForLanguage(language)
	if err != nil {
		return nil, err
	}

	// 3. Create initial session
	now := time.Now()
	sess := &types.SandboxSession{
		ID:               sbID,
		JobID:            jobID,
		Language:         language,
		Image:            img,
		State:            types.StateCreated,
		CreatedAt:        now,
		ExpiresAt:        now.Add(expiration),
		Metadata:         make(map[string]string),
		MemoryLimitBytes: resLimits.MemoryBytes,
		ProcessLimit:     resLimits.ProcessLimit,
	}

	sm.reg.Register(sess)

	// Publish Created event
	if err := sm.lifecycle.Transition(sess, types.StateCreated); err != nil {
		sm.reg.Unregister(sbID)
		return nil, err
	}

	// 4. Transition to Preparing
	if err := sm.lifecycle.Transition(sess, types.StatePreparing); err != nil {
		_ = sm.DestroySandbox(ctx, sbID)
		return nil, err
	}

	// Prepare Workspace
	wkPath, err := sm.workspace.PrepareWorkspace(sbID)
	if err != nil {
		_ = sm.DestroySandbox(ctx, sbID)
		return nil, fmt.Errorf("workspace preparation failed: %w", err)
	}
	sess.SetWorkspacePath(wkPath)

	// Pull Image if needed
	if err := sm.images.PullIfNeeded(ctx, img); err != nil {
		_ = sm.DestroySandbox(ctx, sbID)
		return nil, fmt.Errorf("image pulling failed: %w", err)
	}

	// Fetch network name
	netName, err := sm.network.GetNetworkName(ctx)
	if err != nil {
		_ = sm.DestroySandbox(ctx, sbID)
		return nil, fmt.Errorf("network resolve failed: %w", err)
	}
	sess.SetNetwork(netName)

	// Bind mounts
	binds, err := sm.volumes.GetBinds(wkPath)
	if err != nil {
		_ = sm.DestroySandbox(ctx, sbID)
		return nil, fmt.Errorf("volume binding resolution failed: %w", err)
	}
	sess.SetMounts(binds)

	// 5. Transition to Container Created
	if err := sm.lifecycle.Transition(sess, types.StateContainerCreated); err != nil {
		_ = sm.DestroySandbox(ctx, sbID)
		return nil, err
	}

	// Create Container (infinitely running sleep block to allow workspace edits and execs)
	containerName := fmt.Sprintf(sm.cfg.ContainerNamingPat, sbID)
	cmd := []string{"sh", "-c", "while true; do sleep 3600; done"}
	cfg := runtime.ContainerConfig{
		Image:     img,
		Cmd:       cmd,
		Env:       sanitizedEnv,
		Binds:     binds,
		Network:   netName,
		Name:      containerName,
		Resources: resLimits,
		Security:  secSettings,
	}

	cID, err := sm.adapter.CreateContainer(ctx, cfg)
	if err != nil {
		_ = sm.DestroySandbox(ctx, sbID)
		return nil, fmt.Errorf("container runtime create failed: %w", err)
	}
	sess.SetContainerID(cID)

	sm.auditLogger.Record("policy_applied", sbID, jobID, "applied sandbox security and resource policy profiles", map[string]any{
		"security_profile": secPolicy.ID,
		"resource_profile": resPolicy.ID,
	})

	// Start container to make it ready
	if err := sm.adapter.StartContainer(ctx, cID); err != nil {
		_ = sm.DestroySandbox(ctx, sbID)
		return nil, fmt.Errorf("failed to start container: %w", err)
	}

	// Publish ContainerStarted event
	sm.bus.Publish(events.Event{
		Type:      events.ContainerStarted,
		SandboxID: sbID,
		JobID:     sess.JobID,
		Timestamp: time.Now(),
		Payload:   sess,
	})

	// 6. Transition to Ready
	if err := sm.lifecycle.Transition(sess, types.StateReady); err != nil {
		_ = sm.DestroySandbox(ctx, sbID)
		return nil, err
	}

	return sess, nil
}

// DestroySandbox stops the container, cleans up mounts/volumes, deletes the workspace and removes the container.
func (sm *SandboxManager) DestroySandbox(ctx context.Context, sandboxID string) error {
	sess, err := sm.reg.Get(sandboxID)
	if err != nil {
		return err
	}

	// Unregister early to prevent concurrent access
	defer sm.reg.Unregister(sandboxID)

	// Transition to Cleaning
	_ = sm.lifecycle.Transition(sess, types.StateCleaning)

	// Gracefully/Forcefully stop container if it was created
	cID := sess.GetContainerID()
	if cID != "" {
		_ = sm.adapter.StopContainer(ctx, cID, sm.cfg.ContainerTimeout)
		_ = sm.adapter.RemoveContainer(ctx, cID)
	}

	// Cleanup workspace directory
	wkPath := sess.GetWorkspacePath()
	if wkPath != "" {
		_ = sm.workspace.CleanupWorkspace(wkPath)
	}

	// Transition to Destroyed
	_ = sm.lifecycle.Transition(sess, types.StateDestroyed)

	return nil
}

// Start transitions the sandbox container into the running state.
func (sm *SandboxManager) Start(ctx context.Context, sandboxID string) error {
	sess, err := sm.reg.Get(sandboxID)
	if err != nil {
		return err
	}

	cID := sess.GetContainerID()
	if cID == "" {
		return fmt.Errorf("cannot start sandbox: container not created")
	}

	info, err := sm.adapter.InspectContainer(ctx, cID)
	if err != nil || !info.Running {
		if err := sm.adapter.StartContainer(ctx, cID); err != nil {
			return err
		}
	}

	return sm.lifecycle.Transition(sess, types.StateExecuting)
}

// Stop stops the sandbox container gracefully.
func (sm *SandboxManager) Stop(ctx context.Context, sandboxID string, timeout time.Duration) error {
	sess, err := sm.reg.Get(sandboxID)
	if err != nil {
		return err
	}

	cID := sess.GetContainerID()
	if cID == "" {
		return fmt.Errorf("cannot stop sandbox: container not created")
	}

	if err := sm.adapter.StopContainer(ctx, cID, timeout); err != nil {
		return err
	}

	sm.bus.Publish(events.Event{
		Type:      events.ContainerStopped,
		SandboxID: sess.ID,
		JobID:     sess.JobID,
		Timestamp: time.Now(),
		Payload:   sess,
	})

	return sm.lifecycle.Transition(sess, types.StateReady)
}

// CopyFiles copies scripts, source files or inputs directly into the sandbox workspace.
func (sm *SandboxManager) CopyFiles(ctx context.Context, sandboxID string, files map[string]string) error {
	sess, err := sm.reg.Get(sandboxID)
	if err != nil {
		return err
	}
	return sm.filesystem.InjectFiles(sess.GetWorkspacePath(), files)
}

// CollectLogs streams standard stdout and stderr logs from the running sandbox container.
func (sm *SandboxManager) CollectLogs(ctx context.Context, sandboxID string, stdout, stderr io.Writer) error {
	sess, err := sm.reg.Get(sandboxID)
	if err != nil {
		return err
	}

	cID := sess.GetContainerID()
	if cID == "" {
		return fmt.Errorf("cannot collect logs: container not created")
	}

	return sm.adapter.GetContainerLogs(ctx, cID, stdout, stderr)
}

// Inspect retrieves active sandbox session metadata and runtime statuses.
func (sm *SandboxManager) Inspect(ctx context.Context, sandboxID string) (*types.SandboxSession, error) {
	sess, err := sm.reg.Get(sandboxID)
	if err != nil {
		return nil, err
	}

	cID := sess.GetContainerID()
	if cID != "" {
		info, err := sm.adapter.InspectContainer(ctx, cID)
		if err == nil {
			sess.SetStatus(info.State)
			if info.Running {
				sess.SetStatus("running")
			}
		}
	}

	return sess, nil
}

// Statistics fetches CPU and Memory utilization statistics for the container.
func (sm *SandboxManager) Statistics(ctx context.Context, sandboxID string) (types.Stats, error) {
	sess, err := sm.reg.Get(sandboxID)
	if err != nil {
		return types.Stats{}, err
	}

	cID := sess.GetContainerID()
	if cID == "" {
		return types.Stats{}, fmt.Errorf("cannot fetch stats: container not created")
	}

	return sm.adapter.GetContainerStats(ctx, cID)
}

// Registry returns the active SandboxRegistry.
func (sm *SandboxManager) Registry() *registry.SandboxRegistry {
	return sm.reg
}

// EventBus returns the pub/sub events.Bus instance.
func (sm *SandboxManager) EventBus() *events.Bus {
	return sm.bus
}

func (sm *SandboxManager) GetPolicyRegistry() policies.Registry {
	return sm.policyReg
}

func (sm *SandboxManager) GetAuditLogger() *audit.AuditLogger {
	return sm.auditLogger
}

// Helper to generate secure random IDs
func generateID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
