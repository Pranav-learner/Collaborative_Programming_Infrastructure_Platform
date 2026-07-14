package migration

import (
	"context"
	"fmt"
	"time"

	"cpip/internal/sandbox/events"
	"cpip/internal/sandbox/runtime/controller"
	runtimeEvents "cpip/internal/sandbox/runtime/events"
)

// MigrationReport details the outcome of a migration attempt.
type MigrationReport struct {
	SandboxID      string
	SourceRuntime  string
	TargetRuntime  string
	Success        bool
	MigrationTime  time.Duration
	ValidationLogs []string
	Error          string
}

// MigrationFramework manages sandbox switching between runtimes.
type MigrationFramework struct {
	controller *controller.RuntimeController
	bus        *events.Bus
}

// NewMigrationFramework instantiates a MigrationFramework.
func NewMigrationFramework(ctrl *controller.RuntimeController, bus *events.Bus) *MigrationFramework {
	return &MigrationFramework{
		controller: ctrl,
		bus:        bus,
	}
}

// Migrate transitions a sandbox from its current runtime to a target runtime.
func (f *MigrationFramework) Migrate(ctx context.Context, sandboxID string, targetRuntimeID string) (*MigrationReport, error) {
	startTime := time.Now()
	report := &MigrationReport{
		SandboxID:     sandboxID,
		TargetRuntime: targetRuntimeID,
		Success:       false,
	}

	// 1. Get current runtime
	srcRuntime, err := f.controller.GetRuntimeForSandbox(sandboxID)
	if err != nil {
		report.Error = fmt.Sprintf("Failed to locate source runtime: %v", err)
		return report, err
	}
	report.SourceRuntime = srcRuntime

	f.PublishEvent(runtimeEvents.NewRuntimeEvent(runtimeEvents.RuntimeMigrationStarted, targetRuntimeID, "", "Info", "migration"))

	// 2. Perform compatibility checks on target runtime
	targetDesc, err := f.controller.Capabilities(targetRuntimeID)
	if err != nil {
		report.Error = fmt.Sprintf("Target runtime not registered: %v", err)
		f.PublishEvent(runtimeEvents.NewRuntimeEvent(runtimeEvents.RuntimeMigrationCompleted, targetRuntimeID, "", "Critical", "migration"))
		return report, err
	}
	report.ValidationLogs = append(report.ValidationLogs, fmt.Sprintf("Validated target runtime: %s", targetDesc.DisplayName))

	// 3. Perform comparison (e.g. priority comparison or deprecated flags)
	if targetDesc.Deprecated {
		report.ValidationLogs = append(report.ValidationLogs, "Warning: Target runtime is deprecated.")
	}

	// Simulate migration workload (e.g. copying state or re-creating)
	// For this module, we update the controller map to the new target.
	f.controller.MapSandboxToRuntime(sandboxID, targetRuntimeID)
	report.ValidationLogs = append(report.ValidationLogs, fmt.Sprintf("Updated sandbox runtime association to %s", targetRuntimeID))

	// 4. Test target runtime health
	healthStatus, err := f.controller.Health(ctx, sandboxID)
	if err != nil || healthStatus == "Unhealthy" {
		// Rollback on failure!
		f.controller.MapSandboxToRuntime(sandboxID, srcRuntime)
		report.ValidationLogs = append(report.ValidationLogs, "Rollback executed: Reverted sandbox runtime mapping to source runtime.")
		report.Error = "Target runtime health check failed, rolled back."
		f.PublishEvent(runtimeEvents.NewRuntimeEvent(runtimeEvents.RuntimeMigrationCompleted, targetRuntimeID, "", "Warning", "migration"))
		return report, fmt.Errorf("migration failed and rolled back: %s", report.Error)
	}

	report.Success = true
	report.MigrationTime = time.Since(startTime)
	f.PublishEvent(runtimeEvents.NewRuntimeEvent(runtimeEvents.RuntimeMigrationCompleted, targetRuntimeID, targetDesc.Version, "Info", "migration"))

	return report, nil
}

func (f *MigrationFramework) PublishEvent(evt runtimeEvents.RuntimeEvent) {
	if f.bus == nil {
		return
	}
	f.bus.Publish(events.Event{
		Type:      events.AuditRecorded,
		SandboxID: evt.RuntimeID,
		Timestamp: evt.Timestamp,
		Payload:   evt,
	})
}
