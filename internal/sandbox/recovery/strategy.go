package recovery

import (
	"context"
	"fmt"

	"cpip/internal/sandbox/runtime"
	"cpip/internal/sandbox/types"
)

// Classification defines the type of failure.
type Classification string

const (
	Recoverable       Classification = "Recoverable"
	NonRecoverable     Classification = "NonRecoverable"
	Timeout           Classification = "Timeout"
	HostFailure       Classification = "HostFailure"
	SecurityViolation Classification = "SecurityViolation"
	PolicyViolation   Classification = "PolicyViolation"
	ContainerCrash    Classification = "ContainerCrash"
	WorkspaceFailure  Classification = "WorkspaceFailure"
)

// RecoveryStrategy defines the behavior required to recover from specific classifications of failures.
type RecoveryStrategy interface {
	Classification() Classification
	CanRecover(ctx context.Context, sess *types.SandboxSession, err error) bool
	Recover(ctx context.Context, sess *types.SandboxSession, adapter runtime.RuntimeAdapter) error
	CleanupRequired() bool
	EscalationPolicy() string
}

// ContainerCrashStrategy recovers from container crashes.
type ContainerCrashStrategy struct{}

func (s *ContainerCrashStrategy) Classification() Classification { return ContainerCrash }
func (s *ContainerCrashStrategy) CanRecover(ctx context.Context, sess *types.SandboxSession, err error) bool {
	// Recover if container exited with non-zero or crashed but still exists
	return sess.GetContainerID() != ""
}
func (s *ContainerCrashStrategy) Recover(ctx context.Context, sess *types.SandboxSession, adapter runtime.RuntimeAdapter) error {
	cID := sess.GetContainerID()
	if cID == "" {
		return fmt.Errorf("no container to restart")
	}
	info, err := adapter.InspectContainer(ctx, cID)
	if err != nil {
		return err
	}
	if !info.Running {
		return adapter.StartContainer(ctx, cID)
	}
	return nil
}
func (s *ContainerCrashStrategy) CleanupRequired() bool { return false }
func (s *ContainerCrashStrategy) EscalationPolicy() string { return "recreate" }

// TimeoutStrategy handles timed-out executions.
type TimeoutStrategy struct{}

func (s *TimeoutStrategy) Classification() Classification { return Timeout }
func (s *TimeoutStrategy) CanRecover(ctx context.Context, sess *types.SandboxSession, err error) bool {
	return true
}
func (s *TimeoutStrategy) Recover(ctx context.Context, sess *types.SandboxSession, adapter runtime.RuntimeAdapter) error {
	// Timeout recovery involves stopping container execution gracefully then forcefully
	cID := sess.GetContainerID()
	if cID != "" {
		_ = adapter.StopContainer(ctx, cID, 0)
	}
	return nil
}
func (s *TimeoutStrategy) CleanupRequired() bool { return true }
func (s *TimeoutStrategy) EscalationPolicy() string { return "terminate" }

// PolicyViolationStrategy handles CPU/Memory policy limits exceeded.
type PolicyViolationStrategy struct{}

func (s *PolicyViolationStrategy) Classification() Classification { return PolicyViolation }
func (s *PolicyViolationStrategy) CanRecover(ctx context.Context, sess *types.SandboxSession, err error) bool {
	return false // Policy violations are unrecoverable; must terminate immediately
}
func (s *PolicyViolationStrategy) Recover(ctx context.Context, sess *types.SandboxSession, adapter runtime.RuntimeAdapter) error {
	cID := sess.GetContainerID()
	if cID != "" {
		_ = adapter.StopContainer(ctx, cID, 0)
	}
	return nil
}
func (s *PolicyViolationStrategy) CleanupRequired() bool { return true }
func (s *PolicyViolationStrategy) EscalationPolicy() string { return "kill" }

// SecurityViolationStrategy handles security/sandbox escape detection.
type SecurityViolationStrategy struct{}

func (s *SecurityViolationStrategy) Classification() Classification { return SecurityViolation }
func (s *SecurityViolationStrategy) CanRecover(ctx context.Context, sess *types.SandboxSession, err error) bool {
	return false
}
func (s *SecurityViolationStrategy) Recover(ctx context.Context, sess *types.SandboxSession, adapter runtime.RuntimeAdapter) error {
	cID := sess.GetContainerID()
	if cID != "" {
		_ = adapter.StopContainer(ctx, cID, 0)
	}
	return nil
}
func (s *SecurityViolationStrategy) CleanupRequired() bool { return true }
func (s *SecurityViolationStrategy) EscalationPolicy() string { return "kill_immediate" }

// WorkspaceFailureStrategy handles read-only filesystems or deleted workdirs.
type WorkspaceFailureStrategy struct{}

func (s *WorkspaceFailureStrategy) Classification() Classification { return WorkspaceFailure }
func (s *WorkspaceFailureStrategy) CanRecover(ctx context.Context, sess *types.SandboxSession, err error) bool {
	return false // Cannot easily restore workspace state on raw host failure
}
func (s *WorkspaceFailureStrategy) Recover(ctx context.Context, sess *types.SandboxSession, adapter runtime.RuntimeAdapter) error {
	return fmt.Errorf("workspace failure cannot be automatically recovered")
}
func (s *WorkspaceFailureStrategy) CleanupRequired() bool { return true }
func (s *WorkspaceFailureStrategy) EscalationPolicy() string { return "recreate_workspace" }

// NonRecoverableStrategy is a fallback for fatal issues.
type NonRecoverableStrategy struct{}

func (s *NonRecoverableStrategy) Classification() Classification { return NonRecoverable }
func (s *NonRecoverableStrategy) CanRecover(ctx context.Context, sess *types.SandboxSession, err error) bool {
	return false
}
func (s *NonRecoverableStrategy) Recover(ctx context.Context, sess *types.SandboxSession, adapter runtime.RuntimeAdapter) error {
	return fmt.Errorf("non-recoverable failure")
}
func (s *NonRecoverableStrategy) CleanupRequired() bool { return true }
func (s *NonRecoverableStrategy) EscalationPolicy() string { return "teardown" }
