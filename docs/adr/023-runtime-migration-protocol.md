# ADR 023: Runtime Migration Protocol

## Status
Approved

## Context
When runtimes are deprecated, become unhealthy, or when resource demands shift, active sandbox sessions need to transition to other runtimes (e.g. from Docker to gVisor) without disrupting the live collaborative session.

## Decision
We implement a `MigrationFramework` (`internal/sandbox/runtime/migration`) that manages runtime state transitions. It:
1. Validates configuration compatibility with the target runtime.
2. Registers/associates the sandbox ID with the new runtime.
3. Tests the health and connectivity on the new target.
4. Executes automatic rollback if any validation or health check fails during the migration cycle.

## Consequences
- Session state updates remain resilient.
- Safe, zero-downtime rolling upgrades of underlying sandboxes are now natively supported.
