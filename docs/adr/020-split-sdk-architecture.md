# ADR 020: Split SDK Architecture: Execution and Administrative APIs

## Status
Approved

## Context
A monolithic sandbox SDK interface combines operational runtime tasks (e.g., container lifecycle management, image pulls) with administrative tasks (e.g., capability checks, metrics collecting, benchmarking). This risks exposing destructive lifecycle APIs to telemetry/monitoring tools, violating the principle of least privilege.

## Decision
We split the Runtime SDK into two distinct, decoupled interfaces:
1. `ExecutionAPI`: Encapsulates only lifecycle and configuration tasks (`CreateSandbox`, `StartSandbox`, `StopSandbox`, `DestroySandbox`, `PrepareWorkspace`, `CopyFiles`, `CollectLogs`, `Cleanup`).
2. `AdministrativeAPI`: Encapsulates telemetry, capability querying, version management, and monitoring tasks (`Health`, `Capabilities`, `Statistics`, `Version`, `Benchmark`, `Metadata`, `Configuration`).

## Consequences
- Enhanced security profile: Monitoring services can be given only the `AdministrativeAPI` reference, eliminating the threat of accidental or malicious container destruction.
- Cleaner code organization matching domain boundaries.
