# ADR 019: Runtime Abstraction Layer Architecture

## Status
Approved

## Context
Our platform execution engine originally coupled directly to a single docker adapter implementation. To support multiple container execution environments (such as gVisor and Firecracker) and dynamically select or fail over among them, we require a clean runtime abstraction layer that defines runtime descriptors, capabilities, and lifecycle methods without leaking Docker-specific settings to the core orchestrator.

## Decision
We introduce a decoupled, package-oriented runtime abstraction architecture under `internal/sandbox/runtime`.
- Define a canonical `RuntimeDescriptor` that records registry information, version compatibility, priority, default flags, and capability maps.
- Maintain a runtime registry, pool, and controller to separate capability matching and resource pooling from execution.
- Maintain a uniform `RuntimeAdapter` interface implemented by concrete plugins (Docker, gVisor, Firecracker).

## Consequences
- Dynamic adapter registration is now decoupled from orchestration logic.
- Adding future runtime environments (such as Firecracker microVMs) requires writing a new adapter implementation with zero changes to the core `SandboxManager` composition root.
