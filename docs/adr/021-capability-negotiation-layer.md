# ADR 021: Capability Negotiation Layer

## Status
Approved

## Context
When executing untrusted code or tasks requiring specific resources (e.g., networking, GPU acceleration, or custom limits), hardcoding execution configurations leads to brittle runtimes and security vulnerabilities. A runtime must be selected dynamically based on requirements compared to advertised capabilities.

## Decision
We introduce a capability negotiation layer (`internal/sandbox/runtime/negotiation`) that matches requested `ExecutionRequirements` (required feature flags, language, security, resource profiles) against registered `RuntimeDescriptor` capability maps. The negotiation manager generates compatibility audits and selects the runtime with the highest priority score.

## Consequences
- Allows automated fallbacks: if gVisor is not healthy or available, the system can fallback to a standard Docker runtime or return a clear negotiation report explaining why the execution is blocked.
- Eliminates hardcoded selection logic in the orchestrator.
