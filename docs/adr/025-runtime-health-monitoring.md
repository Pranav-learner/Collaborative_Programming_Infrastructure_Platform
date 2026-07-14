# ADR 025: Runtime Health Monitoring & Heartbeat Protocol

## Status
Approved

## Context
Orchestrating code execution across dynamic backends requires up-to-date health information. If an adapter's connection to the daemon drops, the system must immediately identify the failure to redirect incoming workloads.

## Decision
We introduce the `RuntimeHealthManager` (`internal/sandbox/runtime/health`) that tracks:
- Heartbeats
- Execution latency
- Successive failure counts
- Health state snapshots (e.g. `Healthy`, `Degraded`, `Unhealthy`)
Health state transitions publish events on the core events bus, prompting automatic failover or warnings.

## Consequences
- The orchestrator can dynamically bypass degraded/unhealthy runtime adapters during capability negotiation.
- Enables self-healing monitoring configurations.
