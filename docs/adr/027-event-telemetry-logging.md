# ADR 027: Event Telemetry & Structured Logging

## Status
Approved

## Context
Tracing executions and identifying failure causes (e.g. why did a compatibility check fail?) is difficult in decoupled distributed execution layers. We need a standardized event structure and structured logging format for tracing operational transitions.

## Decision
We enforce a standardized `RuntimeEvent` model carrying sequence numbers, severity, origin, correlation IDs, and runtime metadata. Lifecycle events (registered, loaded, selected, health changed, migration started/completed, benchmarks) publish to the core asynchronous `events.Bus`. In parallel, we use structured logging (`slog`) for immediate stdout tracing.

## Consequences
- Complete audit trail of container execution runtimes.
- Easy integration with downstream APM/log aggregation platforms.
