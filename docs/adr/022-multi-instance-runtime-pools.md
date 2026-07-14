# ADR 022: Multi-Instance Runtime Pools

## Status
Approved

## Context
High-concurrency code execution requires scaling runtime adapters to multiple daemon connections or distributed sandboxes. Managing a single global connection introduces resource contention and limits execution scalability.

## Decision
We introduce a thread-safe `RuntimePool` (`internal/sandbox/runtime/pool`) managing multiple `RuntimeInstance` registrations per runtime ID. Leases are distributed using a connection count load balancer (least-connection algorithm), tracking and releasing adapter resources atomically.

## Consequences
- Enables elastic runtime scaling (e.g., dynamically spawning more runsc runtimes when load increases).
- Thread-safe acquisition and connection multiplexing.
- Prepares the platform for distributed multi-node sandbox clustering.
