# ADR 024: Runtime Version Policy & Compatibility

## Status
Approved

## Context
Various execution runtimes present different API boundaries and host operating system dependencies. Using unsupported version configurations will cause execution failures or security leaks.

## Decision
We enforce a strict `VersionPolicy` (`internal/sandbox/runtime/version`) engine. Runtimes must register their engine version and compatibility lifecycle status (`Supported`, `Deprecated`, `Unsupported`). The policy engine rejects execution or issues deprecation migration recommendations during the negotiation phase.

## Consequences
- Prevents accidental usage of deprecated or insecure older runtime engine versions.
- Ensures all active nodes maintain a verified runtime baseline.
