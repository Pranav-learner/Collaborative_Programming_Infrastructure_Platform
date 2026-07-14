# ADR 026: Benchmark Suite & Metric Gathering

## Status
Approved

## Context
Each execution runtime has different latency and resource utilization profiles (e.g. gVisor adds startup overhead compared to raw Docker container execution). To make informed, data-driven decisions on scheduling and routing execution requests, we need standardized benchmarks measuring performance across all adapters.

## Decision
We introduce a structured `BenchmarkFramework` (`internal/sandbox/runtime/benchmark`) that encapsulates multiple sub-benchmarks:
- `StartupBenchmark`: Measures start latency of containers.
- `ExecutionBenchmark`: Measures basic shell execution speeds.
- `FilesystemBenchmark`: Measures I/O throughput.
- `ResourceBenchmark`: Tracks host memory and CPU utilization overhead.
- `StressBenchmark`: Tests performance under peak concurrent requests.
- `CompatibilityBenchmark`: Measures policy evaluation speeds.

## Consequences
- Administrators can compare the cost/performance of Docker versus gVisor.
- Empowers the capability negotiation layer with active performance metrics to make load-distribution decisions.
