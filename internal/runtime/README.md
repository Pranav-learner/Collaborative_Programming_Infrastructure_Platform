# Runtime Subsystem

The `internal/runtime` package implements a modular, adapter-based code execution engine and progress-streaming pipeline for the Collaborative Programming Infrastructure Platform (CPIP). It orchestrates job execution and outputs stdout/stderr chunks and resource statistics in real-time.

## Architecture

The runtime subsystem is composed of four main components:
1. **Manager (`manager/manager.go`)**: The composition root and public facade. It tracks active jobs, handles lifecycle events (e.g. cancellation), and handles language registration checks.
2. **Pipeline (`pipeline/pipeline.go`)**: Orchestrates the end-to-end execution lifecycle of a single job. It coordinates validation, workspace setup, compilation (if needed), limit-bounded execution, statistics collection, streaming, and final cleanup.
3. **Language Adapters (`adapter/`)**: Abstract interface (`LanguageAdapter`) and concrete host-level executors using `os/exec` for:
   - Python
   - Go
   - Bash
   - C
   - C++
   - Java
4. **Buffer & Streaming (`buffer/` & `stream/`)**:
   - `LimitBuffer` and `SharedCounter`: Track individual and joint stdout/stderr buffers to prevent memory overflows.
   - `StreamManager`: Manages asynchronous streaming of output chunks and execution progress events.

---

## Design and Safety

- **Graceful Timeout & Process Escalation**: Process termination uses a non-blocking timeout watcher that escalates from SIGTERM to SIGKILL after a configurable grace period, avoiding zombie process leakage.
- **Unified Workspace Preparation**: The workspace directory and source file are created consistently across both compiled and interpreted languages via standardizing `Compile`.
- **Decoupled Isolation Strategy**: Adapters are isolated from orchestrator state, allowing easy future swapping of host-level executors with sandboxed container runtimes (e.g. Docker or gVisor).
- **Buffered Output Limits**: Tracks output size in real-time. If the limit is exceeded, it writes up to the allowed amount, triggers an overflow callback, cancels execution, and raises `ErrOutputLimitExceeded`.
