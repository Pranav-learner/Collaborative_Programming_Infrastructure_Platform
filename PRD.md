# Collaborative Programming Infrastructure Platform (CPIP)
## Product Requirements Document

| Field | Value |
|---|---|
| **Document status** | Draft v1.0 — pre-implementation |
| **Document owner** | Principal Architect |
| **Last updated** | 2026-07-12 |
| **Audience** | Senior engineering team, technical stakeholders |
| **Classification** | Single source of truth (SSOT) — supersedes ad-hoc design notes |
| **Scope of this document** | Product & engineering requirements only. No code, APIs, schemas, or package layout. |

---

## Table of Contents

1. Executive Summary
2. Product Vision
3. Project Objectives
4. Scope
5. Personas
6. Use Cases
7. Functional Requirements
8. Non-Functional Requirements
9. Engineering Constraints
10. Risks
11. Assumptions
12. Success Criteria
13. Learning Outcomes
14. Development Principles
15. Future Roadmap
16. Conclusion

---

# 1. Executive Summary

## 1.1 Vision

To build a production-inspired backend **infrastructure platform** that lets multiple developers collaboratively edit code in real time and securely execute untrusted code inside isolated sandboxes — and, in doing so, to serve as a rigorous, end-to-end curriculum in modern backend and distributed systems engineering.

CPIP is the plumbing beneath collaborative developer products, not a product itself. It is the layer that Google Docs, Replit, CodeSandbox, GitHub Codespaces, Judge0, and AWS Lambda each had to build before their user-facing features could exist.

## 1.2 Mission

Deliver a horizontally scalable, observable, and secure backend that provides two primary capabilities as first-class infrastructure services:

1. **Real-time collaborative editing** — many concurrent editors converging on a single consistent document state with sub-100ms perceived latency and live presence.
2. **Secure untrusted code execution** — arbitrary user-submitted code executed inside strongly isolated sandboxes with strict resource limits and streamed results.

Every subsystem must be built to production standards — logging, metrics, graceful degradation, backpressure, and horizontal scale — so the platform doubles as a reference implementation of how such systems are actually engineered.

## 1.3 Why This Project Exists

Most engineers learn backend concepts in isolation: a tutorial on WebSockets here, a blog post on CRDTs there, a toy Docker demo elsewhere. Very few get to see how **networking, concurrency, distributed state, streaming, containerization, and observability compose into a single coherent system** under real constraints.

CPIP exists to close that gap. It is deliberately scoped to force contact with the hard, foundational problems of backend engineering:

- How do you keep distributed state consistent without a single lock?
- How do you run code you cannot trust without endangering the host?
- How do you stream results from thousands of concurrent jobs without falling over?
- How do you scale a stateful, connection-oriented service horizontally?
- How do you observe, debug, and reason about a system with many moving parts?

These are the problems that separate application developers from infrastructure engineers. CPIP is the vehicle for crossing that line.

## 1.4 Problem Statement

Collaborative developer tools depend on two notoriously difficult infrastructure capabilities that are almost always treated as solved black boxes:

- **Real-time synchronization at scale.** Coordinating concurrent edits from many clients — with conflict resolution, presence, offline tolerance, and low latency — across a horizontally scaled, stateful fleet is an unsolved-by-default problem. Naive approaches (last-write-wins, server-side locking, full-document broadcast) collapse under concurrency.
- **Safe execution of untrusted code.** Running arbitrary code submitted by anonymous users is one of the most dangerous things a backend can do. Without strong isolation, resource limits, and a hardened execution pipeline, it is a direct path to host compromise, resource exhaustion, and lateral movement.

There is no widely available, well-documented, infrastructure-first reference platform that demonstrates **both** capabilities together, built with production discipline, and explicitly designed to teach the underlying engineering. CPIP fills that void.

## 1.5 Engineering Motivation

The platform is intentionally engineered to require — not merely permit — mastery of:

- **Realtime networking:** WebSocket lifecycle, backpressure, heartbeats, reconnection, fan-out.
- **Distributed systems:** consistency models, CRDTs, presence propagation, coordination across nodes.
- **Concurrency & synchronization:** Go goroutines, channels, worker pools, bounded queues, race-free shared state.
- **Streaming:** Redis Streams as a durable log, consumer groups, at-least-once delivery, ordered output streaming.
- **Operating systems & isolation:** Linux namespaces, cgroups, seccomp, the container/host boundary, gVisor's user-space kernel.
- **Production engineering:** structured logging, metrics, health checks, graceful shutdown, deployment, and horizontal scaling.

The success of this project is measured less by the features shipped than by the **depth of engineering understanding** the build forces and demonstrates.

---

# 2. Product Vision

## 2.1 What CPIP Is

CPIP is a **backend infrastructure platform** exposing two core infrastructure services and the connective tissue around them:

- A **real-time collaboration engine** — session/room management, CRDT-based document synchronization, and a presence system, delivered over a WebSocket gateway.
- A **secure code execution engine** — a job intake pipeline, concurrent worker pool, sandboxed runtime, resource governance, and result streaming.
- The **supporting platform** — authentication, persistence, configuration, observability, and a deployment topology capable of running multiple stateful nodes behind a load balancer.

It is designed to be operated, scaled, observed, and reasoned about like a real production system.

## 2.2 What CPIP Is NOT

CPIP is explicitly **not** an application built on top of this infrastructure. It is not, and will not become:

- A LeetCode or competitive-programming clone.
- A coding-interview product (scheduling, scoring, proctoring, question banks).
- An AI coding assistant or LLM-powered feature layer.
- A resume parser or recruiting tool.
- A Learning Management System (courses, quizzes, grades).
- A social coding network (feeds, follows, likes).
- A polished end-user IDE product.

Those are *applications*. CPIP is the *infrastructure those applications would be built on*. The frontend that exists in this project is a **thin reference client** whose sole purpose is to exercise and demonstrate the backend — not a product surface.

> **Litmus test for every proposed feature:** *Does it teach a backend/distributed-systems/OS/production concept?* If not, it does not belong in CPIP.

## 2.3 Long-Term Vision

Over time, CPIP should mature into a **reference-grade infrastructure platform** that:

- Demonstrates the full lifecycle of a stateful, real-time, horizontally scalable backend.
- Provides a migration path from "good enough" primitives to production-grade equivalents (Docker → gVisor → Firecracker; single-region → multi-region; static workers → autoscaling).
- Serves as a portfolio-defining, senior-level demonstration of backend engineering competence.
- Remains small enough in surface area to be fully understood by one engineer, yet deep enough to exercise every major backend discipline.

## 2.4 Core Philosophy

**Infrastructure over features. Depth over breadth. Production discipline over prototype convenience.**

Every subsystem is chosen and shaped to expose a fundamental engineering concept in its natural, load-bearing context — never as a toy demo, always as a component that must actually work under concurrency, failure, and scale.

## 2.5 Guiding Principles

1. **Every feature must teach.** If a capability doesn't map to a backend engineering concept, it is out of scope.
2. **Build the boring, hard parts well.** Backpressure, graceful shutdown, idempotency, and observability are the point — not afterthoughts.
3. **Correctness under concurrency is non-negotiable.** The system must be race-free and consistent by design, not by luck.
4. **Security is a first-class requirement, not a phase.** Untrusted code is assumed hostile from day one.
5. **Observable by default.** If you can't measure it, you can't claim it works.
6. **Simplicity first, sophistication when justified.** Reach for the simplest primitive that meets the requirement; upgrade only when a measured limit demands it.
7. **Horizontal scalability is a design constraint, not a future retrofit.** State is externalized and nodes are replaceable from the start.

---

# 3. Project Objectives

## 3.1 Primary Objectives

- **P1 — Real-time collaborative editing.** Support multiple concurrent editors on a shared document that converges to a single consistent state via CRDTs, with live cursors and presence.
- **P2 — Secure untrusted code execution.** Execute arbitrary user-submitted code inside isolated sandboxes with enforced CPU, memory, time, process, filesystem, and network limits.
- **P3 — Streaming execution results.** Stream stdout/stderr/status from running jobs back to clients incrementally and in order, backed by a durable log.
- **P4 — Horizontally scalable, stateful backend.** Run multiple backend nodes behind a load balancer with externalized state so any node can serve any client.
- **P5 — Production-grade operability.** Structured logging, metrics, health checks, graceful shutdown, and a repeatable deployment.

## 3.2 Secondary Objectives

- **S1 — Session/room lifecycle management** with authentication and authorization.
- **S2 — Persistence & history** — durable document state and execution records.
- **S3 — Presence & awareness** — who is in a room, where their cursor is, connection status.
- **S4 — Configuration management** — environment-driven, per-deployment configuration.
- **S5 — Resilience** — reconnection, backpressure handling, and graceful degradation under load and partial failure.

## 3.3 Learning Objectives

The project is instrumented to build and demonstrate competence in:

- WebSocket architecture and real-time networking.
- CRDT-based synchronization and eventual consistency.
- Presence and distributed shared state.
- Go concurrency: goroutines, channels, worker pools, context propagation, cancellation.
- Streaming with Redis Streams: consumer groups, delivery semantics, ordering.
- Containerization and Linux isolation: namespaces, cgroups, seccomp.
- Sandboxing and the security model of running untrusted code (Docker → gVisor).
- Resource management and enforcement.
- Observability: logging, metrics, dashboards.
- Deployment, reverse proxying, CI/CD, and horizontal scaling.

## 3.4 Business Objectives (Contextual)

CPIP is a learning-and-portfolio project, not a commercial product, so "business objectives" are reframed as **strategic outcomes**:

- **Demonstrable senior/staff-level backend competence** through a coherent, non-trivial system.
- **A reusable reference architecture** for anyone building collaborative or code-execution products.
- **A credible interview and portfolio artifact** that showcases depth in distributed systems, OS, and production engineering.

There is no revenue, pricing, or go-to-market objective in scope.

---

# 4. Scope

## 4.1 In Scope

**Collaboration**
- Room/session creation, joining, and lifecycle.
- CRDT-based real-time document synchronization (via Yjs on the client, with the backend as relay/persistence authority).
- Presence and awareness (participant list, cursors, selections, connection state).
- WebSocket gateway with connection lifecycle, heartbeats, and reconnection support.

**Code Execution**
- Job submission and intake pipeline.
- Concurrent worker pool consuming jobs from a durable queue.
- Sandboxed execution in isolated containers with enforced resource limits.
- Ordered, incremental streaming of execution output and status back to clients.
- Execution history/records persistence.

**Platform**
- Authentication and authorization for rooms and execution.
- Persistence of document state and execution records (PostgreSQL).
- Redis Streams as the execution job log and output channel.
- Structured logging and metrics; health/readiness endpoints.
- Deployment via Docker Compose with Nginx as reverse proxy/load balancer.
- CI/CD via GitHub Actions.
- Horizontal scalability of stateless-per-connection backend nodes with externalized state.

**Reference client**
- A minimal React/TypeScript/Monaco frontend that exercises collaboration, presence, execution, and streaming — as a test harness, not a product.

## 4.2 Out of Scope

- Any application-layer product: interview platform, LeetCode clone, LMS, social/coding network, competitive-programming site, resume tooling.
- AI/LLM features of any kind (assistants, autocompletion beyond editor defaults, code review bots).
- Rich end-user product UX (theming systems, dashboards, billing, teams/orgs management as a product).
- Multi-language build toolchains beyond what is needed to demonstrate execution (breadth of languages is not a goal; the *pipeline* is).
- Fine-grained access control models (RBAC/ABAC beyond basic room ownership/participation).
- Mobile clients.
- Payment, subscription, or metering/billing systems.
- Formal compliance certifications (SOC 2, ISO 27001, etc.).

## 4.3 Future Scope

Reserved for later phases, explicitly *not* part of the initial build (see §15 for detail):

- Migration from Docker to **gVisor**, and later **Firecracker** microVMs.
- **Kubernetes** orchestration replacing Docker Compose.
- **Multi-region** deployment and geo-distributed state.
- **Autoscaling** worker pools driven by queue depth.
- **Distributed workers** across a fleet of execution nodes.
- **Collaborative debugging**, **session recording & replay**, and a **language plugin / execution-runtime plugin SDK**.
- Full Prometheus + Grafana observability stack (initially may be minimal/optional).

---

# 5. Personas

CPIP is infrastructure; its "users" are primarily builders and the systems/roles they represent. Personas fall into two groups: **the builder/operator** of CPIP, and the **downstream consumers** whose products such infrastructure would power.

## 5.1 The Backend Engineer (Primary Builder)

- **Who:** An engineer building CPIP to master backend and distributed systems.
- **Goals:** Gain hands-on depth in concurrency, networking, streaming, and sandboxing; produce a portfolio-defining system.
- **Needs from CPIP:** Clear subsystem boundaries, each mapping to a concept; production patterns done correctly; measurable behavior under load.
- **Success looks like:** Can explain and defend every design decision to a staff-level interviewer.

## 5.2 The Platform / Infrastructure Engineer (Operator)

- **Who:** The role responsible for running CPIP as a service.
- **Goals:** Deploy, scale, observe, and operate the platform reliably.
- **Needs from CPIP:** Health checks, metrics, structured logs, graceful shutdown, horizontal scaling, repeatable deployment, sane configuration.
- **Success looks like:** Can add a node, drain a node, diagnose a slow execution, and reason about capacity from dashboards.

## 5.3 The Student / Learner

- **Who:** Someone studying distributed systems, OS, or backend engineering.
- **Goals:** See how textbook concepts (CRDTs, cgroups, consumer groups) manifest in a real, working system.
- **Needs from CPIP:** A codebase and PRD that make the concept→implementation mapping explicit (see §13).
- **Success looks like:** Can trace a concept from theory through the running system and back.

## 5.4 Coding Interview Platform (Downstream Consumer)

- **Who:** A hypothetical product team building live technical interviews.
- **What they'd take from CPIP:** Real-time shared editor + secure execution of candidate code.
- **Why CPIP matters to them:** These are exactly the two hardest infrastructure pieces; CPIP is the layer they'd otherwise have to build.

## 5.5 Collaborative IDE / Cloud Editor (Downstream Consumer)

- **Who:** A team building a browser-based collaborative IDE (a Replit/Codespaces-style product).
- **What they'd take from CPIP:** Multi-user editing, presence, and sandboxed run capability as a backend foundation.

## 5.6 Education / Classroom Platform (Downstream Consumer)

- **Who:** A team building interactive coding classrooms.
- **What they'd take from CPIP:** A shared editing surface for instructor + students and a safe sandbox to run exercises.

## 5.7 Developer Tools Company (Downstream Consumer)

- **Who:** A company offering execution-as-a-service, code playgrounds, or embeddable runtimes.
- **What they'd take from CPIP:** The job pipeline, worker pool, isolation model, and streaming results — i.e., a Judge0/Lambda-like core.

> Personas 5.4–5.7 are **illustrative of the demand CPIP's infrastructure addresses**. CPIP does not build features *for* them; it demonstrates the infrastructure they would depend on.

---

# 6. Use Cases

Each use case describes an infrastructure workflow, not a product feature. The emphasis is on what the backend must do.

## 6.1 Simultaneous Multi-Developer Editing

**Scenario:** Several developers open the same room and edit the same document concurrently.

**Workflow:**
1. Each client connects to the WebSocket gateway and joins a room.
2. Local edits generate CRDT updates (Yjs) that are sent to the backend.
3. The backend relays updates to all other participants in the room and persists document state.
4. All clients converge to identical document state regardless of edit order or transient disconnects.
5. Presence updates (cursors, selections, join/leave) propagate live.

**Infrastructure concepts exercised:** CRDT convergence, fan-out over WebSockets, presence propagation, per-room state, consistency under concurrency, reconnection & catch-up.

## 6.2 Online Coding Interview (as an infrastructure workload)

**Scenario:** Two participants share a room; one writes code, both watch it run.

**Workflow:**
1. Both join a room and see each other's presence and edits in real time.
2. A participant submits the current document for execution.
3. The backend enqueues an execution job on the durable log.
4. A worker picks up the job, runs it in an isolated sandbox with resource limits.
5. stdout/stderr/status stream back into the room, in order, as they are produced.

**Infrastructure concepts exercised:** collaboration + execution composed, job intake, worker pool, sandbox isolation, ordered streaming, at-least-once delivery.

## 6.3 Collaborative Classroom Sandbox

**Scenario:** One instructor and many students in a room; students run code frequently.

**Workflow:**
1. Many participants join a single room (fan-out and presence at higher cardinality).
2. Multiple execution jobs are submitted in bursts.
3. The worker pool bounds concurrency; excess jobs queue rather than overload the host.
4. Backpressure and fair scheduling keep the system responsive; each result streams to its submitter.

**Infrastructure concepts exercised:** bounded concurrency, queue backpressure, fairness, resource governance, load isolation.

## 6.4 Secure Execution of Untrusted / Hostile Code

**Scenario:** A submission attempts to exhaust CPU, fork-bomb, allocate unbounded memory, exfiltrate data over the network, or break out of the sandbox.

**Workflow:**
1. Job is accepted and dispatched to a worker.
2. The sandbox enforces CPU time, wall-clock timeout, memory ceiling, process/PID limits, read-only or ephemeral filesystem, and no/blocked network.
3. Violations result in the job being killed and a clean failure status streamed back.
4. The host and other jobs are unaffected; resources are reclaimed deterministically.

**Infrastructure concepts exercised:** Linux isolation (namespaces, cgroups, seccomp), the container/host trust boundary, resource limits, denial-of-service containment, gVisor's stronger isolation as a later upgrade.

## 6.5 Programming Contest / High-Throughput Execution

**Scenario:** A spike of many execution jobs arrives in a short window.

**Workflow:**
1. Jobs land on the durable stream faster than they can be executed.
2. Consumer-group workers across nodes pull jobs, respecting concurrency caps.
3. The stream absorbs the burst; jobs are processed with at-least-once semantics and idempotent handling.
4. Throughput scales by adding worker capacity; latency degrades gracefully rather than failing.

**Infrastructure concepts exercised:** durable queues, consumer groups, horizontal worker scaling, throughput vs. latency trade-offs, graceful degradation.

## 6.6 Educational Sandbox / Playground

**Scenario:** A single learner runs code repeatedly to experiment.

**Workflow:**
1. Learner submits code; each run is an isolated, disposable sandbox.
2. Output streams back incrementally; history of runs is persisted.
3. Repeated runs demonstrate clean setup/teardown and reproducible isolation.

**Infrastructure concepts exercised:** ephemeral sandbox lifecycle, streaming, persistence of execution records, reproducibility.

## 6.7 Node Failure / Rolling Deploy (Operational)

**Scenario:** A backend node is drained or crashes during active sessions.

**Workflow:**
1. Externalized state (Redis/Postgres) means no session-critical state lives only in a node's memory.
2. Clients reconnect (via the load balancer) to a healthy node and resume.
3. In-flight jobs, tracked on the durable stream, are not lost; pending entries are re-processed.
4. Graceful shutdown drains connections and finishes or requeues work before exit.

**Infrastructure concepts exercised:** stateless-node design, externalized state, graceful shutdown, delivery guarantees, resilience.

---

# 7. Functional Requirements

Priorities use **MoSCoW**: **M** = Must, **S** = Should, **C** = Could, **W** = Won't (this phase). Acceptance criteria are behavioral and testable; they intentionally avoid implementation detail.

## 7.1 Authentication & Authorization

**FR-AUTH-1 — Participant identity**
- *Description:* Every connection is associated with an authenticated identity before joining a room or submitting execution.
- *Priority:* Must
- *Dependencies:* None
- *Acceptance criteria:* Unauthenticated connections cannot join rooms or submit jobs; each participant has a stable identity within a session.

**FR-AUTH-2 — Room authorization**
- *Description:* Access to a room is authorized (owner/participant); non-authorized identities are rejected.
- *Priority:* Must
- *Dependencies:* FR-AUTH-1, FR-ROOM-1
- *Acceptance criteria:* A participant lacking authorization for a room receives a clear denial and cannot receive its updates or presence.

## 7.2 Rooms & Sessions

**FR-ROOM-1 — Room lifecycle**
- *Description:* Rooms can be created, joined, and closed; a room owns exactly one shared document.
- *Priority:* Must
- *Dependencies:* FR-AUTH-1
- *Acceptance criteria:* A created room is joinable; joining yields current document state; closing releases associated resources.

**FR-ROOM-2 — Multi-participant membership**
- *Description:* A room supports multiple concurrent participants.
- *Priority:* Must
- *Dependencies:* FR-ROOM-1
- *Acceptance criteria:* N participants can be members simultaneously; membership changes are observable to all members.

**FR-ROOM-3 — Reconnection & state catch-up**
- *Description:* A participant that disconnects and reconnects resumes with correct, current document state.
- *Priority:* Should
- *Dependencies:* FR-ROOM-1, FR-COLLAB-1, FR-COLLAB-3
- *Acceptance criteria:* After a transient disconnect, a reconnecting client converges to the same state as continuously-connected clients.

## 7.3 Presence & Awareness

**FR-PRES-1 — Participant presence**
- *Description:* Each room exposes a live list of connected participants.
- *Priority:* Must
- *Dependencies:* FR-ROOM-2
- *Acceptance criteria:* Joins/leaves are reflected to all members within the presence latency target (§8).

**FR-PRES-2 — Cursor & selection awareness**
- *Description:* Participants' cursor positions and selections are shared live.
- *Priority:* Should
- *Dependencies:* FR-PRES-1, FR-COLLAB-1
- *Acceptance criteria:* Cursor/selection changes propagate to other participants within the presence latency target and are attributed to the correct identity.

**FR-PRES-3 — Connection status**
- *Description:* Presence reflects connection state (connected/disconnected/degraded).
- *Priority:* Could
- *Dependencies:* FR-PRES-1
- *Acceptance criteria:* Loss of a participant's connection is reflected to others within the heartbeat/timeout window.

## 7.4 Real-Time Collaboration

**FR-COLLAB-1 — CRDT synchronization**
- *Description:* Concurrent edits are synchronized via CRDTs so all clients converge to one consistent document.
- *Priority:* Must
- *Dependencies:* FR-ROOM-1
- *Acceptance criteria:* Given arbitrary interleavings of concurrent edits, all clients reach identical final state (convergence); no edit is silently lost.

**FR-COLLAB-2 — Update fan-out**
- *Description:* Edits from one participant are relayed to all other room participants.
- *Priority:* Must
- *Dependencies:* FR-COLLAB-1, FR-WS-1
- *Acceptance criteria:* An edit made by one participant is observable by all others within the edit-propagation latency target (§8).

**FR-COLLAB-3 — Document persistence**
- *Description:* Document state is durably persisted so a room can be restored.
- *Priority:* Must
- *Dependencies:* FR-COLLAB-1
- *Acceptance criteria:* After all participants disconnect and later rejoin, the last converged state is restored.

## 7.5 WebSocket Gateway / Streaming Transport

**FR-WS-1 — Connection lifecycle**
- *Description:* The gateway manages the full WebSocket lifecycle: handshake, auth, room binding, teardown.
- *Priority:* Must
- *Dependencies:* FR-AUTH-1
- *Acceptance criteria:* Connections are established, authenticated, bound to a room, and cleanly torn down with resource release.

**FR-WS-2 — Heartbeats & liveness**
- *Description:* The gateway detects dead connections via heartbeats/timeouts.
- *Priority:* Must
- *Dependencies:* FR-WS-1
- *Acceptance criteria:* A silent/broken connection is detected and reaped within the configured liveness window; associated presence updates.

**FR-WS-3 — Backpressure handling**
- *Description:* Slow or stalled clients must not block or degrade the gateway or other participants.
- *Priority:* Must
- *Dependencies:* FR-WS-1
- *Acceptance criteria:* Under a deliberately slow consumer, other participants continue to receive updates within latency targets; the slow connection is bounded (buffered to a limit, then dropped) rather than unbounded.

## 7.6 Code Execution Pipeline

**FR-EXEC-1 — Job submission & intake**
- *Description:* Clients submit code for execution; jobs are accepted and durably enqueued.
- *Priority:* Must
- *Dependencies:* FR-AUTH-1
- *Acceptance criteria:* A submitted job is acknowledged and appears on the durable execution log; acceptance is decoupled from execution completion.

**FR-EXEC-2 — Concurrent worker pool**
- *Description:* A bounded pool of workers consumes and executes jobs concurrently.
- *Priority:* Must
- *Dependencies:* FR-EXEC-1
- *Acceptance criteria:* Concurrency never exceeds the configured worker count; excess jobs queue; workers process jobs in a fair, non-starving manner.

**FR-EXEC-3 — Delivery semantics & idempotency**
- *Description:* Jobs are processed with at-least-once semantics; handling is idempotent so retries do not cause duplicate side effects visible to the client.
- *Priority:* Must
- *Dependencies:* FR-EXEC-1, FR-EXEC-2
- *Acceptance criteria:* A worker crash mid-job results in the job being retried; the client observes a single coherent result, not corrupted or duplicated output.

**FR-EXEC-4 — Job status lifecycle**
- *Description:* Each job has an observable lifecycle (queued → running → completed/failed/killed/timed-out).
- *Priority:* Must
- *Dependencies:* FR-EXEC-1
- *Acceptance criteria:* Clients can observe the current status; terminal states are accurate (e.g., a timed-out job reports timeout, not generic failure).

## 7.7 Sandbox & Isolation

**FR-SBX-1 — Isolated execution environment**
- *Description:* Each job runs in an isolated sandbox separate from the host and from other jobs.
- *Priority:* Must
- *Dependencies:* FR-EXEC-2
- *Acceptance criteria:* Code in one sandbox cannot observe or affect another sandbox or the host filesystem/processes.

**FR-SBX-2 — Resource limits**
- *Description:* Sandboxes enforce CPU time, wall-clock timeout, memory ceiling, and process/PID limits.
- *Priority:* Must
- *Dependencies:* FR-SBX-1
- *Acceptance criteria:* A job exceeding any limit is terminated deterministically with the correct terminal status; the host remains healthy.

**FR-SBX-3 — Filesystem & network containment**
- *Description:* Sandboxes use an ephemeral/constrained filesystem and have network access disabled or tightly restricted by default.
- *Priority:* Must
- *Dependencies:* FR-SBX-1
- *Acceptance criteria:* A job cannot persist to the host, cannot reach disallowed network destinations, and leaves no residue after teardown.

**FR-SBX-4 — Deterministic teardown & reclamation**
- *Description:* Sandboxes are torn down and resources reclaimed after each job.
- *Priority:* Must
- *Dependencies:* FR-SBX-1
- *Acceptance criteria:* After job completion or kill, all sandbox resources are released; repeated runs do not leak resources over time.

**FR-SBX-5 — Stronger isolation upgrade path**
- *Description:* The sandbox layer is designed so the runtime can migrate from Docker to gVisor without redesigning the pipeline.
- *Priority:* Should (design now, implement later)
- *Dependencies:* FR-SBX-1
- *Acceptance criteria:* The execution pipeline treats the sandbox runtime as a replaceable component; switching runtimes does not require changing intake, workers, or streaming.

## 7.8 Result Streaming

**FR-STREAM-1 — Incremental output streaming**
- *Description:* stdout/stderr are streamed to the client incrementally as produced, not only at completion.
- *Priority:* Must
- *Dependencies:* FR-EXEC-2, FR-WS-1
- *Acceptance criteria:* Output appears progressively during a long-running job; the client does not wait for completion to see initial output.

**FR-STREAM-2 — Ordering & completeness**
- *Description:* Streamed output preserves order and is complete through the job's terminal status.
- *Priority:* Must
- *Dependencies:* FR-STREAM-1
- *Acceptance criteria:* Output ordering matches production order; the client receives a definitive terminal status; no gaps under normal operation.

**FR-STREAM-3 — Durable output channel**
- *Description:* Output is carried over a durable stream so a briefly disconnected client can catch up.
- *Priority:* Should
- *Dependencies:* FR-STREAM-1
- *Acceptance criteria:* A client reconnecting during a job resumes the output stream without losing already-produced output.

## 7.9 History & Persistence

**FR-HIST-1 — Execution records**
- *Description:* Each execution is recorded (submission, status, resource outcome, output reference).
- *Priority:* Should
- *Dependencies:* FR-EXEC-4
- *Acceptance criteria:* Past executions can be retrieved with accurate status and outcome.

**FR-HIST-2 — Document history/restore**
- *Description:* Document state can be restored to its last converged version for a room.
- *Priority:* Should
- *Dependencies:* FR-COLLAB-3
- *Acceptance criteria:* A room reopened later reflects its last persisted state.

## 7.10 Monitoring & Observability

**FR-MON-1 — Structured logging**
- *Description:* All subsystems emit structured, correlatable logs.
- *Priority:* Must
- *Dependencies:* None
- *Acceptance criteria:* A single request/job can be traced across subsystems via correlation identifiers in logs.

**FR-MON-2 — Metrics**
- *Description:* Key metrics are exposed (active connections, room counts, queue depth, worker utilization, execution latency, error rates).
- *Priority:* Should
- *Dependencies:* None
- *Acceptance criteria:* Operators can read current values for the core metrics and observe them change under load.

**FR-MON-3 — Health & readiness**
- *Description:* Each node exposes health and readiness signals.
- *Priority:* Must
- *Dependencies:* None
- *Acceptance criteria:* An unhealthy/not-ready node is detectable by the load balancer and excluded from traffic.

## 7.11 Configuration

**FR-CFG-1 — Environment-driven configuration**
- *Description:* Deployment-specific settings (limits, pool sizes, endpoints, timeouts) are externally configurable.
- *Priority:* Must
- *Dependencies:* None
- *Acceptance criteria:* The same build runs in different environments purely via configuration changes; no environment-specific values are hard-coded.

## 7.12 Deployment & Scaling

**FR-DEP-1 — Reproducible deployment**
- *Description:* The full stack can be deployed reproducibly via Docker Compose with Nginx as reverse proxy.
- *Priority:* Must
- *Dependencies:* All core subsystems
- *Acceptance criteria:* A clean environment can bring up the entire platform from the defined deployment artifacts.

**FR-DEP-2 — Horizontal scaling of backend nodes**
- *Description:* Multiple backend nodes run behind the load balancer with externalized state.
- *Priority:* Must
- *Dependencies:* FR-COLLAB-3, FR-EXEC-1, FR-MON-3
- *Acceptance criteria:* Adding a node increases capacity; any node can serve any client; no node holds unique session-critical in-memory state required for correctness.

**FR-DEP-3 — CI/CD**
- *Description:* Builds and checks run automatically via GitHub Actions.
- *Priority:* Should
- *Dependencies:* None
- *Acceptance criteria:* Commits trigger automated build and verification; failures block promotion.

**FR-DEP-4 — Graceful shutdown & draining**
- *Description:* A node can be drained: it stops accepting new work, finishes or requeues in-flight work, and closes connections cleanly.
- *Priority:* Must
- *Dependencies:* FR-WS-1, FR-EXEC-2
- *Acceptance criteria:* During shutdown, no in-flight job is lost and clients are able to reconnect elsewhere without data loss.

---

# 8. Non-Functional Requirements

Targets are engineering goals for a reference deployment on modest hardware; they define what "working well" means and are meant to be measured, not assumed.

## 8.1 Performance & Latency
- **Edit propagation latency (P95):** ≤ 100 ms server-side fan-out under nominal load; perceived end-to-end collaboration feels real-time.
- **Presence propagation (P95):** ≤ 150 ms for join/leave/cursor updates.
- **Execution intake acknowledgement (P95):** ≤ 50 ms (decoupled from execution time).
- **First-output-byte for a trivial job (P95):** ≤ a small, documented budget once a worker is available (dominated by sandbox startup).

## 8.2 Availability
- **Target:** No single backend node is a single point of failure for serving clients. Loss of one node does not take down active collaboration or the execution pipeline (state is externalized). Redis and Postgres are acknowledged single-instance dependencies in the initial phase (see §10).

## 8.3 Reliability
- **Execution delivery:** at-least-once with idempotent handling; no job silently dropped under worker crash or restart.
- **In-flight safety:** graceful shutdown loses no acknowledged job.

## 8.4 Consistency
- **Documents:** strong eventual consistency via CRDTs — all replicas converge; convergence is guaranteed, ordering of concurrent edits is not.
- **Execution results:** ordered and complete per job through a terminal status.

## 8.5 Scalability
- **Horizontal:** capacity scales by adding backend/worker nodes; externalized state enables any-node-serves-any-client.
- **Concurrency bounds:** worker concurrency and connection buffers are explicitly bounded; the system degrades gracefully (queueing, backpressure) rather than collapsing under overload.
- **Reference target:** support a documented number of concurrent rooms, participants, and in-flight executions on a defined baseline host, with a clear scaling story to increase each.

## 8.6 Maintainability
- Clear subsystem boundaries with high cohesion and low coupling; each subsystem independently reasoned about and tested. Configuration externalized. No hidden global state.

## 8.7 Observability
- Every subsystem is logged (structured, correlatable), measured (core metrics), and health-checked. "If you can't measure it, it isn't done." Correlation identifiers allow tracing a job or session across subsystems.

## 8.8 Security
- Untrusted code is treated as hostile by default: strong isolation, enforced resource limits, no ambient host access, network denied/restricted, deterministic teardown. A defense-in-depth posture with a clear upgrade path to stronger isolation (gVisor). Least privilege throughout the execution path.

## 8.9 Extensibility
- The sandbox runtime, the execution job schema, and the transport are designed as replaceable/extendable seams (e.g., Docker→gVisor→Firecracker; additional runtimes) without redesigning the pipeline.

## 8.10 Developer Experience (of the builder/operator)
- One-command local bring-up; readable logs; obvious configuration; fast feedback via CI. The system should be understandable end-to-end by a single engineer.

## 8.11 Testing
- Concurrency-critical paths (CRDT convergence, worker pool, backpressure, delivery semantics, graceful shutdown) have deterministic, repeatable tests. Race detection is part of the verification pipeline. Failure modes (slow client, worker crash, resource-exceeding job) are tested explicitly.

## 8.12 Portability
- Runs on any standard Linux host with the container runtime available; environment-driven configuration; no hard dependency on a specific cloud. Reproducible via the defined deployment artifacts.

## 8.13 Resource Governance
- Global and per-job resource ceilings prevent any single tenant/job from degrading the platform; the host is protected from resource exhaustion by design.

---

# 9. Engineering Constraints

This section records the load-bearing technology decisions and their trade-offs. Decisions favor the simplest primitive that meets the requirement, with a documented upgrade path.

## 9.1 Why Go
- **For:** First-class concurrency (goroutines, channels, `context`) maps directly to the worker-pool, fan-out, and streaming problems; excellent networking and WebSocket ecosystem; static binaries and small containers; a built-in race detector; strong production tooling.
- **Trade-off:** Less expressive type system than some alternatives; generics are comparatively young. Accepted because the concurrency and operational fit dominate.

## 9.2 Why Redis Streams (for the execution log & output channel)
- **For:** A durable, ordered log with **consumer groups** giving at-least-once delivery, pending-entry tracking, and horizontal worker scaling — exactly the primitives a job pipeline needs — without operating a full message broker.
- **Trade-off:** Not as durable or partition-tolerant as Kafka; a single Redis instance is a availability dependency initially. Accepted: it teaches the right streaming concepts at the right complexity, and the seam allows a later broker upgrade.

## 9.3 Why PostgreSQL (+ pgx)
- **For:** Battle-tested relational store for document state, execution records, and identity; strong consistency; rich querying; `pgx` is a high-performance, idiomatic Go driver.
- **Trade-off:** A relational store is not the most natural fit for CRDT blobs or high-churn ephemeral state; single-instance Postgres is an availability dependency initially. Accepted: durability and operational familiarity outweigh it; ephemeral/real-time state lives in Redis, not Postgres.

## 9.4 Why WebSockets
- **For:** Full-duplex, low-latency, persistent connections are the natural transport for collaborative editing, presence, and live output streaming.
- **Trade-off:** Stateful connections complicate horizontal scaling and load balancing; require heartbeats, backpressure, and reconnection handling. Accepted: this complexity *is* the learning objective, and it is the correct transport for the workload.

## 9.5 Why Docker (initially)
- **For:** The simplest widely-understood way to get process/filesystem/network isolation and resource limits (cgroups/namespaces) for untrusted code; ubiquitous tooling; easy local reproduction.
- **Trade-off:** Shared host kernel means a weaker security boundary than a VM or user-space kernel; container escape is a real threat class. Accepted for phase one, with **gVisor as the explicit hardening upgrade**.

## 9.6 Why gVisor (later)
- **For:** A user-space kernel that intercepts syscalls, dramatically shrinking the host kernel attack surface — much stronger isolation for untrusted code while keeping a container-like workflow.
- **Trade-off:** Performance overhead and some syscall-compatibility limits. Deferred deliberately so the pipeline is proven first and the sandbox seam is designed to swap runtimes cleanly.

## 9.7 Why Yjs (client-side CRDT)
- **For:** A mature, well-documented CRDT implementation with proven Monaco integration; offloads conflict resolution to a correct, battle-tested library so the backend can focus on relay, persistence, and presence.
- **Trade-off:** CRDT metadata overhead; ties the collaboration model to Yjs's format. Accepted: reimplementing a correct CRDT is out of scope and not the primary learning target; the *systems* around it are.

## 9.8 Why NOT Kafka (initially)
- Kafka is the "correct" answer at large scale but brings heavy operational weight (brokers, ZooKeeper/KRaft, partitions, retention tuning) that would dominate the project and obscure the fundamentals. Redis Streams teaches the same core concepts (log, consumer groups, delivery semantics) at a fraction of the operational cost. Kafka remains a future upgrade if throughput/durability demands it.

## 9.9 Why NOT RabbitMQ (initially)
- RabbitMQ is a capable broker, but adds a separate system with its own model (exchanges, queues, bindings) for a job pipeline that Redis Streams already serves — and we already need Redis for real-time/ephemeral state. Choosing Redis Streams avoids a second infrastructure dependency while still teaching durable, at-least-once, ordered delivery.

## 9.10 Why NOT Microservices (initially)
- A distributed microservice topology would multiply operational surface (service discovery, inter-service networking, distributed tracing across many hops) before the core problems are solved. A **well-factored modular monolith** — clear internal boundaries, horizontally scalable — delivers the distributed-systems learning (state externalization, scaling, delivery guarantees) without premature decomposition. Services can be extracted later along the seams already defined.

## 9.11 Why NOT AI/LLM
- AI features are explicitly out of scope: they are application-layer product features, not infrastructure, and would dilute the project's purpose. CPIP's value is in the plumbing, not in intelligence layered on top.

## 9.12 Frontend Stack Constraint
- React + TypeScript + Tailwind + Monaco + Yjs + Zustand exist **only** as a reference client to exercise the backend. The frontend is intentionally thin; no product-grade UX investment is in scope. This keeps effort concentrated on infrastructure.

## 9.13 Deployment Constraint
- Docker Compose + Nginx + GitHub Actions are chosen as the simplest reproducible topology that still demonstrates reverse proxying, load balancing across multiple nodes, and CI/CD. Kubernetes is deferred to avoid orchestration overhead before the platform justifies it.

---

# 10. Risks

## 10.1 Technical Risks
- **CRDT/persistence correctness under concurrency.** Convergence bugs or lost updates are subtle and hard to detect. *Mitigation:* rely on a mature CRDT (Yjs), keep the backend as relay/persistence authority, and test convergence with adversarial interleavings and reconnection scenarios.
- **WebSocket state at scale.** Stateful connections complicate scaling, load balancing, and failure handling. *Mitigation:* externalize state, design for reconnection and any-node-serves-any-client, enforce heartbeats and backpressure, test slow/broken consumers explicitly.
- **Streaming delivery semantics.** At-least-once plus ordering plus idempotency is easy to get subtly wrong. *Mitigation:* lean on Redis Streams consumer-group semantics, make handlers idempotent, and test worker-crash/retry paths.

## 10.2 Operational Risks
- **Single-instance Redis/Postgres.** Both are availability dependencies in phase one. *Mitigation:* acknowledge explicitly, keep them behind clear seams, document the HA upgrade (replication/managed services) as future scope; ensure the system degrades gracefully and recovers cleanly on restart.
- **Graceful shutdown/draining correctness.** Dropping in-flight work during deploys erodes trust. *Mitigation:* make draining a first-class, tested requirement (FR-DEP-4).
- **Observability gaps.** Without good signals, incidents are undiagnosable. *Mitigation:* observability-by-default (FR-MON-*), correlation IDs, core metrics from day one.

## 10.3 Security Risks
- **Container escape / weak isolation (Docker).** Untrusted code on a shared kernel is the highest-severity risk. *Mitigation:* strict resource limits, dropped capabilities, no/blocked network, read-only/ephemeral filesystem, least privilege, and a committed **gVisor** upgrade for stronger isolation.
- **Resource-exhaustion / DoS from hostile jobs.** Fork bombs, memory/CPU exhaustion. *Mitigation:* cgroup-enforced CPU/memory/PID limits, wall-clock timeouts, bounded concurrency, deterministic teardown; the host must survive any single job.
- **Data exfiltration.** *Mitigation:* network denied/restricted by default; ephemeral filesystem; no host mounts of sensitive paths.

## 10.4 Scalability Risks
- **Redis/Postgres as bottlenecks.** Central state can become the ceiling. *Mitigation:* measure early, keep hot/ephemeral state in Redis and durable state in Postgres, define the sharding/replication upgrade path.
- **Fan-out amplification in large rooms.** High-cardinality rooms multiply message volume. *Mitigation:* backpressure, bounded buffers, and documented per-room limits; treat very large rooms as a future optimization.
- **Sandbox startup cost.** Container spin-up may dominate small-job latency. *Mitigation:* measure it, consider warm pools as future work, and be transparent about the latency budget.

## 10.5 Project / Scope Risks
- **Scope creep into application features.** The single biggest risk to the project's identity. *Mitigation:* the §2.2 "what CPIP is NOT" boundary and the "does it teach backend?" litmus test are enforced on every feature.

---

# 11. Assumptions

1. The **frontend is a thin reference client**; production-grade product UX is not required.
2. The initial deployment runs on **standard Linux hosts** with a container runtime available; kernel features (namespaces, cgroups, seccomp) are accessible.
3. **Single-instance Redis and Postgres** are acceptable for phase one; HA is future scope.
4. **Trust in identity is basic** (authenticated participant, room ownership/participation); fine-grained authorization is out of scope.
5. **Language/runtime breadth is not a goal**; a small, representative execution capability is sufficient to prove the pipeline.
6. **Yjs is the CRDT of record**; the backend does not implement its own CRDT algorithm.
7. **Modest hardware baseline** is assumed for stated performance targets; targets scale with resources.
8. **Untrusted code is assumed hostile** by default; no submission is trusted.
9. The project is **built and operated primarily by one engineer** for learning/portfolio purposes; team-scale process (on-call, SLAs) is simulated, not contractual.
10. **Network conditions are imperfect**; clients disconnect and reconnect, and the system must tolerate this.
11. **Docker-first, gVisor-later** is an accepted phased security posture, not a permanent one.
12. Observability tooling (Prometheus/Grafana) may start **minimal or optional** and be enriched later without redesign.

---

# 12. Success Criteria

Success is measured by demonstrable, observable behavior — not feature count.

## 12.1 Functional Milestones
- **M1 — Collaboration works:** N participants edit one document concurrently and provably converge; presence and cursors are live; a reconnecting client catches up correctly.
- **M2 — Execution works securely:** arbitrary code runs in an isolated sandbox with all resource limits enforced; a hostile job (fork bomb, memory hog, infinite loop, network attempt) is contained and the host is unaffected.
- **M3 — Streaming works:** output streams incrementally, in order, through a durable channel, with a definitive terminal status; a briefly disconnected client resumes without loss.
- **M4 — Pipeline is robust:** a worker crash mid-job results in correct at-least-once retry and a single coherent client-visible result.
- **M5 — Horizontal scale works:** multiple backend nodes behind Nginx serve clients interchangeably; adding a node adds capacity; draining a node loses no acknowledged work.

## 12.2 Operational Milestones
- **M6 — Observable:** a single job/session is traceable across subsystems via logs and correlation IDs; core metrics reflect load in real time; unhealthy nodes are excluded from traffic.
- **M7 — Reproducible:** the entire platform comes up from defined deployment artifacts on a clean host with one command; CI verifies builds automatically.

## 12.3 Engineering-Understanding Milestones (the real bar)
- **M8 — Defensible design:** every subsystem's technology choice and trade-off (§9) can be explained and defended at staff level.
- **M9 — Concept mapping proven:** each subsystem demonstrably teaches its mapped concept (§13), evidenced by tests and measurements (e.g., a backpressure test, a convergence test, a resource-limit test).

## 12.4 Quantitative Targets
- Latency and consistency targets in §8 are met and **measured**, not merely claimed.
- Concurrency safety verified under the race detector on critical paths.
- Documented capacity numbers for concurrent rooms, participants, and in-flight executions on the baseline host.

---

# 13. Learning Outcomes

Each subsystem is deliberately chosen to force contact with specific engineering concepts. This mapping is a first-class deliverable — the platform is only "done" when each mapping is demonstrably exercised.

| Subsystem | → Concepts taught |
|---|---|
| **WebSocket Gateway** | Real-time networking · full-duplex transport · connection lifecycle · heartbeats/liveness · backpressure · fan-out |
| **Room / Session Manager** | Stateful service design · lifecycle management · externalized state · authorization boundaries |
| **CRDT Sync Engine** | Distributed systems · eventual consistency · convergence · conflict-free replication · reconnection/catch-up |
| **Presence System** | Distributed shared state · awareness propagation · liveness detection · high-cardinality fan-out |
| **Execution Intake** | Durable logs · decoupling acceptance from processing · idempotency · queue design |
| **Worker Pool** | Concurrency · goroutines/channels · bounded parallelism · fairness/non-starvation · context/cancellation |
| **Redis Streams Pipeline** | Streaming · consumer groups · at-least-once delivery · ordering · pending-entry recovery · backpressure |
| **Docker Sandbox** | Operating systems · Linux namespaces · cgroups · seccomp · the container/host trust boundary · resource limits |
| **gVisor (later)** | Advanced isolation · user-space kernel · syscall interception · attack-surface reduction · security trade-offs |
| **Result Streaming** | Incremental streaming · ordering/completeness · durable catch-up channels |
| **Persistence Layer (Postgres/pgx)** | Durability · relational modeling · consistency · connection pooling · driver performance |
| **Config Subsystem** | 12-factor configuration · environment-driven deployment · portability |
| **Observability Layer** | Structured logging · metrics · correlation/tracing · health/readiness · operability |
| **Deployment (Compose/Nginx/CI)** | Reverse proxying · load balancing · horizontal scaling · reproducible deploys · CI/CD |
| **Graceful Shutdown / Draining** | Resilience · in-flight safety · delivery guarantees under deploy/failure |

**Cross-cutting outcomes:** reasoning about consistency vs. availability trade-offs; designing for failure; measuring before optimizing; protecting a host from hostile workloads; and composing many subsystems into one coherent, operable system.

---

# 14. Development Principles

These are the engineering rules that govern how CPIP is built. They are binding, not aspirational.

1. **Infrastructure-first.** Build and harden the plumbing before any product-shaped feature. If a task doesn't advance the infrastructure or teach a backend concept, it waits.
2. **Simple architecture.** Prefer the simplest primitive that meets the requirement (modular monolith over microservices; Redis Streams over Kafka). Complexity must be *earned* by a measured need.
3. **Package-oriented Go.** Organize by capability/domain with high cohesion; each package has a clear responsibility and a small, intentional surface. (Structure detail belongs to later modules — the *principle* is the constraint here.)
4. **No premature optimization.** Measure first. Optimize only proven bottlenecks. Correctness and clarity precede speed.
5. **Production thinking.** Every subsystem is built as if it will be operated: logs, metrics, health checks, timeouts, graceful shutdown, and failure handling are part of "done," not follow-ups.
6. **High cohesion, low coupling.** Subsystems communicate across explicit seams (transport, sandbox runtime, job log) so components can be replaced (Docker→gVisor) or extracted (monolith→services) without redesign.
7. **Security first.** Untrusted code is hostile by default. Least privilege, strict resource limits, and a defensible trust boundary are non-negotiable, from the first sandbox.
8. **Observability by default.** Nothing is "done" until it is measurable. Correlation IDs, structured logs, and core metrics are built in, not bolted on.
9. **Graceful degradation.** Under overload or partial failure, the system sheds load, applies backpressure, and degrades responsively — it never collapses or silently corrupts.
10. **Deterministic behavior.** Concurrency-critical paths must be race-free and reproducibly testable. The race detector and deterministic tests gate critical code.
11. **Document-first development.** This PRD is the single source of truth; significant decisions are recorded (design notes, trade-offs) before implementation. Understanding precedes code.
12. **Idempotency & at-least-once by design.** The execution pipeline assumes retries; handlers are idempotent so retries never corrupt client-visible results.
13. **Externalize state.** No node holds session-critical state only in memory; any node can serve any client. Scalability and resilience follow from this discipline.

---

# 15. Future Roadmap

Ordered roughly by dependency and value; all items are explicitly *future* and out of the initial scope.

## 15.1 Stronger Isolation
- **gVisor** as the default sandbox runtime (near-term hardening).
- **Firecracker** microVMs for VM-grade isolation with fast startup (longer-term).
- **Warm sandbox pools** to cut per-job startup latency.

## 15.2 Orchestration & Scale
- **Kubernetes** replacing Docker Compose for orchestration, scheduling, and self-healing.
- **Autoscaling** worker pools driven by queue depth / utilization.
- **Distributed workers** across a fleet of dedicated execution nodes, decoupled from the gateway tier.

## 15.3 Data & Availability
- **HA Redis and Postgres** (replication, failover, managed services) to remove single-instance dependencies.
- **Kafka or a durable broker** if throughput/durability demands outgrow Redis Streams.
- **Sharding / partitioning** of rooms and job streams.

## 15.4 Multi-Region
- **Geo-distributed deployment** with region-aware routing and replicated/partitioned state; latency-optimized collaboration across regions.

## 15.5 Collaboration Features (infrastructure-flavored)
- **Collaborative debugging** — shared breakpoints/step state as a synchronized distributed-state problem.
- **Session recording & replay** — durable event logs replayed deterministically (an event-sourcing exercise).

## 15.6 Extensibility
- **Language / runtime plugin SDK** — pluggable execution runtimes behind a stable sandbox contract.
- **Transport/plugin seams** for alternative clients and integrations.

## 15.7 Observability Maturity
- Full **Prometheus + Grafana** dashboards, alerting, SLO tracking, and distributed tracing.

Each roadmap item is chosen so it, too, teaches a concept — VM isolation, orchestration, HA, event sourcing, plugin architecture — keeping the "every feature teaches" philosophy intact as the platform grows.

---

# 16. Conclusion

CPIP is a deliberate answer to a specific ambition: to learn backend and distributed systems engineering not through disconnected tutorials, but by building the real, hard infrastructure that powers collaborative developer products — and building it to production standards.

It fuses the two most difficult infrastructure problems in this space — **real-time distributed synchronization** and **secure execution of untrusted code** — into one coherent, observable, horizontally scalable platform. In doing so it forces genuine mastery of networking, concurrency, streaming, operating-system isolation, resource governance, and production operability. Every subsystem is chosen to teach; every principle enforces production discipline; every trade-off is recorded and defensible.

What makes this an exceptional backend engineering project is not any single feature — it is the **composition**: a WebSocket gateway with real backpressure, a CRDT relay with real convergence, a worker pool with real bounded concurrency, a sandbox with a real trust boundary, and a streaming pipeline with real delivery guarantees — all running across replaceable nodes with real observability, and all with a clear, principled upgrade path from "simplest thing that works" to "how the industry actually does it at scale."

CPIP is not an application. It is the infrastructure applications are built on — and building it, correctly and observably, is exactly the education it is designed to deliver.

This document is the single source of truth. Implementation, API design, data modeling, and package structure follow in subsequent modules, governed by the objectives, constraints, and principles established here.

---

*End of PRD v1.0.*
