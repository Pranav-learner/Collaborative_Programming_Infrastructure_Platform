# CPIP Sandbox Security Engine

This package implements Stage 3, Module 2 of the Collaborative Programming Infrastructure Platform (CPIP), providing production-grade resource isolation, security policy enforcement, and audit logs.

## Architecture

The Sandbox Security Engine decouples security and resource policy resolution from the container execution runtime:

```
                  ┌──────────────────────┐
                  │    SandboxManager    │
                  └──────────┬───────────┘
                             │
            ┌────────────────┴────────────────┐
            ▼                                 ▼
┌──────────────────────┐             ┌──────────────────┐
│    PolicyResolver    │             │ ResourceMonitor  │◄──(polls stats)
└──────────┬───────────┘             └────────┬─────────┘
           │                                  │
           ├────────────────────────┐         │ (exceeded limits)
           ▼                        ▼         ▼
┌────────────────────┐   ┌────────────────────┐
│   SecurityEngine   │   │   ResourceEngine   │
└──────────┬─────────┘   └──────────┬─────────┘
           │                        │
           └──────────┬─────────────┘
                      ▼
        ┌──────────────────────────┐
        │  ContainerConfig Limits  │
        └─────────────┬────────────┘
                      ▼
        ┌──────────────────────────┐
        │   DockerRuntimeAdapter   │
        └──────────────────────────┘
```

The system comprises:
- **Registry & Validator**: Manages and checks the bounds of security/resource policies, protecting against oversizing.
- **Resolver**: Merges standard profiles with user-defined overlays and validates final policies.
- **Engines**: Translates high-level resolved policies into low-level runtime configurations (Cgroups limits, capabilities, network/filesystem controls).
- **Monitor**: Polls runtime resource stats to detect threshold breaches (Memory limit, Process limit) and triggers remediation handlers.
- **Audit Logger**: Hooked to the event bus, logs security/lifecycle events via structured logs and fires auditing events.

---

## Profile Hierarchies

### Resource Profiles
| Profile ID | Memory Limit | CPU Shares | Process Limit | File Descriptors |
| :--- | :--- | :--- | :--- | :--- |
| **Tiny** | 64 MB | 256 | 20 | 64 |
| **Small** | 256 MB | 512 | 50 | 128 |
| **Medium** | 1024 MB | 1024 | 150 | 256 |
| **Large** | 4096 MB | 2048 | 500 | 512 |

### Security Profiles
| Profile ID | Read-Only Root | Writable Workspace | Network Allowed | Linux Capabilities |
| :--- | :--- | :--- | :--- | :--- |
| **Default** | Yes | Yes | Yes | None |
| **HighSecurity** | Yes | Yes | No | None |
| **ReadOnly** | Yes | No | No | None |
| **Permissive** | No | Yes | Yes | NET_RAW, SYS_CHROOT |

---

## Integration

The `SandboxManager` applies policies when creating sandboxes:
```go
sess, err := mgr.CreateSandbox(ctx, jobID, language, expiration, "Default", "Small", customOverrides)
```

The `ResourceMonitor` automatically monitors memory and processes running inside the sandbox container. If the container exceeds limits, it triggers a violation callback that kills the container and generates an audit log entry.
