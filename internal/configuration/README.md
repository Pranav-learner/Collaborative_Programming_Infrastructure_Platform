# Configuration, Secrets & Feature Management Platform — Stage 5 Module 2

> Enterprise-grade, high-performance, and unified configuration infrastructure for CPIP.  
> Business services never read raw OS environment variables, directly access secrets files, or write custom feature flag rollouts. Everything is decoupled and managed via the **Configuration SDK**.

---

## 1. Folder Tree

```
internal/configuration/
├── config/              Self-configuration for the config platform itself
├── events/              Thread-safe event system (Bus & Handler system)
├── featureflags/        Feature flag engine (rollout, targeting, kill switches)
├── logger/              Structured slog-based logging framework with masking
├── manager/             Central orchestrator for loads, validations, & rollbacks
├── metrics/             Metrics Recorder interfaces and in-memory tracking
├── middleware/          Logging and metrics decorators for provider operations
├── profiles/            Active environment profiles and inheritance fallback resolution
├── providers/           YAML, JSON, Env, and Memory configuration providers
├── registry/            Ordered, priority-based configuration provider registry
├── runtime/             Dynamic runtime override engine (hot reloading overrides)
├── sdk/                 Primary public API surface for target applications
├── secrets/             Masked secret manager & Memory/Env/Encrypted File providers
├── validation/          Schema, type, range, dependency, and custom validation rules
├── versioning/          Snapshot version managers, history logs, and diff engines
└── configuration_test.go Core integration and race check test suite
```

---

## 2. Package Responsibilities

*   **`config`**: Configures configuration platform behavior (e.g. active profile, provider order, reload/watch intervals, mask character, max versions limit).
*   **`events`**: Implements a thread-safe publish-subscribe event bus supporting lifecycle events (`ConfigurationLoaded`, `SecretRotated`, `FeatureFlagChanged`, etc.).
*   **`featureflags`**: Evaluates Boolean flags, user-specific and role-specific whitelists, environment profiles, and percentage rollouts utilizing consistent FNV-1a hashing.
*   **`logger`**: Wraps the standard Go `slog` structure to support platform logs, masking secret values so sensitive data never leaks into logs.
*   **`manager`**: Orchestrates provider loading, executes validation rules, tracks snapshot histories, initiates hot-reloads, and coordinates rollbacks.
*   **`metrics`**: Exposes telemetry counters and gauges (e.g., active providers count, total validations, secret lookups count).
*   **`middleware`**: Transparently decorates providers and secret providers to log access and capture operation duration.
*   **`profiles`**: Manages environment profile hierarchies (Local -> Development -> Common) and applies inheritance-based overrides.
*   **`providers`**: Implements reading configuration values from OS environment variables, YAML flat indented files, JSON files, and memory.
*   **`registry`**: Registers providers and sorts them in ascending order of priority numbers (where lower number = higher precedence).
*   **`runtime`**: Supports setting dynamic configuration overrides at runtime, notifying registered listeners.
*   **`sdk`**: Acts as the single entrypoint for applications, providing type conversion (`GetInt`, `GetBool`, `GetDuration`) and delegation to secrets and flags.
*   **`secrets`**: Handles loading, masking, metadata tracking, and rotating secrets. Implements Memory, Env, and Encrypted File (AES-GCM) secret providers.
*   **`validation`**: Performs schema rules verification including value type matching, number range limits, dependency checks, and custom hooks.
*   **`versioning`**: Stores previous configuration snapshots, maintains rollback logs, and calculates diffs between versions.

---

## 3. Configuration Architecture Diagram

```
                              [ Target Application ]
                                         │
                                         ▼
                                 [  Config SDK  ]
                                         │
                                         ▼
                                 [  Manager  ]
                                         │
                     ┌───────────────────┼───────────────────┐
                     ▼                   ▼                   ▼
             [   Registry   ]    [  Validator  ]    [ Versioning  ]
                     │
         ┌───────────┴───────────┐
         ▼                       ▼
   [ Providers ]         [ SecretManager ]
   ├── EnvProv           ├── EnvSecretProv
   ├── YAMLProv          ├── MemorySecretProv
   └── JSONProv          └── EncryptedFileSecretProv (AES-GCM)
```

---

## 4. Provider Resolution Workflow

1. **Initialize Registry**: Providers are registered in the priority-based `Registry`.
2. **Order sorting**: The registry sorts providers based on their `Priority()` value (e.g., Env = 10, YAML = 20, JSON = 30).
3. **Provider Execution**: On `Load(ctx)`:
    *   Iterate in reverse order (lowest precedence first).
    *   Load raw data map from the provider.
    *   Apply profile inheritance rules (`ResolveConfig()`) on that provider's raw data to resolve overrides.
    *   Write key-values to `mergedResolved` map.
4. **Precedence Application**: Higher-precedence provider values overwrite lower-precedence provider values in the final merged map.
5. **Runtime Overrides**: Dynamic runtime overrides from the `RuntimeEngine` are merged last.
6. **Validation & Versioning**: The merged configuration map is validated against rules and stored as a new version `Snapshot`.

---

## 5. Secret Management Workflow

```
            SDK.GetSecret("db.password")
                         │
                         ▼
        SecretManager iterates providers:
        ├── 1. MemorySecretProvider
        ├── 2. EnvSecretProvider
        └── 3. EncryptedFileSecretProvider (Decrypts AES-GCM data)
                         │
                         ├─► Found? ──► Log masked access ──► Return plaintext value
                         ▼
                     Not Found? ──► Return ErrSecretNotFound
```

*   **Masking Rule**: Masking functions cap mask character output and show only the last 3 characters (e.g. `••••••••ass`).
*   **Decryption**: Encrypted files are stored on disk using AES-GCM with a 256-bit key. Decompression/decryption runs automatically on first lookup.

---

## 6. Feature Flag Evaluation Flow

```
                      SDK.EvaluateFlag("feature-x", Context)
                                         │
                                         ▼
                           Flag registered in Platform?
                                         │
                       ┌─────────────────┴─────────────────┐
                       ▼ No                                ▼ Yes
                 Return false                         Is Kill Switch?
                                                           │
                                         ┌─────────────────┴─────────────────┐
                                         ▼ Yes                               ▼ No
                                   Return false                          Global Switch
                                                                         (ff.Enabled)
                                                                             │
                                         ┌───────────────────────────────────┤
                                         ▼ Yes                               ▼ No
                                    Check whitelists:                  Return false
                                    ├── AllowedUsers
                                    └── AllowedRoles
                                         │
                                         ├─► Whitelist configured & unmatched? ─► Return false
                                         ▼
                                    Check TargetProfiles
                                         │
                                         ├─► TargetProfiles configured & unmatched? ─► Return false
                                         ▼
                                    Check RolloutPercent
                                         │
                                         ├─► Hash(UserID / SessionID) % 100 < percent?
                                         │   ├── Yes ─► Return true
                                         │   └── No  ─► Return false
                                         ▼
                                    Return true
```

---

## 7. Configuration Reload & Watcher Workflow

1. **File Watcher Setup**: Polling watcher is initialized with a configurable `WatchInterval`.
2. **Path Monitoring**: Files like `/etc/cpip/config.yaml` are registered with modification time tracking.
3. **Change Detection**: On each watch tick, the watcher calls `os.Stat(path)`. If `currentModTime.After(lastRecordedTime)`:
    *   Trigger `ReloadCallback` asynchronously.
    *   Update watcher's last recorded modification time.
4. **Reload Execution**: Manager calls `Reload(ctx)`:
    *   Loads all configuration sources fresh.
    *   Re-evaluates profile inheritance and validates schema.
    *   Records new snapshot version.
    *   Publishes `ConfigurationReloaded` event.
    *   Computes and logs configuration Diffs (added/changed/removed keys).

---

## 8. Version Rollback Workflow

1. **Call rollback**: Client requests `Rollback(ctx, targetVersion)`.
2. **Snapshot Lookup**: The manager fetches the target snapshot from the versioning history log.
3. **Validation check**: The manager validates the target snapshot data against the active validator schema rules.
    *   *If validation fails, rollback is aborted to prevent application instability.*
4. **Apply state**: The manager records a new version snapshot containing the target snapshot's keys (acting as a forward rollback transaction).
5. **Publish events**: The manager publishes a `ConfigurationRolledBack` event to let other systems re-apply settings.

---

## 9. Environment Profile Hierarchy

Environment profiles inherit default configurations through a falling back chain:

```
                  [ Common Defaults ("common") ]
                                │
          ┌─────────────────────┼─────────────────────┐
          ▼                     ▼                     ▼
    [ Staging ]           [ Production ]       [ Development ]
                                                      │
                                            ┌─────────┴─────────┐
                                            ▼                   ▼
                                         [ Local ]          [ Testing ]
```

*   If active profile is **Local**, a search for `database.host` checks:
    1. `local.database.host`
    2. `development.database.host`
    3. `common.database.host`
    4. `database.host` (unscoped)

---

## 10. Concurrency Strategy

*   **Read-Write Mutexes (`sync.RWMutex`)**: Utilized across all shared state managers (e.g. `Registry`, `VersionManager`, `FeatureFlagPlatform`, `RuntimeEngine`, `Watcher`).
*   **Unshared Data Propagation**: When configuration snapshots are loaded or rolled back, a deep copy of the map is stored inside the Snapshot. Reading from the SDK returns immutable primitive values (or copies), preventing pointer race conditions.
*   **Asynchronous Notify**: Event notifications and watcher reload callbacks are spawned inside separate goroutines, ensuring reload operations are non-blocking.
*   **Atomic Stats**: Metrics and counters use lock-protected structures to allow concurrent execution stress tests without data corruption.

---

## 11. Future Integration Points

This module is designed to integrate future providers with zero changes to target business logic:

*   **HashiCorp Vault**: 
    *   Create a new provider struct implementing `providers.SecretProvider`.
    *   Implement `GetSecret` to request secret data from `/v1/secret/data/{key}` using an authenticated Vault HTTP client.
    *   Register it via `SecretManager.RegisterProvider(vaultSecretProv)`.
*   **Consul**:
    *   Create a provider implementing `providers.Provider`.
    *   Implement `Load` and `Get` targeting Consul's KV endpoint `/v1/kv/`.
    *   Return `Watch() = true` and support consul long-polling KV updates.
*   **Kubernetes ConfigMaps**:
    *   Implement `providers.Provider` wrapping read directories. Kubernetes mounts ConfigMaps as files, so our `YAMLProvider`/`JSONProvider` already supports this out of the box.
*   **Cloud Secret Managers (AWS, Google, Azure)**:
    *   Implement a wrapper struct implementing `providers.SecretProvider` that calls AWS SecretsManager API (`GetSecretValue`), Azure KeyVault API, or Google Secret Manager API (`AccessSecretVersion`).
