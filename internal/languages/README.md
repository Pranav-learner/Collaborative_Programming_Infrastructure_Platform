# Extensible Language Plugin Framework

This framework provides an abstraction layer over programming language execution. By implementing the Plugin SDK, new language plugins can be registered, validated, initialized, and executed dynamically without modifying the core system.

## Architecture

The framework consists of a set of modular packages designed to avoid circular dependencies:

- **sdk**: Defines the core interface (`PluginSDK`) that all language plugins implement.
- **manager**: The orchestrator facade managing registration, load, hot-reload, unloading, and stats collection.
- **plugins**: Implements the plugin wrapper and its thread-safe lifecycle state machine.
- **compiler**: Standardizes compilation requests, results, diagnostic severity levels, and logs.
- **runtime**: Generalizes runtime environments, arguments, variables, and process execution outputs.
- **profiles**: Defines CPU, Memory, and File limit execution policies, as well as medium/large resource envelopes.
- **templates**: Stores and provides starter templates for language codes (HelloWorld, Class, Function).
- **events**: Emits asynchronous lifecycle events (e.g. `PluginRegistered`, `PluginReady`, `PluginExecutionStarted`).
- **validation**: Enforces version and capability checks before loading plugins.

---

## SDK Interface

Every language plugin must implement:

```go
type PluginSDK interface {
	Initialize(ctx context.Context, cfg config.PluginConfig) error
	Validate(ctx context.Context, source string) error
	Compile(ctx context.Context, req compiler.CompileRequest) (compiler.CompileResult, error)
	Run(ctx context.Context, input runtime.RunInput) (runtime.RunResult, error)
	Cleanup(ctx context.Context, sessionID string) error
	Capabilities() []string
	Metadata() types.LanguageMetadata
	Health(ctx context.Context) error
	Version() string
}
```

---

## Plugin Lifecycle States

Plugins transition through the following states:

```
Registered
    ↓
Validated
    ↓
  Loaded
    ↓
Initialized
    ↓
  Ready   ⇄   Executing
    ↓
  Idle
    ↓
Unloaded
    ↓
 Removed
```

Transitions are protected via thread-safe state machines and emit corresponding event bus topics.
