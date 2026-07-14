package sdk

import (
	"context"

	"cpip/internal/languages/compiler"
	"cpip/internal/languages/config"
	"cpip/internal/languages/runtime"
	"cpip/internal/languages/types"
)

// PluginSDK is the interface that every language plugin must implement.
// It decouples the engine's runtime implementation details from specific languages.
type PluginSDK interface {
	// Initialize prepares the plugin with custom and environment configuration.
	Initialize(ctx context.Context, cfg config.PluginConfig) error

	// Validate checks if the given source code conforms to language syntax rules or safety boundaries.
	Validate(ctx context.Context, source string) error

	// Compile performs workspace build and compilation. For interpreted languages,
	// it writes the script to disk and returns the execution path (no-op compilation).
	Compile(ctx context.Context, req compiler.CompileRequest) (compiler.CompileResult, error)

	// Run executes the prepared program inside the given environment using arguments, env vars, etc.
	Run(ctx context.Context, input runtime.RunInput) (runtime.RunResult, error)

	// Cleanup deletes all session-related files and releases any compiler/runtime resources.
	Cleanup(ctx context.Context, sessionID string) error

	// Capabilities returns a list of capabilities supported by the plugin (e.g. "concurrency", "networking").
	Capabilities() []string

	// Metadata returns the static language properties and capabilities descriptor.
	Metadata() types.LanguageMetadata

	// Health verifies if local compilers/interpreters and runtime system packages are present.
	Health(ctx context.Context) error

	// Version returns the plugin version string.
	Version() string
}
