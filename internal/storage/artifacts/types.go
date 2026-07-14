package artifacts

// Type is the taxonomy of artifacts the platform stores. It drives bucket
// routing, default retention, and default compression policy.
type Type string

const (
	ExecutionLog          Type = "execution_log"
	CompiledBinary        Type = "compiled_binary"
	ExecutionOutput       Type = "execution_output"
	CollaborationSnapshot Type = "collaboration_snapshot"
	WorkspaceArchive      Type = "workspace_archive"
	SourceArchive         Type = "source_archive"
	UploadedFile          Type = "uploaded_file"
	Template              Type = "template"
	RuntimeLog            Type = "runtime_log"
	DebugBundle           Type = "debug_bundle"
	// AIArtifact is reserved for a future AI-generation module.
	AIArtifact Type = "ai_artifact"
)

// Valid reports whether t is a recognized artifact type.
func (t Type) Valid() bool {
	switch t {
	case ExecutionLog, CompiledBinary, ExecutionOutput, CollaborationSnapshot,
		WorkspaceArchive, SourceArchive, UploadedFile, Template, RuntimeLog,
		DebugBundle, AIArtifact:
		return true
	}
	return false
}

// Compressible reports whether this artifact type benefits from compression by
// default. Already-compressed binaries are skipped to avoid wasted CPU.
func (t Type) Compressible() bool {
	switch t {
	case CompiledBinary, UploadedFile:
		// Binaries/uploads are often already compressed; let policy/heuristics decide.
		return false
	default:
		return true
	}
}

// Algorithm identifies a compression algorithm.
type Algorithm string

const (
	// None means the bytes are stored verbatim.
	None Algorithm = "none"
	// Gzip is the default general-purpose algorithm (stdlib, portable).
	Gzip Algorithm = "gzip"
	// Zstd/LZ4 are reserved for a future algorithm-selection feature.
	Zstd Algorithm = "zstd"
	LZ4  Algorithm = "lz4"
)

// AllTypes returns every known artifact type (for registry defaults).
func AllTypes() []Type {
	return []Type{
		ExecutionLog, CompiledBinary, ExecutionOutput, CollaborationSnapshot,
		WorkspaceArchive, SourceArchive, UploadedFile, Template, RuntimeLog,
		DebugBundle, AIArtifact,
	}
}
