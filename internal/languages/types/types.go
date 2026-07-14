package types

// LanguageMetadata encapsulates all properties of a registered programming language plugin.
type LanguageMetadata struct {
	ID               string            `json:"id"`
	DisplayName      string            `json:"display_name"`
	Version          string            `json:"version"`
	Compiler         string            `json:"compiler"`
	Runtime          string            `json:"runtime"`
	Extension        string            `json:"extension"`
	CompileRequired  bool              `json:"compile_required"`
	ExecutionProfile string            `json:"execution_profile"`
	ResourceProfile  string            `json:"resource_profile"`
	Capabilities     []string          `json:"capabilities"`
	DefaultTemplate  string            `json:"default_template"`
	Status           string            `json:"status"` // "stable", "beta", "deprecated", "disabled"
	PluginVersion    string            `json:"plugin_version"`
	Metadata         map[string]string `json:"metadata,omitempty"`
}

// LanguageStats holds running execution statistics for a language plugin.
type LanguageStats struct {
	TotalExecutions      int64 `json:"total_executions"`
	SuccessfulExecutions int64 `json:"successful_executions"`
	FailedExecutions     int64 `json:"failed_executions"`
	TimedOutExecutions   int64 `json:"timed_out_executions"`
	CancelledExecutions  int64 `json:"cancelled_executions"`
	TotalCompilationTime int64 `json:"total_compilation_time_ms"`
	TotalExecutionTime   int64 `json:"total_execution_time_ms"`
}
