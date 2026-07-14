package compiler

import "time"

// DiagnosticType represents the severity of a compiler message.
type DiagnosticType string

const (
	SeverityError   DiagnosticType = "error"
	SeverityWarning DiagnosticType = "warning"
	SeverityInfo    DiagnosticType = "info"
)

// Diagnostic holds a compiler warning, error, or informational message.
type Diagnostic struct {
	File     string         `json:"file,omitempty"`
	Line     int            `json:"line,omitempty"`
	Column   int            `json:"column,omitempty"`
	Message  string         `json:"message"`
	Severity DiagnosticType `json:"severity"`
	Code     string         `json:"code,omitempty"`
}

// CompileRequest defines parameters for a compilation task.
type CompileRequest struct {
	SessionID string   `json:"session_id"`
	Source    string   `json:"source"`
	Options   []string `json:"options,omitempty"`
}

// CompileResult wraps the compiler output and artifact path info.
type CompileResult struct {
	Success        bool          `json:"success"`
	ExecutablePath string        `json:"executable_path"`
	Output         string        `json:"output"`
	Diagnostics    []Diagnostic  `json:"diagnostics,omitempty"`
	Duration       time.Duration `json:"duration"`
	Artifacts      []string      `json:"artifacts,omitempty"`
}
