// Package language implements the registry of supported execution languages. It
// is metadata only: it describes how a language would be compiled and run, and
// the resource envelope it is permitted, but performs no compilation or
// execution. The future runtime manager consumes this metadata.
package language

import (
	"errors"
	"sort"
	"sync"
	"time"

	"cpip/internal/execution/job"
)

// Status describes the availability of a language in the platform.
type Status uint8

const (
	// StatusStable is a fully supported language.
	StatusStable Status = iota
	// StatusBeta is available but not yet fully supported.
	StatusBeta
	// StatusDeprecated is scheduled for removal; still runnable.
	StatusDeprecated
	// StatusDisabled is registered but not runnable.
	StatusDisabled
)

// String returns the lowercase name of the status.
func (s Status) String() string {
	switch s {
	case StatusStable:
		return "stable"
	case StatusBeta:
		return "beta"
	case StatusDeprecated:
		return "deprecated"
	case StatusDisabled:
		return "disabled"
	default:
		return "unknown"
	}
}

// Language describes a supported language and its default execution envelope.
type Language struct {
	ID            string `json:"id"`
	DisplayName   string `json:"display_name"`
	Version       string `json:"version"`
	Compiler      string `json:"compiler"`
	Runtime       string `json:"runtime"`
	FileExtension string `json:"file_extension"`
	// CompileRequired reports whether a compile step precedes execution.
	CompileRequired bool `json:"compile_required"`
	// Profile is the default resource profile applied to jobs in this language.
	Profile job.ResourceProfile `json:"profile"`
	// Limits is the maximum resource envelope a request may ask for.
	Limits job.ResourceProfile `json:"limits"`
	// DefaultTimeout is the default wall-clock timeout for this language.
	DefaultTimeout time.Duration `json:"default_timeout"`
	// DefaultMemory is the default memory ceiling, in bytes.
	DefaultMemory int64 `json:"default_memory"`
	// Status is the availability of the language.
	Status Status `json:"status"`
	// PluginID is a forward-looking hook for a runtime plugin. Empty for now.
	PluginID string `json:"plugin_id,omitempty"`
}

// Runnable reports whether jobs may be submitted for this language.
func (l Language) Runnable() bool { return l.Status != StatusDisabled }

// ErrNotRegistered indicates a language ID is unknown to the registry.
var ErrNotRegistered = errors.New("execution/language: not registered")

// ErrAlreadyRegistered indicates a language ID is already present.
var ErrAlreadyRegistered = errors.New("execution/language: already registered")

// Registry is a concurrency-safe registry of supported languages.
type Registry struct {
	mu    sync.RWMutex
	langs map[string]Language
}

// NewRegistry constructs an empty language Registry.
func NewRegistry() *Registry {
	return &Registry{langs: make(map[string]Language)}
}

// Register adds a language. It returns ErrAlreadyRegistered if the ID exists.
func (r *Registry) Register(l Language) error {
	if l.ID == "" {
		return errors.New("execution/language: empty id")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.langs[l.ID]; exists {
		return ErrAlreadyRegistered
	}
	r.langs[l.ID] = l
	return nil
}

// Upsert adds or replaces a language regardless of prior presence.
func (r *Registry) Upsert(l Language) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.langs[l.ID] = l
}

// Get returns the language for the given ID.
func (r *Registry) Get(id string) (Language, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	l, ok := r.langs[id]
	if !ok {
		return Language{}, ErrNotRegistered
	}
	return l, nil
}

// Runnable returns the language for id only if it is runnable, otherwise an
// error suitable for surfacing an unsupported-language rejection.
func (r *Registry) Runnable(id string) (Language, error) {
	l, err := r.Get(id)
	if err != nil {
		return Language{}, err
	}
	if !l.Runnable() {
		return Language{}, ErrNotRegistered
	}
	return l, nil
}

// List returns all registered languages, sorted by ID.
func (r *Registry) List() []Language {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Language, 0, len(r.langs))
	for _, l := range r.langs {
		out = append(out, l)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Remove deletes a language from the registry.
func (r *Registry) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.langs, id)
}

// Count returns the number of registered languages.
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.langs)
}

// Default returns a Registry seeded with a representative set of languages. This
// is metadata for the orchestrator to validate against; nothing here runs code.
func Default() *Registry {
	r := NewRegistry()
	std := func(mem int64, cpu, pids int, to time.Duration) job.ResourceProfile {
		return job.ResourceProfile{
			Tier: "standard", MemoryBytes: mem, CPUMillicores: cpu,
			PidsLimit: pids, TmpfsBytes: 64 * 1024 * 1024, WallTimeout: to,
		}
	}
	seed := []Language{
		{ID: "python3", DisplayName: "Python", Version: "3.12", Compiler: "", Runtime: "cpython",
			FileExtension: ".py", CompileRequired: false,
			Profile:        std(256*1024*1024, 1000, 64, 10*time.Second),
			Limits:         std(512*1024*1024, 2000, 128, 30*time.Second),
			DefaultTimeout: 10 * time.Second, DefaultMemory: 256 * 1024 * 1024, Status: StatusStable},
		{ID: "go", DisplayName: "Go", Version: "1.26", Compiler: "gc", Runtime: "native",
			FileExtension: ".go", CompileRequired: true,
			Profile:        std(512*1024*1024, 2000, 128, 15*time.Second),
			Limits:         std(1024*1024*1024, 4000, 256, 45*time.Second),
			DefaultTimeout: 15 * time.Second, DefaultMemory: 512 * 1024 * 1024, Status: StatusStable},
		{ID: "javascript", DisplayName: "JavaScript", Version: "20", Compiler: "", Runtime: "node",
			FileExtension: ".js", CompileRequired: false,
			Profile:        std(256*1024*1024, 1000, 64, 10*time.Second),
			Limits:         std(512*1024*1024, 2000, 128, 30*time.Second),
			DefaultTimeout: 10 * time.Second, DefaultMemory: 256 * 1024 * 1024, Status: StatusStable},
		{ID: "c", DisplayName: "C", Version: "gcc-14", Compiler: "gcc", Runtime: "native",
			FileExtension: ".c", CompileRequired: true,
			Profile:        std(256*1024*1024, 1000, 64, 10*time.Second),
			Limits:         std(512*1024*1024, 2000, 128, 30*time.Second),
			DefaultTimeout: 10 * time.Second, DefaultMemory: 256 * 1024 * 1024, Status: StatusStable},
		{ID: "cpp", DisplayName: "C++", Version: "g++-14", Compiler: "g++", Runtime: "native",
			FileExtension: ".cpp", CompileRequired: true,
			Profile:        std(512*1024*1024, 2000, 128, 15*time.Second),
			Limits:         std(1024*1024*1024, 4000, 256, 45*time.Second),
			DefaultTimeout: 15 * time.Second, DefaultMemory: 512 * 1024 * 1024, Status: StatusStable},
		{ID: "java", DisplayName: "Java", Version: "21", Compiler: "javac", Runtime: "jvm",
			FileExtension: ".java", CompileRequired: true,
			Profile:        std(512*1024*1024, 2000, 256, 20*time.Second),
			Limits:         std(1024*1024*1024, 4000, 512, 60*time.Second),
			DefaultTimeout: 20 * time.Second, DefaultMemory: 512 * 1024 * 1024, Status: StatusBeta},
	}
	for _, l := range seed {
		r.Upsert(l)
	}
	return r
}
