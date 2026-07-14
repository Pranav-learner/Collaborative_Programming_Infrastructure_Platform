// Package providers — JSON file provider.
package providers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// JSONProvider reads configuration from a JSON file.
type JSONProvider struct {
	path     string
	priority int
	data     map[string]string
}

// NewJSONProvider creates a JSON file provider.
func NewJSONProvider(path string, priority int) *JSONProvider {
	return &JSONProvider{path: path, priority: priority, data: make(map[string]string)}
}

func (p *JSONProvider) Name() string { return "json" }

func (p *JSONProvider) Load(_ context.Context) (map[string]string, error) {
	content, err := os.ReadFile(p.path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]string{}, nil
		}
		return nil, fmt.Errorf("json provider: failed to read %s: %w", p.path, err)
	}

	var raw map[string]any
	if err := json.Unmarshal(content, &raw); err != nil {
		return nil, fmt.Errorf("json provider: failed to parse %s: %w", p.path, err)
	}

	p.data = flattenJSON("", raw)
	result := make(map[string]string, len(p.data))
	for k, v := range p.data {
		result[k] = v
	}
	return result, nil
}

func (p *JSONProvider) Get(_ context.Context, key string) (string, bool, error) {
	v, ok := p.data[key]
	return v, ok, nil
}

func (p *JSONProvider) Set(_ context.Context, _, _ string) error {
	return &ReadOnlyError{Provider: "json"}
}

func (p *JSONProvider) Watch() bool  { return true }
func (p *JSONProvider) Priority() int { return p.priority }

// flattenJSON recursively flattens nested maps into dot-separated keys.
func flattenJSON(prefix string, m map[string]any) map[string]string {
	result := make(map[string]string)
	for k, v := range m {
		fullKey := k
		if prefix != "" {
			fullKey = prefix + "." + k
		}
		switch val := v.(type) {
		case map[string]any:
			for fk, fv := range flattenJSON(fullKey, val) {
				result[fk] = fv
			}
		case string:
			result[fullKey] = val
		case float64:
			if val == float64(int64(val)) {
				result[fullKey] = strconv.FormatInt(int64(val), 10)
			} else {
				result[fullKey] = strconv.FormatFloat(val, 'f', -1, 64)
			}
		case bool:
			result[fullKey] = strconv.FormatBool(val)
		case nil:
			result[fullKey] = ""
		default:
			result[fullKey] = fmt.Sprintf("%v", val)
		}
	}
	return result
}

// MemoryProvider is an in-memory provider for testing and programmatic overrides.
type MemoryProvider struct {
	name     string
	priority int
	data     map[string]string
}

func NewMemoryProvider(name string, priority int) *MemoryProvider {
	return &MemoryProvider{name: name, priority: priority, data: make(map[string]string)}
}

func (p *MemoryProvider) Name() string { return p.name }

func (p *MemoryProvider) Load(_ context.Context) (map[string]string, error) {
	result := make(map[string]string, len(p.data))
	for k, v := range p.data {
		result[k] = v
	}
	return result, nil
}

func (p *MemoryProvider) Get(_ context.Context, key string) (string, bool, error) {
	v, ok := p.data[key]
	return v, ok, nil
}

func (p *MemoryProvider) Set(_ context.Context, key, value string) error {
	p.data[key] = value
	return nil
}

func (p *MemoryProvider) Watch() bool    { return false }
func (p *MemoryProvider) Priority() int  { return p.priority }
func (p *MemoryProvider) SetAll(data map[string]string) { p.data = data }

// Ensure interface compliance (compile-time check).
var (
	_ Provider = (*EnvProvider)(nil)
	_ Provider = (*YAMLProvider)(nil)
	_ Provider = (*JSONProvider)(nil)
	_ Provider = (*MemoryProvider)(nil)
)

// Placeholder: unused import silencer for strings
var _ = strings.Join
