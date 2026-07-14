// Package providers — YAML file provider.
package providers

import (
	"context"
	"fmt"
	"os"
	"strings"
)

// YAMLProvider reads configuration from a YAML file using a minimal key=value
// parser. For production, this could be replaced with gopkg.in/yaml.v3 but we
// keep zero external dependencies for the configuration package.
type YAMLProvider struct {
	path     string
	priority int
	data     map[string]string
}

// NewYAMLProvider creates a YAML file provider.
func NewYAMLProvider(path string, priority int) *YAMLProvider {
	return &YAMLProvider{path: path, priority: priority, data: make(map[string]string)}
}

func (p *YAMLProvider) Name() string { return "yaml" }

func (p *YAMLProvider) Load(_ context.Context) (map[string]string, error) {
	content, err := os.ReadFile(p.path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]string{}, nil
		}
		return nil, fmt.Errorf("yaml provider: failed to read %s: %w", p.path, err)
	}
	p.data = parseSimpleYAML(string(content))
	result := make(map[string]string, len(p.data))
	for k, v := range p.data {
		result[k] = v
	}
	return result, nil
}

func (p *YAMLProvider) Get(_ context.Context, key string) (string, bool, error) {
	v, ok := p.data[key]
	return v, ok, nil
}

func (p *YAMLProvider) Set(_ context.Context, _, _ string) error {
	return &ReadOnlyError{Provider: "yaml"}
}

func (p *YAMLProvider) Watch() bool  { return true }
func (p *YAMLProvider) Priority() int { return p.priority }

// parseSimpleYAML does a best-effort flat key-value parse of YAML content.
// Supports nested keys using indentation (2-space), mapping them to dot notation.
// This handles the 90% case; production can swap in a full YAML library.
func parseSimpleYAML(content string) map[string]string {
	result := make(map[string]string)
	var stack []string
	var indentStack []int

	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		indent := len(line) - len(strings.TrimLeft(line, " "))

		// Pop stack to match current indent
		for len(indentStack) > 0 && indent <= indentStack[len(indentStack)-1] {
			stack = stack[:len(stack)-1]
			indentStack = indentStack[:len(indentStack)-1]
		}

		parts := strings.SplitN(trimmed, ":", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// Remove quotes
		value = strings.Trim(value, `"'`)

		if value == "" {
			// This is a parent key — push onto stack
			stack = append(stack, key)
			indentStack = append(indentStack, indent)
		} else {
			fullKey := key
			if len(stack) > 0 {
				fullKey = strings.Join(stack, ".") + "." + key
			}
			result[fullKey] = value
		}
	}

	return result
}
