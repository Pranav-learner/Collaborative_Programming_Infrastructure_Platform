// Package validation provides schema, type, range, dependency, and custom
// validators for configuration values.
package validation

import (
	"fmt"
	"strconv"
	"strings"
)

// Rule defines a single validation constraint on a configuration key.
type Rule struct {
	Key        string
	Required   bool
	Type       string   // "string", "int", "float", "bool", "duration"
	MinInt     *int64
	MaxInt     *int64
	AllowedSet []string // If non-empty, value must be in this set
	DependsOn  string   // If non-empty, this key is required if DependsOn is present
	CustomFunc func(value string) error
}

// Validator holds a set of validation rules and executes them against a snapshot.
type Validator struct {
	rules []Rule
}

// NewValidator creates a Validator.
func NewValidator() *Validator {
	return &Validator{}
}

// AddRule registers a validation rule.
func (v *Validator) AddRule(r Rule) {
	v.rules = append(v.rules, r)
}

// ValidationError collects all failed validations.
type ValidationError struct {
	Errors []string
}

func (e *ValidationError) Error() string {
	return "configuration validation failed: " + strings.Join(e.Errors, "; ")
}

// Validate checks a snapshot against all rules. Returns nil if valid.
func (v *Validator) Validate(snapshot map[string]string) error {
	var errs []string

	for _, rule := range v.rules {
		val, exists := snapshot[rule.Key]

		// Dependency check
		if rule.DependsOn != "" && exists {
			if _, depExists := snapshot[rule.DependsOn]; !depExists {
				errs = append(errs, fmt.Sprintf("key %q requires key %q to be present", rule.Key, rule.DependsOn))
				continue
			}
		}

		// Required check
		if rule.Required && !exists {
			errs = append(errs, fmt.Sprintf("required key %q is missing", rule.Key))
			continue
		}

		if !exists {
			continue
		}

		// Type check
		switch rule.Type {
		case "int":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				errs = append(errs, fmt.Sprintf("key %q must be an integer, got %q", rule.Key, val))
				continue
			}
			if rule.MinInt != nil && n < *rule.MinInt {
				errs = append(errs, fmt.Sprintf("key %q value %d is below minimum %d", rule.Key, n, *rule.MinInt))
			}
			if rule.MaxInt != nil && n > *rule.MaxInt {
				errs = append(errs, fmt.Sprintf("key %q value %d exceeds maximum %d", rule.Key, n, *rule.MaxInt))
			}
		case "float":
			if _, err := strconv.ParseFloat(val, 64); err != nil {
				errs = append(errs, fmt.Sprintf("key %q must be a float, got %q", rule.Key, val))
			}
		case "bool":
			if _, err := strconv.ParseBool(val); err != nil {
				errs = append(errs, fmt.Sprintf("key %q must be a boolean, got %q", rule.Key, val))
			}
		}

		// Allowed set check
		if len(rule.AllowedSet) > 0 {
			found := false
			for _, allowed := range rule.AllowedSet {
				if val == allowed {
					found = true
					break
				}
			}
			if !found {
				errs = append(errs, fmt.Sprintf("key %q value %q not in allowed set %v", rule.Key, val, rule.AllowedSet))
			}
		}

		// Custom validation
		if rule.CustomFunc != nil {
			if err := rule.CustomFunc(val); err != nil {
				errs = append(errs, fmt.Sprintf("key %q custom validation failed: %v", rule.Key, err))
			}
		}
	}

	if len(errs) > 0 {
		return &ValidationError{Errors: errs}
	}
	return nil
}
