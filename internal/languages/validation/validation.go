package validation

import (
	"errors"
	"fmt"
	"regexp"

	"cpip/internal/languages/types"
)

var (
	semverRegex = regexp.MustCompile(`^v?(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)(?:-(?P<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+(?P<buildmetadata>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$`)
)

// ValidateMetadata performs verification checks on language metadata based on version policy.
func ValidateMetadata(meta types.LanguageMetadata, policy string) error {
	if meta.ID == "" {
		return errors.New("plugin ID is required")
	}
	if meta.DisplayName == "" {
		return errors.New("plugin display_name is required")
	}
	if meta.Version == "" {
		return errors.New("plugin version is required")
	}

	// Validate version formats
	if policy == "strict" {
		if !semverRegex.MatchString(meta.PluginVersion) {
			return fmt.Errorf("plugin version '%s' is not valid SemVer under strict policy", meta.PluginVersion)
		}
	} else {
		if meta.PluginVersion == "" {
			return errors.New("plugin_version cannot be empty")
		}
	}

	switch meta.Status {
	case "stable", "beta", "deprecated", "disabled":
	case "":
		return errors.New("status must be configured")
	default:
		return fmt.Errorf("invalid status value: %s", meta.Status)
	}

	return nil
}

// VerifyCapabilities checks if the plugin satisfies all requested capabilities.
func VerifyCapabilities(pluginCaps []string, reqCaps []string) error {
	capsMap := make(map[string]bool)
	for _, c := range pluginCaps {
		capsMap[c] = true
	}
	for _, req := range reqCaps {
		if !capsMap[req] {
			return fmt.Errorf("requested capability not supported by plugin: %s", req)
		}
	}
	return nil
}
