package filesystem

import (
	"fmt"
	"os"
	"path/filepath"
)

// FilesystemManager handles source files, inputs, and code output gathering from host workspaces.
type FilesystemManager struct{}

// NewFilesystemManager initializes a new FilesystemManager instance.
func NewFilesystemManager() *FilesystemManager {
	return &FilesystemManager{}
}

// InjectFiles writes code files, scripts, or runtime configs directly into the workspace path.
func (fm *FilesystemManager) InjectFiles(workspacePath string, files map[string]string) error {
	for name, content := range files {
		dest := filepath.Join(workspacePath, name)
		// Prevent path traversal attacks
		cleanDest := filepath.Clean(dest)
		cleanWork := filepath.Clean(workspacePath)
		if len(cleanDest) <= len(cleanWork) || cleanDest[:len(cleanWork)] != cleanWork {
			return fmt.Errorf("invalid file path path traversal detected: %s", name)
		}

		// Ensure parent directory exists
		if err := os.MkdirAll(filepath.Dir(cleanDest), 0755); err != nil {
			return fmt.Errorf("failed to create directory for file %s: %w", name, err)
		}

		if err := os.WriteFile(cleanDest, []byte(content), 0644); err != nil {
			return fmt.Errorf("failed to write file %s: %w", name, err)
		}
	}
	return nil
}

// ReadFile reads a file from the sandbox workspace.
func (fm *FilesystemManager) ReadFile(workspacePath string, filename string) (string, error) {
	path := filepath.Join(workspacePath, filename)
	cleanPath := filepath.Clean(path)
	cleanWork := filepath.Clean(workspacePath)
	if len(cleanPath) <= len(cleanWork) || cleanPath[:len(cleanWork)] != cleanWork {
		return "", fmt.Errorf("invalid file path path traversal detected: %s", filename)
	}

	content, err := os.ReadFile(cleanPath)
	if err != nil {
		return "", err
	}
	return string(content), nil
}
