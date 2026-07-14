package adapter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

type GoAdapter struct {
	BaseAdapter
}

func (a *GoAdapter) Validate(ctx context.Context, source string) error {
	if source == "" {
		return errors.New("go source code cannot be empty")
	}
	return nil
}

func (a *GoAdapter) Compile(ctx context.Context, req CompileRequest) (CompileResult, error) {
	start := time.Now()
	dir, err := a.CreateWorkspaceDir(req.SessionID)
	if err != nil {
		return CompileResult{Success: false}, err
	}

	srcPath := filepath.Join(dir, "main.go")
	if err := os.WriteFile(srcPath, []byte(req.Source), 0644); err != nil {
		return CompileResult{Success: false}, err
	}

	// Initialize modular context for Go build
	initCmd := exec.CommandContext(ctx, "go", "mod", "init", "main")
	initCmd.Dir = dir
	_ = initCmd.Run() // ignore failure if already initialized

	// Tidy up or download dependencies if any, but since we are doing simple code execution:
	var compilerBuf bytes.Buffer
	buildArgs := append([]string{"build", "-o", "main", "main.go"}, req.Options...)
	buildCmd := exec.CommandContext(ctx, "go", buildArgs...)
	buildCmd.Dir = dir
	buildCmd.Stdout = &compilerBuf
	buildCmd.Stderr = &compilerBuf

	err = buildCmd.Run()
	duration := time.Since(start)

	if err != nil {
		return CompileResult{
			Success:  false,
			Duration: duration,
			Output:   compilerBuf.String(),
		}, fmt.Errorf("go build failed: %w", err)
	}

	return CompileResult{
		ExecutablePath: filepath.Join(dir, "main"),
		Duration:       duration,
		Output:         compilerBuf.String(),
		Success:        true,
	}, nil
}

func (a *GoAdapter) Run(ctx context.Context, input RunInput) (RunResult, error) {
	dir := a.GetWorkspaceDir(input.SessionID)
	return a.RunCmd(ctx, input.ExecutablePath, input.Args, dir, input.Stdin, input.Stdout, input.Stderr, input.Env, input.ShutdownGrace, input.OnStart)
}

func (a *GoAdapter) Cleanup(ctx context.Context, sessionID string) error {
	return a.CleanupWorkspace(sessionID)
}
