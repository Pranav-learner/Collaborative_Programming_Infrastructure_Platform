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

type CAdapter struct {
	BaseAdapter
}

func (a *CAdapter) Validate(ctx context.Context, source string) error {
	if source == "" {
		return errors.New("c source code cannot be empty")
	}
	return nil
}

func (a *CAdapter) Compile(ctx context.Context, req CompileRequest) (CompileResult, error) {
	start := time.Now()
	dir, err := a.CreateWorkspaceDir(req.SessionID)
	if err != nil {
		return CompileResult{Success: false}, err
	}

	srcPath := filepath.Join(dir, "main.c")
	if err := os.WriteFile(srcPath, []byte(req.Source), 0644); err != nil {
		return CompileResult{Success: false}, err
	}

	var compilerBuf bytes.Buffer
	buildArgs := append([]string{"-o", "main", "main.c"}, req.Options...)
	buildCmd := exec.CommandContext(ctx, "gcc", buildArgs...)
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
		}, fmt.Errorf("gcc compilation failed: %w", err)
	}

	return CompileResult{
		ExecutablePath: filepath.Join(dir, "main"),
		Duration:       duration,
		Output:         compilerBuf.String(),
		Success:        true,
	}, nil
}

func (a *CAdapter) Run(ctx context.Context, input RunInput) (RunResult, error) {
	dir := a.GetWorkspaceDir(input.SessionID)
	return a.RunCmd(ctx, input.ExecutablePath, input.Args, dir, input.Stdin, input.Stdout, input.Stderr, input.Env, input.ShutdownGrace, input.OnStart)
}

func (a *CAdapter) Cleanup(ctx context.Context, sessionID string) error {
	return a.CleanupWorkspace(sessionID)
}
