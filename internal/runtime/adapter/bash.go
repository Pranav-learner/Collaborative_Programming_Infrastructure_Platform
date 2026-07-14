package adapter

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"
)

type BashAdapter struct {
	BaseAdapter
}

func (a *BashAdapter) Validate(ctx context.Context, source string) error {
	if source == "" {
		return errors.New("bash source code cannot be empty")
	}
	return nil
}

func (a *BashAdapter) Compile(ctx context.Context, req CompileRequest) (CompileResult, error) {
	start := time.Now()
	dir, err := a.CreateWorkspaceDir(req.SessionID)
	if err != nil {
		return CompileResult{Success: false}, err
	}

	srcPath := filepath.Join(dir, "script.sh")
	if err := os.WriteFile(srcPath, []byte(req.Source), 0755); err != nil {
		return CompileResult{Success: false}, err
	}

	return CompileResult{
		ExecutablePath: srcPath,
		Duration:       time.Since(start),
		Success:        true,
	}, nil
}

func (a *BashAdapter) Run(ctx context.Context, input RunInput) (RunResult, error) {
	dir := a.GetWorkspaceDir(input.SessionID)
	return a.RunCmd(ctx, "bash", append([]string{input.ExecutablePath}, input.Args...), dir, input.Stdin, input.Stdout, input.Stderr, input.Env, input.ShutdownGrace, input.OnStart)
}

func (a *BashAdapter) Cleanup(ctx context.Context, sessionID string) error {
	return a.CleanupWorkspace(sessionID)
}
