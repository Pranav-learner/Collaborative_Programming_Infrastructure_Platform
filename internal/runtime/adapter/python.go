package adapter

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"
)

type PythonAdapter struct {
	BaseAdapter
}

func (a *PythonAdapter) Validate(ctx context.Context, source string) error {
	if source == "" {
		return errors.New("python source code cannot be empty")
	}
	return nil
}

func (a *PythonAdapter) Compile(ctx context.Context, req CompileRequest) (CompileResult, error) {
	start := time.Now()
	dir, err := a.CreateWorkspaceDir(req.SessionID)
	if err != nil {
		return CompileResult{Success: false}, err
	}

	srcPath := filepath.Join(dir, "main.py")
	if err := os.WriteFile(srcPath, []byte(req.Source), 0644); err != nil {
		return CompileResult{Success: false}, err
	}

	return CompileResult{
		ExecutablePath: srcPath,
		Duration:       time.Since(start),
		Success:        true,
	}, nil
}

func (a *PythonAdapter) Run(ctx context.Context, input RunInput) (RunResult, error) {
	dir := a.GetWorkspaceDir(input.SessionID)
	// Execute python3 main.py
	return a.RunCmd(ctx, "python3", append([]string{input.ExecutablePath}, input.Args...), dir, input.Stdin, input.Stdout, input.Stderr, input.Env, input.ShutdownGrace, input.OnStart)
}

func (a *PythonAdapter) Cleanup(ctx context.Context, sessionID string) error {
	return a.CleanupWorkspace(sessionID)
}
