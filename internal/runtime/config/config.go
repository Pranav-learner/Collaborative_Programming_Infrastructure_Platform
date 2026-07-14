package config

import (
	"errors"
	"time"
)

// Config holds the tunable settings for the execution runtime.
type Config struct {
	MaxOutputSize     int64         // Maximum total bytes captured for output (stdout + stderr)
	MaxStdoutSize     int64         // Maximum stdout bytes
	MaxStderrSize     int64         // Maximum stderr bytes
	ChunkSize         int           // Size of streamed stdout/stderr chunks in bytes
	FlushInterval     time.Duration // Maximum duration to buffer output before flushing
	CompileTimeout    time.Duration // Timeout for compiler steps
	RunTimeout        time.Duration // Timeout for execution steps
	IdleTimeout       time.Duration // Timeout for idle connections/streams
	CleanupTimeout    time.Duration // Timeout for cleanup steps
	BufferSize        int           // Channel buffer size for streaming
	StreamingInterval time.Duration // Interval for execution progress ticks
	ShutdownGrace     time.Duration // Timeout for graceful termination before SIGKILL
}

// Default returns a Config initialized with production-sensible defaults.
func Default() Config {
	return Config{
		MaxOutputSize:     10 * 1024 * 1024, // 10MB
		MaxStdoutSize:     5 * 1024 * 1024,  // 5MB
		MaxStderrSize:     5 * 1024 * 1024,  // 5MB
		ChunkSize:         4096,             // 4KB chunks
		FlushInterval:     50 * time.Millisecond,
		CompileTimeout:    10 * time.Second,
		RunTimeout:        15 * time.Second,
		IdleTimeout:       30 * time.Second,
		CleanupTimeout:    5 * time.Second,
		BufferSize:        1000,
		StreamingInterval: 100 * time.Millisecond,
		ShutdownGrace:     2 * time.Second,
	}
}

// Validate checks that the configuration settings are valid and safe.
func (c Config) Validate() error {
	if c.MaxOutputSize <= 0 {
		return errors.New("MaxOutputSize must be greater than zero")
	}
	if c.MaxStdoutSize <= 0 || c.MaxStderrSize <= 0 {
		return errors.New("MaxStdoutSize and MaxStderrSize must be greater than zero")
	}
	if c.ChunkSize <= 0 {
		return errors.New("ChunkSize must be greater than zero")
	}
	if c.FlushInterval <= 0 {
		return errors.New("FlushInterval must be greater than zero")
	}
	if c.CompileTimeout <= 0 || c.RunTimeout <= 0 {
		return errors.New("CompileTimeout and RunTimeout must be greater than zero")
	}
	if c.CleanupTimeout <= 0 {
		return errors.New("CleanupTimeout must be greater than zero")
	}
	if c.BufferSize <= 0 {
		return errors.New("BufferSize must be greater than zero")
	}
	if c.StreamingInterval <= 0 {
		return errors.New("StreamingInterval must be greater than zero")
	}
	if c.ShutdownGrace <= 0 {
		return errors.New("ShutdownGrace must be greater than zero")
	}
	return nil
}
