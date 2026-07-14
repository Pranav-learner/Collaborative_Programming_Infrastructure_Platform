package config

import (
	"fmt"
	"time"
)

// Config represents PostgreSQL database configuration options.
type Config struct {
	Host             string
	Port             int
	User             string
	Password         string
	DBName           string
	SSLMode          string
	MaxOpenConns     int
	MaxIdleConns     int
	ConnMaxLifetime  time.Duration
	ConnMaxIdleTime  time.Duration
	CommandTimeout   time.Duration
	RetryInterval    time.Duration
	MaxRetries       int
	RunMigrations    bool
	EnableAudit      bool
	DefaultIsolation string // "ReadCommitted", "RepeatableRead", "Serializable"
}

// DefaultConfig returns standard/production-safe default settings.
func DefaultConfig() Config {
	return Config{
		Host:             "localhost",
		Port:             5432,
		User:             "postgres",
		Password:         "postgres",
		DBName:           "cpip",
		SSLMode:          "disable",
		MaxOpenConns:     50,
		MaxIdleConns:     10,
		ConnMaxLifetime:  30 * time.Minute,
		ConnMaxIdleTime:  5 * time.Minute,
		CommandTimeout:   10 * time.Second,
		RetryInterval:    100 * time.Millisecond,
		MaxRetries:       3,
		RunMigrations:    true,
		EnableAudit:      true,
		DefaultIsolation: "ReadCommitted",
	}
}

// DSN generates the connection string for sql.Open.
func (c Config) DSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.DBName, c.SSLMode)
}
