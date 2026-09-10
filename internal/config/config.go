// Package config loads service configuration from environment variables.
package config

import (
	"fmt"
	"strings"

	"github.com/caarlos0/env/v11"
)

// Config holds all runtime configuration, populated from the environment.
type Config struct {
	// HTTP control API.
	HTTPAddr string `env:"HTTP_ADDR" envDefault:":8080"`

	// Salesforce.
	SFClientID     string `env:"SF_CLIENT_ID"`
	SFClientSecret string `env:"SF_CLIENT_SECRET"`
	SFLoginURL     string `env:"SF_LOGIN_URL"`
	SFAPIVersion   string `env:"SF_API_VERSION" envDefault:"60.0"`

	// Postgres.
	DatabaseURL string `env:"DATABASE_URL"`

	// Sync behaviour.
	SyncObjectsRaw    string `env:"SYNC_OBJECTS"`
	MaxConcurrentJobs int    `env:"MAX_CONCURRENT_JOBS" envDefault:"4"`
	APIRateLimit      int    `env:"API_RATE_LIMIT" envDefault:"10"`
	WorkerCount       int    `env:"WORKER_COUNT" envDefault:"4"`

	// Observability.
	LogLevel string `env:"LOG_LEVEL" envDefault:"info"`

	// SyncObjects is the parsed form of SyncObjectsRaw.
	SyncObjects []ObjectSchedule `env:"-"`
}

// ObjectSchedule pairs a Salesforce object name with its cron schedule.
type ObjectSchedule struct {
	Name string
	Cron string
}

// Load reads configuration from the environment and validates it.
func Load() (*Config, error) {
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return nil, fmt.Errorf("parse env: %w", err)
	}

	schedules, err := parseSyncObjects(cfg.SyncObjectsRaw)
	if err != nil {
		return nil, fmt.Errorf("parse SYNC_OBJECTS: %w", err)
	}
	cfg.SyncObjects = schedules

	return &cfg, nil
}

// parseSyncObjects parses a value like "Account:*/15 * * * *,Contact:0 * * * *"
// into a slice of ObjectSchedule. An empty input yields no schedules.
func parseSyncObjects(raw string) ([]ObjectSchedule, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	var out []ObjectSchedule
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		// Split on the first colon only: the cron expression itself contains colons? No,
		// but it does contain spaces and slashes, so we only separate name from schedule.
		name, cron, found := strings.Cut(entry, ":")
		if !found {
			return nil, fmt.Errorf("invalid entry %q: expected \"Object:cron\"", entry)
		}
		name = strings.TrimSpace(name)
		cron = strings.TrimSpace(cron)
		if name == "" || cron == "" {
			return nil, fmt.Errorf("invalid entry %q: empty object or schedule", entry)
		}
		out = append(out, ObjectSchedule{Name: name, Cron: cron})
	}
	return out, nil
}
