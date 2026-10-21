// Package config loads service configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/robfig/cron/v3"
)

// Config holds all runtime configuration, populated from the environment.
type Config struct {
	// HTTP control API.
	HTTPAddr        string        `env:"HTTP_ADDR" envDefault:":8080"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"15s"`

	// Salesforce.
	SFClientID     string        `env:"SF_CLIENT_ID"`
	SFClientSecret string        `env:"SF_CLIENT_SECRET"`
	SFLoginURL     string        `env:"SF_LOGIN_URL"`
	SFAPIVersion   string        `env:"SF_API_VERSION" envDefault:"60.0"`
	SFTokenTTL     time.Duration `env:"SF_TOKEN_TTL" envDefault:"30m"`

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

// objectNamePattern matches Salesforce object API names, including custom and
// namespaced objects such as "Invoice__c" or "ns__Invoice__c". Object names end
// up in SOQL and in table names, so anything outside this set is rejected.
var objectNamePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,79}$`)

// apiVersionPattern matches Salesforce API versions such as "60.0".
var apiVersionPattern = regexp.MustCompile(`^[1-9][0-9]*\.0$`)

// minTokenTTL keeps token refreshes infrequent: the client replaces a token a
// minute before it expires, so a shorter TTL would mean near-constant refreshes.
const minTokenTTL = 5 * time.Minute

// genericLoginHosts serve other OAuth flows but not client credentials, which
// Salesforce only offers on an org's My Domain.
var genericLoginHosts = map[string]bool{
	"login.salesforce.com": true,
	"test.salesforce.com":  true,
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

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &cfg, nil
}

// ObjectNames returns the names of all configured sync objects, in
// configuration order.
func (c *Config) ObjectNames() []string {
	names := make([]string, 0, len(c.SyncObjects))
	for _, o := range c.SyncObjects {
		names = append(names, o.Name)
	}
	return names
}

// validate checks value ranges that the env parser cannot express. All
// problems are reported together so a misconfigured deployment is fixed in one pass.
func (c *Config) validate() error {
	var errs []error

	positive := []struct {
		name  string
		value int
	}{
		{"MAX_CONCURRENT_JOBS", c.MaxConcurrentJobs},
		{"API_RATE_LIMIT", c.APIRateLimit},
		{"WORKER_COUNT", c.WorkerCount},
	}
	for _, p := range positive {
		if p.value < 1 {
			errs = append(errs, fmt.Errorf("%s must be at least 1, got %d", p.name, p.value))
		}
	}

	if c.ShutdownTimeout <= 0 {
		errs = append(errs, fmt.Errorf("SHUTDOWN_TIMEOUT must be positive, got %s", c.ShutdownTimeout))
	}

	required := []struct {
		name  string
		value string
	}{
		{"SF_CLIENT_ID", c.SFClientID},
		{"SF_CLIENT_SECRET", c.SFClientSecret},
		{"SF_LOGIN_URL", c.SFLoginURL},
	}
	for _, r := range required {
		if strings.TrimSpace(r.value) == "" {
			errs = append(errs, fmt.Errorf("%s is required", r.name))
		}
	}

	if c.SFLoginURL != "" {
		if err := validateLoginURL(c.SFLoginURL); err != nil {
			errs = append(errs, fmt.Errorf("SF_LOGIN_URL: %w", err))
		}
	}
	if !apiVersionPattern.MatchString(c.SFAPIVersion) {
		errs = append(errs, fmt.Errorf("SF_API_VERSION must look like \"60.0\", got %q", c.SFAPIVersion))
	}
	if c.SFTokenTTL < minTokenTTL {
		errs = append(errs, fmt.Errorf("SF_TOKEN_TTL must be at least %s, got %s", minTokenTTL, c.SFTokenTTL))
	}

	return errors.Join(errs...)
}

// validateLoginURL requires an absolute HTTPS My Domain URL: the client secret
// is sent to this endpoint, so plain HTTP is never acceptable.
func validateLoginURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse %q: %w", raw, err)
	}
	if u.Scheme != "https" || u.Host == "" {
		return fmt.Errorf("%q must be an absolute https URL", raw)
	}
	if genericLoginHosts[strings.ToLower(u.Hostname())] {
		return fmt.Errorf("%q does not support the client credentials flow; use the org's My Domain URL, e.g. https://example.my.salesforce.com", raw)
	}
	return nil
}

// parseSyncObjects parses a value like "Account:*/15 * * * *,Contact:0 * * * *"
// into a slice of ObjectSchedule. An empty input yields no schedules. Object
// names must be valid Salesforce API names, unique regardless of case (as
// Salesforce treats them), and schedules must be standard five-field cron
// expressions.
func parseSyncObjects(raw string) ([]ObjectSchedule, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	var out []ObjectSchedule
	seen := make(map[string]bool)
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		name, schedule, found := strings.Cut(entry, ":")
		if !found {
			return nil, fmt.Errorf("invalid entry %q: expected \"Object:cron\"", entry)
		}
		name = strings.TrimSpace(name)
		schedule = strings.TrimSpace(schedule)
		if name == "" || schedule == "" {
			return nil, fmt.Errorf("invalid entry %q: empty object or schedule", entry)
		}
		if !objectNamePattern.MatchString(name) {
			return nil, fmt.Errorf("invalid object name %q: expected a Salesforce API name", name)
		}
		key := strings.ToLower(name)
		if seen[key] {
			return nil, fmt.Errorf("duplicate object %q", name)
		}
		seen[key] = true
		if _, err := cron.ParseStandard(schedule); err != nil {
			return nil, fmt.Errorf("invalid schedule %q for %s: %w", schedule, name, err)
		}
		out = append(out, ObjectSchedule{Name: name, Cron: schedule})
	}
	return out, nil
}
