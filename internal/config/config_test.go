package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setRequiredEnv sets the variables Load cannot do without to valid values.
func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("SF_CLIENT_ID", "client-id")
	t.Setenv("SF_CLIENT_SECRET", "client-secret")
	t.Setenv("SF_LOGIN_URL", "https://example.my.salesforce.com")
	t.Setenv("DATABASE_URL", "postgres://sync:sync@localhost:5432/sync")
}

func TestParseSyncObjects(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    []ObjectSchedule
		wantErr bool
	}{
		{
			name: "empty",
			in:   "",
			want: nil,
		},
		{
			name: "single",
			in:   "Account:*/15 * * * *",
			want: []ObjectSchedule{{Name: "Account", Cron: "*/15 * * * *"}},
		},
		{
			name: "multiple with spaces",
			in:   "Account:*/15 * * * * , Contact:0 * * * *",
			want: []ObjectSchedule{
				{Name: "Account", Cron: "*/15 * * * *"},
				{Name: "Contact", Cron: "0 * * * *"},
			},
		},
		{
			name: "trailing comma ignored",
			in:   "Account:0 * * * *,",
			want: []ObjectSchedule{{Name: "Account", Cron: "0 * * * *"}},
		},
		{
			name:    "missing colon",
			in:      "Account",
			wantErr: true,
		},
		{
			name:    "empty cron",
			in:      "Account:",
			wantErr: true,
		},
		{
			name: "custom and namespaced objects",
			in:   "Invoice__c:0 * * * *,ns__Order__c:30 2 * * *",
			want: []ObjectSchedule{
				{Name: "Invoice__c", Cron: "0 * * * *"},
				{Name: "ns__Order__c", Cron: "30 2 * * *"},
			},
		},
		{
			name:    "object name with SOQL metacharacters",
			in:      "Account WHERE Id != null:0 * * * *",
			wantErr: true,
		},
		{
			name:    "object name starting with a digit",
			in:      "1Account:0 * * * *",
			wantErr: true,
		},
		{
			name:    "duplicate object ignoring case",
			in:      "Account:0 * * * *,account:*/5 * * * *",
			wantErr: true,
		},
		{
			name:    "malformed cron",
			in:      "Account:every hour",
			wantErr: true,
		},
		{
			name:    "cron with seconds field",
			in:      "Account:0 0 * * * *",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSyncObjects(tt.in)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLoadAppliesDefaults(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("SYNC_OBJECTS", "Account:*/15 * * * *")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, ":8080", cfg.HTTPAddr)
	assert.Equal(t, "60.0", cfg.SFAPIVersion)
	assert.Equal(t, "info", cfg.LogLevel)
	assert.Equal(t, 4, cfg.MaxConcurrentJobs)
	assert.Equal(t, 10, cfg.APIRateLimit)
	assert.Equal(t, 80, cfg.APIUsageThreshold)
	assert.Equal(t, 4, cfg.WorkerCount)
	assert.Equal(t, 10000, cfg.ImportMaxRecords)
	assert.Equal(t, 5, cfg.ImportMaxAttempts)
	assert.Equal(t, 15*time.Second, cfg.ShutdownTimeout)
	assert.Equal(t, 30*time.Minute, cfg.SFTokenTTL)
	assert.Equal(t, []ObjectSchedule{{Name: "Account", Cron: "*/15 * * * *"}}, cfg.SyncObjects)
}

func TestLoadReadsEnvironment(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("SF_API_VERSION", "64.0")
	t.Setenv("SF_TOKEN_TTL", "1h")
	t.Setenv("HTTP_ADDR", ":9090")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("WORKER_COUNT", "16")
	t.Setenv("IMPORT_MAX_RECORDS", "500")
	t.Setenv("IMPORT_MAX_ATTEMPTS", "3")
	t.Setenv("API_USAGE_THRESHOLD", "0")
	t.Setenv("DATABASE_URL", "postgres://user:pass@db:5432/sync")
	t.Setenv("SYNC_OBJECTS", "")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, ":9090", cfg.HTTPAddr)
	assert.Equal(t, "debug", cfg.LogLevel)
	assert.Equal(t, 16, cfg.WorkerCount)
	assert.Equal(t, 500, cfg.ImportMaxRecords)
	assert.Equal(t, 3, cfg.ImportMaxAttempts)
	assert.Equal(t, 0, cfg.APIUsageThreshold, "zero turns the check off")
	assert.Equal(t, "postgres://user:pass@db:5432/sync", cfg.DatabaseURL)
	assert.Equal(t, "client-id", cfg.SFClientID)
	assert.Equal(t, "client-secret", cfg.SFClientSecret)
	assert.Equal(t, "https://example.my.salesforce.com", cfg.SFLoginURL)
	assert.Equal(t, "64.0", cfg.SFAPIVersion)
	assert.Equal(t, time.Hour, cfg.SFTokenTTL)
	assert.Empty(t, cfg.SyncObjects)
}

func TestLoadRejectsMalformedSyncObjects(t *testing.T) {
	t.Setenv("SYNC_OBJECTS", "Account")

	cfg, err := Load()

	require.Error(t, err)
	assert.Nil(t, cfg)
	assert.Contains(t, err.Error(), "parse SYNC_OBJECTS")
}

func TestLoadRejectsMalformedIntegers(t *testing.T) {
	t.Setenv("WORKER_COUNT", "many")

	cfg, err := Load()

	require.Error(t, err)
	assert.Nil(t, cfg)
	assert.Contains(t, err.Error(), "parse env")
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantMsg string
	}{
		{name: "zero workers", env: map[string]string{"WORKER_COUNT": "0"}, wantMsg: "WORKER_COUNT"},
		{name: "negative concurrency", env: map[string]string{"MAX_CONCURRENT_JOBS": "-1"}, wantMsg: "MAX_CONCURRENT_JOBS"},
		{name: "zero import batch", env: map[string]string{"IMPORT_MAX_RECORDS": "0"}, wantMsg: "IMPORT_MAX_RECORDS"},
		{name: "zero import attempts", env: map[string]string{"IMPORT_MAX_ATTEMPTS": "0"}, wantMsg: "IMPORT_MAX_ATTEMPTS"},
		{name: "zero rate limit", env: map[string]string{"API_RATE_LIMIT": "0"}, wantMsg: "API_RATE_LIMIT"},
		{name: "usage threshold above 100", env: map[string]string{"API_USAGE_THRESHOLD": "101"}, wantMsg: "API_USAGE_THRESHOLD"},
		{name: "negative usage threshold", env: map[string]string{"API_USAGE_THRESHOLD": "-1"}, wantMsg: "API_USAGE_THRESHOLD"},
		{name: "zero shutdown timeout", env: map[string]string{"SHUTDOWN_TIMEOUT": "0s"}, wantMsg: "SHUTDOWN_TIMEOUT"},
		{name: "plain http login URL", env: map[string]string{"SF_LOGIN_URL": "http://login.salesforce.com"}, wantMsg: "SF_LOGIN_URL"},
		{name: "unparsable login URL", env: map[string]string{"SF_LOGIN_URL": "https://[::1"}, wantMsg: "SF_LOGIN_URL"},
		{name: "relative login URL", env: map[string]string{"SF_LOGIN_URL": "login.salesforce.com"}, wantMsg: "SF_LOGIN_URL"},
		{name: "generic production login host", env: map[string]string{"SF_LOGIN_URL": "https://login.salesforce.com"}, wantMsg: "My Domain"},
		{name: "generic sandbox login host", env: map[string]string{"SF_LOGIN_URL": "https://TEST.salesforce.com/"}, wantMsg: "My Domain"},
		{name: "missing client ID", env: map[string]string{"SF_CLIENT_ID": ""}, wantMsg: "SF_CLIENT_ID is required"},
		{name: "blank client secret", env: map[string]string{"SF_CLIENT_SECRET": "  "}, wantMsg: "SF_CLIENT_SECRET is required"},
		{name: "missing login URL", env: map[string]string{"SF_LOGIN_URL": ""}, wantMsg: "SF_LOGIN_URL is required"},
		{name: "missing database URL", env: map[string]string{"DATABASE_URL": ""}, wantMsg: "DATABASE_URL is required"},
		{name: "API version with v prefix", env: map[string]string{"SF_API_VERSION": "v60.0"}, wantMsg: "SF_API_VERSION"},
		{name: "API version without minor", env: map[string]string{"SF_API_VERSION": "60"}, wantMsg: "SF_API_VERSION"},
		{name: "token TTL too short", env: map[string]string{"SF_TOKEN_TTL": "1m"}, wantMsg: "SF_TOKEN_TTL"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setRequiredEnv(t)
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			cfg, err := Load()

			require.Error(t, err)
			assert.Nil(t, cfg)
			assert.Contains(t, err.Error(), tt.wantMsg)
		})
	}
}

func TestLoadReportsAllInvalidValuesAtOnce(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("WORKER_COUNT", "0")
	t.Setenv("API_RATE_LIMIT", "0")

	_, err := Load()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "WORKER_COUNT")
	assert.Contains(t, err.Error(), "API_RATE_LIMIT")
}

func TestObjectNames(t *testing.T) {
	cfg := Config{SyncObjects: []ObjectSchedule{
		{Name: "Account", Cron: "0 * * * *"},
		{Name: "Contact", Cron: "0 * * * *"},
	}}

	assert.Equal(t, []string{"Account", "Contact"}, cfg.ObjectNames())
	assert.Empty(t, (&Config{}).ObjectNames())
}
