package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	t.Setenv("SYNC_OBJECTS", "Account:*/15 * * * *")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, ":8080", cfg.HTTPAddr)
	assert.Equal(t, "60.0", cfg.SFAPIVersion)
	assert.Equal(t, "info", cfg.LogLevel)
	assert.Equal(t, 4, cfg.MaxConcurrentJobs)
	assert.Equal(t, 10, cfg.APIRateLimit)
	assert.Equal(t, 4, cfg.WorkerCount)
	assert.Equal(t, []ObjectSchedule{{Name: "Account", Cron: "*/15 * * * *"}}, cfg.SyncObjects)
}

func TestLoadReadsEnvironment(t *testing.T) {
	t.Setenv("HTTP_ADDR", ":9090")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("WORKER_COUNT", "16")
	t.Setenv("DATABASE_URL", "postgres://user:pass@db:5432/sync")
	t.Setenv("SYNC_OBJECTS", "")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, ":9090", cfg.HTTPAddr)
	assert.Equal(t, "debug", cfg.LogLevel)
	assert.Equal(t, 16, cfg.WorkerCount)
	assert.Equal(t, "postgres://user:pass@db:5432/sync", cfg.DatabaseURL)
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
