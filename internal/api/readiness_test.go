package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func passing(context.Context) error { return nil }

func failing(context.Context) error { return errors.New("token endpoint said invalid_client") }

func TestReadiness(t *testing.T) {
	tests := []struct {
		name       string
		checks     []ReadinessCheck
		wantStatus int
		wantBody   readinessResponse
	}{
		{
			name:       "no checks",
			wantStatus: http.StatusOK,
			wantBody:   readinessResponse{Status: "ready"},
		},
		{
			name:       "all pass",
			checks:     []ReadinessCheck{{Name: "salesforce", Check: passing}, {Name: "postgres", Check: passing}},
			wantStatus: http.StatusOK,
			wantBody:   readinessResponse{Status: "ready", Checks: map[string]string{"salesforce": "ok", "postgres": "ok"}},
		},
		{
			name:       "one fails",
			checks:     []ReadinessCheck{{Name: "salesforce", Check: failing}, {Name: "postgres", Check: passing}},
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   readinessResponse{Status: "not ready", Checks: map[string]string{"salesforce": "fail", "postgres": "ok"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			core, logs := observer.New(zap.WarnLevel)
			h := NewServer(zap.New(core), prometheus.NewRegistry(), Options{ReadinessChecks: tt.checks}).Handler()

			rec := do(t, h, http.MethodGet, "/readyz")

			require.Equal(t, tt.wantStatus, rec.Code)
			var body readinessResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Equal(t, tt.wantBody, body)
			assert.NotContains(t, rec.Body.String(), "invalid_client", "errors stay out of the response")

			if tt.wantStatus != http.StatusOK {
				failures := logs.FilterMessage("readiness check failed").AllUntimed()
				require.Len(t, failures, 1)
				assert.Equal(t, "salesforce", failures[0].ContextMap()["check"])
				assert.Contains(t, failures[0].ContextMap()["error"], "invalid_client")
			}
		})
	}
}

func TestReadinessChecksRunWithADeadline(t *testing.T) {
	var hadDeadline bool
	check := ReadinessCheck{Name: "probe", Check: func(ctx context.Context) error {
		_, hadDeadline = ctx.Deadline()
		return nil
	}}
	h := NewServer(zap.NewNop(), prometheus.NewRegistry(), Options{ReadinessChecks: []ReadinessCheck{check}}).Handler()

	do(t, h, http.MethodGet, "/readyz")

	assert.True(t, hadDeadline)
}
