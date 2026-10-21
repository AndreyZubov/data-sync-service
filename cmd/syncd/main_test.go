package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/AndreyZubov/data-sync-service/internal/config"
	"github.com/AndreyZubov/data-sync-service/internal/salesforce/sftest"
)

// testConfig returns a configuration pointing at the fake org.
func testConfig(org *sftest.Server, secret string) *config.Config {
	return &config.Config{
		ShutdownTimeout: 5 * time.Second,
		SFLoginURL:      org.URL(),
		SFClientID:      sftest.ClientID,
		SFClientSecret:  secret,
		SFAPIVersion:    sftest.APIVersion,
		SFTokenTTL:      time.Hour,
	}
}

// startServe runs the fully wired service on a free loopback port and returns
// the base URL, the cancel function that triggers shutdown, and a channel
// carrying serve's result.
func startServe(t *testing.T, cfg *config.Config, sfHTTP *http.Client) (string, context.CancelFunc, <-chan error) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	log := zaptest.NewLogger(t)
	done := make(chan error, 1)
	go func() { done <- serve(ctx, ln, newHandler(cfg, log, sfHTTP), cfg.ShutdownTimeout, log) }()

	return "http://" + ln.Addr().String(), cancel, done
}

// waitFor returns serve's result, failing the test if it does not arrive in time.
func waitFor(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not return in time")
		return nil
	}
}

// get returns the status code and body of a GET request.
func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(body)
}

func TestServeAnswersUntilCancelledThenShutsDownCleanly(t *testing.T) {
	org := sftest.NewServer(t)
	baseURL, cancel, done := startServe(t, testConfig(org, sftest.ClientSecret), org.HTTPClient())

	status, _ := get(t, baseURL+"/healthz")
	assert.Equal(t, http.StatusOK, status)
	status, body := get(t, baseURL+"/metrics")
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, body, "go_goroutines", "runtime collectors are registered")

	cancel()
	require.NoError(t, waitFor(t, done))

	_, err := http.Get(baseURL + "/healthz")
	assert.Error(t, err, "the listener must be closed after shutdown")
}

func TestReadinessFollowsSalesforceAuthentication(t *testing.T) {
	tests := []struct {
		name       string
		secret     string
		wantStatus int
		wantCheck  string
	}{
		{name: "valid credentials", secret: sftest.ClientSecret, wantStatus: http.StatusOK, wantCheck: `"salesforce":"ok"`},
		{name: "rejected credentials", secret: "wrong", wantStatus: http.StatusServiceUnavailable, wantCheck: `"salesforce":"fail"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			org := sftest.NewServer(t)
			baseURL, _, _ := startServe(t, testConfig(org, tt.secret), org.HTTPClient())

			status, body := get(t, baseURL+"/readyz")
			assert.Equal(t, tt.wantStatus, status)
			assert.Contains(t, body, tt.wantCheck)

			get(t, baseURL+"/readyz")
			if tt.wantStatus == http.StatusOK {
				assert.Equal(t, 1, org.TokenRequests(), "repeated probes reuse the cached token")
			}
		})
	}
}

func TestServeReportsServerFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	require.NoError(t, ln.Close())

	err = serve(context.Background(), ln, http.NotFoundHandler(), time.Second, zaptest.NewLogger(t))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "http server")
}

func TestRunFailsFast(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = busy.Close() })

	tests := []struct {
		name    string
		env     map[string]string
		wantMsg string
	}{
		{name: "invalid config", env: map[string]string{"WORKER_COUNT": "0"}, wantMsg: "invalid config"},
		{name: "missing credentials", env: map[string]string{"SF_CLIENT_SECRET": ""}, wantMsg: "SF_CLIENT_SECRET is required"},
		{name: "unknown log level", env: map[string]string{"LOG_LEVEL": "chatty"}, wantMsg: "parse log level"},
		{name: "address in use", env: map[string]string{"HTTP_ADDR": busy.Addr().String()}, wantMsg: "listen on"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("SF_CLIENT_ID", "client-id")
			t.Setenv("SF_CLIENT_SECRET", "client-secret")
			t.Setenv("SF_LOGIN_URL", "https://example.my.salesforce.com")
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			err := run()

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantMsg)
		})
	}
}
