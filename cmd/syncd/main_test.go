package main

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/AndreyZubov/data-sync-service/internal/config"
)

// startServe runs serve on a free loopback port and returns the base URL, the
// cancel function that triggers shutdown, and a channel carrying serve's result.
func startServe(t *testing.T) (string, context.CancelFunc, <-chan error) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cfg := &config.Config{ShutdownTimeout: 5 * time.Second}
	done := make(chan error, 1)
	go func() { done <- serve(ctx, ln, cfg, zaptest.NewLogger(t)) }()

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

func TestServeAnswersUntilCancelledThenShutsDownCleanly(t *testing.T) {
	baseURL, cancel, done := startServe(t)

	resp, err := http.Get(baseURL + "/healthz")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp, err = http.Get(baseURL + "/metrics")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	cancel()
	require.NoError(t, waitFor(t, done))

	_, err = http.Get(baseURL + "/healthz")
	assert.Error(t, err, "the listener must be closed after shutdown")
}

func TestServeReportsServerFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	require.NoError(t, ln.Close())

	cfg := &config.Config{ShutdownTimeout: time.Second}
	err = serve(context.Background(), ln, cfg, zaptest.NewLogger(t))

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
		{name: "unknown log level", env: map[string]string{"LOG_LEVEL": "chatty"}, wantMsg: "parse log level"},
		{name: "address in use", env: map[string]string{"HTTP_ADDR": busy.Addr().String()}, wantMsg: "listen on"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			err := run()

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantMsg)
		})
	}
}
