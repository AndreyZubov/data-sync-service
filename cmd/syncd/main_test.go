package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/AndreyZubov/data-sync-service/internal/config"
	"github.com/AndreyZubov/data-sync-service/internal/salesforce/sftest"
	"github.com/AndreyZubov/data-sync-service/internal/store/storetest"
)

// testConfig returns a configuration pointing at the fake org and a fresh
// database.
func testConfig(t *testing.T, org *sftest.Server, secret string) *config.Config {
	t.Helper()
	return &config.Config{
		ShutdownTimeout:   5 * time.Second,
		SFLoginURL:        org.URL(),
		SFClientID:        sftest.ClientID,
		SFClientSecret:    secret,
		SFAPIVersion:      sftest.APIVersion,
		SFTokenTTL:        time.Hour,
		DatabaseURL:       storetest.New(t).DSN,
		MaxConcurrentJobs: 2,
		WorkerCount:       2,
		SyncObjects:       []config.ObjectSchedule{{Name: "Account", Cron: "0 0 1 1 *"}},
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
	a, err := newApp(ctx, cfg, log, sfHTTP)
	require.NoError(t, err)
	t.Cleanup(a.store.Close)
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		done <- serve(ctx, ln, a.handler, cfg.ShutdownTimeout, log, a.orch.Shutdown)
	}()
	// Wait for serve, so the store is not closed under a running server.
	t.Cleanup(func() {
		cancel()
		<-finished
	})

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
	baseURL, cancel, done := startServe(t, testConfig(t, org, sftest.ClientSecret), org.HTTPClient())

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
		{name: "valid credentials", secret: sftest.ClientSecret, wantStatus: http.StatusOK, wantCheck: `"database":"ok","salesforce":"ok"`},
		{name: "rejected credentials", secret: "wrong", wantStatus: http.StatusServiceUnavailable, wantCheck: `"salesforce":"fail"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			org := sftest.NewServer(t)
			baseURL, _, _ := startServe(t, testConfig(t, org, tt.secret), org.HTTPClient())

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
	stopped := false
	stop := func(context.Context) error {
		stopped = true
		return errors.New("runs still active")
	}

	err = serve(context.Background(), ln, http.NotFoundHandler(), time.Second, zaptest.NewLogger(t), stop)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "http server")
	assert.Contains(t, err.Error(), "runs still active")
	assert.True(t, stopped, "background work is stopped after a server failure too")
}

func TestExportEndToEnd(t *testing.T) {
	org := sftest.NewServer(t)
	org.SetDescribe(sftest.Describe{Name: "Account", Queryable: true, Fields: []sftest.DescribeField{
		{Name: "Id", Type: "id"}, {Name: "Name", Type: "string"}, {Name: "SystemModstamp", Type: "datetime"},
	}})
	org.OnQuery(func(string, string) ([][]string, error) {
		return [][]string{
			{"Id", "Name", "SystemModstamp"},
			{"001A", "Acme", "2026-10-01T12:00:00.000Z"},
			{"001B", "Globex", "2026-10-01T12:05:00.000Z"},
			{"001C", "Initech", "2026-10-01T12:10:00.000Z"},
		}, nil
	})
	baseURL, _, _ := startServe(t, testConfig(t, org, sftest.ClientSecret), org.HTTPClient())

	resp, err := http.Post(baseURL+"/sync/account/export", "", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	location := resp.Header.Get("Location")
	require.NotEmpty(t, location)

	run := waitForRun(t, baseURL+location)
	assert.Equal(t, "succeeded", run.Status, run.Error)
	assert.Equal(t, 3, run.RecordsOK)
	_, metrics := get(t, baseURL+"/metrics")
	assert.Contains(t, metrics, `sync_records_total{direction="export",object="Account"} 3`)
}

func TestRestartResumesARunEndToEnd(t *testing.T) {
	org := sftest.NewServer(t)
	org.SetDescribe(sftest.Describe{Name: "Account", Queryable: true, Fields: []sftest.DescribeField{
		{Name: "Id", Type: "id"}, {Name: "Name", Type: "string"},
	}})
	org.OnQuery(func(string, string) ([][]string, error) {
		return [][]string{{"Id", "Name"}, {"001A", "Acme"}, {"001B", "Globex"}}, nil
	})
	org.SetAPIUsage(120, 15000)
	org.HoldJobs(true)
	cfg := testConfig(t, org, sftest.ClientSecret)
	baseURL, stop, done := startServe(t, cfg, org.HTTPClient())

	resp, err := http.Post(baseURL+"/sync/account/export", "", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	location := resp.Header.Get("Location")
	require.Eventually(t, func() bool { return len(org.QueryJobs()) == 1 }, 5*time.Second, 10*time.Millisecond)
	stop()
	require.NoError(t, waitFor(t, done))
	org.HoldJobs(false)

	baseURL, _, _ = startServe(t, cfg, org.HTTPClient())

	run := waitForRun(t, baseURL+location)
	assert.Equal(t, "succeeded", run.Status, run.Error)
	assert.Equal(t, 2, run.RecordsOK)
	assert.Len(t, org.QueryJobs(), 1, "the restarted service continues the same query job")
	_, metrics := get(t, baseURL+"/metrics")
	assert.Contains(t, metrics, `sync_runs_resumed_total{direction="export",object="Account"} 1`)
	assert.Contains(t, metrics, `salesforce_api_usage_limit 15000`)
}

// finishedRun is the part of a run response the end-to-end tests check.
type finishedRun struct {
	Status        string `json:"status"`
	RecordsOK     int    `json:"records_ok"`
	RecordsFailed int    `json:"records_failed"`
	Error         string `json:"error"`
}

// waitForRun polls the run at url until it finishes.
func waitForRun(t *testing.T, url string) finishedRun {
	t.Helper()
	var run finishedRun
	require.Eventually(t, func() bool {
		status, body := get(t, url)
		require.Equal(t, http.StatusOK, status)
		require.NoError(t, json.Unmarshal([]byte(body), &run))
		return run.Status != "queued" && run.Status != "running"
	}, 30*time.Second, 50*time.Millisecond)
	return run
}

func TestImportEndToEnd(t *testing.T) {
	org := sftest.NewServer(t)
	org.SetDescribe(sftest.Describe{Name: "Account", Fields: []sftest.DescribeField{
		{Name: "Id", Type: "id"}, {Name: "Name", Type: "string", Createable: true, Updateable: true},
	}})
	cfg := testConfig(t, org, sftest.ClientSecret)
	baseURL, _, _ := startServe(t, cfg, org.HTTPClient())
	db, err := pgx.Connect(context.Background(), cfg.DatabaseURL)
	require.NoError(t, err)
	defer db.Close(context.Background())
	_, err = db.Exec(context.Background(), `
		INSERT INTO sf_outbox (object_name, operation, payload) VALUES
			('Account', 'insert', '{"Name": "Acme"}'),
			('Account', 'insert', '{"Name": "Globex", "Rating": "Hot"}')`)
	require.NoError(t, err)

	resp, err := http.Post(baseURL+"/sync/account/import", "", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	run := waitForRun(t, baseURL+resp.Header.Get("Location"))
	assert.Equal(t, "succeeded", run.Status, run.Error)
	assert.Equal(t, 1, run.RecordsOK)
	assert.Equal(t, 1, run.RecordsFailed)
	require.Len(t, org.IngestJobs(), 1)
	assert.Equal(t, [][]string{{"Acme"}}, org.IngestJobs()[0].Rows)

	status, body := get(t, baseURL+"/sync/account/dead-letters")
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, body, `field \"Rating\" does not exist`)

	retry, err := http.Post(baseURL+"/sync/account/dead-letters/retry", "", nil)
	require.NoError(t, err)
	defer retry.Body.Close()
	raw, err := io.ReadAll(retry.Body)
	require.NoError(t, err)
	assert.JSONEq(t, `{"object": "Account", "requeued": 1}`, string(raw))
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
		{name: "unreachable database", wantMsg: "migrate database"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("SF_CLIENT_ID", "client-id")
			t.Setenv("SF_CLIENT_SECRET", "client-secret")
			t.Setenv("SF_LOGIN_URL", "https://example.my.salesforce.com")
			t.Setenv("DATABASE_URL", "postgres://nobody@127.0.0.1:1/none?sslmode=disable&connect_timeout=5")
			t.Setenv("HTTP_ADDR", "127.0.0.1:0")
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			err := run()

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantMsg)
		})
	}
}
