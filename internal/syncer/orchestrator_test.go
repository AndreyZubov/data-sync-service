package syncer

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/AndreyZubov/data-sync-service/internal/config"
	"github.com/AndreyZubov/data-sync-service/internal/retry"
	"github.com/AndreyZubov/data-sync-service/internal/salesforce"
	"github.com/AndreyZubov/data-sync-service/internal/salesforce/sftest"
	"github.com/AndreyZubov/data-sync-service/internal/store"
	"github.com/AndreyZubov/data-sync-service/internal/store/storetest"
)

var accountDescribe = sftest.Describe{Name: "Account", Queryable: true, Fields: []sftest.DescribeField{
	{Name: "Id", Type: "id"},
	{Name: "IsDeleted", Type: "boolean"},
	{Name: "Name", Type: "string"},
	{Name: "NumberOfEmployees", Type: "int"},
	{Name: "BillingAddress", Type: "address"},
	{Name: "SystemModstamp", Type: "datetime"},
}}

var accountHeader = []string{"Id", "IsDeleted", "Name", "NumberOfEmployees", "SystemModstamp"}

// env is a fake org and a database wired to an orchestrator.
type env struct {
	org  *sftest.Server
	db   *storetest.DB
	orch *Orchestrator
	reg  *prometheus.Registry
}

func newEnv(t *testing.T, maxConcurrent int) *env {
	t.Helper()
	db := storetest.New(t)
	org := sftest.NewServer(t)
	org.SetDescribe(accountDescribe)

	tokens := salesforce.NewClientCredentials(salesforce.ClientCredentialsConfig{
		LoginURL:     org.URL(),
		ClientID:     sftest.ClientID,
		ClientSecret: sftest.ClientSecret,
		TokenTTL:     time.Hour,
		HTTPClient:   org.HTTPClient(),
	})
	log := zaptest.NewLogger(t)
	sf := salesforce.NewClient(salesforce.ClientConfig{
		Tokens:             tokens,
		APIVersion:         sftest.APIVersion,
		HTTPClient:         org.HTTPClient(),
		Retry:              retry.Policy{MaxAttempts: 2, BaseDelay: time.Millisecond, MaxDelay: 10 * time.Millisecond},
		Logger:             log,
		JobPollInterval:    time.Millisecond,
		JobPollMaxInterval: 5 * time.Millisecond,
	})
	reg := prometheus.NewRegistry()
	orch := New(Config{
		Salesforce:        sf,
		Store:             db.Store,
		Workers:           3,
		MaxConcurrentRuns: maxConcurrent,
		BatchSize:         2,
		Logger:            log,
		Registerer:        reg,
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		assert.NoError(t, orch.Shutdown(ctx))
	})
	return &env{org: org, db: db, orch: orch, reg: reg}
}

// waitForStatus polls the run until it reaches one of statuses.
func (e *env) waitForStatus(t *testing.T, id int64, statuses ...store.RunStatus) store.Run {
	t.Helper()
	var run store.Run
	require.Eventually(t, func() bool {
		var err error
		run, err = e.orch.GetRun(context.Background(), id)
		require.NoError(t, err)
		for _, s := range statuses {
			if run.Status == s {
				return true
			}
		}
		return false
	}, 10*time.Second, 5*time.Millisecond, "run %d never reached %v", id, statuses)
	return run
}

func (e *env) export(t *testing.T, full bool) store.Run {
	t.Helper()
	run, err := e.orch.StartExport(context.Background(), "Account", store.Manual, full)
	require.NoError(t, err)
	return e.waitForStatus(t, run.ID, store.Succeeded, store.Failed, store.Cancelled)
}

func (e *env) count(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, e.db.Pool.QueryRow(context.Background(), sql, args...).Scan(&n))
	return n
}

func rowsOf(rows ...[]string) func(string, string) ([][]string, error) {
	return func(string, string) ([][]string, error) {
		return append([][]string{accountHeader}, rows...), nil
	}
}

func TestExportLoadsEveryRecordAndAdvancesTheWatermark(t *testing.T) {
	e := newEnv(t, 2)
	e.org.OnQuery(rowsOf(
		[]string{"001A", "false", "Acme", "250", "2026-10-01T12:00:00.000Z"},
		[]string{"001B", "false", "Globex, \"Global\"", "", "2026-10-01T12:30:00.000Z"},
		[]string{"001C", "true", "Initech", "10", "2026-10-01T12:15:00.000Z"},
		[]string{"001D", "false", "Line\nbreak", "1", "2026-10-01T11:00:00.000Z"},
		[]string{"001E", "false", "Hooli", "5", "2026-10-01T10:00:00.000Z"},
	))

	run := e.export(t, false)

	require.Equal(t, store.Succeeded, run.Status, run.Error)
	assert.EqualValues(t, 5, run.RecordsOK)
	assert.NotEmpty(t, run.BulkJobID)
	assert.Equal(t, 5, e.count(t, `SELECT count(*) FROM sf_records WHERE object_name = 'Account'`))
	assert.Equal(t, 1, e.count(t, `SELECT count(*) FROM sf_records WHERE is_deleted`))

	var data map[string]any
	require.NoError(t, e.db.Pool.QueryRow(context.Background(),
		`SELECT data FROM sf_records WHERE id = '001B'`).Scan(&data))
	assert.Equal(t, `Globex, "Global"`, data["Name"])
	assert.Nil(t, data["NumberOfEmployees"])
	assert.NotContains(t, data, "BillingAddress")

	jobs := e.org.QueryJobs()
	require.Len(t, jobs, 1)
	assert.Equal(t, "queryAll", jobs[0].Operation, "deletions are exported")
	assert.Equal(t, "SELECT Id, IsDeleted, Name, NumberOfEmployees, SystemModstamp FROM Account", jobs[0].Query)

	wm, err := e.db.Store.Watermark(context.Background(), "Account")
	require.NoError(t, err)
	require.NotNil(t, wm)
	assert.True(t, time.Date(2026, 10, 1, 12, 30, 0, 0, time.UTC).Equal(*wm), "got %s", wm)

	assert.InDelta(t, 5, testutil.ToFloat64(e.orch.metrics.records.WithLabelValues("Account", "export")), 0)
	assert.InDelta(t, 1, testutil.ToFloat64(e.orch.metrics.runs.WithLabelValues("Account", "export", "succeeded")), 0)
}

func TestSecondExportIsIncremental(t *testing.T) {
	e := newEnv(t, 1)
	e.org.OnQuery(rowsOf([]string{"001A", "false", "Acme", "1", "2026-10-01T12:00:00.000Z"}))
	require.Equal(t, store.Succeeded, e.export(t, false).Status)

	e.org.OnQuery(rowsOf([]string{"001A", "false", "Acme Corp", "2", "2026-10-02T08:00:00.000Z"}))
	require.Equal(t, store.Succeeded, e.export(t, false).Status)

	e.org.OnQuery(rowsOf())
	full := e.export(t, true)
	require.Equal(t, store.Succeeded, full.Status)
	assert.True(t, full.FullSync)

	jobs := e.org.QueryJobs()
	require.Len(t, jobs, 3)
	assert.True(t, strings.HasSuffix(jobs[1].Query, " WHERE SystemModstamp >= 2026-10-01T11:55:00Z"), jobs[1].Query)
	assert.NotContains(t, jobs[2].Query, "WHERE", "a full sync ignores the watermark")

	wm, err := e.db.Store.Watermark(context.Background(), "Account")
	require.NoError(t, err)
	assert.True(t, time.Date(2026, 10, 2, 8, 0, 0, 0, time.UTC).Equal(*wm), "an empty full sync keeps the watermark")
	var name string
	require.NoError(t, e.db.Pool.QueryRow(context.Background(),
		`SELECT data->>'Name' FROM sf_records WHERE id = '001A'`).Scan(&name))
	assert.Equal(t, "Acme Corp", name)
}

func TestExportOfObjectWithoutSystemFields(t *testing.T) {
	e := newEnv(t, 1)
	e.org.SetDescribe(sftest.Describe{Name: "Account", Queryable: true, Fields: []sftest.DescribeField{
		{Name: "Id", Type: "id"}, {Name: "Name", Type: "string"},
	}})
	e.org.OnQuery(func(string, string) ([][]string, error) {
		return [][]string{{"Id", "Name"}, {"001A", "Acme"}}, nil
	})

	run := e.export(t, false)
	require.Equal(t, store.Succeeded, run.Status, run.Error)
	run = e.export(t, false)
	require.Equal(t, store.Succeeded, run.Status, run.Error)

	jobs := e.org.QueryJobs()
	assert.Equal(t, "query", jobs[1].Operation)
	assert.NotContains(t, jobs[1].Query, "WHERE", "without SystemModstamp every run is a full sync")
	wm, err := e.db.Store.Watermark(context.Background(), "Account")
	require.NoError(t, err)
	assert.Nil(t, wm)
}

func TestFailedExportKeepsTheWatermark(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(org *sftest.Server)
		wantErr string
	}{
		{
			name: "query job fails",
			setup: func(org *sftest.Server) {
				org.OnQuery(func(string, string) ([][]string, error) { return nil, errors.New("QUERY_TIMEOUT") })
			},
			wantErr: "ended in state Failed: QUERY_TIMEOUT",
		},
		{
			name: "result has an unexpected column",
			setup: func(org *sftest.Server) {
				org.OnQuery(func(string, string) ([][]string, error) {
					return [][]string{{"Id", "Mystery"}, {"001A", "x"}}, nil
				})
			},
			wantErr: "result header has 2 columns, expected 5",
		},
		{
			name: "record has a malformed timestamp",
			setup: func(org *sftest.Server) {
				org.OnQuery(rowsOf([]string{"001A", "false", "Acme", "1", "not a time"}))
			},
			wantErr: "parse SystemModstamp",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t, 1)
			tt.setup(e.org)

			run := e.export(t, false)

			assert.Equal(t, store.Failed, run.Status)
			assert.Contains(t, run.Error, tt.wantErr)
			wm, err := e.db.Store.Watermark(context.Background(), "Account")
			require.NoError(t, err)
			assert.Nil(t, wm)
			assert.InDelta(t, 1, testutil.ToFloat64(e.orch.metrics.runs.WithLabelValues("Account", "export", "failed")), 0)
		})
	}
}

func TestExportOfUnknownObject(t *testing.T) {
	e := newEnv(t, 1)

	run, err := e.orch.StartExport(context.Background(), "Invoice__c", store.Manual, false)
	require.NoError(t, err)
	run = e.waitForStatus(t, run.ID, store.Failed)

	assert.Contains(t, run.Error, "describe Invoice__c")
	assert.Empty(t, e.org.QueryJobs())
}

func TestOneActiveExportPerObject(t *testing.T) {
	e := newEnv(t, 2)
	e.org.HoldJobs(true)

	first, err := e.orch.StartExport(context.Background(), "Account", store.Manual, false)
	require.NoError(t, err)
	e.waitForStatus(t, first.ID, store.Running)

	_, err = e.orch.StartExport(context.Background(), "Account", store.Manual, false)
	var inProgress *store.RunInProgressError
	require.ErrorAs(t, err, &inProgress)
	assert.Equal(t, first.ID, inProgress.RunID)

	e.org.HoldJobs(false)
	assert.Equal(t, store.Succeeded, e.waitForStatus(t, first.ID, store.Succeeded, store.Failed).Status)
}

func TestConcurrentRunsAreCapped(t *testing.T) {
	e := newEnv(t, 1)
	e.org.SetDescribe(sftest.Describe{Name: "Contact", Queryable: true, Fields: []sftest.DescribeField{{Name: "Id", Type: "id"}}})
	e.org.HoldJobs(true)

	account, err := e.orch.StartExport(context.Background(), "Account", store.Manual, false)
	require.NoError(t, err)
	e.waitForStatus(t, account.ID, store.Running)
	contact, err := e.orch.StartExport(context.Background(), "Contact", store.Manual, false)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	got, err := e.orch.GetRun(context.Background(), contact.ID)
	require.NoError(t, err)
	assert.Equal(t, store.Queued, got.Status, "waits for the only slot")

	e.org.HoldJobs(false)
	assert.Equal(t, store.Succeeded, e.waitForStatus(t, account.ID, store.Succeeded).Status)
	assert.Equal(t, store.Succeeded, e.waitForStatus(t, contact.ID, store.Succeeded).Status)
}

func TestShutdownCancelsActiveRuns(t *testing.T) {
	e := newEnv(t, 1)
	e.org.SetDescribe(sftest.Describe{Name: "Contact", Queryable: true, Fields: []sftest.DescribeField{{Name: "Id", Type: "id"}}})
	e.org.HoldJobs(true)
	running, err := e.orch.StartExport(context.Background(), "Account", store.Manual, false)
	require.NoError(t, err)
	e.waitForStatus(t, running.ID, store.Running)
	require.Eventually(t, func() bool { return len(e.org.QueryJobs()) == 1 }, 5*time.Second, 5*time.Millisecond)
	queued, err := e.orch.StartExport(context.Background(), "Contact", store.Manual, false)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, e.orch.Shutdown(ctx))

	for _, id := range []int64{running.ID, queued.ID} {
		run, err := e.orch.GetRun(context.Background(), id)
		require.NoError(t, err)
		assert.Equal(t, store.Cancelled, run.Status)
	}
	assert.Equal(t, "Aborted", e.org.QueryJobs()[0].State, "the Salesforce job is aborted")

	_, err = e.orch.StartExport(context.Background(), "Account", store.Manual, false)
	assert.ErrorIs(t, err, ErrShuttingDown)
	assert.ErrorIs(t, e.orch.Schedule(nil), ErrShuttingDown)
}

func TestShutdownGivesUpAtTheDeadline(t *testing.T) {
	e := newEnv(t, 1)
	e.orch.runs.Add(1) // a run that never finishes
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := e.orch.Shutdown(ctx)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	e.orch.runs.Done()
}

func TestScheduledExports(t *testing.T) {
	e := newEnv(t, 1)
	var mu sync.Mutex
	queries := 0
	e.org.OnQuery(func(string, string) ([][]string, error) {
		mu.Lock()
		defer mu.Unlock()
		queries++
		return [][]string{accountHeader}, nil
	})

	require.NoError(t, e.orch.Schedule([]config.ObjectSchedule{{Name: "Account", Cron: "@every 1s"}}))

	require.Eventually(t, func() bool {
		return e.count(t, `SELECT count(*) FROM sync_runs WHERE trigger = 'schedule' AND status = 'succeeded'`) >= 1
	}, 5*time.Second, 20*time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	assert.GreaterOrEqual(t, queries, 1)
}

func TestScheduleRejectsInvalidCron(t *testing.T) {
	e := newEnv(t, 1)
	err := e.orch.Schedule([]config.ObjectSchedule{{Name: "Account", Cron: "not cron"}})
	assert.ErrorContains(t, err, "schedule Account")
}

func TestScheduledExportSkipsWhileARunIsActive(t *testing.T) {
	e := newEnv(t, 1)
	e.org.HoldJobs(true)
	run, err := e.orch.StartExport(context.Background(), "Account", store.Manual, false)
	require.NoError(t, err)

	e.orch.scheduledSync("Account")

	assert.Equal(t, 1, e.count(t, `SELECT count(*) FROM sync_runs`))
	e.org.HoldJobs(false)
	e.waitForStatus(t, run.ID, store.Succeeded)
}

func TestRecoverInterruptedRuns(t *testing.T) {
	e := newEnv(t, 1)
	stale, err := e.db.Store.CreateRun(context.Background(), "Account", store.Export, store.Manual, false)
	require.NoError(t, err)

	require.NoError(t, e.orch.RecoverInterruptedRuns(context.Background()))

	got, err := e.orch.GetRun(context.Background(), stale.ID)
	require.NoError(t, err)
	assert.Equal(t, store.Failed, got.Status)
	_, err = e.orch.StartExport(context.Background(), "Account", store.Manual, false)
	assert.NoError(t, err, "the object is no longer blocked")
}
