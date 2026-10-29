package syncer

// Failure-injection tests: the fake org answers chosen requests with errors,
// or the database refuses writes, part-way through a run. Each test checks
// that the run ends in a state the next run can safely build on.

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AndreyZubov/data-sync-service/internal/salesforce/sftest"
	"github.com/AndreyZubov/data-sync-service/internal/store"
)

// fiveAccounts is a result of five records whose latest SystemModstamp is
// 2026-10-01T14:00:00Z.
var fiveAccounts = rowsOf(
	[]string{"001A", "false", "Acme", "1", "2026-10-01T10:00:00.000Z"},
	[]string{"001B", "false", "Globex", "2", "2026-10-01T11:00:00.000Z"},
	[]string{"001C", "false", "Initech", "3", "2026-10-01T14:00:00.000Z"},
	[]string{"001D", "false", "Hooli", "4", "2026-10-01T12:00:00.000Z"},
	[]string{"001E", "false", "Umbrella", "5", "2026-10-01T13:00:00.000Z"},
)

func (e *env) watermark(t *testing.T) *time.Time {
	t.Helper()
	wm, err := e.db.Store.Watermark(context.Background(), "Account")
	require.NoError(t, err)
	return wm
}

func TestExportRidesOutTransientSalesforceFailures(t *testing.T) {
	tests := []struct {
		name  string
		fault sftest.Fault
	}{
		{
			name:  "describe answers 500",
			fault: sftest.Fault{Method: http.MethodGet, PathSuffix: "/describe", Times: 1, Status: http.StatusInternalServerError},
		},
		{
			name:  "job creation answers 503",
			fault: sftest.Fault{Method: http.MethodPost, PathSuffix: "/jobs/query", Times: 1, Status: http.StatusServiceUnavailable},
		},
		{
			name: "job status answers 502",
			fault: sftest.Fault{
				Method: http.MethodGet, PathSuffix: "/750fake00000001", Skip: 1, Times: 1, Status: http.StatusBadGateway,
			},
		},
		{
			name: "second result page answers 429 with Retry-After",
			fault: sftest.Fault{
				Method: http.MethodGet, PathSuffix: "/results", Skip: 1, Times: 1,
				Status: http.StatusTooManyRequests, Code: "REQUEST_LIMIT_EXCEEDED", RetryAfter: "0",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t, 1)
			e.org.OnQuery(fiveAccounts)
			e.org.SetResultPageSize(2)
			e.org.InjectFault(tt.fault)

			run := e.export(t, false)

			require.Equal(t, store.Succeeded, run.Status, run.Error)
			assert.Equal(t, 1, e.org.FaultsTriggered())
			assert.EqualValues(t, 5, run.RecordsOK)
			assert.Len(t, e.org.QueryJobs(), 1, "a retried request does not start a second job")
			require.NotNil(t, e.watermark(t))
		})
	}
}

func TestExportFailingMidStreamKeepsTheWatermarkAndRecovers(t *testing.T) {
	e := newEnv(t, 1)
	e.org.OnQuery(fiveAccounts)
	e.org.SetResultPageSize(2)
	e.org.InjectFault(sftest.Fault{Method: http.MethodGet, PathSuffix: "/results", Skip: 1, Status: http.StatusInternalServerError})

	failed := e.export(t, false)

	require.Equal(t, store.Failed, failed.Status)
	assert.Contains(t, failed.Error, "500")
	assert.LessOrEqual(t, e.count(t, `SELECT count(*) FROM sf_records`), 2, "at most the first page was loaded")
	assert.Nil(t, e.watermark(t), "a failed run does not move the watermark")

	e.org.ClearFaults()
	run := e.export(t, false)

	require.Equal(t, store.Succeeded, run.Status, run.Error)
	assert.Equal(t, 5, e.count(t, `SELECT count(*) FROM sf_records`))
	assert.True(t, time.Date(2026, 10, 1, 14, 0, 0, 0, time.UTC).Equal(*e.watermark(t)))
	jobs := e.org.QueryJobs()
	require.Len(t, jobs, 2)
	assert.NotContains(t, jobs[1].Query, "WHERE", "the retry reads everything the failed run missed")
}

func TestExportFailingDatabaseWritesKeepsTheWatermark(t *testing.T) {
	e := newEnv(t, 1)
	e.org.OnQuery(fiveAccounts)
	ctx := context.Background()
	_, err := e.db.Pool.Exec(ctx, `
		CREATE FUNCTION refuse_writes() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'injected fault: disk full'; END $$;
		CREATE TRIGGER refuse_writes BEFORE INSERT ON sf_records FOR EACH ROW EXECUTE FUNCTION refuse_writes();`)
	require.NoError(t, err)

	failed := e.export(t, false)

	require.Equal(t, store.Failed, failed.Status)
	assert.Contains(t, failed.Error, "disk full")
	assert.Zero(t, failed.RecordsOK, "counts only cover committed records")
	assert.Nil(t, e.watermark(t))

	_, err = e.db.Pool.Exec(ctx, `DROP TRIGGER refuse_writes ON sf_records`)
	require.NoError(t, err)
	run := e.export(t, false)
	require.Equal(t, store.Succeeded, run.Status, run.Error)
	assert.EqualValues(t, 5, run.RecordsOK)
	assert.NotNil(t, e.watermark(t))
}

func TestImportFailingBeforeSalesforceProcessesReleasesEntries(t *testing.T) {
	tests := []struct {
		name  string
		fault sftest.Fault
	}{
		{
			name:  "upload keeps failing",
			fault: sftest.Fault{Method: http.MethodPut, PathSuffix: "/batches", Status: http.StatusInternalServerError},
		},
		{
			// PATCH is not idempotent, so a 500 is not retried.
			name:  "closing the job fails",
			fault: sftest.Fault{Method: http.MethodPatch, PathSuffix: "/750ingest00001", Times: 1, Status: http.StatusInternalServerError},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t, 1)
			e.org.SetDescribe(contactDescribe)
			id := e.addEntry(t, store.Insert, "", `{"LastName":"Doe"}`)
			e.org.InjectFault(tt.fault)

			run := e.importContacts(t)

			require.Equal(t, store.Failed, run.Status)
			jobs := e.org.IngestJobs()
			require.Len(t, jobs, 1)
			assert.Equal(t, "Aborted", jobs[0].State, "the job is aborted so it cannot run later")
			assert.Equal(t, outboxState{status: store.Pending}, e.entry(t, id),
				"Salesforce processed nothing, so the entry is retried without using an attempt")

			e.org.ClearFaults()
			require.Equal(t, store.Succeeded, e.importContacts(t).Status)
			assert.Equal(t, store.Sent, e.entry(t, id).status)
		})
	}
}

func TestImportWhoseResultsCannotBeReadCollectsThemLater(t *testing.T) {
	e := newEnv(t, 1)
	e.org.SetDescribe(contactDescribe)
	id := e.addEntry(t, store.Insert, "", `{"LastName":"Doe"}`)
	e.org.InjectFault(sftest.Fault{Method: http.MethodGet, PathSuffix: "/successfulResults", Status: http.StatusInternalServerError})

	failed := e.importContacts(t)

	require.Equal(t, store.Failed, failed.Status)
	assert.Equal(t, outboxState{status: store.Pending}, e.entry(t, id),
		"the outcome is unknown, so the entry waits for the next run without using an attempt")
	assert.Equal(t, "JobComplete", e.org.IngestJobs()[0].State, "Salesforce did apply the change")

	e.org.ClearFaults()
	run := e.importContacts(t)

	require.Equal(t, store.Succeeded, run.Status, run.Error)
	assert.Len(t, e.org.IngestJobs(), 1, "the insert is not sent a second time")
	got := e.entry(t, id)
	assert.Equal(t, store.Sent, got.status)
	assert.NotEmpty(t, got.sfID)
}

func TestRequestLimitExceededMidRunPausesFurtherRuns(t *testing.T) {
	e := newEnvWithUsageThreshold(t, 0.8)
	e.org.OnQuery(fiveAccounts)
	e.org.SetResultPageSize(2)
	// The org runs out of API requests while the first page downloads, so the
	// response refusing the next page reports the spent allocation.
	e.org.InjectFault(sftest.Fault{
		Method: http.MethodGet, PathSuffix: "/results", Skip: 1, Status: http.StatusForbidden, Code: "REQUEST_LIMIT_EXCEEDED",
	})
	e.org.BeforeResultPage(func(*http.Request, string, string) { e.org.SetAPIUsage(15000, 15000) })

	run := e.export(t, false)

	require.Equal(t, store.Failed, run.Status)
	assert.Contains(t, run.Error, "REQUEST_LIMIT_EXCEEDED")
	assert.Equal(t, 1, e.org.FaultsTriggered(), "a spent allocation is not retried")
	_, err := e.orch.StartExport(context.Background(), "Account", store.Manual, false)
	var usage *APIUsageError
	assert.ErrorAs(t, err, &usage)
}

func TestTokenEndpointOutageFailsTheRunCleanly(t *testing.T) {
	e := newEnv(t, 1)
	e.org.OnQuery(fiveAccounts)
	e.org.RevokeTokens()
	e.org.OverrideTokenResponse(http.StatusServiceUnavailable, "maintenance")

	run := e.export(t, false)

	require.Equal(t, store.Failed, run.Status)
	assert.Contains(t, run.Error, "token endpoint: 503")
	assert.Empty(t, e.org.QueryJobs())

	e.org.OverrideTokenResponse(0, "")
	assert.Equal(t, store.Succeeded, e.export(t, false).Status)
}
