package syncer

import (
	"context"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AndreyZubov/data-sync-service/internal/store"
)

// These tests inject failures into runs: the service stops in the middle of
// a run, as it does on a deployment or a crash, and a new orchestrator on the
// same database must finish the run without losing or repeating work.

// stopService shuts e's orchestrator down, as a deployment would.
func (e *env) stopService(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, e.orch.Shutdown(ctx))
}

// locatorLog records the result pages the org serves.
type locatorLog struct {
	mu       sync.Mutex
	locators []string
}

func (l *locatorLog) add(locator string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.locators = append(l.locators, locator)
}

func (l *locatorLog) get() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.locators...)
}

func TestExportResumesFromItsCheckpoint(t *testing.T) {
	e := newEnv(t, 1)
	e.org.OnQuery(rowsOf(
		[]string{"001A", "false", "A", "1", "2026-10-01T10:00:00.000Z"},
		[]string{"001B", "false", "B", "1", "2026-10-01T11:00:00.000Z"},
		[]string{"001C", "false", "C", "1", "2026-10-01T15:00:00.000Z"},
		[]string{"001D", "false", "D", "1", "2026-10-01T12:00:00.000Z"},
		[]string{"001E", "false", "E", "1", "2026-10-01T13:00:00.000Z"},
		[]string{"001F", "false", "F", "1", "2026-10-01T14:00:00.000Z"},
	))
	// Pages hold two rows, so the locators are "", "2", and "4". The third
	// page never arrives: the download hangs until the service stops.
	e.org.BeforeResultPage(func(r *http.Request, _, locator string) {
		if locator == "4" {
			<-r.Context().Done()
		}
	})
	run, err := e.orch.StartExport(context.Background(), "Account", store.Manual, false)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		got, err := e.orch.GetRun(context.Background(), run.ID)
		require.NoError(t, err)
		return got.Checkpoint.Locator == "4"
	}, 10*time.Second, 5*time.Millisecond, "the first two pages are committed")
	e.stopService(t)

	var served locatorLog
	e.org.BeforeResultPage(func(_ *http.Request, _, locator string) { served.add(locator) })
	after := e.restart(t)
	require.NoError(t, after.orch.RecoverInterruptedRuns(context.Background()))
	got := after.waitForStatus(t, run.ID, store.Succeeded, store.Failed, store.Cancelled)

	require.Equal(t, store.Succeeded, got.Status, got.Error)
	assert.Equal(t, 1, got.Resumes)
	assert.Equal(t, []string{"4"}, served.get(), "only the page after the checkpoint is downloaded again")
	assert.Len(t, e.org.QueryJobs(), 1, "the query job is reused")
	assert.EqualValues(t, 6, got.RecordsOK)
	assert.Equal(t, 6, e.count(t, `SELECT count(*) FROM sf_records`))
	wm, err := e.db.Store.Watermark(context.Background(), "Account")
	require.NoError(t, err)
	require.NotNil(t, wm)
	assert.True(t, time.Date(2026, 10, 1, 15, 0, 0, 0, time.UTC).Equal(*wm),
		"the watermark covers the pages loaded before the restart, got %s", wm)
	assert.InDelta(t, 1, testutil.ToFloat64(after.orch.metrics.resumed.WithLabelValues("Account", "export")), 0)
}

func TestExportStartsOverWhenItsJobIsGone(t *testing.T) {
	e := newEnv(t, 1)
	e.org.OnQuery(rowsOf([]string{"001A", "false", "A", "1", "2026-10-01T10:00:00.000Z"}))
	run := e.interruptedRun(t, "Account", store.Export, "750expired")
	_, err := e.db.Pool.Exec(context.Background(), `
		UPDATE sync_runs SET checkpoint_locator = 'stale', checkpoint_records = 7 WHERE id = $1`, run.ID)
	require.NoError(t, err)

	require.NoError(t, e.orch.RecoverInterruptedRuns(context.Background()))
	got := e.waitForStatus(t, run.ID, store.Succeeded, store.Failed, store.Cancelled)

	require.Equal(t, store.Succeeded, got.Status, got.Error)
	jobs := e.org.QueryJobs()
	require.Len(t, jobs, 1)
	assert.Equal(t, jobs[0].ID, got.BulkJobID)
	assert.EqualValues(t, 1, got.RecordsOK, "the stale checkpoint is discarded")
}

func TestImportResumesWithoutSendingAgain(t *testing.T) {
	e := newEnv(t, 1)
	e.org.SetDescribe(contactDescribe)
	e.org.HoldJobs(true)
	id := e.addEntry(t, store.Insert, "", `{"LastName":"Doe"}`)
	run, err := e.orch.StartImport(context.Background(), "Contact", store.Manual)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		jobs := e.org.IngestJobs()
		return len(jobs) == 1 && jobs[0].State == "InProgress"
	}, 5*time.Second, 5*time.Millisecond)
	e.stopService(t)
	// Salesforce finishes the job while the service is down.
	e.org.HoldJobs(false)

	after := e.restart(t)
	require.NoError(t, after.orch.RecoverInterruptedRuns(context.Background()))
	got := after.waitForStatus(t, run.ID, store.Succeeded, store.Failed, store.Cancelled)

	require.Equal(t, store.Succeeded, got.Status, got.Error)
	assert.EqualValues(t, 1, got.RecordsOK)
	assert.Len(t, e.org.IngestJobs(), 1, "the insert is not sent twice")
	state := e.entry(t, id)
	assert.Equal(t, store.Sent, state.status)
	assert.Equal(t, 1, state.attempts)
	assert.NotEmpty(t, state.sfID)
}

func TestImportResendsEntriesOfAnUnclosedJob(t *testing.T) {
	e := newEnv(t, 1)
	e.org.SetDescribe(contactDescribe)
	uploading := make(chan struct{})
	var once sync.Once
	// The service stops while the upload is in flight, before the job is
	// closed, so Salesforce never processes it.
	e.org.Handle("PUT /jobs/ingest/750ingest00001/batches", func(_ http.ResponseWriter, r *http.Request) {
		// The server notices the client going away only once the body is read.
		_, _ = io.Copy(io.Discard, r.Body)
		once.Do(func() { close(uploading) })
		<-r.Context().Done()
	})
	id := e.addEntry(t, store.Insert, "", `{"LastName":"Doe"}`)
	other := e.addEntry(t, store.Update, "", `{"Id":"003A","LastName":"Roe"}`)
	run, err := e.orch.StartImport(context.Background(), "Contact", store.Manual)
	require.NoError(t, err)
	<-uploading
	e.stopService(t)

	after := e.restart(t)
	require.NoError(t, after.orch.RecoverInterruptedRuns(context.Background()))
	got := after.waitForStatus(t, run.ID, store.Succeeded, store.Failed, store.Cancelled)

	require.Equal(t, store.Succeeded, got.Status, got.Error)
	jobs := e.org.IngestJobs()
	require.Len(t, jobs, 3)
	assert.Equal(t, "Aborted", jobs[0].State, "the unclosed job is aborted")
	assert.Equal(t, "insert", jobs[1].Operation)
	assert.Equal(t, [][]string{{"Doe"}}, jobs[1].Rows)
	assert.Equal(t, "update", jobs[2].Operation, "entries never uploaded are sent too")
	assert.Equal(t, store.Sent, e.entry(t, id).status)
	assert.Equal(t, store.Sent, e.entry(t, other).status)
	assert.Equal(t, 1, e.entry(t, id).attempts, "resuming is not another attempt")
}

func TestImportResumeDeadLettersEntriesOfAVanishedJob(t *testing.T) {
	e := newEnv(t, 1)
	e.org.SetDescribe(contactDescribe)
	id := e.addEntry(t, store.Insert, "", `{"LastName":"Doe"}`)
	run := e.interruptedRun(t, "Contact", store.Import, "750vanished")
	_, err := e.db.Pool.Exec(context.Background(), `
		UPDATE sf_outbox SET status = 'processing', run_id = $1, attempts = 1, bulk_job_id = '750vanished'
		WHERE id = $2`, run.ID, id)
	require.NoError(t, err)

	require.NoError(t, e.orch.RecoverInterruptedRuns(context.Background()))
	got := e.waitForStatus(t, run.ID, store.Succeeded, store.Failed, store.Cancelled)

	require.Equal(t, store.Succeeded, got.Status, got.Error)
	state := e.entry(t, id)
	assert.Equal(t, store.Dead, state.status, "the insert may have been applied, so it is not sent again")
	assert.Contains(t, state.lastError, "750vanished is no longer available")
	assert.Empty(t, e.org.IngestJobs())
}

func TestRunsInterruptedTooOftenFail(t *testing.T) {
	e := newEnv(t, 1)
	run := e.interruptedRun(t, "Account", store.Export, "750again")
	_, err := e.db.Pool.Exec(context.Background(), `UPDATE sync_runs SET resumes = $2 WHERE id = $1`,
		run.ID, store.MaxResumes)
	require.NoError(t, err)

	require.NoError(t, e.orch.RecoverInterruptedRuns(context.Background()))

	got, err := e.orch.GetRun(context.Background(), run.ID)
	require.NoError(t, err)
	assert.Equal(t, store.Failed, got.Status)
	assert.Contains(t, got.Error, "too many times")
	assert.Empty(t, e.org.QueryJobs())
}

// interruptedRun records a run left running with Salesforce job jobID, as a
// crashed process leaves it: still leased, but long since renewed.
func (e *env) interruptedRun(t *testing.T, object string, dir store.Direction, jobID string) store.Run {
	t.Helper()
	ctx := context.Background()
	run, err := e.db.Store.CreateRun(ctx, object, dir, store.Manual, false)
	require.NoError(t, err)
	require.NoError(t, e.db.Store.StartRun(ctx, run.ID))
	_, err = e.db.Pool.Exec(ctx, `
		UPDATE sync_runs SET bulk_job_id = $2, owner = 'crashed', heartbeat_at = now() - interval '1 hour'
		WHERE id = $1`, run.ID, jobID)
	require.NoError(t, err)
	return run
}
