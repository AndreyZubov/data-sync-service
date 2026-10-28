package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AndreyZubov/data-sync-service/internal/store"
	"github.com/AndreyZubov/data-sync-service/internal/store/storetest"
)

func ptr[T any](v T) *T { return &v }

func TestMigrateIsIdempotent(t *testing.T) {
	db := storetest.New(t)
	require.NoError(t, store.Migrate(db.DSN))
}

func TestOpenRejectsUnreachableDatabase(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := store.Open(ctx, "postgres://nobody@127.0.0.1:1/none?sslmode=disable", 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connect to database")
}

func TestRunLifecycle(t *testing.T) {
	db := storetest.New(t)
	ctx := context.Background()

	run, err := db.Store.CreateRun(ctx, "Account", store.Export, store.Manual, true)
	require.NoError(t, err)
	assert.Equal(t, store.Queued, run.Status)
	assert.True(t, run.FullSync)
	assert.Nil(t, run.StartedAt)

	require.NoError(t, db.Store.StartRun(ctx, run.ID))
	require.NoError(t, db.Store.StartExportJob(ctx, run.ID, "750xx0000000001"))
	require.NoError(t, db.Store.FinishRun(ctx, run.ID, store.Failed, errors.New("job failed")))

	got, err := db.Store.GetRun(ctx, run.ID)
	require.NoError(t, err)
	assert.Equal(t, store.Failed, got.Status)
	assert.Equal(t, "750xx0000000001", got.BulkJobID)
	assert.Equal(t, "job failed", got.Error)
	assert.NotNil(t, got.StartedAt)
	assert.NotNil(t, got.FinishedAt)

	assert.Error(t, db.Store.FinishRun(ctx, run.ID, store.Cancelled, nil), "a finished run stays finished")
	assert.Error(t, db.Store.StartRun(ctx, run.ID), "only queued runs start")
}

func TestGetRunNotFound(t *testing.T) {
	db := storetest.New(t)
	_, err := db.Store.GetRun(context.Background(), 42)
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestOnlyOneActiveRunPerObjectAndDirection(t *testing.T) {
	db := storetest.New(t)
	ctx := context.Background()

	first, err := db.Store.CreateRun(ctx, "Account", store.Export, store.Manual, false)
	require.NoError(t, err)

	_, err = db.Store.CreateRun(ctx, "Account", store.Export, store.Schedule, false)
	var inProgress *store.RunInProgressError
	require.ErrorAs(t, err, &inProgress)
	assert.Equal(t, first.ID, inProgress.RunID)

	_, err = db.Store.CreateRun(ctx, "Contact", store.Export, store.Manual, false)
	require.NoError(t, err, "other objects are independent")
	_, err = db.Store.CreateRun(ctx, "Account", store.Import, store.Manual, false)
	require.NoError(t, err, "other directions are independent")

	require.NoError(t, db.Store.FinishRun(ctx, first.ID, store.Cancelled, nil))
	_, err = db.Store.CreateRun(ctx, "Account", store.Export, store.Manual, false)
	require.NoError(t, err, "a finished run no longer blocks")
}

func TestFinishRunTruncatesLongErrors(t *testing.T) {
	db := storetest.New(t)
	ctx := context.Background()
	run, err := db.Store.CreateRun(ctx, "Account", store.Export, store.Manual, false)
	require.NoError(t, err)

	long := make([]byte, 5000)
	for i := range long {
		long[i] = 'x'
	}
	require.NoError(t, db.Store.FinishRun(ctx, run.ID, store.Failed, errors.New(string(long))))

	got, err := db.Store.GetRun(ctx, run.ID)
	require.NoError(t, err)
	assert.Len(t, got.Error, 2000)
}

func TestCompleteExportAdvancesWatermarkOnlyForward(t *testing.T) {
	db := storetest.New(t)
	ctx := context.Background()
	t1 := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)

	wm, err := db.Store.Watermark(ctx, "Account")
	require.NoError(t, err)
	assert.Nil(t, wm, "no watermark before the first export")

	complete := func(watermark *time.Time) {
		t.Helper()
		run, err := db.Store.CreateRun(ctx, "Account", store.Export, store.Manual, false)
		require.NoError(t, err)
		require.NoError(t, db.Store.StartRun(ctx, run.ID))
		require.NoError(t, db.Store.CompleteExport(ctx, run, watermark))
		got, err := db.Store.GetRun(ctx, run.ID)
		require.NoError(t, err)
		assert.Equal(t, store.Succeeded, got.Status)
	}

	complete(&t2)
	complete(&t1)
	complete(nil)

	wm, err = db.Store.Watermark(ctx, "Account")
	require.NoError(t, err)
	require.NotNil(t, wm)
	assert.True(t, t2.Equal(*wm), "got %s", wm)
}

func TestCompleteExportRequiresARunningRun(t *testing.T) {
	db := storetest.New(t)
	ctx := context.Background()
	run, err := db.Store.CreateRun(ctx, "Account", store.Export, store.Manual, false)
	require.NoError(t, err)

	err = db.Store.CompleteExport(ctx, run, ptr(time.Now()))

	require.Error(t, err)
	wm, err := db.Store.Watermark(ctx, "Account")
	require.NoError(t, err)
	assert.Nil(t, wm, "the watermark is not advanced without a successful run")
}

func TestExportCheckpoints(t *testing.T) {
	db := storetest.New(t)
	ctx := context.Background()
	run, err := db.Store.CreateRun(ctx, "Account", store.Export, store.Manual, false)
	require.NoError(t, err)
	require.NoError(t, db.Store.StartRun(ctx, run.ID))
	require.NoError(t, db.Store.StartExportJob(ctx, run.ID, "750a"))
	require.NoError(t, db.Store.UpsertRecords(ctx, run.ID, "Account", []store.Record{{ID: "001A", Data: json.RawMessage(`{}`)}}))

	wm := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	cp := store.ExportCheckpoint{Locator: "page-2", Watermark: &wm, Records: 1}
	require.NoError(t, db.Store.SaveCheckpoint(ctx, run.ID, cp))
	got, err := db.Store.GetRun(ctx, run.ID)
	require.NoError(t, err)
	assert.Equal(t, cp.Locator, got.Checkpoint.Locator)
	assert.True(t, wm.Equal(*got.Checkpoint.Watermark))
	assert.EqualValues(t, 1, got.Checkpoint.Records)

	require.NoError(t, db.Store.StartExportJob(ctx, run.ID, "750b"))
	got, err = db.Store.GetRun(ctx, run.ID)
	require.NoError(t, err)
	assert.Equal(t, "750b", got.BulkJobID)
	assert.Equal(t, store.ExportCheckpoint{}, got.Checkpoint, "a new job starts from its first page")
	assert.Zero(t, got.RecordsOK, "records of the old job are loaded again")

	require.NoError(t, db.Store.FinishRun(ctx, run.ID, store.Failed, nil))
	assert.Error(t, db.Store.SaveCheckpoint(ctx, run.ID, cp), "only running exports have checkpoints")
	assert.Error(t, db.Store.StartExportJob(ctx, run.ID, "750c"))
}

func TestRecoverRuns(t *testing.T) {
	db := storetest.New(t)
	ctx := context.Background()
	start := func(object string, dir store.Direction, jobID string) store.Run {
		t.Helper()
		run, err := db.Store.CreateRun(ctx, object, dir, store.Manual, false)
		require.NoError(t, err)
		require.NoError(t, db.Store.StartRun(ctx, run.ID))
		if jobID != "" {
			_, err := db.Pool.Exec(ctx, `UPDATE sync_runs SET bulk_job_id = $2 WHERE id = $1`, run.ID, jobID)
			require.NoError(t, err)
		}
		return run
	}
	queued, err := db.Store.CreateRun(ctx, "Account", store.Export, store.Manual, false)
	require.NoError(t, err)
	noJob := start("Contact", store.Export, "")
	export := start("Lead", store.Export, "750export")
	require.NoError(t, db.Store.UpsertRecords(ctx, export.ID, "Lead", []store.Record{
		{ID: "00QA", Data: json.RawMessage(`{}`)}, {ID: "00QB", Data: json.RawMessage(`{}`)},
	}))
	require.NoError(t, db.Store.SaveCheckpoint(ctx, export.ID, store.ExportCheckpoint{Locator: "next", Records: 1}))
	imp := start("Lead", store.Import, "750ingest")
	worn := start("Case", store.Export, "750worn")
	_, err = db.Pool.Exec(ctx, `UPDATE sync_runs SET resumes = $2 WHERE id = $1`, worn.ID, store.MaxResumes)
	require.NoError(t, err)
	abandon(t, db, queued.ID, noJob.ID, export.ID, imp.ID, worn.ID)
	live := start("Task", store.Export, "750live")

	failed, resumed, err := db.Store.RecoverRuns(ctx, store.MaxResumes, time.Minute)

	require.NoError(t, err)
	assert.EqualValues(t, 3, failed)
	require.Len(t, resumed, 2)
	assert.Equal(t, export.ID, resumed[0].ID)
	assert.Equal(t, store.Running, resumed[0].Status)
	assert.Equal(t, 1, resumed[0].Resumes)
	assert.Equal(t, "next", resumed[0].Checkpoint.Locator)
	assert.EqualValues(t, 1, resumed[0].RecordsOK, "pages after the checkpoint are loaded again")
	assert.Equal(t, db.Store.Instance(), resumed[0].Owner, "the resumed run is leased anew")
	assert.Equal(t, imp.ID, resumed[1].ID)
	got, err := db.Store.GetRun(ctx, live.ID)
	require.NoError(t, err)
	assert.Equal(t, 0, got.Resumes, "a run with a live lease is left alone")

	for id, msg := range map[int64]string{
		queued.ID: "interrupted by a service restart",
		noJob.ID:  "interrupted by a service restart",
		worn.ID:   "interrupted by a service restart too many times",
	} {
		got, err := db.Store.GetRun(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, store.Failed, got.Status)
		assert.Equal(t, msg, got.Error)
	}
}

// abandon makes runs look like a crashed instance left them: leased, with a
// lease that lapsed long ago.
func abandon(t *testing.T, db *storetest.DB, ids ...int64) {
	t.Helper()
	_, err := db.Pool.Exec(context.Background(), `
		UPDATE sync_runs SET owner = 'crashed', heartbeat_at = now() - interval '1 hour' WHERE id = ANY($1)`, ids)
	require.NoError(t, err)
}

func TestRunLeases(t *testing.T) {
	db := storetest.New(t)
	ctx := context.Background()
	peer, err := store.Open(ctx, db.DSN, 0)
	require.NoError(t, err)
	t.Cleanup(peer.Close)
	require.NotEqual(t, db.Store.Instance(), peer.Instance())

	run, err := db.Store.CreateRun(ctx, "Account", store.Export, store.Manual, false)
	require.NoError(t, err)
	assert.Equal(t, db.Store.Instance(), run.Owner)
	require.NoError(t, db.Store.StartRun(ctx, run.ID))
	require.NoError(t, db.Store.StartExportJob(ctx, run.ID, "750a"))

	owned, err := db.Store.Heartbeat(ctx)
	require.NoError(t, err)
	assert.Equal(t, []int64{run.ID}, owned)
	owned, err = peer.Heartbeat(ctx)
	require.NoError(t, err)
	assert.Empty(t, owned)

	_, resumed, err := peer.RecoverRuns(ctx, store.MaxResumes, time.Minute)
	require.NoError(t, err)
	assert.Empty(t, resumed, "a renewed lease keeps peers off the run")
	cp := store.ExportCheckpoint{Locator: "p2"}
	assert.ErrorContains(t, peer.SaveCheckpoint(ctx, run.ID, cp), "leased by another instance")
	assert.Error(t, peer.FinishRun(ctx, run.ID, store.Failed, nil))

	// The owner stalls longer than the lease lasts; a peer takes over.
	_, err = db.Pool.Exec(ctx, `UPDATE sync_runs SET heartbeat_at = now() - interval '2 minutes' WHERE id = $1`, run.ID)
	require.NoError(t, err)
	_, resumed, err = peer.RecoverRuns(ctx, store.MaxResumes, time.Minute)
	require.NoError(t, err)
	require.Len(t, resumed, 1)
	assert.Equal(t, peer.Instance(), resumed[0].Owner)

	assert.Error(t, db.Store.SaveCheckpoint(ctx, run.ID, cp), "the former owner can no longer write")
	owned, err = db.Store.Heartbeat(ctx)
	require.NoError(t, err)
	assert.Empty(t, owned, "the former owner learns it lost the lease")
	require.NoError(t, peer.SaveCheckpoint(ctx, run.ID, cp))

	require.NoError(t, peer.ReleaseRun(ctx, run.ID))
	_, resumed, err = db.Store.RecoverRuns(ctx, store.MaxResumes, time.Hour)
	require.NoError(t, err)
	require.Len(t, resumed, 1, "a released run is resumable at once")
	require.NoError(t, db.Store.CompleteRun(ctx, run.ID))
	got, err := db.Store.GetRun(ctx, run.ID)
	require.NoError(t, err)
	assert.Empty(t, got.Owner, "a finished run holds no lease")
}

type storedRecord struct {
	modstamp *time.Time
	deleted  bool
	data     map[string]any
}

func readRecord(t *testing.T, db *storetest.DB, object, id string) storedRecord {
	t.Helper()
	var r storedRecord
	err := db.Pool.QueryRow(context.Background(),
		`SELECT system_modstamp, is_deleted, data FROM sf_records WHERE object_name = $1 AND id = $2`,
		object, id).Scan(&r.modstamp, &r.deleted, &r.data)
	require.NoError(t, err)
	return r
}

func TestUpsertRecords(t *testing.T) {
	db := storetest.New(t)
	ctx := context.Background()
	run, err := db.Store.CreateRun(ctx, "Account", store.Export, store.Manual, false)
	require.NoError(t, err)
	older := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	newer := older.Add(time.Minute)

	require.NoError(t, db.Store.UpsertRecords(ctx, run.ID, "Account", []store.Record{
		{ID: "001A", SystemModstamp: &newer, Data: json.RawMessage(`{"Name":"Acme","Employees":10}`)},
		{ID: "001B", SystemModstamp: &older, Data: json.RawMessage(`{"Name":"Globex"}`)},
	}))
	require.NoError(t, db.Store.UpsertRecords(ctx, run.ID, "Account", []store.Record{
		// Stale: must not overwrite the newer stored version.
		{ID: "001A", SystemModstamp: &older, Data: json.RawMessage(`{"Name":"Old Acme"}`)},
		// Fresh: replaces the stored version.
		{ID: "001B", SystemModstamp: &newer, IsDeleted: true, Data: json.RawMessage(`{"Name":"Globex Corp"}`)},
	}))
	require.NoError(t, db.Store.UpsertRecords(ctx, run.ID, "Account", nil))

	a := readRecord(t, db, "Account", "001A")
	assert.Equal(t, map[string]any{"Name": "Acme", "Employees": float64(10)}, a.data)
	assert.True(t, newer.Equal(*a.modstamp))

	b := readRecord(t, db, "Account", "001B")
	assert.Equal(t, "Globex Corp", b.data["Name"])
	assert.True(t, b.deleted)

	got, err := db.Store.GetRun(ctx, run.ID)
	require.NoError(t, err)
	assert.EqualValues(t, 4, got.RecordsOK, "every received record is counted")
}

func TestUpsertRecordsKeepsObjectsApartAndToleratesDuplicates(t *testing.T) {
	db := storetest.New(t)
	ctx := context.Background()
	run, err := db.Store.CreateRun(ctx, "Account", store.Export, store.Manual, false)
	require.NoError(t, err)
	t1 := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Second)

	require.NoError(t, db.Store.UpsertRecords(ctx, run.ID, "Account", []store.Record{
		{ID: "X1", SystemModstamp: &t1, Data: json.RawMessage(`{"v":1}`)},
		{ID: "X1", SystemModstamp: &t2, Data: json.RawMessage(`{"v":2}`)},
	}))
	require.NoError(t, db.Store.UpsertRecords(ctx, run.ID, "Lead", []store.Record{
		{ID: "X1", Data: json.RawMessage(`{"v":"lead"}`)},
	}))

	assert.InDelta(t, 2, readRecord(t, db, "Account", "X1").data["v"], 0)
	assert.Equal(t, "lead", readRecord(t, db, "Lead", "X1").data["v"])
}
