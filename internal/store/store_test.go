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
	require.NoError(t, db.Store.SetBulkJob(ctx, run.ID, "750xx0000000001"))
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

func TestFailInterruptedRuns(t *testing.T) {
	db := storetest.New(t)
	ctx := context.Background()
	queued, err := db.Store.CreateRun(ctx, "Account", store.Export, store.Manual, false)
	require.NoError(t, err)
	running, err := db.Store.CreateRun(ctx, "Contact", store.Export, store.Manual, false)
	require.NoError(t, err)
	require.NoError(t, db.Store.StartRun(ctx, running.ID))

	n, err := db.Store.FailInterruptedRuns(ctx)

	require.NoError(t, err)
	assert.EqualValues(t, 2, n)
	for _, id := range []int64{queued.ID, running.ID} {
		got, err := db.Store.GetRun(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, store.Failed, got.Status)
		assert.Contains(t, got.Error, "restart")
	}
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
