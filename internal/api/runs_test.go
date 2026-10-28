package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AndreyZubov/data-sync-service/internal/salesforce"
	"github.com/AndreyZubov/data-sync-service/internal/store"
	"github.com/AndreyZubov/data-sync-service/internal/syncer"
)

// fakeRuns returns run for every call, or err when set, and records the
// arguments of the last run started.
type fakeRuns struct {
	run store.Run
	err error

	object    string
	direction store.Direction
	trigger   store.Trigger
	full      bool
	id        int64
}

func (f *fakeRuns) StartExport(_ context.Context, object string, trigger store.Trigger, full bool) (store.Run, error) {
	f.object, f.direction, f.trigger, f.full = object, store.Export, trigger, full
	if f.err != nil {
		return store.Run{}, f.err
	}
	return f.run, nil
}

func (f *fakeRuns) StartImport(_ context.Context, object string, trigger store.Trigger) (store.Run, error) {
	f.object, f.direction, f.trigger = object, store.Import, trigger
	if f.err != nil {
		return store.Run{}, f.err
	}
	return f.run, nil
}

func (f *fakeRuns) GetRun(_ context.Context, id int64) (store.Run, error) {
	f.id = id
	if f.err != nil {
		return store.Run{}, f.err
	}
	return f.run, nil
}

var sampleRun = store.Run{
	ID:        7,
	Object:    "Account",
	Direction: store.Export,
	Trigger:   store.Manual,
	Status:    store.Queued,
	CreatedAt: time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC),
}

func TestExportStartsARun(t *testing.T) {
	tests := []struct {
		name     string
		target   string
		wantFull bool
	}{
		{name: "incremental", target: "/sync/account/export", wantFull: false},
		{name: "full", target: "/sync/Account/export?full=true", wantFull: true},
		{name: "explicitly incremental", target: "/sync/Account/export?full=false", wantFull: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runs := &fakeRuns{run: sampleRun}

			rec := do(t, newServerWithRuns(t, runs), http.MethodPost, tt.target)

			require.Equal(t, http.StatusAccepted, rec.Code)
			assert.Equal(t, "/runs/7", rec.Header().Get("Location"))
			assert.JSONEq(t, `{
				"id": 7, "object": "Account", "direction": "export", "trigger": "manual",
				"status": "queued", "full_sync": false, "records_ok": 0, "records_failed": 0,
				"created_at": "2026-10-21T09:00:00Z"
			}`, rec.Body.String())
			assert.Equal(t, "Account", runs.object, "the configured spelling is used")
			assert.Equal(t, store.Manual, runs.trigger)
			assert.Equal(t, tt.wantFull, runs.full)
		})
	}
}

func TestExportErrors(t *testing.T) {
	tests := []struct {
		name       string
		target     string
		err        error
		wantStatus int
		wantBody   string
	}{
		{
			name:       "invalid full flag",
			target:     "/sync/Account/export?full=maybe",
			wantStatus: http.StatusBadRequest,
			wantBody:   `{"error":"full must be true or false"}`,
		},
		{
			name:       "already running",
			target:     "/sync/Account/export",
			err:        &store.RunInProgressError{RunID: 3},
			wantStatus: http.StatusConflict,
			wantBody:   `{"error":"an export of this object is already in progress","run_id":3}`,
		},
		{
			name:       "shutting down",
			target:     "/sync/Account/export",
			err:        syncer.ErrShuttingDown,
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   `{"error":"service is shutting down"}`,
		},
		{
			name:   "org API usage above the threshold",
			target: "/sync/Account/export",
			err: &syncer.APIUsageError{
				Usage: salesforce.APIUsage{Used: 13500, Max: 15000}, Threshold: 0.8, RetryAfter: 5 * time.Minute,
			},
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   `{"error":"the org has used 13500 of 15000 daily API requests (90%), above the 80% threshold"}`,
		},
		{
			name:       "unexpected failure is not exposed",
			target:     "/sync/Account/export",
			err:        errors.New("dial tcp 10.0.0.5:5432: connection refused"),
			wantStatus: http.StatusInternalServerError,
			wantBody:   `{"error":"could not start the export"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(t, newServerWithRuns(t, &fakeRuns{err: tt.err}), http.MethodPost, tt.target)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.JSONEq(t, tt.wantBody, rec.Body.String())
			var usage *syncer.APIUsageError
			if errors.As(tt.err, &usage) {
				assert.Equal(t, "300", rec.Header().Get("Retry-After"))
			}
		})
	}
}

func TestGetRun(t *testing.T) {
	started := time.Date(2026, 10, 21, 9, 0, 1, 0, time.UTC)
	finished := started.Add(time.Minute)
	run := sampleRun
	run.Status = store.Failed
	run.FullSync = true
	run.BulkJobID = "750xx"
	run.RecordsOK = 1500
	run.Error = "bulk job 750xx ended in state Failed"
	run.Resumes = 1
	run.StartedAt = &started
	run.FinishedAt = &finished
	runs := &fakeRuns{run: run}

	rec := do(t, newServerWithRuns(t, runs), http.MethodGet, "/runs/7")

	require.Equal(t, http.StatusOK, rec.Code)
	assert.EqualValues(t, 7, runs.id)
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "failed", body["status"])
	assert.Equal(t, true, body["full_sync"])
	assert.Equal(t, "750xx", body["bulk_job_id"])
	assert.InDelta(t, 1500, body["records_ok"], 0)
	assert.Equal(t, run.Error, body["error"])
	assert.InDelta(t, 1, body["resumes"], 0)
	assert.Equal(t, "2026-10-21T09:00:01Z", body["started_at"])
	assert.Equal(t, "2026-10-21T09:01:01Z", body["finished_at"])
}

func TestGetRunErrors(t *testing.T) {
	tests := []struct {
		name       string
		target     string
		err        error
		wantStatus int
	}{
		{name: "not a number", target: "/runs/abc", wantStatus: http.StatusBadRequest},
		{name: "zero", target: "/runs/0", wantStatus: http.StatusBadRequest},
		{name: "overflow", target: "/runs/99999999999999999999", wantStatus: http.StatusBadRequest},
		{name: "unknown", target: "/runs/9", err: store.ErrNotFound, wantStatus: http.StatusNotFound},
		{name: "store failure", target: "/runs/9", err: errors.New("boom"), wantStatus: http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(t, newServerWithRuns(t, &fakeRuns{err: tt.err}), http.MethodGet, tt.target)

			assert.Equal(t, tt.wantStatus, rec.Code)
			var body map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.NotEmpty(t, body["error"])
			assert.NotContains(t, body["error"], "boom")
		})
	}
}

func TestImportStartsARun(t *testing.T) {
	run := sampleRun
	run.Direction = store.Import
	runs := &fakeRuns{run: run}

	rec := do(t, newServerWithRuns(t, runs), http.MethodPost, "/sync/contact/import")

	require.Equal(t, http.StatusAccepted, rec.Code)
	assert.Equal(t, "/runs/7", rec.Header().Get("Location"))
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "import", body["direction"])
	assert.Equal(t, "Contact", runs.object, "the configured spelling is used")
	assert.Equal(t, store.Import, runs.direction)
	assert.Equal(t, store.Manual, runs.trigger)
}

func TestImportErrors(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantBody   string
	}{
		{
			name:       "already running",
			err:        &store.RunInProgressError{RunID: 4},
			wantStatus: http.StatusConflict,
			wantBody:   `{"error":"an import of this object is already in progress","run_id":4}`,
		},
		{
			name:       "shutting down",
			err:        syncer.ErrShuttingDown,
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   `{"error":"service is shutting down"}`,
		},
		{
			name:       "unexpected failure is not exposed",
			err:        errors.New("connection refused"),
			wantStatus: http.StatusInternalServerError,
			wantBody:   `{"error":"could not start the import"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(t, newServerWithRuns(t, &fakeRuns{err: tt.err}), http.MethodPost, "/sync/Account/import")

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.JSONEq(t, tt.wantBody, rec.Body.String())
		})
	}
}

// fakeOutbox returns entries and requeued, or err when set, and records the
// arguments of the last call.
type fakeOutbox struct {
	entries  []store.OutboxEntry
	requeued int64
	err      error

	object string
	limit  int
}

func (f *fakeOutbox) DeadLetters(_ context.Context, object string, limit int) ([]store.OutboxEntry, error) {
	f.object, f.limit = object, limit
	return f.entries, f.err
}

func (f *fakeOutbox) RetryDeadLetters(_ context.Context, object string) (int64, error) {
	f.object = object
	return f.requeued, f.err
}

func TestDeadLetters(t *testing.T) {
	created := time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)
	outbox := &fakeOutbox{entries: []store.OutboxEntry{{
		ID:              12,
		Object:          "Account",
		Operation:       store.Upsert,
		ExternalIDField: "Ext_Id__c",
		Payload:         json.RawMessage(`{"Ext_Id__c":"A-1","Name":"Acme"}`),
		Status:          store.Dead,
		Attempts:        5,
		LastError:       "REQUIRED_FIELD_MISSING:Required fields are missing: [Industry]:Industry --",
		CreatedAt:       created,
		UpdatedAt:       created.Add(time.Hour),
	}}}
	h := newServerWith(t, &fakeRuns{}, outbox)

	rec := do(t, h, http.MethodGet, "/sync/account/dead-letters")

	require.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"object": "Account", "entries": [{
		"id": 12, "operation": "upsert", "external_id_field": "Ext_Id__c",
		"payload": {"Ext_Id__c": "A-1", "Name": "Acme"}, "status": "dead", "attempts": 5,
		"last_error": "REQUIRED_FIELD_MISSING:Required fields are missing: [Industry]:Industry --",
		"created_at": "2026-10-21T09:00:00Z", "updated_at": "2026-10-21T10:00:00Z"
	}]}`, rec.Body.String())
	assert.Equal(t, "Account", outbox.object)
	assert.Equal(t, 100, outbox.limit, "default limit")

	rec = do(t, h, http.MethodGet, "/sync/Account/dead-letters?limit=5")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 5, outbox.limit)

	outbox.entries = nil
	rec = do(t, h, http.MethodGet, "/sync/Account/dead-letters")
	assert.JSONEq(t, `{"object": "Account", "entries": []}`, rec.Body.String())
}

func TestDeadLettersErrors(t *testing.T) {
	tests := []struct {
		name       string
		target     string
		wantStatus int
	}{
		{name: "limit not a number", target: "/sync/Account/dead-letters?limit=all", wantStatus: http.StatusBadRequest},
		{name: "limit zero", target: "/sync/Account/dead-letters?limit=0", wantStatus: http.StatusBadRequest},
		{name: "limit too large", target: "/sync/Account/dead-letters?limit=1001", wantStatus: http.StatusBadRequest},
		{name: "store failure", target: "/sync/Account/dead-letters", wantStatus: http.StatusInternalServerError},
		{name: "unknown object", target: "/sync/Lead/dead-letters", wantStatus: http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(t, newServerWith(t, &fakeRuns{}, &fakeOutbox{err: errors.New("boom")}), http.MethodGet, tt.target)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.NotContains(t, rec.Body.String(), "boom")
		})
	}
}

func TestRetryDeadLetters(t *testing.T) {
	outbox := &fakeOutbox{requeued: 3}

	rec := do(t, newServerWith(t, &fakeRuns{}, outbox), http.MethodPost, "/sync/invoice__c/dead-letters/retry")

	require.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"object": "Invoice__c", "requeued": 3}`, rec.Body.String())
	assert.Equal(t, "Invoice__c", outbox.object)

	rec = do(t, newServerWith(t, &fakeRuns{}, &fakeOutbox{err: errors.New("boom")}),
		http.MethodPost, "/sync/Account/dead-letters/retry")
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.JSONEq(t, `{"error": "could not requeue the dead letters"}`, rec.Body.String())
}
