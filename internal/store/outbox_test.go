package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AndreyZubov/data-sync-service/internal/store"
	"github.com/AndreyZubov/data-sync-service/internal/store/storetest"
)

// addEntry writes an outbox entry as a producing system would.
func addEntry(t *testing.T, db *storetest.DB, object string, op store.Operation, externalIDField, payload string) int64 {
	t.Helper()
	var ext *string
	if externalIDField != "" {
		ext = &externalIDField
	}
	var id int64
	require.NoError(t, db.Pool.QueryRow(context.Background(), `
		INSERT INTO sf_outbox (object_name, operation, external_id_field, payload)
		VALUES ($1, $2, $3, $4) RETURNING id`, object, op, ext, payload).Scan(&id))
	return id
}

func entryIDs(entries []store.OutboxEntry) []int64 {
	ids := make([]int64, len(entries))
	for i, e := range entries {
		ids[i] = e.ID
	}
	return ids
}

type entryState struct {
	status      store.OutboxStatus
	attempts    int
	lastError   *string
	sfID        *string
	bulkJobID   *string
	nextAttempt time.Time
}

func readEntry(t *testing.T, db *storetest.DB, id int64) entryState {
	t.Helper()
	var e entryState
	require.NoError(t, db.Pool.QueryRow(context.Background(), `
		SELECT status, attempts, last_error, salesforce_id, bulk_job_id, next_attempt_at FROM sf_outbox WHERE id = $1`, id).
		Scan(&e.status, &e.attempts, &e.lastError, &e.sfID, &e.bulkJobID, &e.nextAttempt))
	return e
}

func importRun(t *testing.T, db *storetest.DB, object string) store.Run {
	t.Helper()
	run, err := db.Store.CreateRun(context.Background(), object, store.Import, store.Manual, false)
	require.NoError(t, err)
	require.NoError(t, db.Store.StartRun(context.Background(), run.ID))
	return run
}

func TestOutboxRejectsMalformedEntries(t *testing.T) {
	db := storetest.New(t)
	ctx := context.Background()
	insert := func(op, ext, payload any) error {
		_, err := db.Pool.Exec(ctx, `
			INSERT INTO sf_outbox (object_name, operation, external_id_field, payload) VALUES ('Account', $1, $2, $3)`,
			op, ext, payload)
		return err
	}

	assert.Error(t, insert("merge", nil, `{}`), "unknown operation")
	assert.Error(t, insert("upsert", nil, `{"Name":"Acme"}`), "upsert without external ID field")
	assert.Error(t, insert("update", "Ext__c", `{"Id":"001A"}`), "external ID field on update")
	assert.Error(t, insert("insert", nil, `["Acme"]`), "payload is not an object")
	assert.NoError(t, insert("upsert", "Ext__c", `{"Ext__c":"X1"}`))
}

func TestClaimOutboxTakesTheOldestChangeOfEachRecord(t *testing.T) {
	db := storetest.New(t)
	ctx := context.Background()
	update1 := addEntry(t, db, "Account", store.Update, "", `{"Id":"001A","Name":"One"}`)
	update2 := addEntry(t, db, "Account", store.Update, "", `{"Id":"001A","Name":"Two"}`)
	insert1 := addEntry(t, db, "Account", store.Insert, "", `{"Name":"New"}`)
	insert2 := addEntry(t, db, "Account", store.Insert, "", `{"Name":"New"}`)
	upsert1 := addEntry(t, db, "Account", store.Upsert, "Ext__c", `{"Ext__c":"X1","Name":"A"}`)
	upsert2 := addEntry(t, db, "Account", store.Upsert, "Ext__c", `{"Ext__c":"X1","Name":"B"}`)
	deleteB := addEntry(t, db, "Account", store.Delete, "", `{"Id":"001B"}`)
	addEntry(t, db, "Contact", store.Update, "", `{"Id":"003A"}`)
	run := importRun(t, db, "Account")

	claimed, err := db.Store.ClaimOutbox(ctx, run.ID, "Account", 100)

	require.NoError(t, err)
	assert.Equal(t, []int64{update1, insert1, insert2, upsert1, deleteB}, entryIDs(claimed),
		"inserts are independent; later changes to a record wait")
	assert.Equal(t, store.Processing, claimed[0].Status)
	assert.Equal(t, 1, claimed[0].Attempts)
	assert.Equal(t, store.Update, claimed[0].Operation)
	assert.JSONEq(t, `{"Id":"001A","Name":"One"}`, string(claimed[0].Payload))
	assert.Equal(t, "Ext__c", claimed[3].ExternalIDField)

	again, err := db.Store.ClaimOutbox(ctx, run.ID, "Account", 100)
	require.NoError(t, err)
	assert.Empty(t, again, "later changes wait while the earlier one is in flight")

	_, err = db.Store.RecordOutcomes(ctx, run.ID, []store.Outcome{
		{EntryID: update1, Kind: store.Delivered, SalesforceID: "001A"},
		{EntryID: upsert1, Kind: store.Retry, Error: "UNABLE_TO_LOCK_ROW"},
	}, store.RetryPolicy{MaxAttempts: 5, BaseDelay: time.Hour, MaxDelay: time.Hour})
	require.NoError(t, err)

	next, err := db.Store.ClaimOutbox(ctx, run.ID, "Account", 100)
	require.NoError(t, err)
	assert.Equal(t, []int64{update2}, entryIDs(next), "a change waiting for its retry holds back the next one")
	assert.Equal(t, store.Pending, readEntry(t, db, upsert2).status)
}

func TestClaimOutboxHonoursTheLimit(t *testing.T) {
	db := storetest.New(t)
	first := addEntry(t, db, "Account", store.Insert, "", `{"Name":"A"}`)
	second := addEntry(t, db, "Account", store.Insert, "", `{"Name":"B"}`)
	addEntry(t, db, "Account", store.Insert, "", `{"Name":"C"}`)
	run := importRun(t, db, "Account")

	claimed, err := db.Store.ClaimOutbox(context.Background(), run.ID, "Account", 2)

	require.NoError(t, err)
	assert.Equal(t, []int64{first, second}, entryIDs(claimed))
}

func TestHasDueOutbox(t *testing.T) {
	db := storetest.New(t)
	ctx := context.Background()

	due, err := db.Store.HasDueOutbox(ctx, "Account")
	require.NoError(t, err)
	assert.False(t, due)

	id := addEntry(t, db, "Account", store.Insert, "", `{"Name":"A"}`)
	due, err = db.Store.HasDueOutbox(ctx, "Account")
	require.NoError(t, err)
	assert.True(t, due)

	_, err = db.Pool.Exec(ctx, `UPDATE sf_outbox SET next_attempt_at = now() + interval '1 hour' WHERE id = $1`, id)
	require.NoError(t, err)
	due, err = db.Store.HasDueOutbox(ctx, "Account")
	require.NoError(t, err)
	assert.False(t, due, "an entry waiting for its retry is not due")
}

func TestRecordOutcomes(t *testing.T) {
	db := storetest.New(t)
	ctx := context.Background()
	delivered := addEntry(t, db, "Account", store.Insert, "", `{"Name":"A"}`)
	retried := addEntry(t, db, "Account", store.Insert, "", `{"Name":"B"}`)
	exhausted := addEntry(t, db, "Account", store.Insert, "", `{"Name":"C"}`)
	rejected := addEntry(t, db, "Account", store.Insert, "", `{"Name":"D"}`)
	_, err := db.Pool.Exec(ctx, `UPDATE sf_outbox SET attempts = 2 WHERE id = $1`, exhausted)
	require.NoError(t, err)
	run := importRun(t, db, "Account")
	_, err = db.Store.ClaimOutbox(ctx, run.ID, "Account", 100)
	require.NoError(t, err)
	policy := store.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Minute, MaxDelay: time.Hour}

	before := time.Now()
	dead, err := db.Store.RecordOutcomes(ctx, run.ID, []store.Outcome{
		{EntryID: delivered, Kind: store.Delivered, SalesforceID: "001NEW"},
		{EntryID: retried, Kind: store.Retry, Error: "UNABLE_TO_LOCK_ROW"},
		{EntryID: exhausted, Kind: store.Retry, Error: "UNABLE_TO_LOCK_ROW"},
		{EntryID: rejected, Kind: store.Rejected, Error: "field Foo does not exist"},
	}, policy)

	require.NoError(t, err)
	assert.EqualValues(t, 2, dead)

	d := readEntry(t, db, delivered)
	assert.Equal(t, store.Sent, d.status)
	require.NotNil(t, d.sfID)
	assert.Equal(t, "001NEW", *d.sfID)
	assert.Nil(t, d.lastError)

	r := readEntry(t, db, retried)
	assert.Equal(t, store.Pending, r.status)
	require.NotNil(t, r.lastError)
	assert.Equal(t, "UNABLE_TO_LOCK_ROW", *r.lastError)
	assert.WithinDuration(t, before.Add(time.Minute), r.nextAttempt, 10*time.Second, "first retry after the base delay")

	assert.Equal(t, store.Dead, readEntry(t, db, exhausted).status, "attempts used up")
	assert.Equal(t, store.Dead, readEntry(t, db, rejected).status)

	got, err := db.Store.GetRun(ctx, run.ID)
	require.NoError(t, err)
	assert.EqualValues(t, 1, got.RecordsOK)
	assert.EqualValues(t, 3, got.RecordsFailed)

	_, err = db.Store.RecordOutcomes(ctx, run.ID, []store.Outcome{{EntryID: delivered, Kind: store.Delivered}}, policy)
	assert.ErrorContains(t, err, "not processing", "an outcome is recorded once")
}

func TestRecordOutcomesBacksOffExponentially(t *testing.T) {
	db := storetest.New(t)
	ctx := context.Background()
	id := addEntry(t, db, "Account", store.Insert, "", `{"Name":"A"}`)
	_, err := db.Pool.Exec(ctx, `UPDATE sf_outbox SET attempts = 3 WHERE id = $1`, id)
	require.NoError(t, err)
	run := importRun(t, db, "Account")
	_, err = db.Store.ClaimOutbox(ctx, run.ID, "Account", 1)
	require.NoError(t, err)

	before := time.Now()
	_, err = db.Store.RecordOutcomes(ctx, run.ID, []store.Outcome{{EntryID: id, Kind: store.Retry, Error: "x"}},
		store.RetryPolicy{MaxAttempts: 10, BaseDelay: time.Minute, MaxDelay: 5 * time.Minute})

	require.NoError(t, err)
	assert.WithinDuration(t, before.Add(5*time.Minute), readEntry(t, db, id).nextAttempt, 10*time.Second,
		"the fourth attempt would wait 8 minutes, capped at 5")
}

func TestEndingARunReleasesItsUndecidedEntries(t *testing.T) {
	for name, end := range map[string]func(*store.Store, int64) error{
		"finished": func(s *store.Store, id int64) error {
			return s.FinishRun(context.Background(), id, store.Cancelled, nil)
		},
		"completed": func(s *store.Store, id int64) error { return s.CompleteRun(context.Background(), id) },
	} {
		t.Run(name, func(t *testing.T) {
			db := storetest.New(t)
			ctx := context.Background()
			decided := addEntry(t, db, "Account", store.Insert, "", `{"Name":"A"}`)
			undecided := addEntry(t, db, "Account", store.Insert, "", `{"Name":"B"}`)
			run := importRun(t, db, "Account")
			_, err := db.Store.ClaimOutbox(ctx, run.ID, "Account", 10)
			require.NoError(t, err)
			require.NoError(t, db.Store.AssignIngestJob(ctx, run.ID, "750a", []int64{decided, undecided}))
			_, err = db.Store.RecordOutcomes(ctx, run.ID, []store.Outcome{{EntryID: decided, Kind: store.Delivered}},
				store.RetryPolicy{MaxAttempts: 5})
			require.NoError(t, err)

			require.NoError(t, end(db.Store, run.ID))

			assert.Equal(t, store.Sent, readEntry(t, db, decided).status)
			e := readEntry(t, db, undecided)
			assert.Equal(t, store.Pending, e.status)
			assert.Equal(t, 0, e.attempts, "an attempt without an outcome does not count")
			require.NotNil(t, e.bulkJobID)
			assert.Equal(t, "750a", *e.bulkJobID, "the next run collects the outcome from the job")
		})
	}
}

func TestAssignIngestJobAndRunOutbox(t *testing.T) {
	db := storetest.New(t)
	ctx := context.Background()
	a := addEntry(t, db, "Account", store.Insert, "", `{"Name":"A"}`)
	b := addEntry(t, db, "Account", store.Insert, "", `{"Name":"B"}`)
	c := addEntry(t, db, "Account", store.Insert, "", `{"Name":"C"}`)
	run := importRun(t, db, "Account")
	_, err := db.Store.ClaimOutbox(ctx, run.ID, "Account", 10)
	require.NoError(t, err)

	require.NoError(t, db.Store.AssignIngestJob(ctx, run.ID, "750a", []int64{a, c}))
	_, err = db.Store.RecordOutcomes(ctx, run.ID, []store.Outcome{{EntryID: c, Kind: store.Delivered}},
		store.RetryPolicy{MaxAttempts: 5})
	require.NoError(t, err)

	entries, err := db.Store.RunOutbox(ctx, run.ID)
	require.NoError(t, err)
	assert.Equal(t, []int64{a, b}, entryIDs(entries), "only undecided entries")
	assert.Equal(t, "750a", entries[0].BulkJobID)
	assert.Empty(t, entries[1].BulkJobID)
	got, err := db.Store.GetRun(ctx, run.ID)
	require.NoError(t, err)
	assert.Equal(t, "750a", got.BulkJobID)

	err = db.Store.AssignIngestJob(ctx, run.ID, "750b", []int64{b, c})
	assert.ErrorContains(t, err, "1 of 2 entries are not processing")
	entries, err = db.Store.RunOutbox(ctx, run.ID)
	require.NoError(t, err)
	assert.Empty(t, entries[1].BulkJobID, "a failed assignment changes nothing")
}

func TestClaimOutboxReturnsTheJobOfAnUndecidedEntry(t *testing.T) {
	db := storetest.New(t)
	ctx := context.Background()
	id := addEntry(t, db, "Account", store.Insert, "", `{"Name":"A"}`)
	first := importRun(t, db, "Account")
	_, err := db.Store.ClaimOutbox(ctx, first.ID, "Account", 10)
	require.NoError(t, err)
	require.NoError(t, db.Store.AssignIngestJob(ctx, first.ID, "750a", []int64{id}))
	require.NoError(t, db.Store.FinishRun(ctx, first.ID, store.Failed, nil))

	second := importRun(t, db, "Account")
	entries, err := db.Store.ClaimOutbox(ctx, second.ID, "Account", 10)

	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "750a", entries[0].BulkJobID)
	assert.Equal(t, 1, entries[0].Attempts)
}

func TestRetriedDeadLettersAreUploadedAgain(t *testing.T) {
	db := storetest.New(t)
	ctx := context.Background()
	id := addEntry(t, db, "Account", store.Insert, "", `{"Name":"A"}`)
	_, err := db.Pool.Exec(ctx, `UPDATE sf_outbox SET status = 'dead', bulk_job_id = '750a' WHERE id = $1`, id)
	require.NoError(t, err)

	_, err = db.Store.RetryDeadLetters(ctx, "Account")

	require.NoError(t, err)
	assert.Nil(t, readEntry(t, db, id).bulkJobID)
}

func TestRetriedEntriesAreUploadedAgain(t *testing.T) {
	db := storetest.New(t)
	ctx := context.Background()
	id := addEntry(t, db, "Account", store.Insert, "", `{"Name":"A"}`)
	first := importRun(t, db, "Account")
	_, err := db.Store.ClaimOutbox(ctx, first.ID, "Account", 10)
	require.NoError(t, err)
	require.NoError(t, db.Store.AssignIngestJob(ctx, first.ID, "750a", []int64{id}))
	_, err = db.Store.RecordOutcomes(ctx, first.ID, []store.Outcome{{EntryID: id, Kind: store.Retry, Error: "x"}},
		store.RetryPolicy{MaxAttempts: 5})
	require.NoError(t, err)
	require.NoError(t, db.Store.CompleteRun(ctx, first.ID))
	_, err = db.Pool.Exec(ctx, `UPDATE sf_outbox SET next_attempt_at = now() WHERE id = $1`, id)
	require.NoError(t, err)

	second := importRun(t, db, "Account")
	entries, err := db.Store.ClaimOutbox(ctx, second.ID, "Account", 10)

	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Empty(t, entries[0].BulkJobID)
	assert.Nil(t, readEntry(t, db, id).bulkJobID)
}

func TestRecoverRunsReleasesEntriesOfFailedRunsOnly(t *testing.T) {
	db := storetest.New(t)
	ctx := context.Background()
	sent := addEntry(t, db, "Account", store.Insert, "", `{"Name":"A"}`)
	unsent := addEntry(t, db, "Contact", store.Insert, "", `{"LastName":"B"}`)
	resumable := importRun(t, db, "Account")
	_, err := db.Store.ClaimOutbox(ctx, resumable.ID, "Account", 10)
	require.NoError(t, err)
	require.NoError(t, db.Store.AssignIngestJob(ctx, resumable.ID, "750a", []int64{sent}))
	failing := importRun(t, db, "Contact")
	_, err = db.Store.ClaimOutbox(ctx, failing.ID, "Contact", 10)
	require.NoError(t, err)

	abandon(t, db, resumable.ID, failing.ID)
	_, resumed, err := db.Store.RecoverRuns(ctx, store.MaxResumes, time.Minute)

	require.NoError(t, err)
	require.Len(t, resumed, 1)
	assert.Equal(t, resumable.ID, resumed[0].ID)
	assert.Equal(t, store.Processing, readEntry(t, db, sent).status, "the resumed run collects its result")
	assert.Equal(t, store.Pending, readEntry(t, db, unsent).status)
}

func TestDeadLettersAndRetry(t *testing.T) {
	db := storetest.New(t)
	ctx := context.Background()
	first := addEntry(t, db, "Account", store.Insert, "", `{"Name":"A"}`)
	second := addEntry(t, db, "Account", store.Upsert, "Ext__c", `{"Ext__c":"X1"}`)
	addEntry(t, db, "Account", store.Insert, "", `{"Name":"pending"}`)
	other := addEntry(t, db, "Contact", store.Insert, "", `{"LastName":"B"}`)
	_, err := db.Pool.Exec(ctx, `
		UPDATE sf_outbox SET status = 'dead', attempts = 5, last_error = 'boom' WHERE id = ANY($1)`,
		[]int64{first, second, other})
	require.NoError(t, err)

	dead, err := db.Store.DeadLetters(ctx, "Account", 10)
	require.NoError(t, err)
	assert.Equal(t, []int64{first, second}, entryIDs(dead))
	assert.Equal(t, "Ext__c", dead[1].ExternalIDField)
	assert.Equal(t, "boom", dead[0].LastError)
	assert.Equal(t, 5, dead[0].Attempts)

	limited, err := db.Store.DeadLetters(ctx, "Account", 1)
	require.NoError(t, err)
	assert.Equal(t, []int64{first}, entryIDs(limited))

	n, err := db.Store.RetryDeadLetters(ctx, "Account")
	require.NoError(t, err)
	assert.EqualValues(t, 2, n)
	e := readEntry(t, db, first)
	assert.Equal(t, store.Pending, e.status)
	assert.Equal(t, 0, e.attempts)
	assert.Equal(t, store.Dead, readEntry(t, db, other).status, "other objects are untouched")
}
