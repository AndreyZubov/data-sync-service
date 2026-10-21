package syncer

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AndreyZubov/data-sync-service/internal/salesforce/sftest"
	"github.com/AndreyZubov/data-sync-service/internal/store"
)

var contactDescribe = sftest.Describe{Name: "Contact", Queryable: true, Fields: []sftest.DescribeField{
	{Name: "Id", Type: "id"},
	{Name: "LastName", Type: "string", Createable: true, Updateable: true},
	{Name: "Email", Type: "email", Createable: true, Updateable: true},
	{Name: "Ext_Id__c", Type: "string", Createable: true, Updateable: true},
}}

func (e *env) addEntry(t *testing.T, op store.Operation, ext, payload string) int64 {
	t.Helper()
	var extField *string
	if ext != "" {
		extField = &ext
	}
	var id int64
	require.NoError(t, e.db.Pool.QueryRow(context.Background(), `
		INSERT INTO sf_outbox (object_name, operation, external_id_field, payload)
		VALUES ('Contact', $1, $2, $3) RETURNING id`, op, extField, payload).Scan(&id))
	return id
}

type outboxState struct {
	status    store.OutboxStatus
	attempts  int
	sfID      string
	lastError string
}

func (e *env) entry(t *testing.T, id int64) outboxState {
	t.Helper()
	var s outboxState
	require.NoError(t, e.db.Pool.QueryRow(context.Background(), `
		SELECT status, attempts, coalesce(salesforce_id, ''), coalesce(last_error, '') FROM sf_outbox WHERE id = $1`, id).
		Scan(&s.status, &s.attempts, &s.sfID, &s.lastError))
	return s
}

func (e *env) importContacts(t *testing.T) store.Run {
	t.Helper()
	run, err := e.orch.StartImport(context.Background(), "Contact", store.Manual)
	require.NoError(t, err)
	return e.waitForStatus(t, run.ID, store.Succeeded, store.Failed, store.Cancelled)
}

func TestImportSendsOutboxEntries(t *testing.T) {
	e := newEnv(t, 2)
	e.org.SetDescribe(contactDescribe)
	e.org.OnIngest(func(job sftest.IngestJob, record map[string]string) (string, error) {
		switch {
		case record["LastName"] == "Invalid":
			return "", errors.New("FIELD_CUSTOM_VALIDATION_EXCEPTION:Last name is not allowed:LastName --")
		case job.Operation == "insert":
			return "003NEW" + record["LastName"], nil
		case job.Operation == "upsert":
			return "003UPS" + record["Ext_Id__c"], nil
		default:
			return record["Id"], nil
		}
	})
	inserted := e.addEntry(t, store.Insert, "", `{"LastName":"Doe","Email":"doe@example.com"}`)
	updated := e.addEntry(t, store.Update, "", `{"Id":"003A","Email":null}`)
	invalid := e.addEntry(t, store.Insert, "", `{"LastName":"Invalid"}`)
	upserted := e.addEntry(t, store.Upsert, "Ext_Id__c", `{"Ext_Id__c":"E-1","LastName":"Roe"}`)
	unknown := e.addEntry(t, store.Insert, "", `{"LastName":"Poe","Shoe_Size__c":9}`)
	inserted2 := e.addEntry(t, store.Insert, "", `{"LastName":"Moe"}`)

	run := e.importContacts(t)

	require.Equal(t, store.Succeeded, run.Status, run.Error)
	assert.EqualValues(t, 4, run.RecordsOK)
	assert.EqualValues(t, 2, run.RecordsFailed)

	assert.Equal(t, outboxState{status: store.Sent, attempts: 1, sfID: "003NEWDoe"}, e.entry(t, inserted))
	assert.Equal(t, outboxState{status: store.Sent, attempts: 1, sfID: "003NEWMoe"}, e.entry(t, inserted2))
	assert.Equal(t, outboxState{status: store.Sent, attempts: 1, sfID: "003A"}, e.entry(t, updated))
	assert.Equal(t, outboxState{status: store.Sent, attempts: 1, sfID: "003UPSE-1"}, e.entry(t, upserted))
	assert.Equal(t, outboxState{
		status: store.Pending, attempts: 1,
		lastError: "FIELD_CUSTOM_VALIDATION_EXCEPTION:Last name is not allowed:LastName --",
	}, e.entry(t, invalid), "a rejected record is retried later")
	got := e.entry(t, unknown)
	assert.Equal(t, store.Dead, got.status, "an entry that cannot succeed is dead-lettered at once")
	assert.Contains(t, got.lastError, `field "Shoe_Size__c" does not exist`)

	jobs := e.org.IngestJobs()
	require.Len(t, jobs, 3, "one job per operation")
	assert.Equal(t, "insert", jobs[0].Operation)
	assert.Equal(t, []string{"Email", "LastName"}, jobs[0].Header)
	assert.Equal(t, [][]string{{"doe@example.com", "Doe"}, {"", "Invalid"}, {"", "Moe"}}, jobs[0].Rows)
	assert.Equal(t, "update", jobs[1].Operation)
	assert.Equal(t, [][]string{{"003A", "#N/A"}}, jobs[1].Rows)
	assert.Equal(t, "upsert", jobs[2].Operation)
	assert.Equal(t, "Ext_Id__c", jobs[2].ExternalIDField)
	assert.Equal(t, jobs[2].ID, run.BulkJobID, "the run shows its latest job")

	assert.InDelta(t, 4, testutil.ToFloat64(e.orch.metrics.records.WithLabelValues("Contact", "import")), 0)
	assert.InDelta(t, 2, testutil.ToFloat64(e.orch.metrics.failures.WithLabelValues("Contact", "import")), 0)
	assert.InDelta(t, 1, testutil.ToFloat64(e.orch.metrics.deadLetters.WithLabelValues("Contact")), 0)
}

func TestImportWithNothingDue(t *testing.T) {
	e := newEnv(t, 1)

	run := e.importContacts(t)

	assert.Equal(t, store.Succeeded, run.Status)
	assert.Zero(t, run.RecordsOK)
	assert.Empty(t, e.org.IngestJobs())
}

func TestImportDeadLettersAfterTheLastAttempt(t *testing.T) {
	e := newEnv(t, 1)
	e.org.SetDescribe(contactDescribe)
	e.org.OnIngest(func(sftest.IngestJob, map[string]string) (string, error) {
		return "", errors.New("UNABLE_TO_LOCK_ROW:unable to obtain exclusive access to this record")
	})
	id := e.addEntry(t, store.Insert, "", `{"LastName":"Doe"}`)
	_, err := e.db.Pool.Exec(context.Background(), `UPDATE sf_outbox SET attempts = 4 WHERE id = $1`, id)
	require.NoError(t, err)

	run := e.importContacts(t)

	require.Equal(t, store.Succeeded, run.Status, "record failures do not fail the run")
	got := e.entry(t, id)
	assert.Equal(t, store.Dead, got.status)
	assert.Equal(t, 5, got.attempts)
	assert.Contains(t, got.lastError, "UNABLE_TO_LOCK_ROW")
}

func TestImportJobFailureRetriesItsEntries(t *testing.T) {
	e := newEnv(t, 1)
	e.org.SetDescribe(contactDescribe)
	e.org.FailIngestJobs("InvalidBatch : Failed to process query")
	id := e.addEntry(t, store.Insert, "", `{"LastName":"Doe"}`)

	run := e.importContacts(t)

	require.Equal(t, store.Failed, run.Status)
	assert.Contains(t, run.Error, "1 of the run's ingest jobs failed")
	assert.Contains(t, run.Error, "Failed to process query")
	got := e.entry(t, id)
	assert.Equal(t, store.Pending, got.status)
	assert.Equal(t, 1, got.attempts)
	assert.Contains(t, got.lastError, "Failed to process query")
}

func TestImportReleasesEntriesWhenTheRunFailsBeforeSending(t *testing.T) {
	e := newEnv(t, 1)
	id := e.addEntry(t, store.Insert, "", `{"LastName":"Doe"}`)

	run := e.importContacts(t)

	require.Equal(t, store.Failed, run.Status)
	assert.Contains(t, run.Error, "describe Contact")
	assert.Equal(t, outboxState{status: store.Pending}, e.entry(t, id), "nothing was sent, so the attempt does not count")
}

func TestShutdownCancelsImports(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, e.orch.Shutdown(ctx))

	got, err := e.orch.GetRun(context.Background(), run.ID)
	require.NoError(t, err)
	assert.Equal(t, store.Cancelled, got.Status)
	assert.Equal(t, "Aborted", e.org.IngestJobs()[0].State, "the Salesforce job is aborted")
	assert.Equal(t, outboxState{status: store.Pending}, e.entry(t, id))
}

func TestScheduledSyncImportsOnlyWhenEntriesAreDue(t *testing.T) {
	e := newEnv(t, 2)
	e.org.SetDescribe(contactDescribe)
	e.org.HoldJobs(true)
	imports := func() int {
		return e.count(t, `SELECT count(*) FROM sync_runs WHERE direction = 'import' AND trigger = 'schedule'`)
	}

	e.orch.scheduledSync("Contact")
	assert.Equal(t, 0, imports())
	assert.Equal(t, 1, e.count(t, `SELECT count(*) FROM sync_runs WHERE direction = 'export'`))

	e.addEntry(t, store.Insert, "", `{"LastName":"Doe"}`)
	e.orch.scheduledSync("Contact")
	assert.Equal(t, 1, imports())
	e.org.HoldJobs(false)
}

func TestImportDeadLettersEntriesWithoutAResult(t *testing.T) {
	e := newEnv(t, 1)
	e.org.SetDescribe(contactDescribe)
	// Salesforce echoes the value differently than it was uploaded, so the
	// result cannot be matched to its entry.
	e.org.Handle("GET /jobs/ingest/750ingest00001/successfulResults", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write([]byte("sf__Id,sf__Created,LastName\n003NEW,true,DOE\n"))
	})
	id := e.addEntry(t, store.Insert, "", `{"LastName":"Doe"}`)

	run := e.importContacts(t)

	require.Equal(t, store.Succeeded, run.Status, run.Error)
	got := e.entry(t, id)
	assert.Equal(t, store.Dead, got.status, "an insert that may have been applied is not sent again")
	assert.Equal(t, errNoResult, got.lastError)
}
