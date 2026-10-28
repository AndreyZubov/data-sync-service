package salesforce

import (
	"context"
	"encoding/csv"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/AndreyZubov/data-sync-service/internal/salesforce/sftest"
)

func newBulkClient(t *testing.T, org *sftest.Server) *Client {
	t.Helper()
	tokens, _ := newProvider(org, time.Hour)
	return NewClient(ClientConfig{
		Tokens:             tokens,
		APIVersion:         sftest.APIVersion,
		HTTPClient:         org.HTTPClient(),
		Retry:              fastRetry,
		Logger:             zaptest.NewLogger(t),
		JobPollInterval:    time.Millisecond,
		JobPollMaxInterval: 4 * time.Millisecond,
	})
}

func TestDescribe(t *testing.T) {
	org := sftest.NewServer(t)
	org.SetDescribe(sftest.Describe{Name: "Account", Queryable: true, Fields: []sftest.DescribeField{
		{Name: "Id", Type: "id"},
		{Name: "BillingAddress", Type: "address"},
	}})
	c := newBulkClient(t, org)

	desc, err := c.Describe(context.Background(), "Account")

	require.NoError(t, err)
	assert.True(t, desc.Queryable)
	assert.Equal(t, []Field{{Name: "Id", Type: "id"}, {Name: "BillingAddress", Type: "address"}}, desc.Fields)
	assert.True(t, desc.HasField("Id"))
	assert.False(t, desc.HasField("Name"))

	_, err = c.Describe(context.Background(), "Nope__c")
	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusNotFound, apiErr.StatusCode)
}

func TestQueryJobLifecycle(t *testing.T) {
	org := sftest.NewServer(t)
	org.SetResultPageSize(2)
	org.OnQuery(func(string, string) ([][]string, error) {
		return [][]string{{"Id", "Name"}, {"001A", "Acme"}, {"001B", "Globex, Inc."}, {"001C", "Initech"}}, nil
	})
	c := newBulkClient(t, org)
	ctx := context.Background()

	job, err := c.CreateQueryJob(ctx, QueryAll, "SELECT Id, Name FROM Account")
	require.NoError(t, err)
	assert.Equal(t, JobUploadComplete, job.State)

	done, err := c.WaitForQueryJob(ctx, job.ID)
	require.NoError(t, err)
	assert.Equal(t, JobComplete, done.State)
	assert.EqualValues(t, 3, done.NumberRecordsProcessed)

	read := func(from string) (rows [][]string, next []string) {
		err := c.StreamQueryResults(ctx, job.ID, from, func(body io.Reader, locator string) error {
			records, err := csv.NewReader(body).ReadAll()
			require.NoError(t, err)
			require.Equal(t, []string{"Id", "Name"}, records[0], "every page starts with the header")
			rows = append(rows, records[1:]...)
			next = append(next, locator)
			return nil
		})
		require.NoError(t, err)
		return rows, next
	}
	rows, next := read("")
	assert.Equal(t, [][]string{{"001A", "Acme"}, {"001B", "Globex, Inc."}, {"001C", "Initech"}}, rows)
	assert.Equal(t, []string{"2", ""}, next, "each page names the next one; the last names none")

	rows, next = read("2")
	assert.Equal(t, [][]string{{"001C", "Initech"}}, rows, "a download resumes at a page's locator")
	assert.Equal(t, []string{""}, next)

	jobs := org.QueryJobs()
	require.Len(t, jobs, 1)
	assert.Equal(t, "queryAll", jobs[0].Operation)
	assert.Equal(t, "SELECT Id, Name FROM Account", jobs[0].Query)
}

func TestStreamQueryResultsStopsOnCallbackError(t *testing.T) {
	org := sftest.NewServer(t)
	org.SetResultPageSize(1)
	org.OnQuery(func(string, string) ([][]string, error) {
		return [][]string{{"Id"}, {"1"}, {"2"}, {"3"}}, nil
	})
	c := newBulkClient(t, org)
	ctx := context.Background()
	job, err := c.CreateQueryJob(ctx, Query, "SELECT Id FROM Account")
	require.NoError(t, err)
	_, err = c.WaitForQueryJob(ctx, job.ID)
	require.NoError(t, err)

	stop := errors.New("stop")
	calls := 0
	err = c.StreamQueryResults(ctx, job.ID, "", func(io.Reader, string) error {
		calls++
		return stop
	})

	require.ErrorIs(t, err, stop)
	assert.Equal(t, 1, calls)
}

func TestStreamQueryResultsOfIncompleteJob(t *testing.T) {
	org := sftest.NewServer(t)
	c := newBulkClient(t, org)
	job, err := c.CreateQueryJob(context.Background(), Query, "SELECT Id FROM Account")
	require.NoError(t, err)

	err = c.StreamQueryResults(context.Background(), job.ID, "", func(io.Reader, string) error { return nil })

	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "INVALIDJOBSTATE", apiErr.Code)
}

func TestWaitForFailedQueryJob(t *testing.T) {
	org := sftest.NewServer(t)
	org.OnQuery(func(string, string) ([][]string, error) {
		return nil, errors.New("INVALID_FIELD: No such column 'Foo'")
	})
	c := newBulkClient(t, org)
	job, err := c.CreateQueryJob(context.Background(), Query, "SELECT Foo FROM Account")
	require.NoError(t, err)

	_, err = c.WaitForQueryJob(context.Background(), job.ID)

	var jobErr *JobError
	require.ErrorAs(t, err, &jobErr)
	assert.Equal(t, JobFailed, jobErr.State)
	assert.Contains(t, jobErr.Error(), "No such column")
}

func TestAbortQueryJob(t *testing.T) {
	org := sftest.NewServer(t)
	org.HoldJobs(true)
	c := newBulkClient(t, org)
	ctx := context.Background()
	job, err := c.CreateQueryJob(ctx, Query, "SELECT Id FROM Account")
	require.NoError(t, err)

	require.NoError(t, c.AbortQueryJob(ctx, job.ID))
	_, err = c.WaitForQueryJob(ctx, job.ID)

	var jobErr *JobError
	require.ErrorAs(t, err, &jobErr)
	assert.Equal(t, JobAborted, jobErr.State)
	assert.Equal(t, "bulk job "+job.ID+" ended in state Aborted", jobErr.Error())
}

func TestWaitForQueryJobRespectsTheContext(t *testing.T) {
	org := sftest.NewServer(t)
	org.HoldJobs(true)
	c := newBulkClient(t, org)
	job, err := c.CreateQueryJob(context.Background(), Query, "SELECT Id FROM Account")
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err = c.WaitForQueryJob(ctx, job.ID)

	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestJobStateTerminal(t *testing.T) {
	for state, want := range map[JobState]bool{
		JobOpen: false, JobUploadComplete: false, JobInProgress: false,
		JobComplete: true, JobFailed: true, JobAborted: true,
	} {
		assert.Equal(t, want, state.Terminal(), state)
	}
}
