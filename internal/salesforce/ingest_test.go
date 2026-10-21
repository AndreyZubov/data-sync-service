package salesforce

import (
	"context"
	"encoding/csv"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AndreyZubov/data-sync-service/internal/salesforce/sftest"
)

func readCSV(t *testing.T, c *Client, id string, set IngestResultSet) [][]string {
	t.Helper()
	var rows [][]string
	require.NoError(t, c.ReadIngestResults(context.Background(), id, set, func(body io.Reader) error {
		var err error
		rows, err = csv.NewReader(body).ReadAll()
		return err
	}))
	return rows
}

func TestIngestJobLifecycle(t *testing.T) {
	org := sftest.NewServer(t)
	org.OnIngest(func(_ sftest.IngestJob, record map[string]string) (string, error) {
		if record["Name"] == "" {
			return "", errors.New("REQUIRED_FIELD_MISSING:Required fields are missing: [Name]:Name --")
		}
		return "001" + record["Ext__c"], nil
	})
	c := newBulkClient(t, org)
	ctx := context.Background()

	job, err := c.CreateIngestJob(ctx, "Account", Upsert, "Ext__c")
	require.NoError(t, err)
	assert.Equal(t, JobOpen, job.State)
	require.NoError(t, c.UploadIngestData(ctx, job.ID, []byte("Ext__c,Name\nA,Acme\nB,\n")))
	require.NoError(t, c.CloseIngestJob(ctx, job.ID))

	done, err := c.WaitForIngestJob(ctx, job.ID)
	require.NoError(t, err)
	assert.Equal(t, JobComplete, done.State)
	assert.EqualValues(t, 2, done.NumberRecordsProcessed)
	assert.EqualValues(t, 1, done.NumberRecordsFailed)

	assert.Equal(t, [][]string{{"sf__Id", "sf__Created", "Ext__c", "Name"}, {"001A", "false", "A", "Acme"}},
		readCSV(t, c, job.ID, SuccessfulResults))
	assert.Equal(t, [][]string{
		{"sf__Id", "sf__Error", "Ext__c", "Name"},
		{"", "REQUIRED_FIELD_MISSING:Required fields are missing: [Name]:Name --", "B", ""},
	}, readCSV(t, c, job.ID, FailedResults))

	jobs := org.IngestJobs()
	require.Len(t, jobs, 1)
	assert.Equal(t, "Account", jobs[0].Object)
	assert.Equal(t, "upsert", jobs[0].Operation)
	assert.Equal(t, "Ext__c", jobs[0].ExternalIDField)
	assert.Equal(t, []string{"Ext__c", "Name"}, jobs[0].Header)
}

func TestIngestJobFailure(t *testing.T) {
	org := sftest.NewServer(t)
	org.FailIngestJobs("InvalidBatch : Field name not found : Nope__c")
	c := newBulkClient(t, org)
	ctx := context.Background()
	job, err := c.CreateIngestJob(ctx, "Account", Insert, "")
	require.NoError(t, err)
	require.NoError(t, c.UploadIngestData(ctx, job.ID, []byte("Nope__c\nx\n")))
	require.NoError(t, c.CloseIngestJob(ctx, job.ID))

	done, err := c.WaitForIngestJob(ctx, job.ID)

	var jobErr *JobError
	require.ErrorAs(t, err, &jobErr)
	assert.Equal(t, JobFailed, jobErr.State)
	assert.Contains(t, jobErr.Error(), "Field name not found")
	assert.Equal(t, JobFailed, done.State, "the final status is returned with the error")
	assert.Equal(t, [][]string{{"Nope__c"}, {"x"}}, readCSV(t, c, job.ID, UnprocessedRecords))
}

func TestAbortIngestJob(t *testing.T) {
	org := sftest.NewServer(t)
	c := newBulkClient(t, org)
	ctx := context.Background()
	job, err := c.CreateIngestJob(ctx, "Account", Delete, "")
	require.NoError(t, err)

	require.NoError(t, c.AbortIngestJob(ctx, job.ID))

	_, err = c.WaitForIngestJob(ctx, job.ID)
	var jobErr *JobError
	require.ErrorAs(t, err, &jobErr)
	assert.Equal(t, JobAborted, jobErr.State)
}

func TestIngestJobErrors(t *testing.T) {
	org := sftest.NewServer(t)
	c := newBulkClient(t, org)
	ctx := context.Background()

	_, err := c.CreateIngestJob(ctx, "Account", Upsert, "")
	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "INVALIDJOB", apiErr.Code)

	job, err := c.CreateIngestJob(ctx, "Account", Insert, "")
	require.NoError(t, err)
	err = c.UploadIngestData(ctx, job.ID, make([]byte, MaxIngestUpload+1))
	assert.ErrorContains(t, err, "exceed the 100000000 byte limit")
	assert.Empty(t, org.IngestJobs()[0].Header, "an oversized upload is not sent")

	err = c.CloseIngestJob(ctx, job.ID)
	require.ErrorAs(t, err, &apiErr, "a job without data cannot be closed")
	assert.Equal(t, "INVALIDJOBSTATE", apiErr.Code)

	err = c.ReadIngestResults(ctx, job.ID, SuccessfulResults, func(io.Reader) error { return nil })
	require.ErrorAs(t, err, &apiErr, "an open job has no results")

	_, err = c.GetIngestJob(ctx, "750nope")
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "NOT_FOUND", apiErr.Code)

	stop := errors.New("stop")
	require.NoError(t, c.UploadIngestData(ctx, job.ID, []byte("Name\nAcme\n")))
	require.NoError(t, c.CloseIngestJob(ctx, job.ID))
	_, err = c.WaitForIngestJob(ctx, job.ID)
	require.NoError(t, err)
	err = c.ReadIngestResults(ctx, job.ID, SuccessfulResults, func(body io.Reader) error {
		raw, _ := io.ReadAll(body)
		assert.True(t, strings.HasPrefix(string(raw), "sf__Id,sf__Created,Name\n"))
		return stop
	})
	assert.ErrorIs(t, err, stop)
}
