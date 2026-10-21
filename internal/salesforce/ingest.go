package salesforce

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// MaxIngestUpload is the largest CSV upload Salesforce accepts for one ingest
// job: 150 MB after the base64 encoding it applies internally, which leaves
// about 100 MB of raw data.
const MaxIngestUpload = 100_000_000

// IngestOperation is the change an ingest job applies to its records.
type IngestOperation string

// Ingest operations.
const (
	Insert IngestOperation = "insert"
	Update IngestOperation = "update"
	Upsert IngestOperation = "upsert"
	Delete IngestOperation = "delete"
)

// IngestJob is the status of a Bulk API 2.0 ingest job.
type IngestJob struct {
	ID                     string          `json:"id"`
	Object                 string          `json:"object"`
	Operation              IngestOperation `json:"operation"`
	State                  JobState        `json:"state"`
	ErrorMessage           string          `json:"errorMessage"`
	NumberRecordsProcessed int64           `json:"numberRecordsProcessed"`
	NumberRecordsFailed    int64           `json:"numberRecordsFailed"`
}

// IngestResultSet names one of the per-record result lists of an ingest job.
type IngestResultSet string

// Ingest result sets. Successful results start with the columns sf__Id and
// sf__Created, failed results with sf__Id and sf__Error; both then repeat the
// uploaded columns. Unprocessed records, left over by a failed or aborted
// job, carry only the uploaded columns.
const (
	SuccessfulResults  IngestResultSet = "successfulResults"
	FailedResults      IngestResultSet = "failedResults"
	UnprocessedRecords IngestResultSet = "unprocessedrecords"
)

// CreateIngestJob opens an ingest job that applies op to records of object.
// externalIDField names the field an upsert matches on and must be empty for
// other operations.
func (c *Client) CreateIngestJob(ctx context.Context, object string, op IngestOperation, externalIDField string) (IngestJob, error) {
	in := map[string]string{
		"object":          object,
		"operation":       string(op),
		"contentType":     "CSV",
		"columnDelimiter": "COMMA",
		"lineEnding":      "LF",
	}
	if externalIDField != "" {
		in["externalIdFieldName"] = externalIDField
	}
	var job IngestJob
	if err := c.DoJSON(ctx, http.MethodPost, "/jobs/ingest", in, &job); err != nil {
		return IngestJob{}, fmt.Errorf("create ingest job: %w", err)
	}
	return job, nil
}

// UploadIngestData uploads the job's records as CSV with a header row of
// field API names. A job takes one upload of at most MaxIngestUpload bytes.
func (c *Client) UploadIngestData(ctx context.Context, id string, data []byte) error {
	if len(data) > MaxIngestUpload {
		return fmt.Errorf("upload to ingest job %s: %d bytes exceed the %d byte limit", id, len(data), MaxIngestUpload)
	}
	resp, err := c.Do(ctx, Request{
		Method: http.MethodPut,
		Path:   ingestPath(id) + "/batches",
		Header: http.Header{"Content-Type": {"text/csv"}},
		Body:   func() (io.Reader, error) { return bytes.NewReader(data), nil },
	})
	if err != nil {
		return fmt.Errorf("upload to ingest job %s: %w", id, err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return nil
}

// CloseIngestJob marks the job's upload complete, which queues it for
// processing.
func (c *Client) CloseIngestJob(ctx context.Context, id string) error {
	return c.setIngestState(ctx, id, JobUploadComplete)
}

// AbortIngestJob asks Salesforce to stop an ingest job. Records it has
// already processed stay changed.
func (c *Client) AbortIngestJob(ctx context.Context, id string) error {
	return c.setIngestState(ctx, id, JobAborted)
}

func (c *Client) setIngestState(ctx context.Context, id string, state JobState) error {
	in := map[string]string{"state": string(state)}
	if err := c.DoJSON(ctx, http.MethodPatch, ingestPath(id), in, nil); err != nil {
		return fmt.Errorf("set ingest job %s to %s: %w", id, state, err)
	}
	return nil
}

// GetIngestJob returns the current status of an ingest job.
func (c *Client) GetIngestJob(ctx context.Context, id string) (IngestJob, error) {
	var job IngestJob
	if err := c.DoJSON(ctx, http.MethodGet, ingestPath(id), nil, &job); err != nil {
		return IngestJob{}, fmt.Errorf("get ingest job %s: %w", id, err)
	}
	return job, nil
}

// WaitForIngestJob polls an ingest job until it reaches a terminal state. It
// returns the job's final status, together with a *JobError if the job
// failed or was aborted; the result sets of such a job are still readable.
func (c *Client) WaitForIngestJob(ctx context.Context, id string) (IngestJob, error) {
	var job IngestJob
	err := c.waitForJob(ctx, "ingest", id, func(ctx context.Context) (JobState, string, error) {
		var err error
		job, err = c.GetIngestJob(ctx, id)
		return job.State, job.ErrorMessage, err
	})
	return job, err
}

// ReadIngestResults downloads one result set of a finished ingest job and
// passes its CSV body, header row included, to read.
func (c *Client) ReadIngestResults(ctx context.Context, id string, set IngestResultSet, read func(csv io.Reader) error) error {
	resp, err := c.Do(ctx, Request{
		Method: http.MethodGet,
		Path:   ingestPath(id) + "/" + string(set),
		Header: http.Header{"Accept": {"text/csv"}},
	})
	if err != nil {
		return fmt.Errorf("get %s of ingest job %s: %w", set, id, err)
	}
	defer func() { _ = resp.Body.Close() }()
	return read(resp.Body)
}

func ingestPath(id string) string { return "/jobs/ingest/" + url.PathEscape(id) }
