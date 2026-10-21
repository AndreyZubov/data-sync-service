package salesforce

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"go.uber.org/zap"

	"github.com/AndreyZubov/data-sync-service/internal/retry"
)

// Default Bulk API job polling intervals. Small jobs finish in seconds, large
// ones take many minutes; doubling the wait keeps both responsive without
// spending API calls on a slow job.
const (
	defaultJobPollInterval    = time.Second
	defaultJobPollMaxInterval = 30 * time.Second
)

// JobState is the state of a Bulk API 2.0 job.
type JobState string

// Bulk API 2.0 query job states. A query job starts in UploadComplete, moves
// to InProgress, and ends in JobComplete, Failed, or Aborted.
const (
	JobUploadComplete JobState = "UploadComplete"
	JobInProgress     JobState = "InProgress"
	JobComplete       JobState = "JobComplete"
	JobFailed         JobState = "Failed"
	JobAborted        JobState = "Aborted"
)

// Terminal reports whether the job can no longer change state.
func (s JobState) Terminal() bool {
	return s == JobComplete || s == JobFailed || s == JobAborted
}

// QueryOperation selects which records a query job returns.
type QueryOperation string

// Query operations.
const (
	// Query returns live records.
	Query QueryOperation = "query"
	// QueryAll also returns deleted records still in the Recycle Bin, so
	// deletions reach the destination.
	QueryAll QueryOperation = "queryAll"
)

// QueryJob is the status of a Bulk API 2.0 query job.
type QueryJob struct {
	ID                     string         `json:"id"`
	Object                 string         `json:"object"`
	Operation              QueryOperation `json:"operation"`
	State                  JobState       `json:"state"`
	ErrorMessage           string         `json:"errorMessage"`
	NumberRecordsProcessed int64          `json:"numberRecordsProcessed"`
}

// JobError reports a Bulk API job that ended in Failed or Aborted.
type JobError struct {
	JobID   string
	State   JobState
	Message string
}

func (e *JobError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("bulk job %s ended in state %s", e.JobID, e.State)
	}
	return fmt.Sprintf("bulk job %s ended in state %s: %s", e.JobID, e.State, e.Message)
}

// CreateQueryJob starts a Bulk API 2.0 query job for soql.
func (c *Client) CreateQueryJob(ctx context.Context, op QueryOperation, soql string) (QueryJob, error) {
	in := map[string]string{
		"operation":       string(op),
		"query":           soql,
		"contentType":     "CSV",
		"columnDelimiter": "COMMA",
		"lineEnding":      "LF",
	}
	var job QueryJob
	if err := c.DoJSON(ctx, http.MethodPost, "/jobs/query", in, &job); err != nil {
		return QueryJob{}, fmt.Errorf("create query job: %w", err)
	}
	return job, nil
}

// GetQueryJob returns the current status of a query job.
func (c *Client) GetQueryJob(ctx context.Context, id string) (QueryJob, error) {
	var job QueryJob
	if err := c.DoJSON(ctx, http.MethodGet, "/jobs/query/"+url.PathEscape(id), nil, &job); err != nil {
		return QueryJob{}, fmt.Errorf("get query job %s: %w", id, err)
	}
	return job, nil
}

// AbortQueryJob asks Salesforce to stop a query job.
func (c *Client) AbortQueryJob(ctx context.Context, id string) error {
	in := map[string]string{"state": string(JobAborted)}
	if err := c.DoJSON(ctx, http.MethodPatch, "/jobs/query/"+url.PathEscape(id), in, nil); err != nil {
		return fmt.Errorf("abort query job %s: %w", id, err)
	}
	return nil
}

// WaitForQueryJob polls a query job until it reaches a terminal state. It
// returns the completed job, or a *JobError if the job failed or was aborted.
func (c *Client) WaitForQueryJob(ctx context.Context, id string) (QueryJob, error) {
	interval := c.pollInterval
	for {
		job, err := c.GetQueryJob(ctx, id)
		if err != nil {
			return QueryJob{}, err
		}
		switch job.State {
		case JobComplete:
			return job, nil
		case JobFailed, JobAborted:
			return job, &JobError{JobID: id, State: job.State, Message: job.ErrorMessage}
		}

		c.log.Debug("waiting for query job", zap.String("job_id", id),
			zap.String("state", string(job.State)), zap.Duration("delay", interval))
		if err := retry.Sleep(ctx, interval); err != nil {
			return QueryJob{}, fmt.Errorf("wait for query job %s: %w", id, err)
		}
		interval = min(2*interval, c.pollMaxInterval)
	}
}

// StreamQueryResults downloads the results of a completed query job page by
// page and calls page with each page's CSV body, header row included. Pages
// are read straight from the network, so memory use does not grow with the
// result size. Iteration stops at the first error page returns.
func (c *Client) StreamQueryResults(ctx context.Context, id string, page func(csv io.Reader) error) error {
	path := "/jobs/query/" + url.PathEscape(id) + "/results"
	locator := ""
	for {
		req := Request{Method: http.MethodGet, Path: path, Header: http.Header{"Accept": {"text/csv"}}}
		if locator != "" {
			req.Query = url.Values{"locator": {locator}}
		}
		resp, err := c.Do(ctx, req)
		if err != nil {
			return fmt.Errorf("get results of query job %s: %w", id, err)
		}
		err = page(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return err
		}

		// Salesforce sends the literal string "null" after the last page.
		locator = resp.Header.Get("Sforce-Locator")
		if locator == "" || locator == "null" {
			return nil
		}
	}
}
