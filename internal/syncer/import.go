package syncer

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"slices"
	"time"

	"go.uber.org/zap"

	"github.com/AndreyZubov/data-sync-service/internal/salesforce"
	"github.com/AndreyZubov/data-sync-service/internal/store"
)

// abortTimeout bounds the best-effort abort of an ingest job the run will
// not see through.
const abortTimeout = 10 * time.Second

// importer sends outbox entries of one object to Salesforce.
type importer struct {
	sf         *salesforce.Client
	store      *store.Store
	maxRecords int
	retry      store.RetryPolicy
	metrics    *metrics
}

// importRun claims due outbox entries of run's object and sends them through
// one ingest job per operation, or more if an upload would exceed the size
// limit. A resumed run instead finishes the entries it had already claimed.
// Each job's outcomes are recorded as soon as it finishes, so a later
// failure loses no progress.
//
// A job that fails as a whole leaves its entries to be retried and fails the
// run, but the remaining jobs are still sent. If ctx is cancelled, a job in
// progress is left running, so that the run can be resumed when the service
// starts again.
func (im *importer) importRun(ctx context.Context, run store.Run, log *zap.Logger) error {
	var entries []store.OutboxEntry
	var err error
	if run.Resumes > 0 {
		entries, err = im.store.RunOutbox(ctx, run.ID)
	} else {
		entries, err = im.store.ClaimOutbox(ctx, run.ID, run.Object, im.maxRecords)
	}
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		log.Info("outbox has no due entries")
		return nil
	}
	log.Info("outbox entries claimed", zap.Int("entries", len(entries)), zap.Int("resumes", run.Resumes))

	desc, err := im.sf.Describe(ctx, run.Object)
	if err != nil {
		return err
	}
	return im.process(ctx, run, desc, entries, log)
}

// process finishes entries: those already uploaded in an ingest job, by a
// resumed run or by a run that ended before learning their outcome, get that
// job's outcome, and the rest are sent now.
func (im *importer) process(
	ctx context.Context, run store.Run, desc *salesforce.ObjectDescription, entries []store.OutboxEntry, log *zap.Logger,
) error {
	var unsent []store.OutboxEntry
	byJob := make(map[string][]store.OutboxEntry)
	var jobIDs []string
	for _, e := range entries {
		if e.BulkJobID == "" {
			unsent = append(unsent, e)
			continue
		}
		if _, seen := byJob[e.BulkJobID]; !seen {
			jobIDs = append(jobIDs, e.BulkJobID)
		}
		byJob[e.BulkJobID] = append(byJob[e.BulkJobID], e)
	}

	var jobErrs []error
	for _, id := range jobIDs {
		outcomes, again, err := im.recover(ctx, desc, id, byJob[id], log.With(zap.String("bulk_job_id", id)))
		if recErr := im.record(ctx, run, outcomes, log); recErr != nil {
			return errors.Join(err, recErr)
		}
		var jobErr *salesforce.JobError
		if errors.As(err, &jobErr) {
			jobErrs = append(jobErrs, err)
		} else if err != nil {
			return err
		}
		unsent = append(unsent, again...)
	}

	// Entries of one record must still go out oldest first.
	slices.SortFunc(unsent, func(a, b store.OutboxEntry) int { return cmp.Compare(a.ID, b.ID) })
	more, err := im.sendAll(ctx, run, desc, unsent, log)
	if err != nil {
		return err
	}
	return jobFailures(append(jobErrs, more...))
}

// recover returns the outcomes of entries uploaded earlier in the ingest job
// jobID, waiting for the job if it is still in progress. Entries Salesforce
// never received are returned for sending again.
func (im *importer) recover(
	ctx context.Context, desc *salesforce.ObjectDescription, jobID string, entries []store.OutboxEntry, log *zap.Logger,
) (outcomes []store.Outcome, unsent []store.OutboxEntry, err error) {
	job, err := im.sf.GetIngestJob(ctx, jobID)
	var apiErr *salesforce.APIError
	if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
		log.Warn("earlier ingest job no longer exists; its outcomes are unknown")
		for _, e := range entries {
			outcomes = append(outcomes, store.Outcome{
				EntryID: e.ID, Kind: store.Rejected,
				Error: fmt.Sprintf("the result of ingest job %s is no longer available", jobID),
			})
		}
		return outcomes, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	switch {
	case job.State == salesforce.JobOpen:
		// Salesforce processes nothing before the upload is marked complete,
		// so the entries can safely go out in a new job.
		log.Info("earlier ingest job was never closed; sending its entries again")
		im.abort(ctx, jobID, log)
		return nil, entries, nil
	case job.State == salesforce.JobAborted && job.NumberRecordsProcessed == 0:
		// A run that failed to upload aborts its job before ending.
		log.Info("earlier ingest job was aborted before processing anything; sending its entries again")
		return nil, entries, nil
	}

	// Planning the same entries again yields the CSV the job was given, which
	// is what its results are matched against.
	g := &ingestGroup{operation: entries[0].Operation, externalIDField: entries[0].ExternalIDField, entries: entries}
	plan, err := planIngest(desc, g, math.MaxInt)
	if err != nil {
		return nil, nil, err
	}
	for _, r := range plan.rejected {
		// The entry was uploaded but no longer passes the object's describe
		// result, so its row cannot be rebuilt to match its result.
		outcomes = append(outcomes, store.Outcome{EntryID: r.EntryID, Kind: store.Rejected, Error: errNoResult})
	}
	if len(plan.batches) == 0 {
		return outcomes, nil, nil
	}
	log.Info("collecting the outcomes of an earlier ingest job", zap.String("state", string(job.State)))
	more, err := im.finish(ctx, jobID, plan.batches[0], log)
	return append(outcomes, more...), nil, err
}

// sendAll sends entries through as many ingest jobs as they need and records
// the outcomes. It returns the errors of jobs that failed as a whole, which
// do not stop the remaining jobs, and any other error, which does.
func (im *importer) sendAll(
	ctx context.Context, run store.Run, desc *salesforce.ObjectDescription, entries []store.OutboxEntry, log *zap.Logger,
) ([]error, error) {
	var jobErrs []error
	for _, g := range groupEntries(entries) {
		plan, err := planIngest(desc, g, salesforce.MaxIngestUpload)
		if err != nil {
			return nil, err
		}
		if err := im.record(ctx, run, plan.rejected, log); err != nil {
			return nil, err
		}
		for _, b := range plan.batches {
			outcomes, err := im.send(ctx, run, g, b, log)
			if recErr := im.record(ctx, run, outcomes, log); recErr != nil {
				return nil, errors.Join(err, recErr)
			}
			var jobErr *salesforce.JobError
			if errors.As(err, &jobErr) {
				jobErrs = append(jobErrs, err)
			} else if err != nil {
				return nil, err
			}
		}
	}
	return jobErrs, nil
}

// jobFailures summarises the errors of ingest jobs that failed as a whole.
func jobFailures(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%d of the run's ingest jobs failed: %w", len(errs), errors.Join(errs...))
}

// send uploads b as one ingest job and returns the outcome of each of its
// entries. The entries are assigned to the job before the upload, so a
// resumed run can find their outcome. A job that failed as a whole is
// reported as a *JobError alongside outcomes that retry its unprocessed
// entries.
func (im *importer) send(ctx context.Context, run store.Run, g *ingestGroup, b *ingestBatch, log *zap.Logger) ([]store.Outcome, error) {
	job, err := im.sf.CreateIngestJob(ctx, run.Object, salesforce.IngestOperation(g.operation), g.externalIDField)
	if err != nil {
		return nil, err
	}
	log = log.With(zap.String("bulk_job_id", job.ID))
	log.Info("ingest job created", zap.String("operation", string(g.operation)), zap.Int("records", len(b.rows)))
	if err := im.store.AssignIngestJob(ctx, run.ID, job.ID, b.entryIDs); err != nil {
		im.abort(ctx, job.ID, log)
		return nil, err
	}
	if err := im.upload(ctx, job.ID, b.data); err != nil {
		// A cancelled run leaves the job for its resumption to deal with: a
		// close request may have reached Salesforce even so.
		if ctx.Err() == nil {
			im.abort(ctx, job.ID, log)
		}
		return nil, err
	}
	return im.finish(ctx, job.ID, b, log)
}

// upload sends data to an open ingest job and marks the upload complete.
func (im *importer) upload(ctx context.Context, jobID string, data []byte) error {
	if err := im.sf.UploadIngestData(ctx, jobID, data); err != nil {
		return err
	}
	return im.sf.CloseIngestJob(ctx, jobID)
}

// finish waits for an ingest job to end and returns the outcome of each of
// b's entries. Entries without a result are rejected if the job completed,
// since Salesforce may have applied them, and retried if it failed as a
// whole, which is then reported as a *JobError.
func (im *importer) finish(ctx context.Context, jobID string, b *ingestBatch, log *zap.Logger) ([]store.Outcome, error) {
	done, err := im.sf.WaitForIngestJob(ctx, jobID)
	var jobErr *salesforce.JobError
	switch {
	case errors.As(err, &jobErr):
		log.Warn("ingest job did not complete", zap.Error(err))
	case err != nil:
		return nil, err
	default:
		log.Info("ingest job complete", zap.Int64("processed", done.NumberRecordsProcessed),
			zap.Int64("failed", done.NumberRecordsFailed))
	}

	outcomes, unmatched, err := im.collect(ctx, jobID, b)
	if err != nil {
		return nil, err
	}
	for _, id := range unmatched {
		if jobErr != nil {
			// Salesforce did not process the record.
			outcomes = append(outcomes, store.Outcome{EntryID: id, Kind: store.Retry, Error: jobErr.Error()})
		} else {
			outcomes = append(outcomes, store.Outcome{EntryID: id, Kind: store.Rejected, Error: errNoResult})
		}
	}
	if jobErr != nil {
		return outcomes, jobErr
	}
	return outcomes, nil
}

// collect matches the results of a finished job to b's entries and returns
// their outcomes, along with the entries no result was found for.
func (im *importer) collect(ctx context.Context, jobID string, b *ingestBatch) ([]store.Outcome, []int64, error) {
	m := newResultMatcher(b)
	outcomes := make([]store.Outcome, 0, len(b.rows))
	sets := []struct {
		set     salesforce.IngestResultSet
		outcome func(id int64, row map[string]string) store.Outcome
	}{
		{salesforce.SuccessfulResults, func(id int64, row map[string]string) store.Outcome {
			return store.Outcome{EntryID: id, Kind: store.Delivered, SalesforceID: row["sf__Id"]}
		}},
		{salesforce.FailedResults, func(id int64, row map[string]string) store.Outcome {
			return store.Outcome{EntryID: id, Kind: store.Retry, Error: row["sf__Error"]}
		}},
	}
	for _, s := range sets {
		err := im.sf.ReadIngestResults(ctx, jobID, s.set, func(body io.Reader) error {
			return m.read(body, func(id int64, row map[string]string) {
				outcomes = append(outcomes, s.outcome(id, row))
			})
		})
		if err != nil {
			return nil, nil, err
		}
	}
	return outcomes, m.unmatched(), nil
}

// record stores outcomes. Salesforce has already acted on them, so the write
// goes ahead even if the run is being cancelled: losing it would send the
// entries again.
func (im *importer) record(ctx context.Context, run store.Run, outcomes []store.Outcome, log *zap.Logger) error {
	if len(outcomes) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), statusWriteTimeout)
	defer cancel()
	dead, err := im.store.RecordOutcomes(ctx, run.ID, outcomes, im.retry)
	if err != nil {
		return err
	}

	delivered := 0
	for _, o := range outcomes {
		if o.Kind == store.Delivered {
			delivered++
		}
	}
	failed := len(outcomes) - delivered
	im.metrics.records.WithLabelValues(run.Object, string(store.Import)).Add(float64(delivered))
	im.metrics.failures.WithLabelValues(run.Object, string(store.Import)).Add(float64(failed))
	im.metrics.deadLetters.WithLabelValues(run.Object).Add(float64(dead))
	if failed > 0 {
		log.Warn("outbox entries not delivered", zap.Int("failed", failed), zap.Int64("dead_lettered", dead))
	}
	return nil
}

// abort stops an ingest job the run will not see through. Failure is only
// logged: an unfinished job expires on its own.
func (im *importer) abort(ctx context.Context, jobID string, log *zap.Logger) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), abortTimeout)
	defer cancel()
	if err := im.sf.AbortIngestJob(ctx, jobID); err != nil {
		log.Warn("abort ingest job", zap.Error(err))
		return
	}
	log.Info("ingest job aborted")
}
