package syncer

import (
	"context"
	"errors"
	"fmt"
	"io"

	"go.uber.org/zap"

	"github.com/AndreyZubov/data-sync-service/internal/salesforce"
	"github.com/AndreyZubov/data-sync-service/internal/store"
)

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
// limit. Each job's outcomes are recorded as soon as it finishes, so a later
// failure loses no progress. Entries whose outcome is still unknown when the
// run stops go back to pending.
//
// A job that fails as a whole leaves its entries to be retried and fails the
// run, but the remaining jobs are still sent.
func (im *importer) importRun(ctx context.Context, run store.Run, log *zap.Logger) error {
	entries, err := im.store.ClaimOutbox(ctx, run.ID, run.Object, im.maxRecords)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		log.Info("outbox has no due entries")
		return nil
	}
	log.Info("outbox entries claimed", zap.Int("entries", len(entries)))
	defer im.release(ctx, run.ID, log)

	desc, err := im.sf.Describe(ctx, run.Object)
	if err != nil {
		return err
	}

	var jobErrs []error
	for _, g := range groupEntries(entries) {
		plan, err := planIngest(desc, g, salesforce.MaxIngestUpload)
		if err != nil {
			return err
		}
		if err := im.record(ctx, run, plan.rejected, log); err != nil {
			return err
		}
		for _, b := range plan.batches {
			// A cancelled send may still return the outcomes it salvaged.
			outcomes, err := im.send(ctx, run, g, b, log)
			if recErr := im.record(ctx, run, outcomes, log); recErr != nil {
				return errors.Join(err, recErr)
			}
			var jobErr *salesforce.JobError
			if errors.As(err, &jobErr) {
				jobErrs = append(jobErrs, err)
			} else if err != nil {
				return err
			}
		}
	}
	if len(jobErrs) > 0 {
		return fmt.Errorf("%d of the run's ingest jobs failed: %w", len(jobErrs), errors.Join(jobErrs...))
	}
	return nil
}

// send uploads b as one ingest job and returns the outcome of each of its
// entries. A job that failed as a whole is reported as a *JobError alongside
// outcomes that retry its unprocessed entries. If ctx is cancelled while the
// job runs, the job is aborted and the outcomes of the records it had already
// processed are returned with the error. Entries without an outcome stay
// undecided.
func (im *importer) send(ctx context.Context, run store.Run, g *ingestGroup, b *ingestBatch, log *zap.Logger) ([]store.Outcome, error) {
	job, err := im.sf.CreateIngestJob(ctx, run.Object, salesforce.IngestOperation(g.operation), g.externalIDField)
	if err != nil {
		return nil, err
	}
	log = log.With(zap.String("bulk_job_id", job.ID))
	log.Info("ingest job created", zap.String("operation", string(g.operation)), zap.Int("records", len(b.rows)))
	if err := im.store.SetBulkJob(ctx, run.ID, job.ID); err != nil {
		im.abort(ctx, job.ID, log)
		return nil, err
	}
	if err := im.sf.UploadIngestData(ctx, job.ID, b.data); err != nil {
		im.abort(ctx, job.ID, log)
		return nil, err
	}
	if err := im.sf.CloseIngestJob(ctx, job.ID); err != nil {
		im.abort(ctx, job.ID, log)
		return nil, err
	}

	done, err := im.sf.WaitForIngestJob(ctx, job.ID)
	var jobErr *salesforce.JobError
	switch {
	case errors.As(err, &jobErr):
		log.Warn("ingest job did not complete", zap.Error(err))
	case err != nil && ctx.Err() != nil:
		return im.salvage(ctx, job.ID, b, log), err
	case err != nil:
		return nil, err
	default:
		log.Info("ingest job complete", zap.Int64("processed", done.NumberRecordsProcessed),
			zap.Int64("failed", done.NumberRecordsFailed))
	}

	outcomes, unmatched, err := im.collect(ctx, job.ID, b)
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

// salvage aborts the job of a cancelled run and returns the outcomes of the
// records Salesforce processed before it stopped, so they are not sent again.
// It is best effort: any failure leaves every entry undecided.
func (im *importer) salvage(ctx context.Context, jobID string, b *ingestBatch, log *zap.Logger) []store.Outcome {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), abortTimeout)
	defer cancel()
	im.abort(ctx, jobID, log)
	var jobErr *salesforce.JobError
	if _, err := im.sf.WaitForIngestJob(ctx, jobID); err != nil && !errors.As(err, &jobErr) {
		log.Warn("wait for aborted ingest job", zap.Error(err))
		return nil
	}
	outcomes, _, err := im.collect(ctx, jobID, b)
	if err != nil {
		log.Warn("collect results of aborted ingest job", zap.Error(err))
		return nil
	}
	return outcomes
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

// release returns the run's undecided entries to pending.
func (im *importer) release(ctx context.Context, runID int64, log *zap.Logger) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), statusWriteTimeout)
	defer cancel()
	n, err := im.store.ReleaseOutbox(ctx, runID)
	if err != nil {
		log.Error("release outbox entries", zap.Error(err))
		return
	}
	if n > 0 {
		log.Warn("outbox entries returned to pending", zap.Int64("entries", n))
	}
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
