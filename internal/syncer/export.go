package syncer

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/AndreyZubov/data-sync-service/internal/salesforce"
	"github.com/AndreyZubov/data-sync-service/internal/store"
)

// exporter copies one object from Salesforce into the store.
type exporter struct {
	sf        *salesforce.Client
	store     *store.Store
	workers   int
	batchSize int
	metrics   *metrics
}

// export runs a query job for run's object and upserts its results. A
// resumed run continues the job it already has from its checkpoint, unless
// Salesforce no longer has the job. It returns the highest SystemModstamp
// exported, which becomes the object's watermark once the run is recorded as
// succeeded; nil means the watermark stays as it is.
//
// If ctx is cancelled the query job is left running, so that the run can be
// resumed when the service starts again.
func (e *exporter) export(ctx context.Context, run store.Run, log *zap.Logger) (*time.Time, error) {
	desc, err := e.sf.Describe(ctx, run.Object)
	if err != nil {
		return nil, err
	}
	fields, err := exportFields(desc)
	if err != nil {
		return nil, err
	}
	incremental := desc.HasField(fieldSystemModstamp)

	jobID, cp := "", store.ExportCheckpoint{}
	if run.BulkJobID != "" {
		ok, err := e.jobExists(ctx, run.BulkJobID)
		switch {
		case err != nil:
			return nil, err
		case ok:
			jobID, cp = run.BulkJobID, run.Checkpoint
			log.Info("resuming query job", zap.String("bulk_job_id", jobID),
				zap.String("locator", cp.Locator), zap.Int64("records_loaded", cp.Records))
		default:
			log.Warn("query job of the interrupted run no longer exists; starting over",
				zap.String("bulk_job_id", run.BulkJobID))
		}
	}
	if jobID == "" {
		if jobID, err = e.startJob(ctx, run, desc, fields, incremental, log); err != nil {
			return nil, err
		}
	}
	log = log.With(zap.String("bulk_job_id", jobID))

	done, err := e.sf.WaitForQueryJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	log.Info("query job complete", zap.Int64("records", done.NumberRecordsProcessed))

	watermark, err := e.load(ctx, run, jobID, fields, cp)
	if err != nil || !incremental {
		return nil, err
	}
	return watermark, nil
}

// startJob creates a query job for run and records it as the run's job.
func (e *exporter) startJob(
	ctx context.Context, run store.Run, desc *salesforce.ObjectDescription, fields []salesforce.Field,
	incremental bool, log *zap.Logger,
) (string, error) {
	var since *time.Time
	if incremental && !run.FullSync {
		var err error
		if since, err = e.store.Watermark(ctx, run.Object); err != nil {
			return "", err
		}
	}
	op := salesforce.Query
	if desc.HasField(fieldIsDeleted) {
		op = salesforce.QueryAll
	}

	job, err := e.sf.CreateQueryJob(ctx, op, buildQuery(run.Object, fields, since))
	if err != nil {
		return "", err
	}
	log.Info("query job created", zap.String("bulk_job_id", job.ID), zap.String("operation", string(op)),
		zap.Int("fields", len(fields)), zap.Timep("since", since))
	if err := e.store.StartExportJob(ctx, run.ID, job.ID); err != nil {
		return "", err
	}
	return job.ID, nil
}

// jobExists reports whether Salesforce still has the query job; it deletes
// jobs some days after they finish.
func (e *exporter) jobExists(ctx context.Context, id string) (bool, error) {
	_, err := e.sf.GetQueryJob(ctx, id)
	var apiErr *salesforce.APIError
	if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
		return false, nil
	}
	return err == nil, err
}

// pageBatch is a batch of records from one result page.
type pageBatch struct {
	records []store.Record
	page    *pageProgress
}

// load streams the job's results into the store, starting at the page cp
// names, and saves a checkpoint after every page it has fully committed. It
// returns the highest SystemModstamp of the run's records, cp's included.
//
// One goroutine parses the CSV into batches and a pool of workers upserts
// them; the bounded channel between them applies backpressure, so a slow
// database slows the download instead of buffering the result in memory. A
// batch never spans two pages, so each page's progress can be tracked.
func (e *exporter) load(
	ctx context.Context, run store.Run, jobID string, fields []salesforce.Field, cp store.ExportCheckpoint,
) (*time.Time, error) {
	g, gctx := errgroup.WithContext(ctx)
	batches := make(chan pageBatch, e.workers)
	progress := newCheckpointer(cp, func(cp store.ExportCheckpoint) error {
		return e.store.SaveCheckpoint(gctx, run.ID, cp)
	})

	g.Go(func() error {
		defer close(batches)
		return e.sf.StreamQueryResults(gctx, jobID, cp.Locator, func(body io.Reader, next string) error {
			page := progress.open()
			var (
				batch     []store.Record
				records   int64
				watermark *time.Time
			)
			send := func() error {
				if len(batch) == 0 {
					return nil
				}
				progress.sent(page)
				select {
				case batches <- pageBatch{records: batch, page: page}:
					batch = nil
					return nil
				case <-gctx.Done():
					return gctx.Err()
				}
			}

			err := e.parsePage(body, fields, func(rec store.Record) error {
				records++
				watermark = later(watermark, rec.SystemModstamp)
				batch = append(batch, rec)
				if len(batch) == e.batchSize {
					return send()
				}
				return nil
			})
			if err != nil {
				return err
			}
			if err := send(); err != nil {
				return err
			}
			return progress.close(page, next, records, watermark)
		})
	})

	for range e.workers {
		g.Go(func() error {
			for b := range batches {
				if err := e.store.UpsertRecords(gctx, run.ID, run.Object, b.records); err != nil {
					return err
				}
				e.metrics.records.WithLabelValues(run.Object, string(store.Export)).Add(float64(len(b.records)))
				if err := progress.written(b.page); err != nil {
					return err
				}
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return progress.watermark(), nil
}

// parsePage decodes one CSV result page and passes each record to emit.
func (e *exporter) parsePage(body io.Reader, fields []salesforce.Field, emit func(store.Record) error) error {
	r := csv.NewReader(body)
	r.ReuseRecord = true
	header, err := r.Read()
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read result header: %w", err)
	}
	layout, err := newPageLayout(header, fields)
	if err != nil {
		return err
	}
	for {
		row, err := r.Read()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read result row: %w", err)
		}
		rec, err := layout.decode(row)
		if err != nil {
			return err
		}
		if err := emit(rec); err != nil {
			return err
		}
	}
}

// later returns the later of two optional times.
func later(a, b *time.Time) *time.Time {
	if a == nil || (b != nil && b.After(*a)) {
		return b
	}
	return a
}
