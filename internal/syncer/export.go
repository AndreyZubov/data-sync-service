package syncer

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/AndreyZubov/data-sync-service/internal/salesforce"
	"github.com/AndreyZubov/data-sync-service/internal/store"
)

// abortTimeout bounds the best-effort abort of a query job whose run was
// cancelled.
const abortTimeout = 10 * time.Second

// exporter copies one object from Salesforce into the store.
type exporter struct {
	sf        *salesforce.Client
	store     *store.Store
	workers   int
	batchSize int
	metrics   *metrics
}

// export runs a query job for run's object and upserts its results. It
// returns the highest SystemModstamp exported, which becomes the object's
// watermark once the run is recorded as succeeded; nil means the watermark
// stays as it is.
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

	var since *time.Time
	if incremental && !run.FullSync {
		if since, err = e.store.Watermark(ctx, run.Object); err != nil {
			return nil, err
		}
	}
	op := salesforce.Query
	if desc.HasField(fieldIsDeleted) {
		op = salesforce.QueryAll
	}

	job, err := e.sf.CreateQueryJob(ctx, op, buildQuery(run.Object, fields, since))
	if err != nil {
		return nil, err
	}
	log = log.With(zap.String("bulk_job_id", job.ID))
	log.Info("query job created", zap.String("operation", string(op)),
		zap.Int("fields", len(fields)), zap.Timep("since", since))
	if err := e.store.SetBulkJob(ctx, run.ID, job.ID); err != nil {
		return nil, err
	}

	done, err := e.sf.WaitForQueryJob(ctx, job.ID)
	if err != nil {
		if ctx.Err() != nil {
			e.abort(ctx, job.ID, log)
		}
		return nil, err
	}
	log.Info("query job complete", zap.Int64("records", done.NumberRecordsProcessed))

	watermark, err := e.load(ctx, run, job.ID, fields)
	if err != nil || !incremental {
		return nil, err
	}
	return watermark, nil
}

// load streams the job's results into the store. One goroutine parses the CSV
// into batches and a pool of workers upserts them; the bounded channel between
// them applies backpressure, so a slow database slows the download instead of
// buffering the result in memory.
func (e *exporter) load(ctx context.Context, run store.Run, jobID string, fields []salesforce.Field) (*time.Time, error) {
	g, gctx := errgroup.WithContext(ctx)
	batches := make(chan []store.Record, e.workers)
	var watermark *time.Time

	g.Go(func() error {
		defer close(batches)
		batch := make([]store.Record, 0, e.batchSize)
		send := func() error {
			select {
			case batches <- batch:
				batch = make([]store.Record, 0, e.batchSize)
				return nil
			case <-gctx.Done():
				return gctx.Err()
			}
		}

		err := e.sf.StreamQueryResults(gctx, jobID, func(body io.Reader) error {
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
				if ts := rec.SystemModstamp; ts != nil && (watermark == nil || ts.After(*watermark)) {
					watermark = ts
				}
				batch = append(batch, rec)
				if len(batch) == e.batchSize {
					if err := send(); err != nil {
						return err
					}
				}
			}
		})
		if err != nil {
			return err
		}
		if len(batch) > 0 {
			return send()
		}
		return nil
	})

	for range e.workers {
		g.Go(func() error {
			for batch := range batches {
				if err := e.store.UpsertRecords(gctx, run.ID, run.Object, batch); err != nil {
					return err
				}
				e.metrics.records.WithLabelValues(run.Object, string(store.Export)).Add(float64(len(batch)))
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return watermark, nil
}

// abort stops a query job whose run was cancelled, so it does not keep using
// org resources. Failure is only logged: the job expires on its own.
func (e *exporter) abort(ctx context.Context, jobID string, log *zap.Logger) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), abortTimeout)
	defer cancel()
	if err := e.sf.AbortQueryJob(ctx, jobID); err != nil {
		log.Warn("abort query job", zap.Error(err))
		return
	}
	log.Info("query job aborted")
}
