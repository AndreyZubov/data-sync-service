// Package syncer runs sync jobs: it records each run, limits how many run at
// once, schedules them, and drives the export pipeline from a Salesforce Bulk
// API query job into the store.
package syncer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"

	"github.com/AndreyZubov/data-sync-service/internal/config"
	"github.com/AndreyZubov/data-sync-service/internal/salesforce"
	"github.com/AndreyZubov/data-sync-service/internal/store"
)

// defaultBatchSize is the number of records upserted per transaction: large
// enough to amortize the per-transaction overhead of COPY, small enough to
// keep a batch's memory and lock time modest.
const defaultBatchSize = 2000

// statusWriteTimeout bounds recording a run's final status, which happens
// with the run's own context possibly cancelled.
const statusWriteTimeout = 10 * time.Second

// ErrShuttingDown is returned when a run is requested after Shutdown began.
var ErrShuttingDown = errors.New("service is shutting down")

// Config configures an Orchestrator.
type Config struct {
	Salesforce *salesforce.Client
	Store      *store.Store
	// Workers is the number of goroutines upserting records within one run.
	Workers int
	// MaxConcurrentRuns caps how many runs execute at once; further runs wait
	// in the queued state.
	MaxConcurrentRuns int
	// BatchSize is the number of records per upsert transaction; zero means
	// the default of 2000.
	BatchSize int
	Logger    *zap.Logger
	// Registerer receives the sync metrics.
	Registerer prometheus.Registerer
}

// Orchestrator starts and tracks sync runs. Runs execute in the background;
// their progress and outcome are recorded in the store. It is safe for
// concurrent use.
type Orchestrator struct {
	store    *store.Store
	exporter *exporter
	log      *zap.Logger
	metrics  *metrics
	slots    chan struct{}

	// ctx is the parent of every run; Shutdown cancels it.
	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	closed   bool
	runs     sync.WaitGroup
	schedule *cron.Cron
}

// New returns an orchestrator for cfg.
func New(cfg Config) *Orchestrator {
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	m := newMetrics(cfg.Registerer)
	ctx, cancel := context.WithCancel(context.Background())
	return &Orchestrator{
		store: cfg.Store,
		exporter: &exporter{
			sf:        cfg.Salesforce,
			store:     cfg.Store,
			workers:   cfg.Workers,
			batchSize: batchSize,
			metrics:   m,
		},
		log:     cfg.Logger,
		metrics: m,
		slots:   make(chan struct{}, cfg.MaxConcurrentRuns),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// RecoverInterruptedRuns fails runs left active by a previous process, which
// would otherwise block new runs of their objects forever. Call it once at
// startup, before starting any run.
func (o *Orchestrator) RecoverInterruptedRuns(ctx context.Context) error {
	n, err := o.store.FailInterruptedRuns(ctx)
	if err != nil {
		return err
	}
	if n > 0 {
		o.log.Warn("failed runs interrupted by a restart", zap.Int64("runs", n))
	}
	return nil
}

// StartExport records a queued export run of object and executes it in the
// background. Unless full is set, the run reads only records changed since
// the object's watermark. It returns a *store.RunInProgressError if the object
// is already being exported, and ErrShuttingDown after Shutdown.
func (o *Orchestrator) StartExport(ctx context.Context, object string, trigger store.Trigger, full bool) (store.Run, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return store.Run{}, ErrShuttingDown
	}
	run, err := o.store.CreateRun(ctx, object, store.Export, trigger, full)
	if err != nil {
		return store.Run{}, err
	}
	o.runs.Add(1)
	// The run outlives the request that started it, so it runs under the
	// orchestrator's context, which Shutdown cancels, rather than ctx.
	go o.execute(o.ctx, run) //nolint:contextcheck // deliberately not derived from ctx
	return run, nil
}

// GetRun returns the run with the given ID, or store.ErrNotFound.
func (o *Orchestrator) GetRun(ctx context.Context, id int64) (store.Run, error) {
	return o.store.GetRun(ctx, id)
}

// Schedule starts exporting each object on its cron schedule, in the local
// time zone. A tick that finds the object still being exported is skipped.
func (o *Orchestrator) Schedule(schedules []config.ObjectSchedule) error {
	c := cron.New()
	for _, s := range schedules {
		object := s.Name
		if _, err := c.AddFunc(s.Cron, func() { o.scheduledExport(object) }); err != nil {
			return fmt.Errorf("schedule %s: %w", object, err)
		}
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return ErrShuttingDown
	}
	o.schedule = c
	c.Start()
	return nil
}

func (o *Orchestrator) scheduledExport(object string) {
	run, err := o.StartExport(o.ctx, object, store.Schedule, false)
	var inProgress *store.RunInProgressError
	switch {
	case err == nil:
		o.log.Info("scheduled export started", zap.String("object", object), zap.Int64("run_id", run.ID))
	case errors.As(err, &inProgress):
		o.log.Info("scheduled export skipped: previous run still active",
			zap.String("object", object), zap.Int64("active_run_id", inProgress.RunID))
	case errors.Is(err, ErrShuttingDown):
	default:
		o.log.Error("scheduled export not started", zap.String("object", object), zap.Error(err))
	}
}

// Shutdown stops the schedule, refuses new runs, and cancels the active ones.
// It waits until they have recorded their outcome or ctx ends. Cancelled
// exports are safe to repeat: records were upserted idempotently and the
// watermark was not advanced.
func (o *Orchestrator) Shutdown(ctx context.Context) error {
	o.mu.Lock()
	o.closed = true
	if o.schedule != nil {
		o.schedule.Stop()
	}
	o.mu.Unlock()

	o.cancel()
	done := make(chan struct{})
	go func() {
		o.runs.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for active runs: %w", ctx.Err())
	}
}

// execute waits for a concurrency slot, runs the export, and records the
// outcome. Cancelling ctx cancels the run.
func (o *Orchestrator) execute(ctx context.Context, run store.Run) {
	defer o.runs.Done()
	log := o.log.With(zap.Int64("run_id", run.ID), zap.String("object", run.Object),
		zap.String("direction", string(run.Direction)), zap.String("trigger", string(run.Trigger)))

	select {
	case o.slots <- struct{}{}:
		defer func() { <-o.slots }()
	case <-ctx.Done():
		o.finish(ctx, run, store.Cancelled, ctx.Err(), log)
		return
	}

	if err := o.store.StartRun(ctx, run.ID); err != nil {
		o.finish(ctx, run, store.Failed, err, log)
		return
	}
	run.Status = store.Running
	log.Info("run started", zap.Bool("full_sync", run.FullSync))
	o.metrics.active.Inc()
	defer o.metrics.active.Dec()
	start := time.Now()

	watermark, err := o.exporter.export(ctx, run, log)
	if err == nil {
		err = o.store.CompleteExport(ctx, run, watermark)
	}
	o.metrics.duration.WithLabelValues(run.Object, string(run.Direction)).Observe(time.Since(start).Seconds())

	switch {
	case err == nil:
		o.metrics.runs.WithLabelValues(run.Object, string(run.Direction), string(store.Succeeded)).Inc()
		log.Info("run succeeded", zap.Duration("duration", time.Since(start)), zap.Timep("watermark", watermark))
	case ctx.Err() != nil:
		o.finish(ctx, run, store.Cancelled, err, log)
	default:
		o.finish(ctx, run, store.Failed, err, log)
	}
}

// finish records a run's unsuccessful outcome. ctx may be cancelled by now,
// so the write only inherits its values.
func (o *Orchestrator) finish(ctx context.Context, run store.Run, status store.RunStatus, cause error, log *zap.Logger) {
	o.metrics.runs.WithLabelValues(run.Object, string(run.Direction), string(status)).Inc()
	if status == store.Cancelled {
		log.Warn("run cancelled", zap.Error(cause))
	} else {
		log.Error("run failed", zap.Error(cause))
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), statusWriteTimeout)
	defer cancel()
	if err := o.store.FinishRun(ctx, run.ID, status, cause); err != nil {
		log.Error("record run outcome", zap.Error(err))
	}
}
