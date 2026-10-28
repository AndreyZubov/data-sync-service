// Package syncer runs sync jobs: it records each run, limits how many run at
// once, schedules them, and drives the pipelines between Salesforce and the
// store: exports through Bulk API query jobs, and imports of outbox entries
// through Bulk API ingest jobs.
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

// Defaults for importing outbox entries.
const (
	defaultImportMaxRecords  = 10_000
	defaultImportMaxAttempts = 5
)

// Retry delays of failed outbox entries: long enough for a lock or a
// misconfiguration in the org to clear, short enough that a fix takes effect
// within the hour.
const (
	importRetryBaseDelay = time.Minute
	importRetryMaxDelay  = time.Hour
)

// statusWriteTimeout bounds recording a run's final status, which happens
// with the run's own context possibly cancelled.
const statusWriteTimeout = 10 * time.Second

// apiUsageRefreshAfter is how old the last API usage report may be before a
// run refused for high usage asks Salesforce for current figures. Usage only
// drops as old requests leave the rolling 24-hour window, so checking more
// often gains little.
const apiUsageRefreshAfter = 5 * time.Minute

// defaultLeaseTTL is how long a run's lease lasts without renewal. Leases are
// renewed every third of it, so a run survives two missed heartbeats; once it
// lapses, a crashed instance's run is taken over within about one more TTL.
const defaultLeaseTTL = time.Minute

// ErrShuttingDown is returned when a run is requested after Shutdown began.
var ErrShuttingDown = errors.New("service is shutting down")

// APIUsageError is returned when a run is not started because the org has
// used more of its daily API allocation than the configured threshold. The
// allocation is shared with every other integration of the org, so the
// service leaves them the rest.
type APIUsageError struct {
	Usage     salesforce.APIUsage
	Threshold float64
	// RetryAfter is how long until a refused run asks Salesforce for current
	// usage again.
	RetryAfter time.Duration
}

func (e *APIUsageError) Error() string {
	return fmt.Sprintf("the org has used %d of %d daily API requests (%.0f%%), above the %.0f%% threshold",
		e.Usage.Used, e.Usage.Max, 100*e.Usage.Fraction(), 100*e.Threshold)
}

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
	// ImportMaxRecords caps the outbox entries one import run sends; zero
	// means the default of 10000.
	ImportMaxRecords int
	// ImportMaxAttempts is the number of import runs that may try an outbox
	// entry before it is dead-lettered; zero means the default of 5.
	ImportMaxAttempts int
	// APIUsageThreshold is the fraction of the org's daily API allocation
	// above which no new run starts; zero disables the check. Runs already
	// started, and resumed runs, are not stopped.
	APIUsageThreshold float64
	// LeaseTTL is how long a run's lease lasts without renewal; zero means
	// the default of one minute.
	LeaseTTL time.Duration
	Logger   *zap.Logger
	// Registerer receives the sync metrics.
	Registerer prometheus.Registerer
}

// Orchestrator starts and tracks sync runs. Runs execute in the background;
// their progress and outcome are recorded in the store. Every run is leased
// by the orchestrator executing it, so several instances can share one
// database: each renews the leases of its runs and takes over the runs of
// instances that stopped renewing theirs. It is safe for concurrent use.
type Orchestrator struct {
	sf       *salesforce.Client
	store    *store.Store
	exporter *exporter
	importer *importer
	log      *zap.Logger
	metrics  *metrics
	slots    chan struct{}

	usageThreshold    float64
	usageRefreshAfter time.Duration
	leaseTTL          time.Duration

	// ctx is the parent of every run; Shutdown cancels it.
	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	closed   bool
	runs     sync.WaitGroup
	schedule *cron.Cron
	// active cancels each run this instance executes, for a lost lease.
	active  map[int64]context.CancelFunc
	keeping bool
}

// New returns an orchestrator for cfg.
func New(cfg Config) *Orchestrator {
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	maxRecords := cfg.ImportMaxRecords
	if maxRecords <= 0 {
		maxRecords = defaultImportMaxRecords
	}
	maxAttempts := cfg.ImportMaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultImportMaxAttempts
	}
	leaseTTL := cfg.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = defaultLeaseTTL
	}
	m := newMetrics(cfg.Registerer)
	ctx, cancel := context.WithCancel(context.Background())
	return &Orchestrator{
		sf:    cfg.Salesforce,
		store: cfg.Store,
		exporter: &exporter{
			sf:        cfg.Salesforce,
			store:     cfg.Store,
			workers:   cfg.Workers,
			batchSize: batchSize,
			metrics:   m,
		},
		importer: &importer{
			sf:         cfg.Salesforce,
			store:      cfg.Store,
			maxRecords: maxRecords,
			retry: store.RetryPolicy{
				MaxAttempts: maxAttempts,
				BaseDelay:   importRetryBaseDelay,
				MaxDelay:    importRetryMaxDelay,
			},
			metrics: m,
		},
		log:               cfg.Logger,
		metrics:           m,
		slots:             make(chan struct{}, cfg.MaxConcurrentRuns),
		usageThreshold:    cfg.APIUsageThreshold,
		usageRefreshAfter: apiUsageRefreshAfter,
		leaseTTL:          leaseTTL,
		ctx:               ctx,
		cancel:            cancel,
		active:            make(map[int64]context.CancelFunc),
	}
}

// RecoverInterruptedRuns settles the runs whose lease has lapsed, which would
// otherwise block new runs of their objects forever, and keeps doing so in
// the background until Shutdown, while renewing the leases of this
// instance's runs. Lapsed runs that already had a Salesforce job are resumed
// here, unless they have been resumed store.MaxResumes times already; the
// others are failed and their outbox entries return to pending. Call it once
// at startup, before starting any run.
//
// A run suspended by a shutdown holds no lease and is resumed at once; the
// runs of a crashed instance are resumed once their lease has lapsed.
func (o *Orchestrator) RecoverInterruptedRuns(ctx context.Context) error {
	if err := o.recoverRuns(ctx); err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return ErrShuttingDown
	}
	if !o.keeping {
		o.keeping = true
		o.runs.Add(1)
		go o.keepLeases()
	}
	return nil
}

// recoverRuns makes one pass of RecoverInterruptedRuns.
func (o *Orchestrator) recoverRuns(ctx context.Context) error {
	failed, resumed, err := o.store.RecoverRuns(ctx, store.MaxResumes, o.leaseTTL)
	if err != nil {
		return err
	}
	if failed > 0 {
		o.log.Warn("failed runs interrupted by a restart", zap.Int64("runs", failed))
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	for _, run := range resumed {
		if o.closed {
			// Give the run back for another instance, or the next start.
			o.release(ctx, run)
			continue
		}
		o.log.Info("resuming run interrupted by a restart", zap.Int64("run_id", run.ID),
			zap.String("object", run.Object), zap.String("direction", string(run.Direction)),
			zap.String("bulk_job_id", run.BulkJobID), zap.Int("resumes", run.Resumes))
		o.metrics.resumed.WithLabelValues(run.Object, string(run.Direction)).Inc()
		o.launch(run, o.workFor(run.Direction)) //nolint:contextcheck // runs outlive the caller
	}
	return nil
}

// keepLeases renews this instance's run leases and takes over lapsed runs,
// every third of the lease TTL, until Shutdown.
func (o *Orchestrator) keepLeases() {
	defer o.runs.Done()
	ticker := time.NewTicker(o.leaseTTL / 3)
	defer ticker.Stop()
	for {
		select {
		case <-o.ctx.Done():
			return
		case <-ticker.C:
		}
		o.renewLeases()
		if err := o.recoverRuns(o.ctx); err != nil && o.ctx.Err() == nil {
			o.log.Error("recover interrupted runs", zap.Error(err))
		}
	}
}

// renewLeases renews the leases of this instance's runs and stops the runs
// whose lease another instance has taken over. If the renewal fails, runs go
// on: the store refuses their writes should their lease lapse meanwhile.
func (o *Orchestrator) renewLeases() {
	owned, err := o.store.Heartbeat(o.ctx)
	if err != nil {
		if o.ctx.Err() == nil {
			o.log.Error("renew run leases", zap.Error(err))
		}
		return
	}
	held := make(map[int64]bool, len(owned))
	for _, id := range owned {
		held[id] = true
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for id, cancel := range o.active {
		if !held[id] {
			o.log.Warn("run lease lost; stopping the run", zap.Int64("run_id", id))
			cancel()
		}
	}
}

// release gives up the lease of a run this instance will not continue. ctx
// may be cancelled by now, so the write only inherits its values.
func (o *Orchestrator) release(ctx context.Context, run store.Run) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), statusWriteTimeout)
	defer cancel()
	if err := o.store.ReleaseRun(ctx, run.ID); err != nil {
		o.log.Warn("release run lease", zap.Int64("run_id", run.ID), zap.Error(err))
	}
}

// launch executes run in the background. The caller must hold o.mu.
func (o *Orchestrator) launch(run store.Run, w work) {
	ctx, cancel := context.WithCancel(o.ctx)
	o.active[run.ID] = cancel
	o.runs.Add(1)
	go func() {
		defer func() {
			cancel()
			o.mu.Lock()
			delete(o.active, run.ID)
			o.mu.Unlock()
		}()
		o.execute(ctx, run, w)
	}()
}

// StartExport records a queued export run of object and executes it in the
// background. Unless full is set, the run reads only records changed since
// the object's watermark. It returns a *store.RunInProgressError if the object
// is already being exported, and ErrShuttingDown after Shutdown.
func (o *Orchestrator) StartExport(ctx context.Context, object string, trigger store.Trigger, full bool) (store.Run, error) {
	return o.start(ctx, object, store.Export, trigger, full, o.runExport)
}

// StartImport records a queued import run of object and executes it in the
// background. The run sends the object's due outbox entries to Salesforce. It
// returns a *store.RunInProgressError if the object is already being
// imported, and ErrShuttingDown after Shutdown.
func (o *Orchestrator) StartImport(ctx context.Context, object string, trigger store.Trigger) (store.Run, error) {
	return o.start(ctx, object, store.Import, trigger, false, o.runImport)
}

// work is the body of a run, including recording its success.
type work func(ctx context.Context, run store.Run, log *zap.Logger) error

func (o *Orchestrator) workFor(dir store.Direction) work {
	if dir == store.Import {
		return o.runImport
	}
	return o.runExport
}

func (o *Orchestrator) start(
	ctx context.Context, object string, dir store.Direction, trigger store.Trigger, full bool, w work,
) (store.Run, error) {
	if err := o.checkAPIUsage(ctx); err != nil {
		return store.Run{}, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return store.Run{}, ErrShuttingDown
	}
	run, err := o.store.CreateRun(ctx, object, dir, trigger, full)
	if err != nil {
		return store.Run{}, err
	}
	// The run outlives the request that started it, so it runs under the
	// orchestrator's context, which Shutdown cancels, rather than ctx.
	o.launch(run, w) //nolint:contextcheck // deliberately not derived from ctx
	return run, nil
}

// checkAPIUsage returns an *APIUsageError if the org's API usage is above
// the threshold. A report older than usageRefreshAfter is refreshed first,
// since usage falls as requests age out of the 24-hour window; without any
// report, or if refreshing fails, the run may start.
func (o *Orchestrator) checkAPIUsage(ctx context.Context) error {
	if o.usageThreshold <= 0 {
		return nil
	}
	usage, ok := o.sf.APIUsage()
	if !ok || usage.Fraction() < o.usageThreshold {
		return nil
	}
	if time.Since(usage.ObservedAt) >= o.usageRefreshAfter {
		fresh, err := o.sf.RefreshAPIUsage(ctx)
		if err != nil {
			o.log.Warn("refresh org API usage", zap.Error(err))
			return nil
		}
		if usage = fresh; usage.Fraction() < o.usageThreshold {
			return nil
		}
	}
	return &APIUsageError{Usage: usage, Threshold: o.usageThreshold, RetryAfter: o.usageRefreshAfter}
}

// runImport sends the run's outbox entries and records the run's success.
func (o *Orchestrator) runImport(ctx context.Context, run store.Run, log *zap.Logger) error {
	if err := o.importer.importRun(ctx, run, log); err != nil {
		return err
	}
	return o.store.CompleteRun(ctx, run.ID)
}

// runExport exports the run's object and advances its watermark together
// with recording the run's success.
func (o *Orchestrator) runExport(ctx context.Context, run store.Run, log *zap.Logger) error {
	watermark, err := o.exporter.export(ctx, run, log)
	if err != nil {
		return err
	}
	log.Info("export loaded", zap.Timep("watermark", watermark))
	return o.store.CompleteExport(ctx, run, watermark)
}

// GetRun returns the run with the given ID, or store.ErrNotFound.
func (o *Orchestrator) GetRun(ctx context.Context, id int64) (store.Run, error) {
	return o.store.GetRun(ctx, id)
}

// Schedule runs each object's sync on its cron schedule, in the local time
// zone: an export, and an import whenever the object has due outbox entries.
// A tick that finds a run of the same object and direction still active
// skips it.
func (o *Orchestrator) Schedule(schedules []config.ObjectSchedule) error {
	c := cron.New()
	for _, s := range schedules {
		object := s.Name
		if _, err := c.AddFunc(s.Cron, func() { o.scheduledSync(object) }); err != nil {
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

func (o *Orchestrator) scheduledSync(object string) {
	o.logScheduled(object, store.Export)(o.StartExport(o.ctx, object, store.Schedule, false))

	due, err := o.store.HasDueOutbox(o.ctx, object)
	if err != nil {
		o.log.Error("scheduled import not started", zap.String("object", object), zap.Error(err))
		return
	}
	if due {
		o.logScheduled(object, store.Import)(o.StartImport(o.ctx, object, store.Schedule))
	}
}

// logScheduled returns a function that logs the result of starting a
// scheduled run.
func (o *Orchestrator) logScheduled(object string, dir store.Direction) func(store.Run, error) {
	return func(run store.Run, err error) {
		log := o.log.With(zap.String("object", object), zap.String("direction", string(dir)))
		var inProgress *store.RunInProgressError
		var usage *APIUsageError
		switch {
		case err == nil:
			log.Info("scheduled run started", zap.Int64("run_id", run.ID))
		case errors.As(err, &inProgress):
			log.Info("scheduled run skipped: previous run still active", zap.Int64("active_run_id", inProgress.RunID))
		case errors.As(err, &usage):
			log.Warn("scheduled run skipped: org API usage is high", zap.Error(err))
		case errors.Is(err, ErrShuttingDown):
		default:
			log.Error("scheduled run not started", zap.Error(err))
		}
	}
}

// Shutdown stops the schedule, refuses new runs, and stops the active ones.
// It waits until they have stopped or ctx ends.
//
// A run that already has a Salesforce job is suspended: the job keeps
// running, and the run stays active in the store without a lease, to be
// resumed by RecoverInterruptedRuns at the next start or by another instance. Other runs are cancelled, which
// is safe to repeat: records were upserted idempotently, the watermark was
// not advanced, and outbox entries return to pending.
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

// execute waits for a concurrency slot, runs w, and records the outcome.
// Cancelling ctx cancels the run.
func (o *Orchestrator) execute(ctx context.Context, run store.Run, w work) {
	defer o.runs.Done()
	log := o.log.With(zap.Int64("run_id", run.ID), zap.String("object", run.Object),
		zap.String("direction", string(run.Direction)), zap.String("trigger", string(run.Trigger)))

	select {
	case o.slots <- struct{}{}:
		defer func() { <-o.slots }()
	case <-ctx.Done():
		o.stop(ctx, run, ctx.Err(), log)
		return
	}

	if run.Status == store.Queued {
		if err := o.store.StartRun(ctx, run.ID); err != nil {
			o.finish(ctx, run, store.Failed, err, log)
			return
		}
		run.Status = store.Running
	}
	log.Info("run started", zap.Bool("full_sync", run.FullSync), zap.Int("resumes", run.Resumes))
	o.metrics.active.Inc()
	defer o.metrics.active.Dec()
	start := time.Now()

	err := w(ctx, run, log)
	o.metrics.duration.WithLabelValues(run.Object, string(run.Direction)).Observe(time.Since(start).Seconds())

	switch {
	case err == nil:
		o.metrics.runs.WithLabelValues(run.Object, string(run.Direction), string(store.Succeeded)).Inc()
		log.Info("run succeeded", zap.Duration("duration", time.Since(start)))
	case ctx.Err() != nil:
		o.stop(ctx, run, err, log)
	default:
		o.finish(ctx, run, store.Failed, err, log)
	}
}

// stop ends a run whose context was cancelled, by Shutdown or by a lost
// lease. A run another instance has taken over is left to it. A run with a
// Salesforce job is suspended rather than cancelled, so it can be resumed.
func (o *Orchestrator) stop(ctx context.Context, run store.Run, cause error, log *zap.Logger) {
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), statusWriteTimeout)
	defer cancel()
	current, err := o.store.GetRun(readCtx, run.ID)
	if err != nil {
		log.Error("read run before stopping it", zap.Error(err))
		return
	}
	switch {
	case current.Owner != o.store.Instance():
		log.Warn("run stopped: another instance holds its lease", zap.String("owner", current.Owner))
	case current.BulkJobID == "":
		o.finish(ctx, run, store.Cancelled, cause, log)
	default:
		o.release(ctx, run)
		log.Warn("run suspended; it resumes when the service starts again",
			zap.String("bulk_job_id", current.BulkJobID), zap.NamedError("cause", cause))
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
