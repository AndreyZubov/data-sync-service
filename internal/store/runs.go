package store

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Direction is the way a run moves data.
type Direction string

// Run directions.
const (
	Export Direction = "export"
	Import Direction = "import"
)

// Trigger records what started a run.
type Trigger string

// Run triggers.
const (
	Manual   Trigger = "manual"
	Schedule Trigger = "schedule"
)

// RunStatus is a run's lifecycle state. A run is created queued, becomes
// running once it holds a concurrency slot, and ends in one of the terminal
// states.
type RunStatus string

// Run statuses.
const (
	Queued    RunStatus = "queued"
	Running   RunStatus = "running"
	Succeeded RunStatus = "succeeded"
	Failed    RunStatus = "failed"
	Cancelled RunStatus = "cancelled"
)

// maxErrorLength bounds stored failure messages; Salesforce and Postgres
// errors are short, but a wrapped chain should not bloat the tables.
const maxErrorLength = 2000

// Run is one sync of one object in one direction.
type Run struct {
	ID        int64
	Object    string
	Direction Direction
	Trigger   Trigger
	Status    RunStatus
	// FullSync is set when the run ignores the watermark and reads every record.
	FullSync bool
	// BulkJobID is the Salesforce Bulk API job, once one has been created.
	BulkJobID     string
	RecordsOK     int64
	RecordsFailed int64
	// Error describes why the run failed or was cancelled.
	Error      string
	CreatedAt  time.Time
	StartedAt  *time.Time
	FinishedAt *time.Time
}

// RunInProgressError is returned by CreateRun when the object already has a
// queued or running run in the same direction.
type RunInProgressError struct {
	// RunID identifies the active run.
	RunID int64
}

func (e *RunInProgressError) Error() string {
	return fmt.Sprintf("run %d is already in progress", e.RunID)
}

const runColumns = `id, object_name, direction, trigger, status, full_sync, coalesce(bulk_job_id, ''),
	records_ok, records_failed, coalesce(error, ''), created_at, started_at, finished_at`

// CreateRun records a new queued run. If the object already has an active run
// in the same direction it returns a *RunInProgressError.
func (s *Store) CreateRun(ctx context.Context, object string, dir Direction, trigger Trigger, fullSync bool) (Run, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO sync_runs (object_name, direction, trigger, status, full_sync)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING `+runColumns,
		object, dir, trigger, Queued, fullSync)
	run, err := scanRun(row)
	if err == nil {
		return run, nil
	}

	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.ConstraintName != "sync_runs_one_active" {
		return Run{}, fmt.Errorf("create run: %w", err)
	}
	var activeID int64
	err = s.pool.QueryRow(ctx, `
		SELECT id FROM sync_runs
		WHERE object_name = $1 AND direction = $2 AND status IN ('queued', 'running')`,
		object, dir).Scan(&activeID)
	if err != nil {
		// The active run finished in the meantime; the caller may simply retry.
		return Run{}, fmt.Errorf("find active run: %w", err)
	}
	return Run{}, &RunInProgressError{RunID: activeID}
}

// GetRun returns the run with the given ID, or ErrNotFound.
func (s *Store) GetRun(ctx context.Context, id int64) (Run, error) {
	run, err := scanRun(s.pool.QueryRow(ctx, `SELECT `+runColumns+` FROM sync_runs WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, ErrNotFound
	}
	if err != nil {
		return Run{}, fmt.Errorf("get run %d: %w", id, err)
	}
	return run, nil
}

// StartRun moves a queued run to running.
func (s *Store) StartRun(ctx context.Context, id int64) error {
	return s.execOne(ctx, "start run", id, `
		UPDATE sync_runs SET status = 'running', started_at = now()
		WHERE id = $1 AND status = 'queued'`, id)
}

// SetBulkJob records the Salesforce Bulk API job that serves a run.
func (s *Store) SetBulkJob(ctx context.Context, id int64, jobID string) error {
	return s.execOne(ctx, "set bulk job", id, `UPDATE sync_runs SET bulk_job_id = $2 WHERE id = $1`, id, jobID)
}

// FinishRun moves an active run to a terminal status. cause is stored as the
// run's error and may be nil.
func (s *Store) FinishRun(ctx context.Context, id int64, status RunStatus, cause error) error {
	var msg *string
	if cause != nil {
		m := truncateError(cause.Error())
		msg = &m
	}
	return s.execOne(ctx, "finish run", id, `
		UPDATE sync_runs SET status = $2, error = $3, finished_at = now()
		WHERE id = $1 AND status IN ('queued', 'running')`, id, status, msg)
}

// CompleteRun marks a running run succeeded.
func (s *Store) CompleteRun(ctx context.Context, id int64) error {
	return s.execOne(ctx, "complete run", id, `
		UPDATE sync_runs SET status = 'succeeded', finished_at = now()
		WHERE id = $1 AND status = 'running'`, id)
}

// CompleteExport marks an export run succeeded and, when watermark is not nil,
// advances the object's watermark to it, in one transaction: the watermark
// never covers records whose run did not succeed. The watermark only moves
// forward, so a full resync cannot rewind it.
func (s *Store) CompleteExport(ctx context.Context, run Run, watermark *time.Time) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE sync_runs SET status = 'succeeded', finished_at = now()
			WHERE id = $1 AND status = 'running'`, run.ID)
		if err != nil {
			return fmt.Errorf("complete run %d: %w", run.ID, err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("complete run %d: run is not running", run.ID)
		}
		if watermark == nil {
			return nil
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO sync_state (object_name, last_watermark, last_run_id)
			VALUES ($1, $2, $3)
			ON CONFLICT (object_name) DO UPDATE SET
				last_watermark = GREATEST(sync_state.last_watermark, EXCLUDED.last_watermark),
				last_run_id    = EXCLUDED.last_run_id,
				updated_at     = now()`,
			run.Object, *watermark, run.ID)
		if err != nil {
			return fmt.Errorf("advance watermark of %s: %w", run.Object, err)
		}
		return nil
	})
}

// FailInterruptedRuns marks every queued or running run as failed and returns
// the outbox entries they were sending to pending. It is meant for startup,
// when no run can still be in progress, and returns the number of runs it
// closed. It assumes a single service instance per database.
func (s *Store) FailInterruptedRuns(ctx context.Context) (int64, error) {
	var n int64
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE sync_runs SET status = 'failed', error = 'interrupted by a service restart', finished_at = now()
			WHERE status IN ('queued', 'running')`)
		if err != nil {
			return fmt.Errorf("fail interrupted runs: %w", err)
		}
		n = tag.RowsAffected()
		if _, err := tx.Exec(ctx, releaseOutboxSQL); err != nil {
			return fmt.Errorf("release outbox entries of interrupted runs: %w", err)
		}
		return nil
	})
	return n, err
}

// Watermark returns the object's watermark, or nil if it has none yet.
func (s *Store) Watermark(ctx context.Context, object string) (*time.Time, error) {
	var watermark *time.Time
	err := s.pool.QueryRow(ctx, `SELECT last_watermark FROM sync_state WHERE object_name = $1`, object).Scan(&watermark)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get watermark of %s: %w", object, err)
	}
	return watermark, nil
}

// truncateError bounds an error message to maxErrorLength bytes without
// splitting a UTF-8 sequence.
func truncateError(msg string) string {
	if len(msg) <= maxErrorLength {
		return msg
	}
	cut := maxErrorLength
	for cut > 0 && !utf8.RuneStart(msg[cut]) {
		cut--
	}
	return msg[:cut]
}

// execOne runs a statement that must change exactly the run with the given ID.
func (s *Store) execOne(ctx context.Context, action string, id int64, sql string, args ...any) error {
	tag, err := s.pool.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("%s %d: %w", action, id, err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%s %d: run not found or not in the expected state", action, id)
	}
	return nil
}

func scanRun(row pgx.Row) (Run, error) {
	var r Run
	err := row.Scan(&r.ID, &r.Object, &r.Direction, &r.Trigger, &r.Status, &r.FullSync, &r.BulkJobID,
		&r.RecordsOK, &r.RecordsFailed, &r.Error, &r.CreatedAt, &r.StartedAt, &r.FinishedAt)
	return r, err
}
