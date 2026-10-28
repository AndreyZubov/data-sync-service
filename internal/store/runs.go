package store

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
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
	// BulkJobID is the Salesforce Bulk API job, once one has been created;
	// for an import, the latest of its jobs.
	BulkJobID     string
	RecordsOK     int64
	RecordsFailed int64
	// Error describes why the run failed or was cancelled.
	Error string
	// Resumes counts the service restarts the run has been resumed after.
	Resumes int
	// Checkpoint is an export's progress through its query job's results.
	Checkpoint ExportCheckpoint
	// Owner is the service instance holding the run's lease; empty when none
	// does.
	Owner      string
	CreatedAt  time.Time
	StartedAt  *time.Time
	FinishedAt *time.Time
}

// ExportCheckpoint is how far an export has loaded its query job's results:
// every page before Locator is committed to the store.
type ExportCheckpoint struct {
	// Locator names the next result page to load; empty means the first.
	Locator string
	// Watermark is the highest SystemModstamp of the committed pages.
	Watermark *time.Time
	// Records is the number of records on the committed pages.
	Records int64
}

// MaxResumes is how many restarts a run is resumed after. A run interrupted
// more often may be what brings the service down, so it is failed instead.
const MaxResumes = 3

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
	records_ok, records_failed, coalesce(error, ''), resumes, coalesce(checkpoint_locator, ''),
	checkpoint_watermark, checkpoint_records, coalesce(owner, ''), created_at, started_at, finished_at`

// CreateRun records a new queued run, leased by this instance. If the object
// already has an active run in the same direction it returns a
// *RunInProgressError.
func (s *Store) CreateRun(ctx context.Context, object string, dir Direction, trigger Trigger, fullSync bool) (Run, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO sync_runs (object_name, direction, trigger, status, full_sync, owner, heartbeat_at)
		VALUES ($1, $2, $3, $4, $5, $6, now())
		RETURNING `+runColumns,
		object, dir, trigger, Queued, fullSync, s.instance)
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

// The methods below that change an active run do so only while this
// instance holds the run's lease. An instance that lost it, for example after
// a long database outage, gets an error instead of overwriting the progress
// of the instance that took the run over.

// StartRun moves a queued run to running.
func (s *Store) StartRun(ctx context.Context, id int64) error {
	return s.execOne(ctx, "start run", id, `
		UPDATE sync_runs SET status = 'running', started_at = now()
		WHERE id = $1 AND status = 'queued' AND owner = $2`, id, s.instance)
}

// StartExportJob records the query job an export reads from. Records loaded
// from an earlier job of the run are read again, so the run's checkpoint and
// record count start over.
func (s *Store) StartExportJob(ctx context.Context, id int64, jobID string) error {
	return s.execOne(ctx, "start export job", id, `
		UPDATE sync_runs SET bulk_job_id = $2, records_ok = 0,
			checkpoint_locator = NULL, checkpoint_watermark = NULL, checkpoint_records = 0
		WHERE id = $1 AND status = 'running' AND owner = $3`, id, jobID, s.instance)
}

// SaveCheckpoint records an export's progress, so a resumed run continues
// from there.
func (s *Store) SaveCheckpoint(ctx context.Context, id int64, cp ExportCheckpoint) error {
	return s.execOne(ctx, "save checkpoint", id, `
		UPDATE sync_runs SET checkpoint_locator = nullif($2, ''), checkpoint_watermark = $3, checkpoint_records = $4
		WHERE id = $1 AND status = 'running' AND owner = $5`, id, cp.Locator, cp.Watermark, cp.Records, s.instance)
}

// FinishRun moves an active run to a terminal status and returns the outbox
// entries it had not decided to pending. cause is stored as the run's error
// and may be nil.
func (s *Store) FinishRun(ctx context.Context, id int64, status RunStatus, cause error) error {
	var msg *string
	if cause != nil {
		m := truncateError(cause.Error())
		msg = &m
	}
	return s.endRun(ctx, "finish run", id, `
		UPDATE sync_runs SET status = $2, error = $3, finished_at = now(), owner = NULL
		WHERE id = $1 AND status IN ('queued', 'running') AND owner = $4`, id, status, msg, s.instance)
}

// CompleteRun marks a running run succeeded. Any outbox entry it left
// undecided goes back to pending.
func (s *Store) CompleteRun(ctx context.Context, id int64) error {
	return s.endRun(ctx, "complete run", id, `
		UPDATE sync_runs SET status = 'succeeded', finished_at = now(), owner = NULL
		WHERE id = $1 AND status = 'running' AND owner = $2`, id, s.instance)
}

// endRun runs a statement that ends the run with the given ID and releases
// the run's undecided outbox entries, in one transaction, so no entry is left
// claimed by a run that is over.
func (s *Store) endRun(ctx context.Context, action string, id int64, sql string, args ...any) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, sql, args...)
		if err != nil {
			return fmt.Errorf("%s %d: %w", action, id, err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("%s %d: %w", action, id, errRunState)
		}
		if _, err := tx.Exec(ctx, releaseOutboxSQL+` AND run_id = $1`, id); err != nil {
			return fmt.Errorf("%s %d: release outbox entries: %w", action, id, err)
		}
		return nil
	})
}

// CompleteExport marks an export run succeeded and, when watermark is not nil,
// advances the object's watermark to it, in one transaction: the watermark
// never covers records whose run did not succeed. The watermark only moves
// forward, so a full resync cannot rewind it.
func (s *Store) CompleteExport(ctx context.Context, run Run, watermark *time.Time) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE sync_runs SET status = 'succeeded', finished_at = now(), owner = NULL
			WHERE id = $1 AND status = 'running' AND owner = $2`, run.ID, s.instance)
		if err != nil {
			return fmt.Errorf("complete run %d: %w", run.ID, err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("complete run %d: %w", run.ID, errRunState)
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

// RecoverRuns settles the active runs whose lease has lapsed: those no
// instance holds, and those whose owner has not renewed the lease within
// leaseTTL, presumably because it crashed. Every instance may call it at any
// time; row locks make sure only one of them takes over a given run.
//
// A lapsed running run that already has a Salesforce job is resumable: this
// instance takes its lease, its resume count goes up, and it is returned,
// still running, for the caller to continue. A resumed export's record count
// goes back to its checkpoint, because the pages after it are loaded again.
// Every other lapsed run is failed, as is a run already resumed maxResumes
// times, and the outbox entries of failed runs go back to pending. failed is
// the number of runs failed.
func (s *Store) RecoverRuns(ctx context.Context, maxResumes int, leaseTTL time.Duration) (failed int64, resumed []Run, err error) {
	// lapsed matches active runs whose lease has lapsed, given the lease TTL
	// in microseconds as query parameter n.
	lapsed := func(n int) string {
		return fmt.Sprintf(`status IN ('queued', 'running') AND (owner IS NULL OR heartbeat_at IS NULL
			OR heartbeat_at < now() - $%d * interval '1 microsecond')`, n)
	}
	ttl := float64(leaseTTL.Microseconds())
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE sync_runs SET status = 'failed', finished_at = now(), owner = NULL,
				error = CASE WHEN resumes >= $1 THEN 'interrupted by a service restart too many times'
				             ELSE 'interrupted by a service restart' END
			WHERE `+lapsed(2)+` AND (status = 'queued' OR bulk_job_id IS NULL OR resumes >= $1)`,
			maxResumes, ttl)
		if err != nil {
			return fmt.Errorf("fail interrupted runs: %w", err)
		}
		failed = tag.RowsAffected()
		if _, err := tx.Exec(ctx, releaseOutboxSQL+`
			AND run_id IN (SELECT id FROM sync_runs WHERE status NOT IN ('queued', 'running'))`); err != nil {
			return fmt.Errorf("release outbox entries of interrupted runs: %w", err)
		}

		rows, err := tx.Query(ctx, `
			UPDATE sync_runs SET owner = $2, heartbeat_at = now(), resumes = resumes + 1,
				records_ok = CASE direction WHEN 'export' THEN checkpoint_records ELSE records_ok END
			WHERE `+lapsed(1)+`
			RETURNING `+runColumns, ttl, s.instance)
		if err != nil {
			return fmt.Errorf("resume interrupted runs: %w", err)
		}
		resumed, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (Run, error) { return scanRun(row) })
		if err != nil {
			return fmt.Errorf("resume interrupted runs: %w", err)
		}
		return nil
	})
	if err != nil {
		return 0, nil, err
	}
	slices.SortFunc(resumed, func(a, b Run) int { return cmp.Compare(a.ID, b.ID) })
	return failed, resumed, nil
}

// Heartbeat renews the lease of every active run this instance holds and
// returns their IDs. A run missing from the result has been taken over by
// another instance, or finished, and must not be worked on any further.
func (s *Store) Heartbeat(ctx context.Context) ([]int64, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE sync_runs SET heartbeat_at = now()
		WHERE owner = $1 AND status IN ('queued', 'running')
		RETURNING id`, s.instance)
	if err != nil {
		return nil, fmt.Errorf("renew run leases: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return nil, fmt.Errorf("renew run leases: %w", err)
	}
	return ids, nil
}

// ReleaseRun gives up this instance's lease of an active run without ending
// it, so that any instance may resume it straight away.
func (s *Store) ReleaseRun(ctx context.Context, id int64) error {
	return s.execOne(ctx, "release run", id, `
		UPDATE sync_runs SET owner = NULL
		WHERE id = $1 AND status IN ('queued', 'running') AND owner = $2`, id, s.instance)
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
		return fmt.Errorf("%s %d: %w", action, id, errRunState)
	}
	return nil
}

// errRunState reports a run change that matched no run.
var errRunState = errors.New("run not found, not in the expected state, or leased by another instance")

func scanRun(row pgx.Row) (Run, error) {
	var r Run
	err := row.Scan(&r.ID, &r.Object, &r.Direction, &r.Trigger, &r.Status, &r.FullSync, &r.BulkJobID,
		&r.RecordsOK, &r.RecordsFailed, &r.Error, &r.Resumes, &r.Checkpoint.Locator, &r.Checkpoint.Watermark,
		&r.Checkpoint.Records, &r.Owner, &r.CreatedAt, &r.StartedAt, &r.FinishedAt)
	return r, err
}
