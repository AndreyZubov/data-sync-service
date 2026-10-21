package store

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
)

// Operation is the change an outbox entry asks Salesforce to make.
type Operation string

// Outbox operations.
const (
	// Insert creates a record from the payload.
	Insert Operation = "insert"
	// Update changes the record whose Id the payload names.
	Update Operation = "update"
	// Upsert updates the record matching the payload's external ID field, or
	// creates one if none matches.
	Upsert Operation = "upsert"
	// Delete moves the record whose Id the payload names to the Recycle Bin.
	Delete Operation = "delete"
)

// OutboxStatus is an outbox entry's delivery state.
type OutboxStatus string

// Outbox statuses. An entry is pending until an import run claims it,
// processing while the run sends it, and then sent, pending again for a
// retry, or dead once it can no longer succeed.
const (
	Pending    OutboxStatus = "pending"
	Processing OutboxStatus = "processing"
	Sent       OutboxStatus = "sent"
	Dead       OutboxStatus = "dead"
)

// OutboxEntry is one change waiting to be, or already, written to Salesforce.
type OutboxEntry struct {
	ID        int64
	Object    string
	Operation Operation
	// ExternalIDField is the field an upsert matches on; empty otherwise.
	ExternalIDField string
	// Payload holds field values as a JSON object keyed by API name.
	Payload json.RawMessage
	Status  OutboxStatus
	// Attempts counts the import runs that have tried to send the entry.
	Attempts  int
	LastError string
	// SalesforceID is the record's Id, once Salesforce has accepted the entry.
	SalesforceID string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// OutcomeKind classifies what an import run learned about an entry.
type OutcomeKind int

// Outcome kinds.
const (
	// Delivered means Salesforce applied the change.
	Delivered OutcomeKind = iota
	// Retry means the change failed but may succeed later; the entry is
	// retried with a growing delay until it has used up its attempts.
	Retry
	// Rejected means the change can never succeed as written; the entry is
	// dead-lettered at once.
	Rejected
)

// Outcome is the result of sending one outbox entry.
type Outcome struct {
	EntryID int64
	Kind    OutcomeKind
	// SalesforceID is the affected record's Id, for delivered entries.
	SalesforceID string
	// Error explains a failure or rejection.
	Error string
}

// RetryPolicy decides when a failed entry is tried again.
type RetryPolicy struct {
	// MaxAttempts is the number of runs that may try an entry before it is
	// dead-lettered.
	MaxAttempts int
	// BaseDelay is the wait after the first failure; it doubles with every
	// further failure up to MaxDelay.
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

const outboxColumns = `id, object_name, operation, coalesce(external_id_field, ''), payload, status, attempts,
	coalesce(last_error, ''), coalesce(salesforce_id, ''), created_at, updated_at`

// ClaimOutbox marks up to limit due pending entries of object as processing
// by run and returns them, oldest first. It counts the attempt.
//
// Only the oldest pending entry of each record is eligible, so changes to one
// record reach Salesforce in the order they were written and never share a
// Bulk API job, which processes its rows in no particular order. While that
// entry waits for a retry, newer changes to the record wait behind it.
// Callers must hold the object's active import run, which keeps two runs from
// claiming concurrently.
func (s *Store) ClaimOutbox(ctx context.Context, runID int64, object string, limit int) ([]OutboxEntry, error) {
	rows, err := s.pool.Query(ctx, `
		WITH heads AS (
			SELECT DISTINCT ON (coalesce(record_key, '#' || id)) id, status, next_attempt_at
			FROM sf_outbox
			WHERE object_name = $1 AND status IN ('pending', 'processing')
			ORDER BY coalesce(record_key, '#' || id), id
		), claimed AS (
			SELECT id FROM heads WHERE status = 'pending' AND next_attempt_at <= now() ORDER BY id LIMIT $3
		)
		UPDATE sf_outbox o SET status = 'processing', run_id = $2, attempts = attempts + 1, updated_at = now()
		FROM claimed
		WHERE o.id = claimed.id
		RETURNING o.id, o.object_name, o.operation, coalesce(o.external_id_field, ''), o.payload, o.status,
			o.attempts, coalesce(o.last_error, ''), coalesce(o.salesforce_id, ''), o.created_at, o.updated_at`,
		object, runID, limit)
	if err != nil {
		return nil, fmt.Errorf("claim outbox entries of %s: %w", object, err)
	}
	entries, err := pgx.CollectRows(rows, scanOutboxEntry)
	if err != nil {
		return nil, fmt.Errorf("claim outbox entries of %s: %w", object, err)
	}
	// UPDATE ... RETURNING yields rows in no particular order.
	slices.SortFunc(entries, func(a, b OutboxEntry) int { return cmp.Compare(a.ID, b.ID) })
	return entries, nil
}

// HasDueOutbox reports whether object has a pending entry ready to be sent.
func (s *Store) HasDueOutbox(ctx context.Context, object string) (bool, error) {
	var due bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM sf_outbox
			WHERE object_name = $1 AND status = 'pending' AND next_attempt_at <= now()
		)`, object).Scan(&due)
	if err != nil {
		return false, fmt.Errorf("check outbox of %s: %w", object, err)
	}
	return due, nil
}

// RecordOutcomes applies the outcomes of entries a run sent and adds them to
// the run's counters, in one transaction. Failed entries go back to pending
// with a delay from policy, or are dead-lettered once they have used up their
// attempts. It returns the number of entries dead-lettered.
func (s *Store) RecordOutcomes(ctx context.Context, runID int64, outcomes []Outcome, policy RetryPolicy) (int64, error) {
	if len(outcomes) == 0 {
		return 0, nil
	}
	var (
		ids, kinds    []int64
		sfIDs, errMsg []string
		delivered     int64
	)
	for _, o := range outcomes {
		ids = append(ids, o.EntryID)
		kinds = append(kinds, int64(o.Kind))
		sfIDs = append(sfIDs, o.SalesforceID)
		errMsg = append(errMsg, truncateError(o.Error))
		if o.Kind == Delivered {
			delivered++
		}
	}

	var dead int64
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE sf_outbox o SET
				status = CASE
					WHEN r.kind = $5 THEN 'sent'
					WHEN r.kind = $6 AND o.attempts < $7 THEN 'pending'
					ELSE 'dead'
				END,
				salesforce_id   = nullif(r.sf_id, ''),
				last_error      = nullif(r.error, ''),
				next_attempt_at = CASE
					WHEN r.kind = $6 THEN now() + least($8 * power(2, o.attempts - 1), $9) * interval '1 microsecond'
					ELSE o.next_attempt_at
				END,
				updated_at = now()
			FROM unnest($1::bigint[], $2::bigint[], $3::text[], $4::text[]) AS r(id, kind, sf_id, error)
			WHERE o.id = r.id AND o.run_id = $10 AND o.status = 'processing'`,
			ids, kinds, sfIDs, errMsg,
			int64(Delivered), int64(Retry), policy.MaxAttempts,
			float64(policy.BaseDelay.Microseconds()), float64(policy.MaxDelay.Microseconds()), runID)
		if err != nil {
			return fmt.Errorf("record outbox outcomes: %w", err)
		}
		if tag.RowsAffected() != int64(len(outcomes)) {
			return fmt.Errorf("record outbox outcomes: %d of %d entries are not processing in run %d",
				int64(len(outcomes))-tag.RowsAffected(), len(outcomes), runID)
		}

		if err := tx.QueryRow(ctx, `
			SELECT count(*) FROM sf_outbox WHERE id = ANY($1) AND status = 'dead'`, ids).Scan(&dead); err != nil {
			return fmt.Errorf("count dead letters: %w", err)
		}
		_, err = tx.Exec(ctx, `
			UPDATE sync_runs SET records_ok = records_ok + $2, records_failed = records_failed + $3
			WHERE id = $1`, runID, delivered, int64(len(outcomes))-delivered)
		if err != nil {
			return fmt.Errorf("count run records: %w", err)
		}
		return nil
	})
	return dead, err
}

// ReleaseOutbox returns the entries a run still has in processing to pending,
// without counting the attempt, for runs that stop before learning the
// entries' outcome. It returns the number of entries released.
func (s *Store) ReleaseOutbox(ctx context.Context, runID int64) (int64, error) {
	tag, err := s.pool.Exec(ctx, releaseOutboxSQL+` AND run_id = $1`, runID)
	if err != nil {
		return 0, fmt.Errorf("release outbox entries of run %d: %w", runID, err)
	}
	return tag.RowsAffected(), nil
}

const releaseOutboxSQL = `
	UPDATE sf_outbox SET status = 'pending', attempts = greatest(attempts - 1, 0), updated_at = now()
	WHERE status = 'processing'`

// DeadLetters returns up to limit dead-lettered entries of object, oldest
// first.
func (s *Store) DeadLetters(ctx context.Context, object string, limit int) ([]OutboxEntry, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+outboxColumns+` FROM sf_outbox
		WHERE object_name = $1 AND status = 'dead'
		ORDER BY id LIMIT $2`, object, limit)
	if err != nil {
		return nil, fmt.Errorf("list dead letters of %s: %w", object, err)
	}
	entries, err := pgx.CollectRows(rows, scanOutboxEntry)
	if err != nil {
		return nil, fmt.Errorf("list dead letters of %s: %w", object, err)
	}
	return entries, nil
}

// RetryDeadLetters returns every dead-lettered entry of object to pending
// with a fresh set of attempts, for after the cause has been fixed. It
// returns the number of entries requeued.
func (s *Store) RetryDeadLetters(ctx context.Context, object string) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE sf_outbox SET status = 'pending', attempts = 0, next_attempt_at = now(), updated_at = now()
		WHERE object_name = $1 AND status = 'dead'`, object)
	if err != nil {
		return 0, fmt.Errorf("retry dead letters of %s: %w", object, err)
	}
	return tag.RowsAffected(), nil
}

func scanOutboxEntry(row pgx.CollectableRow) (OutboxEntry, error) {
	var e OutboxEntry
	err := row.Scan(&e.ID, &e.Object, &e.Operation, &e.ExternalIDField, &e.Payload, &e.Status, &e.Attempts,
		&e.LastError, &e.SalesforceID, &e.CreatedAt, &e.UpdatedAt)
	return e, err
}
