package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Record is one Salesforce record to store.
type Record struct {
	ID string
	// SystemModstamp is the record's last system modification time; nil for
	// objects without the field.
	SystemModstamp *time.Time
	IsDeleted      bool
	// Data holds every exported field as a JSON object keyed by API name.
	Data json.RawMessage
}

// UpsertRecords writes records of object in one transaction and adds their
// count to the run's records_ok, so the counter always matches committed data.
//
// Rows are copied into a temporary staging table and merged from there: COPY
// is far faster than row-by-row inserts but cannot express ON CONFLICT. A
// stored record is only replaced by one at least as recent, so a slow, stale
// write cannot overwrite newer data.
func (s *Store) UpsertRecords(ctx context.Context, runID int64, object string, records []Record) error {
	if len(records) == 0 {
		return nil
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			CREATE TEMP TABLE sf_records_stage (
				id              TEXT,
				system_modstamp TIMESTAMPTZ,
				is_deleted      BOOLEAN,
				data            JSONB
			) ON COMMIT DROP`)
		if err != nil {
			return fmt.Errorf("create staging table: %w", err)
		}

		_, err = tx.CopyFrom(ctx,
			pgx.Identifier{"sf_records_stage"},
			[]string{"id", "system_modstamp", "is_deleted", "data"},
			pgx.CopyFromSlice(len(records), func(i int) ([]any, error) {
				r := records[i]
				return []any{r.ID, r.SystemModstamp, r.IsDeleted, r.Data}, nil
			}))
		if err != nil {
			return fmt.Errorf("copy records: %w", err)
		}

		// DISTINCT ON guards against a batch naming one ID twice, which ON
		// CONFLICT DO UPDATE rejects outright.
		_, err = tx.Exec(ctx, `
			INSERT INTO sf_records (object_name, id, system_modstamp, is_deleted, data, synced_at)
			SELECT DISTINCT ON (id) $1, id, system_modstamp, is_deleted, data, now()
			FROM sf_records_stage
			ORDER BY id, system_modstamp DESC NULLS LAST
			ON CONFLICT (object_name, id) DO UPDATE SET
				system_modstamp = EXCLUDED.system_modstamp,
				is_deleted      = EXCLUDED.is_deleted,
				data            = EXCLUDED.data,
				synced_at       = EXCLUDED.synced_at
			WHERE sf_records.system_modstamp IS NULL
			   OR EXCLUDED.system_modstamp IS NULL
			   OR sf_records.system_modstamp <= EXCLUDED.system_modstamp`,
			object)
		if err != nil {
			return fmt.Errorf("merge records: %w", err)
		}

		_, err = tx.Exec(ctx, `UPDATE sync_runs SET records_ok = records_ok + $2 WHERE id = $1`, runID, len(records))
		if err != nil {
			return fmt.Errorf("count records: %w", err)
		}
		return nil
	})
}
