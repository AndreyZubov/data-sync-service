package syncer

import (
	"fmt"
	"strings"
	"time"

	"github.com/AndreyZubov/data-sync-service/internal/salesforce"
)

// System fields the exporter relies on.
const (
	fieldID             = "Id"
	fieldSystemModstamp = "SystemModstamp"
	fieldIsDeleted      = "IsDeleted"
)

// watermarkLookback re-reads a window before the watermark on every
// incremental run. A record's SystemModstamp is set when its transaction
// starts, so a long transaction can commit a timestamp older than one already
// exported; without the overlap such records would be skipped for good. The
// re-read records are upserted idempotently.
const watermarkLookback = 5 * time.Minute

// unsupportedFieldTypes cannot be read through Bulk API queries: compound
// fields must be queried through their components, which are listed as
// fields of their own, and binary fields are not available at all.
var unsupportedFieldTypes = map[string]bool{
	"address":  true,
	"location": true,
	"base64":   true,
}

// exportFields returns the fields of desc that an export reads, in describe
// order.
func exportFields(desc *salesforce.ObjectDescription) ([]salesforce.Field, error) {
	if !desc.Queryable {
		return nil, fmt.Errorf("object %s is not queryable", desc.Name)
	}
	if !desc.HasField(fieldID) {
		return nil, fmt.Errorf("object %s has no %s field", desc.Name, fieldID)
	}
	fields := make([]salesforce.Field, 0, len(desc.Fields))
	for _, f := range desc.Fields {
		if f.DeprecatedAndHidden || unsupportedFieldTypes[f.Type] {
			continue
		}
		fields = append(fields, f)
	}
	return fields, nil
}

// buildQuery returns the SOQL query for an export of object. When since is
// not nil, only records modified at or after it, less watermarkLookback, are
// selected. The bound is truncated to whole seconds, the precision of SOQL
// date-time literals, and inclusive, so no record at the boundary is missed.
func buildQuery(object string, fields []salesforce.Field, since *time.Time) string {
	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = f.Name
	}
	q := "SELECT " + strings.Join(names, ", ") + " FROM " + object
	if since != nil {
		bound := since.Add(-watermarkLookback).UTC().Truncate(time.Second)
		q += " WHERE " + fieldSystemModstamp + " >= " + bound.Format("2006-01-02T15:04:05Z")
	}
	return q
}
