package syncer

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/AndreyZubov/data-sync-service/internal/salesforce"
	"github.com/AndreyZubov/data-sync-service/internal/store"
)

// nullValue is how Bulk API CSV sets a field to null; an empty value leaves
// the field unchanged on update.
const nullValue = "#N/A"

// errNoResult explains entries a completed job reported nothing about. They
// are not retried, because the change may have been applied.
const errNoResult = "Salesforce reported no result for this record; not retried, as it may have been applied"

// ingestBatch is the CSV upload of one ingest job.
type ingestBatch struct {
	columns  []string
	entryIDs []int64 // by row
	rows     [][]string
	data     []byte
}

// ingestPlan splits outbox entries of one operation into ingest jobs.
type ingestPlan struct {
	batches []*ingestBatch
	// rejected holds entries that cannot be sent as written.
	rejected []store.Outcome
}

// ingestGroup is the outbox entries that can share ingest jobs.
type ingestGroup struct {
	operation       store.Operation
	externalIDField string
	entries         []store.OutboxEntry
}

// groupEntries groups entries by operation and upsert key, keeping the order
// in which each group first appears.
func groupEntries(entries []store.OutboxEntry) []*ingestGroup {
	var groups []*ingestGroup
	index := make(map[[2]string]*ingestGroup)
	for _, e := range entries {
		key := [2]string{string(e.Operation), e.ExternalIDField}
		g, ok := index[key]
		if !ok {
			g = &ingestGroup{operation: e.Operation, externalIDField: e.ExternalIDField}
			index[key] = g
			groups = append(groups, g)
		}
		g.entries = append(g.entries, e)
	}
	return groups
}

// planIngest validates g's entries against desc and lays out the valid ones
// as CSV uploads of at most maxBytes each. Checking fields here keeps one bad
// entry from failing a whole job: Salesforce rejects an upload outright when
// a column is unknown or not writable.
func planIngest(desc *salesforce.ObjectDescription, g *ingestGroup, maxBytes int) (ingestPlan, error) {
	var plan ingestPlan
	reject := func(id int64, format string, args ...any) {
		plan.rejected = append(plan.rejected, store.Outcome{
			EntryID: id, Kind: store.Rejected, Error: fmt.Sprintf(format, args...),
		})
	}

	fields := make(map[string]salesforce.Field, len(desc.Fields))
	for _, f := range desc.Fields {
		fields[strings.ToLower(f.Name)] = f
	}
	key := fieldID
	if g.operation == store.Upsert {
		f, ok := fields[strings.ToLower(g.externalIDField)]
		if !ok {
			for _, e := range g.entries {
				reject(e.ID, "external ID field %q does not exist on %s", g.externalIDField, desc.Name)
			}
			return plan, nil
		}
		key = f.Name
	}

	type valid struct {
		id     int64
		values map[string]string
	}
	var entries []valid
	columns := map[string]bool{}
	for _, e := range g.entries {
		values, err := ingestValues(e, g.operation, key, fields)
		if err != nil {
			reject(e.ID, "%v", err)
			continue
		}
		for name := range values {
			columns[name] = true
		}
		entries = append(entries, valid{id: e.ID, values: values})
	}
	if len(entries) == 0 {
		return plan, nil
	}

	// The key column goes first, for readable uploads and results.
	header := make([]string, 0, len(columns))
	for name := range columns {
		if name != key {
			header = append(header, name)
		}
	}
	slices.Sort(header)
	if columns[key] {
		header = append([]string{key}, header...)
	}

	headerCSV, err := encodeCSVRow(header)
	if err != nil {
		return ingestPlan{}, err
	}
	var batch *ingestBatch
	var buf bytes.Buffer
	flush := func() {
		if batch != nil {
			batch.data = bytes.Clone(buf.Bytes())
			plan.batches = append(plan.batches, batch)
			batch = nil
		}
	}
	for _, e := range entries {
		row := make([]string, len(header))
		for i, name := range header {
			row[i] = e.values[name]
		}
		rowCSV, err := encodeCSVRow(row)
		if err != nil {
			return ingestPlan{}, err
		}
		if len(headerCSV)+len(rowCSV) > maxBytes {
			reject(e.id, "record is larger than the %d byte upload limit", maxBytes)
			continue
		}
		if batch != nil && buf.Len()+len(rowCSV) > maxBytes {
			flush()
		}
		if batch == nil {
			batch = &ingestBatch{columns: header}
			buf.Reset()
			buf.Write(headerCSV)
		}
		buf.Write(rowCSV)
		batch.entryIDs = append(batch.entryIDs, e.id)
		batch.rows = append(batch.rows, row)
	}
	flush()
	return plan, nil
}

// ingestValues converts an entry's payload into CSV values keyed by the
// fields' API names. key is the field that identifies the record: Id, or the
// external ID field of an upsert.
func ingestValues(e store.OutboxEntry, op store.Operation, key string, fields map[string]salesforce.Field) (map[string]string, error) {
	dec := json.NewDecoder(bytes.NewReader(e.Payload))
	dec.UseNumber()
	var payload map[string]any
	if err := dec.Decode(&payload); err != nil {
		return nil, fmt.Errorf("payload is not a JSON object: %w", err)
	}

	values := make(map[string]string, len(payload))
	for name, raw := range payload {
		f, ok := fields[strings.ToLower(name)]
		if !ok {
			return nil, fmt.Errorf("field %q does not exist", name)
		}
		if _, dup := values[f.Name]; dup {
			return nil, fmt.Errorf("field %s is given twice", f.Name)
		}
		if op == store.Delete && f.Name != key {
			continue
		}
		if err := checkWritable(f, op, key); err != nil {
			return nil, err
		}
		v, err := csvValue(raw)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", f.Name, err)
		}
		values[f.Name] = v
	}

	if len(values) == 0 {
		return nil, errors.New("payload sets no fields")
	}
	if op != store.Insert {
		if v := values[key]; v == "" || v == nullValue {
			return nil, fmt.Errorf("%s requires a value for %s", op, key)
		}
	}
	return values, nil
}

// checkWritable reports whether f may be set by op. The key field is only
// used to find the record.
func checkWritable(f salesforce.Field, op store.Operation, key string) error {
	if unsupportedFieldTypes[f.Type] {
		return fmt.Errorf("field %s of type %s cannot be written through the Bulk API", f.Name, f.Type)
	}
	if op != store.Insert && f.Name == key {
		return nil
	}
	writable := false
	switch op {
	case store.Insert:
		writable = f.Createable
	case store.Update:
		writable = f.Updateable
	case store.Upsert:
		writable = f.Createable || f.Updateable
	}
	if !writable {
		return fmt.Errorf("field %s is not writable by %s", f.Name, op)
	}
	return nil
}

// csvValue renders a JSON scalar as a Bulk API CSV value. Null and the empty
// string both clear the field.
func csvValue(v any) (string, error) {
	switch v := v.(type) {
	case nil:
		return nullValue, nil
	case string:
		if v == "" {
			return nullValue, nil
		}
		return v, nil
	case json.Number:
		return v.String(), nil
	case bool:
		return fmt.Sprint(v), nil
	default:
		return "", errors.New("objects and arrays are not supported")
	}
}

func encodeCSVRow(row []string) ([]byte, error) {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	if err := w.Write(row); err != nil {
		return nil, err
	}
	w.Flush()
	return buf.Bytes(), w.Error()
}

// resultMatcher pairs the rows of an ingest job's result sets with the
// entries they report on. Salesforce returns results in no particular order
// and without any caller-supplied tag, but echoes every uploaded column, so a
// result belongs to an uploaded row with the same values. Rows with equal
// values are interchangeable, as they are the same change.
type resultMatcher struct {
	columns []string
	pending map[string][]int64
}

func newResultMatcher(b *ingestBatch) *resultMatcher {
	m := &resultMatcher{columns: b.columns, pending: make(map[string][]int64, len(b.rows))}
	for i, row := range b.rows {
		k := matchKey(row)
		m.pending[k] = append(m.pending[k], b.entryIDs[i])
	}
	return m
}

// read consumes one result set. For every row it matches it calls outcome
// with the entry's ID and the row's values keyed by column name, including
// the sf__ columns.
func (m *resultMatcher) read(body io.Reader, outcome func(entryID int64, row map[string]string)) error {
	r := csv.NewReader(body)
	header, err := r.Read()
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read result header: %w", err)
	}

	// Positions of the uploaded columns in the result rows.
	positions := make([]int, len(m.columns))
	for i, name := range m.columns {
		positions[i] = slices.IndexFunc(header, func(h string) bool { return strings.EqualFold(h, name) })
		if positions[i] < 0 {
			return fmt.Errorf("result has no column %s", name)
		}
	}

	values := make([]string, len(m.columns))
	for {
		row, err := r.Read()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read result row: %w", err)
		}
		for i, p := range positions {
			values[i] = row[p]
		}
		k := matchKey(values)
		ids := m.pending[k]
		if len(ids) == 0 {
			// Not ours or already matched; the entry it may belong to is
			// reported as unmatched.
			continue
		}
		m.pending[k] = ids[1:]
		named := make(map[string]string, len(header))
		for i, h := range header {
			named[h] = row[i]
		}
		outcome(ids[0], named)
	}
}

// unmatched returns the entries no result has been matched to yet.
func (m *resultMatcher) unmatched() []int64 {
	var ids []int64
	for _, pending := range m.pending {
		ids = append(ids, pending...)
	}
	slices.Sort(ids)
	return ids
}

// matchKey joins values with a separator that cannot occur in Salesforce
// field values.
func matchKey(values []string) string { return strings.Join(values, "\x00") }
