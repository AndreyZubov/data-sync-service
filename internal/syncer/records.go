package syncer

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/AndreyZubov/data-sync-service/internal/salesforce"
	"github.com/AndreyZubov/data-sync-service/internal/store"
)

// pageLayout maps the columns of one CSV result page to the exported fields.
// Each page carries its own header row, so the layout is built per page
// rather than assumed from the query.
type pageLayout struct {
	fields   []salesforce.Field // by column
	id       int
	modstamp int // -1 when the object has no SystemModstamp
	deleted  int // -1 when the object has no IsDeleted
}

// newPageLayout matches header against fields. Salesforce echoes field names
// as queried, but matching ignores case to be safe; any missing or unexpected
// column is an error, because silently dropping data is worse than failing.
func newPageLayout(header []string, fields []salesforce.Field) (*pageLayout, error) {
	byName := make(map[string]salesforce.Field, len(fields))
	for _, f := range fields {
		byName[strings.ToLower(f.Name)] = f
	}
	if len(header) != len(fields) {
		return nil, fmt.Errorf("result header has %d columns, expected %d", len(header), len(fields))
	}

	l := &pageLayout{fields: make([]salesforce.Field, len(header)), id: -1, modstamp: -1, deleted: -1}
	for i, name := range header {
		f, ok := byName[strings.ToLower(name)]
		if !ok {
			return nil, fmt.Errorf("unexpected result column %q", name)
		}
		delete(byName, strings.ToLower(name))
		l.fields[i] = f
		switch f.Name {
		case fieldID:
			l.id = i
		case fieldSystemModstamp:
			l.modstamp = i
		case fieldIsDeleted:
			l.deleted = i
		}
	}
	if l.id < 0 {
		return nil, fmt.Errorf("result has no %s column", fieldID)
	}
	return l, nil
}

// decode converts one CSV row into a record.
func (l *pageLayout) decode(row []string) (store.Record, error) {
	rec := store.Record{ID: row[l.id]}
	if rec.ID == "" {
		return store.Record{}, fmt.Errorf("record without %s", fieldID)
	}
	if l.modstamp >= 0 && row[l.modstamp] != "" {
		ts, err := time.Parse(time.RFC3339Nano, row[l.modstamp])
		if err != nil {
			return store.Record{}, fmt.Errorf("record %s: parse %s: %w", rec.ID, fieldSystemModstamp, err)
		}
		rec.SystemModstamp = &ts
	}
	if l.deleted >= 0 {
		rec.IsDeleted = row[l.deleted] == "true"
	}

	data := make(map[string]any, len(row))
	for i, value := range row {
		data[l.fields[i].Name] = jsonValue(l.fields[i].Type, value)
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return store.Record{}, fmt.Errorf("record %s: encode: %w", rec.ID, err)
	}
	rec.Data = raw
	return rec, nil
}

// jsonValue converts a CSV value to its JSON form. Bulk API CSV cannot tell
// null from an empty string; both become null. Booleans and numbers become
// JSON booleans and numbers, numbers keeping their exact digits; anything
// unexpected stays a string rather than being lost.
func jsonValue(fieldType, value string) any {
	if value == "" {
		return nil
	}
	switch fieldType {
	case "boolean":
		switch value {
		case "true":
			return true
		case "false":
			return false
		}
	case "int", "long", "double", "currency", "percent":
		if isJSONNumber(value) {
			return json.Number(value)
		}
	}
	return value
}

func isJSONNumber(s string) bool {
	if s[0] != '-' && (s[0] < '0' || s[0] > '9') {
		return false
	}
	var n json.Number
	return json.Unmarshal([]byte(s), &n) == nil
}
