package syncer

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AndreyZubov/data-sync-service/internal/salesforce"
)

func TestExportFields(t *testing.T) {
	desc := &salesforce.ObjectDescription{Name: "Account", Queryable: true, Fields: []salesforce.Field{
		{Name: "Id", Type: "id"},
		{Name: "Name", Type: "string"},
		{Name: "BillingAddress", Type: "address"},
		{Name: "BillingCity", Type: "string"},
		{Name: "Location__c", Type: "location"},
		{Name: "Logo__c", Type: "base64"},
		{Name: "Legacy__c", Type: "string", DeprecatedAndHidden: true},
	}}

	fields, err := exportFields(desc)

	require.NoError(t, err)
	assert.Equal(t, []salesforce.Field{
		{Name: "Id", Type: "id"}, {Name: "Name", Type: "string"}, {Name: "BillingCity", Type: "string"},
	}, fields)
}

func TestExportFieldsRejectsUnsupportedObjects(t *testing.T) {
	_, err := exportFields(&salesforce.ObjectDescription{Name: "Feed", Queryable: false})
	assert.ErrorContains(t, err, "not queryable")

	_, err = exportFields(&salesforce.ObjectDescription{
		Name: "Odd", Queryable: true,
		Fields: []salesforce.Field{{Name: "Name", Type: "string"}},
	})
	assert.ErrorContains(t, err, "no Id field")
}

func TestBuildQuery(t *testing.T) {
	fields := []salesforce.Field{{Name: "Id"}, {Name: "Name"}, {Name: "SystemModstamp"}}
	since := time.Date(2026, 10, 1, 12, 0, 30, 999_000_000, time.FixedZone("CEST", 2*3600))

	assert.Equal(t, "SELECT Id, Name, SystemModstamp FROM Account", buildQuery("Account", fields, nil))
	assert.Equal(t,
		"SELECT Id, Name, SystemModstamp FROM Account WHERE SystemModstamp >= 2026-10-01T09:55:30Z",
		buildQuery("Account", fields, &since),
		"converted to UTC, moved back by the lookback, truncated to seconds")
}

func TestJSONValue(t *testing.T) {
	tests := []struct {
		fieldType, value string
		want             any
	}{
		{"string", "", nil},
		{"boolean", "", nil},
		{"string", "Acme", "Acme"},
		{"boolean", "true", true},
		{"boolean", "false", false},
		{"boolean", "yes", "yes"},
		{"int", "42", json.Number("42")},
		{"double", "-1.5E3", json.Number("-1.5E3")},
		{"currency", "12345678901234567890.12", json.Number("12345678901234567890.12")},
		{"percent", "NaN", "NaN"},
		{"int", "0x10", "0x10"},
		{"int", "Infinity", "Infinity"},
		{"datetime", "2026-10-01T12:00:00.000Z", "2026-10-01T12:00:00.000Z"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, jsonValue(tt.fieldType, tt.value), "%s %q", tt.fieldType, tt.value)
	}
}

func TestPageLayoutDecode(t *testing.T) {
	fields := []salesforce.Field{
		{Name: "Id", Type: "id"},
		{Name: "Name", Type: "string"},
		{Name: "NumberOfEmployees", Type: "int"},
		{Name: "IsDeleted", Type: "boolean"},
		{Name: "SystemModstamp", Type: "datetime"},
	}
	// Columns in a different order and case than the fields.
	layout, err := newPageLayout([]string{"systemmodstamp", "Id", "Name", "IsDeleted", "NumberOfEmployees"}, fields)
	require.NoError(t, err)

	rec, err := layout.decode([]string{"2026-10-01T12:00:00.123Z", "001A", "Acme", "true", "250"})

	require.NoError(t, err)
	assert.Equal(t, "001A", rec.ID)
	assert.True(t, rec.IsDeleted)
	require.NotNil(t, rec.SystemModstamp)
	assert.True(t, time.Date(2026, 10, 1, 12, 0, 0, 123_000_000, time.UTC).Equal(*rec.SystemModstamp))
	assert.JSONEq(t,
		`{"Id":"001A","Name":"Acme","NumberOfEmployees":250,"IsDeleted":true,"SystemModstamp":"2026-10-01T12:00:00.123Z"}`,
		string(rec.Data))
}

func TestPageLayoutErrors(t *testing.T) {
	fields := []salesforce.Field{{Name: "Id", Type: "id"}, {Name: "SystemModstamp", Type: "datetime"}}

	_, err := newPageLayout([]string{"Id"}, fields)
	assert.ErrorContains(t, err, "1 columns, expected 2")
	_, err = newPageLayout([]string{"Id", "Surprise"}, fields)
	assert.ErrorContains(t, err, `unexpected result column "Surprise"`)
	_, err = newPageLayout([]string{"Id", "id"}, fields)
	assert.ErrorContains(t, err, `unexpected result column "id"`, "duplicate columns are rejected")
	_, err = newPageLayout([]string{"Name"}, []salesforce.Field{{Name: "Name"}})
	assert.ErrorContains(t, err, "no Id column")

	layout, err := newPageLayout([]string{"Id", "SystemModstamp"}, fields)
	require.NoError(t, err)
	_, err = layout.decode([]string{"", "2026-10-01T12:00:00.000Z"})
	assert.ErrorContains(t, err, "record without Id")
	_, err = layout.decode([]string{"001A", "yesterday"})
	assert.ErrorContains(t, err, "parse SystemModstamp")

	rec, err := layout.decode([]string{"001A", ""})
	require.NoError(t, err)
	assert.Nil(t, rec.SystemModstamp)
}
