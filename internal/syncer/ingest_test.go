package syncer

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AndreyZubov/data-sync-service/internal/salesforce"
	"github.com/AndreyZubov/data-sync-service/internal/store"
)

var contactDescription = &salesforce.ObjectDescription{Name: "Contact", Fields: []salesforce.Field{
	{Name: "Id", Type: "id"},
	{Name: "LastName", Type: "string", Createable: true, Updateable: true},
	{Name: "Email", Type: "email", Createable: true, Updateable: true},
	{Name: "NumberOfLegs__c", Type: "double", Createable: true, Updateable: true},
	{Name: "DoNotCall", Type: "boolean", Createable: true, Updateable: true},
	{Name: "Ext_Id__c", Type: "string", Createable: true, Updateable: true},
	{Name: "CreatedDate", Type: "datetime"},
	{Name: "Seeded__c", Type: "string", Createable: true},
	{Name: "MailingAddress", Type: "address"},
}}

func entry(id int64, op store.Operation, payload string) store.OutboxEntry {
	return store.OutboxEntry{ID: id, Operation: op, Payload: json.RawMessage(payload)}
}

func group(op store.Operation, ext string, entries ...store.OutboxEntry) *ingestGroup {
	return &ingestGroup{operation: op, externalIDField: ext, entries: entries}
}

func TestGroupEntries(t *testing.T) {
	entries := []store.OutboxEntry{
		entry(1, store.Insert, `{}`),
		{ID: 2, Operation: store.Upsert, ExternalIDField: "Ext_Id__c"},
		entry(3, store.Insert, `{}`),
		{ID: 4, Operation: store.Upsert, ExternalIDField: "Other__c"},
		entry(5, store.Delete, `{}`),
	}

	groups := groupEntries(entries)

	require.Len(t, groups, 4)
	assert.Equal(t, store.Insert, groups[0].operation)
	assert.Equal(t, []int64{1, 3}, entryIDs(groups[0].entries))
	assert.Equal(t, "Ext_Id__c", groups[1].externalIDField)
	assert.Equal(t, "Other__c", groups[2].externalIDField)
	assert.Equal(t, store.Delete, groups[3].operation)
}

func entryIDs(entries []store.OutboxEntry) []int64 {
	ids := make([]int64, len(entries))
	for i, e := range entries {
		ids[i] = e.ID
	}
	return ids
}

func TestPlanIngestLaysOutOneCSV(t *testing.T) {
	g := group(store.Update, "",
		entry(1, store.Update, `{"id":"003A","LastName":"Doe, \"JD\"","NumberOfLegs__c":2.5}`),
		entry(2, store.Update, `{"Id":"003B","DoNotCall":true,"Email":null,"LastName":""}`),
	)

	plan, err := planIngest(contactDescription, g, salesforce.MaxIngestUpload)

	require.NoError(t, err)
	assert.Empty(t, plan.rejected)
	require.Len(t, plan.batches, 1)
	b := plan.batches[0]
	assert.Equal(t, []string{"Id", "DoNotCall", "Email", "LastName", "NumberOfLegs__c"}, b.columns,
		"the key first, the rest sorted, names as described")
	assert.Equal(t, []int64{1, 2}, b.entryIDs)
	assert.Equal(t, "Id,DoNotCall,Email,LastName,NumberOfLegs__c\n"+
		"003A,,,\"Doe, \"\"JD\"\"\",2.5\n"+
		"003B,true,#N/A,#N/A,\n", string(b.data),
		"absent fields stay empty and unchanged; null and empty strings clear the field")
}

func TestPlanIngestRejectsEntriesThatCannotSucceed(t *testing.T) {
	tests := []struct {
		name    string
		op      store.Operation
		ext     string
		payload string
		wantErr string
	}{
		{"unknown field", store.Insert, "", `{"LastName":"Doe","Shoe_Size__c":9}`, `field "Shoe_Size__c" does not exist`},
		{"read-only field", store.Insert, "", `{"LastName":"Doe","CreatedDate":"2026-01-01T00:00:00Z"}`, "CreatedDate is not writable by insert"},
		{"create-only field on update", store.Update, "", `{"Id":"003A","Seeded__c":"x"}`, "Seeded__c is not writable by update"},
		{"Id on insert", store.Insert, "", `{"Id":"003A","LastName":"Doe"}`, "Id is not writable by insert"},
		{"compound field", store.Insert, "", `{"MailingAddress":"Main St"}`, "MailingAddress of type address cannot be written"},
		{"nested value", store.Insert, "", `{"LastName":{"first":"J"}}`, "LastName: objects and arrays are not supported"},
		{"same field twice", store.Insert, "", `{"LastName":"A","lastname":"B"}`, "LastName is given twice"},
		{"update without Id", store.Update, "", `{"LastName":"Doe"}`, "update requires a value for Id"},
		{"delete with null Id", store.Delete, "", `{"Id":null}`, "delete requires a value for Id"},
		{"upsert without key", store.Upsert, "Ext_Id__c", `{"LastName":"Doe"}`, "upsert requires a value for Ext_Id__c"},
		{"empty payload", store.Insert, "", `{}`, "payload sets no fields"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := entry(7, tt.op, tt.payload)
			e.ExternalIDField = tt.ext

			plan, err := planIngest(contactDescription, group(tt.op, tt.ext, e), salesforce.MaxIngestUpload)

			require.NoError(t, err)
			assert.Empty(t, plan.batches)
			require.Len(t, plan.rejected, 1)
			assert.Equal(t, int64(7), plan.rejected[0].EntryID)
			assert.Equal(t, store.Rejected, plan.rejected[0].Kind)
			assert.Contains(t, plan.rejected[0].Error, tt.wantErr)
		})
	}
}

func TestPlanIngestUpsertAndDelete(t *testing.T) {
	upsert, err := planIngest(contactDescription, group(store.Upsert, "ext_id__c",
		entry(1, store.Upsert, `{"LastName":"Doe","Ext_Id__c":42}`),
	), salesforce.MaxIngestUpload)
	require.NoError(t, err)
	require.Len(t, upsert.batches, 1)
	assert.Equal(t, "Ext_Id__c,LastName\n42,Doe\n", string(upsert.batches[0].data))

	missing, err := planIngest(contactDescription, group(store.Upsert, "Nope__c",
		entry(1, store.Upsert, `{"Nope__c":"x"}`), entry(2, store.Upsert, `{"Nope__c":"y"}`),
	), salesforce.MaxIngestUpload)
	require.NoError(t, err)
	assert.Len(t, missing.rejected, 2)
	assert.Contains(t, missing.rejected[0].Error, `external ID field "Nope__c" does not exist`)

	del, err := planIngest(contactDescription, group(store.Delete, "",
		entry(1, store.Delete, `{"Id":"003A","LastName":"ignored"}`),
	), salesforce.MaxIngestUpload)
	require.NoError(t, err)
	require.Len(t, del.batches, 1)
	assert.Equal(t, "Id\n003A\n", string(del.batches[0].data), "a delete needs only the Id")
}

func TestPlanIngestSplitsLargeUploads(t *testing.T) {
	var entries []store.OutboxEntry
	for i := range 5 {
		entries = append(entries, entry(int64(i+1), store.Insert, `{"LastName":"`+strings.Repeat("x", 10)+`"}`))
	}
	entries = append(entries, entry(6, store.Insert, `{"LastName":"`+strings.Repeat("y", 100)+`"}`))

	// The header takes 9 bytes and every regular row 11, so two rows fit.
	plan, err := planIngest(contactDescription, group(store.Insert, "", entries...), 32)

	require.NoError(t, err)
	require.Len(t, plan.batches, 3)
	assert.Equal(t, []int64{1, 2}, plan.batches[0].entryIDs)
	assert.Equal(t, []int64{3, 4}, plan.batches[1].entryIDs)
	assert.Equal(t, []int64{5}, plan.batches[2].entryIDs)
	for _, b := range plan.batches {
		assert.LessOrEqual(t, len(b.data), 32)
		assert.True(t, strings.HasPrefix(string(b.data), "LastName\n"), "every upload has the header")
	}
	require.Len(t, plan.rejected, 1)
	assert.Equal(t, int64(6), plan.rejected[0].EntryID)
	assert.Contains(t, plan.rejected[0].Error, "larger than the 32 byte upload limit")
}

func TestResultMatcher(t *testing.T) {
	b := &ingestBatch{
		columns:  []string{"Id", "LastName"},
		entryIDs: []int64{10, 11, 12, 13},
		rows:     [][]string{{"003A", "Doe"}, {"003B", "Roe"}, {"003C", "Same"}, {"003C", "Same"}},
	}
	m := newResultMatcher(b)
	got := map[int64]string{}
	collect := func(id int64, row map[string]string) { got[id] = row["sf__Id"] + "/" + row["sf__Error"] }

	require.NoError(t, m.read(strings.NewReader(
		"sf__Id,sf__Created,LASTNAME,Id\n"+
			"003B,false,Roe,003B\n"+
			"003C,false,Same,003C\n"+
			"003Z,false,Stranger,003Z\n"), collect))
	require.NoError(t, m.read(strings.NewReader("sf__Id,sf__Error,Id,LastName\n"), collect))
	require.NoError(t, m.read(strings.NewReader(""), collect))

	assert.Equal(t, map[int64]string{11: "003B/", 12: "003C/"}, got,
		"matched by values in any order and column case; equal rows are interchangeable")
	assert.Equal(t, []int64{10, 13}, m.unmatched())

	err := m.read(strings.NewReader("sf__Id,sf__Error,Id\n"), collect)
	assert.ErrorContains(t, err, "result has no column LastName")
}
