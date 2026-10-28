package syncer

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AndreyZubov/data-sync-service/internal/store"
)

func TestCheckpointerCommitsPagesInOrder(t *testing.T) {
	at := func(hour int) *time.Time {
		ts := time.Date(2026, 10, 1, hour, 0, 0, 0, time.UTC)
		return &ts
	}
	var saved []store.ExportCheckpoint
	c := newCheckpointer(store.ExportCheckpoint{Locator: "start", Watermark: at(9), Records: 10},
		func(cp store.ExportCheckpoint) error {
			saved = append(saved, cp)
			return nil
		})

	first, second := c.open(), c.open()
	c.sent(first)
	c.sent(first)
	c.sent(second)
	require.NoError(t, c.close(first, "p2", 3, at(8)))
	require.NoError(t, c.close(second, "p3", 1, at(12)))

	require.NoError(t, c.written(second))
	require.NoError(t, c.written(first))
	assert.Empty(t, saved, "a page counts only once all of its batches are written")

	require.NoError(t, c.written(first))
	require.Len(t, saved, 1, "both pages commit together")
	assert.Equal(t, "p3", saved[0].Locator)
	assert.EqualValues(t, 14, saved[0].Records)
	assert.Equal(t, at(12), saved[0].Watermark)

	last := c.open()
	require.NoError(t, c.close(last, "", 0, nil))
	assert.Len(t, saved, 1, "the last page saves no checkpoint")
	assert.Equal(t, at(12), c.watermark())
}

func TestCheckpointerReportsSaveErrors(t *testing.T) {
	boom := errors.New("boom")
	c := newCheckpointer(store.ExportCheckpoint{}, func(store.ExportCheckpoint) error { return boom })

	assert.ErrorIs(t, c.close(c.open(), "p2", 0, nil), boom)
}
