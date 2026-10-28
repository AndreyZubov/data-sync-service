package syncer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AndreyZubov/data-sync-service/internal/salesforce/sftest"
	"github.com/AndreyZubov/data-sync-service/internal/store"
	"github.com/AndreyZubov/data-sync-service/internal/store/storetest"
)

func newEnvWithUsageThreshold(t *testing.T, threshold float64) *env {
	t.Helper()
	org := sftest.NewServer(t)
	org.SetDescribe(accountDescribe)
	return newEnvOn(t, org, storetest.New(t), Config{MaxConcurrentRuns: 1, APIUsageThreshold: threshold})
}

func TestRunsWaitWhileOrgAPIUsageIsHigh(t *testing.T) {
	e := newEnvWithUsageThreshold(t, 0.8)
	e.org.SetAPIUsage(13500, 15000)

	first := e.export(t, false)
	require.Equal(t, store.Succeeded, first.Status, "usage is unknown until Salesforce first reports it")

	_, err := e.orch.StartExport(context.Background(), "Account", store.Manual, false)
	var usage *APIUsageError
	require.ErrorAs(t, err, &usage)
	assert.Equal(t, int64(13500), usage.Usage.Used)
	assert.InDelta(t, 0.8, usage.Threshold, 0)
	assert.Equal(t, apiUsageRefreshAfter, usage.RetryAfter)
	assert.EqualError(t, err, "the org has used 13500 of 15000 daily API requests (90%), above the 80% threshold")

	e.orch.scheduledSync("Account")
	assert.Equal(t, 1, e.count(t, `SELECT count(*) FROM sync_runs`), "scheduled runs are skipped too")

	// Usage drops as old requests leave the 24-hour window; a stale report
	// is refreshed before a run is refused.
	e.org.SetAPIUsage(9000, 15000)
	e.orch.usageRefreshAfter = 0
	run, err := e.orch.StartExport(context.Background(), "Account", store.Manual, false)
	require.NoError(t, err)
	e.waitForStatus(t, run.ID, store.Succeeded)
}

func TestAPIUsageCheckCanBeDisabled(t *testing.T) {
	e := newEnvWithUsageThreshold(t, 0)
	e.org.SetAPIUsage(15000, 15000)
	require.Equal(t, store.Succeeded, e.export(t, false).Status)

	assert.Equal(t, store.Succeeded, e.export(t, false).Status)
}
