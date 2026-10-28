package syncer

import (
	"context"
	"net/http"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AndreyZubov/data-sync-service/internal/salesforce/sftest"
	"github.com/AndreyZubov/data-sync-service/internal/store"
)

// peer returns another service instance on e's org and database, with its
// own store and therefore its own instance ID, using leaseTTL.
func (e *env) peer(t *testing.T, leaseTTL time.Duration) *env {
	t.Helper()
	st, err := store.Open(context.Background(), e.db.DSN, 0)
	require.NoError(t, err)
	t.Cleanup(st.Close)
	db := *e.db
	db.Store = st
	return newEnvOn(t, e.org, &db, Config{MaxConcurrentRuns: 1, LeaseTTL: leaseTTL})
}

func TestPeerTakesOverARunWhoseLeaseLapsed(t *testing.T) {
	a := newEnv(t, 1)
	a.org.HoldJobs(true)
	run, err := a.orch.StartExport(context.Background(), "Account", store.Manual, false)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		got, err := a.orch.GetRun(context.Background(), run.ID)
		require.NoError(t, err)
		return got.BulkJobID != ""
	}, 5*time.Second, 5*time.Millisecond)

	patient := a.peer(t, time.Hour)
	require.NoError(t, patient.orch.RecoverInterruptedRuns(context.Background()))
	got, err := a.orch.GetRun(context.Background(), run.ID)
	require.NoError(t, err)
	assert.Equal(t, a.db.Store.Instance(), got.Owner, "a live lease keeps peers off the run")

	// a stops renewing, as if it hung or lost its database connection; a
	// peer with a short lease takes the run over.
	b := a.peer(t, 100*time.Millisecond)
	require.NoError(t, b.orch.RecoverInterruptedRuns(context.Background()))
	require.Eventually(t, func() bool {
		got, err := b.orch.GetRun(context.Background(), run.ID)
		require.NoError(t, err)
		return got.Owner == b.db.Store.Instance()
	}, 5*time.Second, 10*time.Millisecond)

	// a learns of it at its next renewal and stops without touching the run.
	a.orch.renewLeases()
	require.Eventually(t, func() bool {
		a.orch.mu.Lock()
		defer a.orch.mu.Unlock()
		return len(a.orch.active) == 0
	}, 5*time.Second, 5*time.Millisecond)

	a.org.HoldJobs(false)
	got = b.waitForStatus(t, run.ID, store.Succeeded, store.Failed, store.Cancelled)
	require.Equal(t, store.Succeeded, got.Status, got.Error)
	assert.Equal(t, 1, got.Resumes)
	assert.Empty(t, got.Owner)
	assert.Len(t, a.org.QueryJobs(), 1, "the peer continues the same query job")
	assert.InDelta(t, 1, testutil.ToFloat64(b.orch.metrics.resumed.WithLabelValues("Account", "export")), 0)
}

func TestImportCollectsTheOutcomeOfAFailedRunsJob(t *testing.T) {
	e := newEnv(t, 1)
	e.org.SetDescribe(contactDescribe)
	// Salesforce becomes unreachable once the job is closed, so the first run
	// fails without learning whether the insert was applied.
	status := regexp.MustCompile(`/jobs/ingest/[^/]+$`)
	var unreachable atomic.Bool
	unreachable.Store(true)
	e.org.Intercept(func(w http.ResponseWriter, r *http.Request) bool {
		if unreachable.Load() && r.Method == http.MethodGet && status.MatchString(r.URL.Path) {
			sftest.WriteError(w, http.StatusServiceUnavailable, "SERVER_UNAVAILABLE", "try again later")
			return true
		}
		return false
	})
	id := e.addEntry(t, store.Insert, "", `{"LastName":"Doe"}`)

	first := e.importContacts(t)
	require.Equal(t, store.Failed, first.Status)
	assert.Equal(t, outboxState{status: store.Pending}, e.entry(t, id))

	unreachable.Store(false)
	second := e.importContacts(t)

	require.Equal(t, store.Succeeded, second.Status, second.Error)
	assert.Len(t, e.org.IngestJobs(), 1, "the insert is not uploaded again")
	got := e.entry(t, id)
	assert.Equal(t, store.Sent, got.status)
	assert.Equal(t, 1, got.attempts)
	assert.NotEmpty(t, got.sfID)
}
