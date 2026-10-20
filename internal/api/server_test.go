package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"
)

// testObjects are the sync objects every test server is configured with.
var testObjects = []string{"Account", "Contact", "Invoice__c"}

// newTestServer builds a Server with a logger bound to the test and a fresh
// metrics registry.
func newTestServer(t *testing.T) http.Handler {
	t.Helper()
	return NewServer(zaptest.NewLogger(t), prometheus.NewRegistry(), testObjects).Handler()
}

// do issues a request against the router and returns the recorded response.
func do(t *testing.T, h http.Handler, method, target string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, target, nil))
	return rec
}

func TestHealthAndReadiness(t *testing.T) {
	h := newTestServer(t)

	tests := []struct {
		name   string
		path   string
		status string
	}{
		{name: "healthz", path: "/healthz", status: "ok"},
		{name: "readyz", path: "/readyz", status: "ready"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(t, h, http.MethodGet, tt.path)

			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

			var body map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Equal(t, tt.status, body["status"])
		})
	}
}

func TestMetricsEndpointServesGivenRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	marker := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_marker_total", Help: "marker"})
	reg.MustRegister(marker)
	h := NewServer(zaptest.NewLogger(t), reg, testObjects).Handler()

	do(t, h, http.MethodGet, "/healthz")
	rec := do(t, h, http.MethodGet, "/metrics")

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "test_marker_total")
	assert.Contains(t, body, `http_requests_total{method="GET",route="/healthz",status="200"} 1`)
	assert.Contains(t, body, "http_request_duration_seconds_bucket")
}

func TestHTTPMetricsUseRoutePatterns(t *testing.T) {
	reg := prometheus.NewRegistry()
	s := NewServer(zaptest.NewLogger(t), reg, testObjects)
	h := s.Handler()

	do(t, h, http.MethodGet, "/runs/1")
	do(t, h, http.MethodGet, "/runs/2")
	do(t, h, http.MethodGet, "/no/such/path")
	do(t, h, http.MethodGet, "/another/unknown")

	requests := s.metrics.requests
	assert.InDelta(t, 2, testutil.ToFloat64(requests.WithLabelValues("GET", "/runs/{id}", "501")), 0)
	assert.InDelta(t, 2, testutil.ToFloat64(requests.WithLabelValues("GET", unmatchedRoute, "404")), 0)
}

// The sync pipelines are not implemented yet: the routes must exist and answer
// 501 rather than 404, and must echo back the path parameter they received.
func TestSyncStubsReportNotImplemented(t *testing.T) {
	h := newTestServer(t)

	tests := []struct {
		name    string
		method  string
		target  string
		key     string
		wantVal string
	}{
		{name: "export", method: http.MethodPost, target: "/sync/Account/export", key: "object", wantVal: "Account"},
		{name: "import", method: http.MethodPost, target: "/sync/Contact/import", key: "object", wantVal: "Contact"},
		{name: "custom object", method: http.MethodPost, target: "/sync/Invoice__c/export", key: "object", wantVal: "Invoice__c"},
		{name: "case-insensitive name", method: http.MethodPost, target: "/sync/account/export", key: "object", wantVal: "Account"},
		{name: "run lookup", method: http.MethodGet, target: "/runs/abc-123", key: "id", wantVal: "abc-123"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(t, h, tt.method, tt.target)

			require.Equal(t, http.StatusNotImplemented, rec.Code)

			var body map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Equal(t, tt.wantVal, body[tt.key])
			assert.NotEmpty(t, body["status"])
		})
	}
}

func TestSyncRejectsUnconfiguredObjects(t *testing.T) {
	h := newTestServer(t)

	tests := []struct {
		name   string
		target string
	}{
		{name: "unknown object", target: "/sync/Opportunity/export"},
		{name: "unknown object on import", target: "/sync/Lead/import"},
		{name: "SOQL injection attempt", target: "/sync/Account%20WHERE%20Id%20!=%20null/export"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(t, h, http.MethodPost, tt.target)

			require.Equal(t, http.StatusNotFound, rec.Code)
			var body map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.NotEmpty(t, body["error"])
		})
	}
}

func TestRequestLogging(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	h := NewServer(zap.New(core), prometheus.NewRegistry(), testObjects).Handler()

	do(t, h, http.MethodPost, "/sync/Opportunity/export")
	do(t, h, http.MethodGet, "/healthz")
	do(t, h, http.MethodPost, "/sync/Account/export")

	entries := logs.FilterMessage("http request").AllUntimed()
	require.Len(t, entries, 3)

	export := entries[0].ContextMap()
	assert.Equal(t, zap.InfoLevel, entries[0].Level)
	assert.Equal(t, "/sync/{object}/export", export["route"])
	assert.Equal(t, "/sync/Opportunity/export", export["path"])
	assert.EqualValues(t, http.StatusNotFound, export["status"])
	assert.NotEmpty(t, export["request_id"])

	assert.Equal(t, zap.DebugLevel, entries[1].Level, "successful probes are logged at debug")
	assert.Equal(t, zap.ErrorLevel, entries[2].Level, "5xx responses are logged at error")
	assert.Empty(t, entries[2].Stack, "access log entries carry no stack trace")
}

func TestRoutingErrors(t *testing.T) {
	h := newTestServer(t)

	t.Run("unknown path is 404", func(t *testing.T) {
		assert.Equal(t, http.StatusNotFound, do(t, h, http.MethodGet, "/nope").Code)
	})

	t.Run("wrong method is 405", func(t *testing.T) {
		assert.Equal(t, http.StatusMethodNotAllowed, do(t, h, http.MethodGet, "/sync/Account/export").Code)
	})
}

// A panicking handler must be contained by middleware.Recoverer rather than
// taking the process down.
func TestRecovererContainsPanics(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	s := NewServer(zap.New(core), prometheus.NewRegistry(), testObjects)
	s.router.Get("/boom", func(http.ResponseWriter, *http.Request) {
		panic("boom")
	})

	rec := do(t, s.Handler(), http.MethodGet, "/boom")
	assert.Equal(t, http.StatusInternalServerError, rec.Code)

	// The panic is still observed: logged at error level and counted as a 500.
	entries := logs.FilterMessage("http request").AllUntimed()
	require.Len(t, entries, 1)
	assert.Equal(t, zap.ErrorLevel, entries[0].Level)
	assert.InDelta(t, 1, testutil.ToFloat64(s.metrics.requests.WithLabelValues("GET", "/boom", "500")), 0)
}
