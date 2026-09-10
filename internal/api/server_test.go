package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// newTestServer builds a Server with a logger bound to the test.
func newTestServer(t *testing.T) http.Handler {
	t.Helper()
	return NewServer(zaptest.NewLogger(t)).Handler()
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

func TestMetricsEndpoint(t *testing.T) {
	rec := do(t, newTestServer(t), http.MethodGet, "/metrics")

	require.Equal(t, http.StatusOK, rec.Code)
	// The Go collector is registered by default, so this metric is always present.
	assert.Contains(t, rec.Body.String(), "go_goroutines")
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
	s := NewServer(zaptest.NewLogger(t))
	s.router.Get("/boom", func(http.ResponseWriter, *http.Request) {
		panic("boom")
	})

	rec := do(t, s.Handler(), http.MethodGet, "/boom")
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}
