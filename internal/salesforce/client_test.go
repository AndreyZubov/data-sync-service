package salesforce

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/AndreyZubov/data-sync-service/internal/retry"
	"github.com/AndreyZubov/data-sync-service/internal/salesforce/sftest"
)

// fastRetry keeps retry tests quick while exercising real backoff.
var fastRetry = retry.Policy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 50 * time.Millisecond}

func newTestClient(t *testing.T, org *sftest.Server) *Client {
	t.Helper()
	tokens, _ := newProvider(org, time.Hour)
	return NewClient(ClientConfig{
		Tokens:     tokens,
		APIVersion: sftest.APIVersion,
		HTTPClient: org.HTTPClient(),
		Retry:      fastRetry,
		Logger:     zaptest.NewLogger(t),
	})
}

// sequence answers successive requests with the given statuses, then 200.
// It returns the handler and a counter of requests received.
func sequence(statuses ...int) (http.HandlerFunc, *atomic.Int32) {
	var calls atomic.Int32
	return func(w http.ResponseWriter, _ *http.Request) {
		n := int(calls.Add(1))
		if n <= len(statuses) {
			sftest.WriteError(w, statuses[n-1], "TEST_ERROR", "injected failure")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}, &calls
}

func TestDoSendsAuthenticatedRequestToTheDataAPI(t *testing.T) {
	org := sftest.NewServer(t)
	var got *http.Request
	org.Handle("GET /jobs/query/750x/results", func(w http.ResponseWriter, r *http.Request) {
		got = r
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write([]byte("Id\n001\n"))
	})
	c := newTestClient(t, org)

	resp, err := c.Do(context.Background(), Request{
		Method: http.MethodGet,
		Path:   "/jobs/query/750x/results",
		Query:  map[string][]string{"locator": {"abc"}, "maxRecords": {"1000"}},
		Header: http.Header{"Accept": {"text/csv"}},
	})
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Equal(t, "Id\n001\n", string(body))
	assert.Equal(t, "abc", got.URL.Query().Get("locator"))
	assert.Equal(t, "text/csv", got.Header.Get("Accept"))
	assert.Equal(t, userAgent, got.Header.Get("User-Agent"))
	assert.Regexp(t, `^Bearer \S+$`, got.Header.Get("Authorization"))
}

func TestDoJSONRoundTrip(t *testing.T) {
	org := sftest.NewServer(t)
	org.Handle("POST /jobs/ingest", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		var in map[string]string
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&in))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"id": "750x", "query": in["query"]})
	})
	c := newTestClient(t, org)

	var out struct {
		ID    string `json:"id"`
		Query string `json:"query"`
	}
	err := c.DoJSON(context.Background(), http.MethodPost, "/jobs/ingest",
		map[string]string{"query": "SELECT Id FROM Account"}, &out)

	require.NoError(t, err)
	assert.Equal(t, "750x", out.ID)
	assert.Equal(t, "SELECT Id FROM Account", out.Query)
}

func TestDoJSONWithoutBodies(t *testing.T) {
	org := sftest.NewServer(t)
	handler, calls := sequence()
	org.Handle("DELETE /jobs/query/750x", handler)
	c := newTestClient(t, org)

	require.NoError(t, c.DoJSON(context.Background(), http.MethodDelete, "/jobs/query/750x", nil, nil))
	assert.EqualValues(t, 1, calls.Load())
}

func TestDoJSONReportsUndecodableResponse(t *testing.T) {
	org := sftest.NewServer(t)
	org.Handle("GET /limits", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not json"))
	})
	c := newTestClient(t, org)

	var out map[string]any
	err := c.DoJSON(context.Background(), http.MethodGet, "/limits", nil, &out)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode GET /limits response")
}

func TestDoReauthenticatesOnceAfterRevokedSession(t *testing.T) {
	org := sftest.NewServer(t)
	handler, calls := sequence()
	org.Handle("GET /limits", handler)
	c := newTestClient(t, org)
	ctx := context.Background()

	require.NoError(t, c.DoJSON(ctx, http.MethodGet, "/limits", nil, nil))
	org.RevokeTokens()
	require.NoError(t, c.DoJSON(ctx, http.MethodGet, "/limits", nil, nil))

	assert.Equal(t, 2, org.TokenRequests(), "one token per session")
	assert.EqualValues(t, 2, calls.Load(), "the rejected request never reached the handler")
}

func TestDoGivesUpWhenAFreshTokenIsAlsoRejected(t *testing.T) {
	org := sftest.NewServer(t)
	handler, calls := sequence(http.StatusUnauthorized, http.StatusUnauthorized, http.StatusUnauthorized)
	org.Handle("GET /limits", handler)
	c := newTestClient(t, org)

	err := c.DoJSON(context.Background(), http.MethodGet, "/limits", nil, nil)

	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusUnauthorized, apiErr.StatusCode)
	assert.EqualValues(t, 2, calls.Load(), "original attempt plus one re-authentication")
}

func TestDoRetryPolicy(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		statuses   []int
		wantErr    bool
		wantStatus int
		wantCalls  int32
	}{
		{name: "503 then success", method: http.MethodPost, statuses: []int{503}, wantCalls: 2},
		{name: "429 then success", method: http.MethodPost, statuses: []int{429}, wantCalls: 2},
		{name: "500 on GET is retried", method: http.MethodGet, statuses: []int{500, 502}, wantCalls: 3},
		{name: "500 on POST is not retried", method: http.MethodPost, statuses: []int{500}, wantErr: true, wantStatus: 500, wantCalls: 1},
		{name: "400 is final", method: http.MethodGet, statuses: []int{400}, wantErr: true, wantStatus: 400, wantCalls: 1},
		{name: "403 daily limit is final", method: http.MethodGet, statuses: []int{403}, wantErr: true, wantStatus: 403, wantCalls: 1},
		{name: "attempts run out", method: http.MethodGet, statuses: []int{503, 503, 503, 503}, wantErr: true, wantStatus: 503, wantCalls: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			org := sftest.NewServer(t)
			handler, calls := sequence(tt.statuses...)
			org.Handle(tt.method+" /jobs/ingest", handler)
			c := newTestClient(t, org)

			err := c.DoJSON(context.Background(), tt.method, "/jobs/ingest", nil, nil)

			assert.Equal(t, tt.wantCalls, calls.Load())
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			var apiErr *APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantStatus, apiErr.StatusCode)
			assert.Equal(t, "TEST_ERROR", apiErr.Code)
		})
	}
}

func TestDoHonoursRetryAfter(t *testing.T) {
	t.Run("within the policy", func(t *testing.T) {
		org := sftest.NewServer(t)
		var calls atomic.Int32
		org.Handle("GET /limits", func(w http.ResponseWriter, _ *http.Request) {
			if calls.Add(1) == 1 {
				w.Header().Set("Retry-After", "0")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			_, _ = w.Write([]byte(`{}`))
		})

		require.NoError(t, newTestClient(t, org).DoJSON(context.Background(), http.MethodGet, "/limits", nil, nil))
		assert.EqualValues(t, 2, calls.Load())
	})

	t.Run("beyond the policy gives up", func(t *testing.T) {
		org := sftest.NewServer(t)
		var calls atomic.Int32
		org.Handle("GET /limits", func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			w.Header().Set("Retry-After", "3600")
			w.WriteHeader(http.StatusServiceUnavailable)
		})

		err := newTestClient(t, org).DoJSON(context.Background(), http.MethodGet, "/limits", nil, nil)

		var apiErr *APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, http.StatusServiceUnavailable, apiErr.StatusCode)
		assert.EqualValues(t, 1, calls.Load())
	})
}

func TestDoReplaysTheBodyOnRetry(t *testing.T) {
	org := sftest.NewServer(t)
	var bodies []string
	org.Handle("PUT /jobs/ingest/750x/batches", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(raw))
		if len(bodies) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusCreated)
	})
	c := newTestClient(t, org)

	resp, err := c.Do(context.Background(), Request{
		Method: http.MethodPut,
		Path:   "/jobs/ingest/750x/batches",
		Header: http.Header{"Content-Type": {"text/csv"}},
		Body:   func() (io.Reader, error) { return strings.NewReader("Name\nAcme\n"), nil },
	})
	require.NoError(t, err)
	_ = resp.Body.Close()

	assert.Equal(t, []string{"Name\nAcme\n", "Name\nAcme\n"}, bodies)
}

func TestDoRetriesNetworkErrorsOnlyForIdempotentMethods(t *testing.T) {
	tests := []struct {
		method    string
		wantErr   bool
		wantCalls int32
	}{
		{method: http.MethodGet, wantErr: false, wantCalls: 2},
		{method: http.MethodPost, wantErr: true, wantCalls: 1},
	}

	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			org := sftest.NewServer(t)
			var calls atomic.Int32
			org.Handle(tt.method+" /limits", func(w http.ResponseWriter, _ *http.Request) {
				if calls.Add(1) == 1 {
					panic(http.ErrAbortHandler) // drops the connection mid-request
				}
				_, _ = w.Write([]byte(`{}`))
			})

			err := newTestClient(t, org).DoJSON(context.Background(), tt.method, "/limits", nil, nil)

			assert.Equal(t, tt.wantCalls, calls.Load())
			if tt.wantErr {
				require.Error(t, err)
				var apiErr *APIError
				assert.False(t, errors.As(err, &apiErr), "a transport error is not an API error")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDoStopsRetryingWhenTheContextEnds(t *testing.T) {
	org := sftest.NewServer(t)
	handler, _ := sequence(503, 503, 503)
	org.Handle("GET /limits", handler)
	tokens, _ := newProvider(org, time.Hour)
	c := NewClient(ClientConfig{
		Tokens:     tokens,
		APIVersion: sftest.APIVersion,
		HTTPClient: org.HTTPClient(),
		Retry:      retry.Policy{MaxAttempts: 3, BaseDelay: time.Hour, MaxDelay: time.Hour},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := c.DoJSON(ctx, http.MethodGet, "/limits", nil, nil)

	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestDoReportsTokenFailures(t *testing.T) {
	org := sftest.NewServer(t)
	org.OverrideTokenResponse(http.StatusBadRequest, `{"error":"invalid_client","error_description":"bad"}`)
	c := newTestClient(t, org)

	err := c.DoJSON(context.Background(), http.MethodGet, "/limits", nil, nil)

	var authErr *AuthError
	require.ErrorAs(t, err, &authErr)
	assert.Equal(t, "invalid_client", authErr.Code)
}

func TestReadAPIErrorFormats(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		wantCode string
		wantMsg  string
	}{
		{name: "array", body: `[{"errorCode":"INVALIDJOB","message":"Job not found"}]`, wantCode: "INVALIDJOB", wantMsg: "Job not found"},
		{name: "object", body: `{"errorCode":"NOT_FOUND","message":"No such job"}`, wantCode: "NOT_FOUND", wantMsg: "No such job"},
		{name: "plain text", body: "  upstream failure \n", wantMsg: "upstream failure"},
		{name: "empty", body: "", wantMsg: "Bad Request"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &http.Response{
				StatusCode: http.StatusBadRequest,
				Body:       io.NopCloser(strings.NewReader(tt.body)),
			}

			apiErr := readAPIError(resp)

			assert.Equal(t, tt.wantCode, apiErr.Code)
			assert.Equal(t, tt.wantMsg, apiErr.Message)
			assert.Contains(t, apiErr.Error(), tt.wantMsg)
		})
	}
}

func TestTruncateKeepsValidUTF8(t *testing.T) {
	assert.Equal(t, "short", truncate("short", 10))
	assert.Equal(t, "ab…", truncate("abcdef", 2))
	assert.Equal(t, "…", truncate("ё", 1), "a split rune is dropped, not emitted half")
}
