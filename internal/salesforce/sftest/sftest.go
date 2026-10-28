// Package sftest provides an in-process fake Salesforce org for tests: an
// OAuth client credentials token endpoint, object describes, Bulk API 2.0
// query and ingest jobs, and any further data API handlers tests register. It serves
// HTTPS, so production URL validation applies unchanged.
package sftest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// Connected app credentials and API version the fake org accepts.
const (
	ClientID     = "test-client-id"
	ClientSecret = "test-client-secret"
	APIVersion   = "60.0"
)

// defaultAPIMax is the org's daily API request allocation until a test sets
// another with SetAPIUsage.
const defaultAPIMax = 15000

// Server is a fake Salesforce org. Create it with NewServer.
type Server struct {
	srv *httptest.Server
	mux *http.ServeMux

	mu            sync.Mutex
	issued        int
	validTokens   map[string]bool
	tokenRequests int
	tokenOverride *tokenOverride

	apiUsed, apiMax int
	intercept       func(w http.ResponseWriter, r *http.Request) bool

	describes  map[string]Describe
	onQuery    QueryFunc
	pageSize   int
	holdJobs   bool
	jobs       map[string]*queryJob
	jobOrder   []string
	beforePage func(r *http.Request, jobID, locator string)

	onIngest      IngestFunc
	ingestFailure string
	issuedIDs     int
	ingestJobs    map[string]*ingestJob
	ingestOrder   []string
}

type tokenOverride struct {
	status int
	body   string
}

// NewServer starts a fake org that is shut down when the test ends.
func NewServer(t testing.TB) *Server {
	t.Helper()
	s := &Server{
		mux:         http.NewServeMux(),
		validTokens: make(map[string]bool),
		describes:   make(map[string]Describe),
		apiMax:      defaultAPIMax,
		pageSize:    defaultPageSize,
		jobs:        make(map[string]*queryJob),
		ingestJobs:  make(map[string]*ingestJob),
	}
	s.mux.HandleFunc("POST /services/oauth2/token", s.handleToken)
	s.Handle("GET /limits", s.handleLimits)
	s.Handle("GET /sobjects/{object}/describe", s.handleDescribe)
	s.Handle("POST /jobs/query", s.handleCreateQueryJob)
	s.Handle("GET /jobs/query/{id}", s.handleGetQueryJob)
	s.Handle("PATCH /jobs/query/{id}", s.handleAbortQueryJob)
	s.Handle("GET /jobs/query/{id}/results", s.handleQueryResults)
	s.Handle("POST /jobs/ingest", s.handleCreateIngestJob)
	s.Handle("PUT /jobs/ingest/{id}/batches", s.handleUploadIngestData)
	s.Handle("PATCH /jobs/ingest/{id}", s.handleSetIngestState)
	s.Handle("GET /jobs/ingest/{id}", s.handleGetIngestJob)
	for _, set := range []string{"successfulResults", "failedResults", "unprocessedrecords"} {
		s.Handle("GET /jobs/ingest/{id}/"+set, s.handleIngestResults(set))
	}
	s.srv = httptest.NewTLSServer(s.mux)
	t.Cleanup(s.srv.Close)
	return s
}

// URL is the org's base URL, used both as login URL and instance URL.
func (s *Server) URL() string { return s.srv.URL }

// HTTPClient returns a client that trusts the server's TLS certificate.
func (s *Server) HTTPClient() *http.Client { return s.srv.Client() }

// Handle registers h for a data API route. pattern is a method and a path
// relative to /services/data/vXX.X, e.g. "GET /limits". Requests without a
// currently valid access token are answered with 401 INVALID_SESSION_ID
// before h runs.
func (s *Server) Handle(pattern string, h http.HandlerFunc) {
	method, path, ok := strings.Cut(pattern, " ")
	if !ok {
		panic(fmt.Sprintf("sftest: pattern %q must be \"METHOD /path\"", pattern))
	}
	s.mux.HandleFunc(method+" /services/data/v"+APIVersion+path, s.requireToken(h))
}

// SetAPIUsage sets the org's API usage, which every data API response
// reports in its Sforce-Limit-Info header and GET /limits returns.
func (s *Server) SetAPIUsage(used, limit int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.apiUsed, s.apiMax = used, limit
}

// Intercept sets a function that sees every authenticated data API request
// first and answers it itself by returning true, for injecting failures into
// any route. nil removes it.
func (s *Server) Intercept(fn func(w http.ResponseWriter, r *http.Request) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.intercept = fn
}

// TokenRequests returns how many token requests the org has received.
func (s *Server) TokenRequests() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tokenRequests
}

// RevokeTokens invalidates every token issued so far, as when an admin ends
// all sessions of the integration user.
func (s *Server) RevokeTokens() {
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.validTokens)
}

// OverrideTokenResponse makes the token endpoint answer every request with
// status and body, without issuing a token, until called again with status 0.
func (s *Server) OverrideTokenResponse(status int, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if status == 0 {
		s.tokenOverride = nil
		return
	}
	s.tokenOverride = &tokenOverride{status: status, body: body}
}

// WriteError writes a Salesforce-style error: a JSON array with one entry.
func WriteError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, []map[string]string{{"errorCode": code, "message": message}})
}

func (s *Server) handleToken(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokenRequests++

	if o := s.tokenOverride; o != nil {
		w.WriteHeader(o.status)
		_, _ = w.Write([]byte(o.body))
		return
	}
	if err := r.ParseForm(); err != nil || r.PostForm.Get("grant_type") != "client_credentials" {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "unsupported_grant_type", "error_description": "grant type not supported",
		})
		return
	}
	if r.PostForm.Get("client_id") != ClientID || r.PostForm.Get("client_secret") != ClientSecret {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid_client", "error_description": "invalid client credentials",
		})
		return
	}

	s.issued++
	token := fmt.Sprintf("00Dfake!token-%d", s.issued)
	s.validTokens[token] = true
	writeJSON(w, http.StatusOK, map[string]string{
		"access_token": token,
		"instance_url": s.srv.URL,
		"token_type":   "Bearer",
		"scope":        "api",
	})
}

func (s *Server) requireToken(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		s.mu.Lock()
		valid := s.validTokens[token]
		s.mu.Unlock()
		if !valid {
			WriteError(w, http.StatusUnauthorized, "INVALID_SESSION_ID", "Session expired or invalid")
			return
		}
		s.mu.Lock()
		w.Header().Set("Sforce-Limit-Info", fmt.Sprintf("api-usage=%d/%d", s.apiUsed, s.apiMax))
		intercept := s.intercept
		s.mu.Unlock()
		if intercept != nil && intercept(w, r) {
			return
		}
		h(w, r)
	}
}

func (s *Server) handleLimits(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"DailyApiRequests": map[string]int{"Max": s.apiMax, "Remaining": s.apiMax - s.apiUsed},
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
