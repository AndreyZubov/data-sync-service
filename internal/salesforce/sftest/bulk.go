package sftest

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

// defaultPageSize is small so that tests exercise result pagination with a
// handful of rows.
const defaultPageSize = 2

// Describe is the describe result of one object.
type Describe struct {
	Name      string          `json:"name"`
	Queryable bool            `json:"queryable"`
	Fields    []DescribeField `json:"fields"`
}

// DescribeField is one field of a Describe.
type DescribeField struct {
	Name                string `json:"name"`
	Type                string `json:"type"`
	DeprecatedAndHidden bool   `json:"deprecatedAndHidden,omitempty"`
}

// QueryFunc produces the result of a query job: a header row of field names
// followed by data rows, or an error that fails the job with its message.
type QueryFunc func(operation, soql string) ([][]string, error)

// QueryJob is a query job the org has received.
type QueryJob struct {
	ID        string
	Operation string
	Query     string
	State     string
}

type queryJob struct {
	QueryJob
	errorMessage string
	rows         [][]string // header first
}

// SetDescribe makes the org answer describe requests for d.Name with d.
func (s *Server) SetDescribe(d Describe) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.describes[d.Name] = d
}

// OnQuery sets the function that produces the results of query jobs created
// from now on. Without one, query jobs complete with no rows.
func (s *Server) OnQuery(fn QueryFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onQuery = fn
}

// SetResultPageSize sets how many rows each result page holds.
func (s *Server) SetResultPageSize(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pageSize = n
}

// HoldQueryJobs keeps query jobs in progress while hold is true, for tests of
// runs that must not finish yet.
func (s *Server) HoldQueryJobs(hold bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.holdJobs = hold
}

// QueryJobs returns every query job the org has received, oldest first.
func (s *Server) QueryJobs() []QueryJob {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]QueryJob, 0, len(s.jobOrder))
	for _, id := range s.jobOrder {
		out = append(out, s.jobs[id].QueryJob)
	}
	return out
}

func (s *Server) handleDescribe(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	d, ok := s.describes[r.PathValue("object")]
	s.mu.Unlock()
	if !ok {
		WriteError(w, http.StatusNotFound, "NOT_FOUND", "The requested resource does not exist")
		return
	}
	writeJSON(w, http.StatusOK, d)
}

func (s *Server) handleCreateQueryJob(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Operation   string `json:"operation"`
		Query       string `json:"query"`
		ContentType string `json:"contentType"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Query == "" {
		WriteError(w, http.StatusBadRequest, "INVALIDJOB", "invalid query job request")
		return
	}
	if in.Operation != "query" && in.Operation != "queryAll" {
		WriteError(w, http.StatusBadRequest, "INVALIDJOB", "unsupported operation "+in.Operation)
		return
	}

	s.mu.Lock()
	onQuery := s.onQuery
	s.mu.Unlock()
	job := &queryJob{QueryJob: QueryJob{Operation: in.Operation, Query: in.Query, State: "UploadComplete"}}
	if onQuery != nil {
		rows, err := onQuery(in.Operation, in.Query)
		if err != nil {
			job.errorMessage = err.Error()
		}
		job.rows = rows
	}

	s.mu.Lock()
	job.ID = fmt.Sprintf("750fake%08d", len(s.jobOrder)+1)
	s.jobs[job.ID] = job
	s.jobOrder = append(s.jobOrder, job.ID)
	resp := job.view()
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, resp)
}

// handleGetQueryJob advances the job one step per status request:
// UploadComplete, then InProgress, then JobComplete or Failed.
func (s *Server) handleGetQueryJob(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[r.PathValue("id")]
	if !ok {
		WriteError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
		return
	}
	switch {
	case job.State == "UploadComplete":
		job.State = "InProgress"
	case job.State == "InProgress" && !s.holdJobs:
		job.State = "JobComplete"
		if job.errorMessage != "" {
			job.State = "Failed"
		}
	}
	writeJSON(w, http.StatusOK, job.view())
}

func (s *Server) handleAbortQueryJob(w http.ResponseWriter, r *http.Request) {
	var in struct {
		State string `json:"state"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.State != "Aborted" {
		WriteError(w, http.StatusBadRequest, "INVALIDJOBSTATE", "only aborting is supported")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[r.PathValue("id")]
	if !ok {
		WriteError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
		return
	}
	if job.State == "JobComplete" || job.State == "Failed" {
		WriteError(w, http.StatusBadRequest, "INVALIDJOBSTATE", "job already finished")
		return
	}
	job.State = "Aborted"
	writeJSON(w, http.StatusOK, job.view())
}

// handleQueryResults serves one page per request. The locator is the index of
// the page's first data row; every page repeats the header row, as
// Salesforce's do. A job without rows yields an empty body.
func (s *Server) handleQueryResults(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	job, ok := s.jobs[r.PathValue("id")]
	var state string
	var rows [][]string
	if ok {
		state, rows = job.State, job.rows
	}
	pageSize := s.pageSize
	s.mu.Unlock()

	switch {
	case !ok:
		WriteError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
		return
	case state != "JobComplete":
		WriteError(w, http.StatusBadRequest, "INVALIDJOBSTATE", "job is not complete")
		return
	}

	start := 0
	if l := r.URL.Query().Get("locator"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n < 0 {
			WriteError(w, http.StatusBadRequest, "INVALID_LOCATOR", "invalid locator")
			return
		}
		start = n
	}

	var page [][]string
	next := "null"
	if len(rows) > 0 {
		data := rows[1:]
		end := min(start+pageSize, len(data))
		start = min(start, end)
		page = append([][]string{rows[0]}, data[start:end]...)
		if end < len(data) {
			next = strconv.Itoa(end)
		}
	}
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Sforce-Locator", next)
	w.Header().Set("Sforce-NumberOfRecords", strconv.Itoa(max(len(page)-1, 0)))
	_ = csv.NewWriter(w).WriteAll(page)
}

func (j *queryJob) view() map[string]any {
	v := map[string]any{
		"id":        j.ID,
		"operation": j.Operation,
		"state":     j.State,
	}
	if j.State == "Failed" {
		v["errorMessage"] = j.errorMessage
	}
	if j.State == "JobComplete" && len(j.rows) > 0 {
		v["numberRecordsProcessed"] = len(j.rows) - 1
	}
	return v
}
