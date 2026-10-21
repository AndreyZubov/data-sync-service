package sftest

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
)

// IngestFunc decides the outcome of one record of an ingest job: the
// record's Id on success, or an error whose message becomes the record's
// sf__Error. record maps the uploaded column names to the row's values.
type IngestFunc func(job IngestJob, record map[string]string) (id string, err error)

// IngestJob is an ingest job the org has received.
type IngestJob struct {
	ID              string
	Object          string
	Operation       string
	ExternalIDField string
	State           string
	// Header and Rows are the uploaded CSV.
	Header []string
	Rows   [][]string
}

type ingestJob struct {
	IngestJob
	uploaded     bool
	errorMessage string
	// Result rows, uploaded columns last, as Salesforce reports them.
	successful, failed, unprocessed [][]string
}

// OnIngest sets the function that decides the outcome of every record of
// ingest jobs completing from now on. Without one, every record succeeds:
// inserts get a new Id, and other operations report the uploaded Id.
func (s *Server) OnIngest(fn IngestFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onIngest = fn
}

// FailIngestJobs makes ingest jobs completing from now on fail as a whole
// with message, leaving every record unprocessed; an empty message lets them
// complete again.
func (s *Server) FailIngestJobs(message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ingestFailure = message
}

// IngestJobs returns every ingest job the org has received, oldest first.
func (s *Server) IngestJobs() []IngestJob {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]IngestJob, 0, len(s.ingestOrder))
	for _, id := range s.ingestOrder {
		out = append(out, s.ingestJobs[id].IngestJob)
	}
	return out
}

func (s *Server) handleCreateIngestJob(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Object          string `json:"object"`
		Operation       string `json:"operation"`
		ExternalIDField string `json:"externalIdFieldName"`
		ContentType     string `json:"contentType"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Object == "" || in.ContentType != "CSV" {
		WriteError(w, http.StatusBadRequest, "INVALIDJOB", "invalid ingest job request")
		return
	}
	if !slices.Contains([]string{"insert", "update", "upsert", "delete"}, in.Operation) {
		WriteError(w, http.StatusBadRequest, "INVALIDJOB", "unsupported operation "+in.Operation)
		return
	}
	if (in.Operation == "upsert") != (in.ExternalIDField != "") {
		WriteError(w, http.StatusBadRequest, "INVALIDJOB", "externalIdFieldName is required for upsert only")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	job := &ingestJob{IngestJob: IngestJob{
		ID:              fmt.Sprintf("750ingest%05d", len(s.ingestOrder)+1),
		Object:          in.Object,
		Operation:       in.Operation,
		ExternalIDField: in.ExternalIDField,
		State:           "Open",
	}}
	s.ingestJobs[job.ID] = job
	s.ingestOrder = append(s.ingestOrder, job.ID)
	writeJSON(w, http.StatusOK, job.view())
}

func (s *Server) handleUploadIngestData(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Content-Type") != "text/csv" {
		WriteError(w, http.StatusBadRequest, "INVALIDJOB", "content type must be text/csv")
		return
	}
	records, err := csv.NewReader(r.Body).ReadAll()
	if err != nil || len(records) == 0 {
		WriteError(w, http.StatusBadRequest, "INVALIDJOB", "invalid CSV")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.ingestJobs[r.PathValue("id")]
	switch {
	case !ok:
		WriteError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
	case job.State != "Open" || job.uploaded:
		WriteError(w, http.StatusConflict, "INVALIDJOBSTATE", "job does not accept data")
	default:
		job.Header, job.Rows, job.uploaded = records[0], records[1:], true
		w.WriteHeader(http.StatusCreated)
	}
}

func (s *Server) handleSetIngestState(w http.ResponseWriter, r *http.Request) {
	var in struct {
		State string `json:"state"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		WriteError(w, http.StatusBadRequest, "INVALIDJOBSTATE", "invalid state change")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.ingestJobs[r.PathValue("id")]
	if !ok {
		WriteError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
		return
	}
	switch {
	case in.State == "UploadComplete" && job.State == "Open" && job.uploaded:
		job.State = "UploadComplete"
	case in.State == "Aborted" && (job.State == "Open" || job.State == "UploadComplete" || job.State == "InProgress"):
		job.State = "Aborted"
		job.unprocessed = job.Rows
	default:
		WriteError(w, http.StatusBadRequest, "INVALIDJOBSTATE", "cannot move job from "+job.State+" to "+in.State)
		return
	}
	writeJSON(w, http.StatusOK, job.view())
}

// handleGetIngestJob advances the job one step per status request, like
// handleGetQueryJob, and processes its records on the last step.
func (s *Server) handleGetIngestJob(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.ingestJobs[r.PathValue("id")]
	if !ok {
		WriteError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
		return
	}
	switch {
	case job.State == "UploadComplete":
		job.State = "InProgress"
	case job.State == "InProgress" && !s.holdJobs:
		s.process(job)
	}
	writeJSON(w, http.StatusOK, job.view())
}

// process completes job. Result rows are listed in reverse upload order:
// Salesforce does not promise any order, and clients must not rely on one.
func (s *Server) process(job *ingestJob) {
	if s.ingestFailure != "" {
		job.State, job.errorMessage, job.unprocessed = "Failed", s.ingestFailure, job.Rows
		return
	}
	for i := len(job.Rows) - 1; i >= 0; i-- {
		row := job.Rows[i]
		record := make(map[string]string, len(row))
		for c, name := range job.Header {
			record[name] = row[c]
		}
		id, err := s.ingestOutcome(job, record)
		if err != nil {
			job.failed = append(job.failed, append([]string{id, err.Error()}, row...))
			continue
		}
		created := fmt.Sprint(job.Operation == "insert")
		job.successful = append(job.successful, append([]string{id, created}, row...))
	}
	job.State = "JobComplete"
}

func (s *Server) ingestOutcome(job *ingestJob, record map[string]string) (string, error) {
	if s.onIngest != nil {
		return s.onIngest(job.IngestJob, record)
	}
	if job.Operation == "insert" {
		s.issuedIDs++
		return fmt.Sprintf("001fake%011d", s.issuedIDs), nil
	}
	return record["Id"], nil
}

func (s *Server) handleIngestResults(set string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		job, ok := s.ingestJobs[r.PathValue("id")]
		if !ok {
			WriteError(w, http.StatusNotFound, "NOT_FOUND", "job not found")
			return
		}
		if job.State != "JobComplete" && job.State != "Failed" && job.State != "Aborted" {
			WriteError(w, http.StatusBadRequest, "INVALIDJOBSTATE", "job is not finished")
			return
		}

		var header []string
		var rows [][]string
		switch set {
		case "successfulResults":
			header, rows = append([]string{"sf__Id", "sf__Created"}, job.Header...), job.successful
		case "failedResults":
			header, rows = append([]string{"sf__Id", "sf__Error"}, job.Header...), job.failed
		default:
			header, rows = job.Header, job.unprocessed
		}
		w.Header().Set("Content-Type", "text/csv")
		_ = csv.NewWriter(w).WriteAll(append([][]string{header}, rows...))
	}
}

func (j *ingestJob) view() map[string]any {
	v := map[string]any{
		"id":        j.ID,
		"object":    j.Object,
		"operation": j.Operation,
		"state":     j.State,
	}
	if j.ExternalIDField != "" {
		v["externalIdFieldName"] = j.ExternalIDField
	}
	if j.errorMessage != "" {
		v["errorMessage"] = j.errorMessage
	}
	if j.State == "JobComplete" {
		v["numberRecordsProcessed"] = len(j.Rows)
		v["numberRecordsFailed"] = len(j.failed)
	}
	return v
}
