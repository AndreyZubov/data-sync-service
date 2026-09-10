package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// writeJSON renders v as a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// handleExport triggers an export run for {object}. Stub until the export
// pipeline is implemented (plan stage 3).
func (s *Server) handleExport(w http.ResponseWriter, r *http.Request) {
	object := chi.URLParam(r, "object")
	writeJSON(w, http.StatusNotImplemented, map[string]string{
		"object": object,
		"status": "export not implemented yet",
	})
}

// handleImport triggers an import run for {object}. Stub until the import
// pipeline is implemented (plan stage 5).
func (s *Server) handleImport(w http.ResponseWriter, r *http.Request) {
	object := chi.URLParam(r, "object")
	writeJSON(w, http.StatusNotImplemented, map[string]string{
		"object": object,
		"status": "import not implemented yet",
	})
}

// handleRun returns a run's status and counts. Stub until sync_runs storage
// is implemented (plan stage 3).
func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	writeJSON(w, http.StatusNotImplemented, map[string]string{
		"id":     id,
		"status": "run lookup not implemented yet",
	})
}

// handleHealthz reports liveness — the process is up.
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReadyz reports readiness. Once token/DB checks exist (plan stage 6)
// this will gate on them; for now the process is always ready.
func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
