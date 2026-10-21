package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
)

// writeJSON renders v as a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// resolveObject maps the {object} path parameter to its configured name. If
// the object is not configured for sync it writes 404 and returns false, so
// unvetted input never reaches SOQL or SQL.
func (s *Server) resolveObject(w http.ResponseWriter, r *http.Request) (string, bool) {
	requested := chi.URLParam(r, "object")
	name, ok := s.objects[strings.ToLower(requested)]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{
			"object": requested,
			"error":  "object is not configured for sync",
		})
		return "", false
	}
	return name, true
}

// handleExport triggers an export run for {object}. Stub until the export
// pipeline lands (roadmap milestone 3).
func (s *Server) handleExport(w http.ResponseWriter, r *http.Request) {
	object, ok := s.resolveObject(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusNotImplemented, map[string]string{
		"object": object,
		"status": "export not implemented yet",
	})
}

// handleImport triggers an import run for {object}. Stub until the import
// pipeline lands (roadmap milestone 5).
func (s *Server) handleImport(w http.ResponseWriter, r *http.Request) {
	object, ok := s.resolveObject(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusNotImplemented, map[string]string{
		"object": object,
		"status": "import not implemented yet",
	})
}

// handleRun returns a run's status and counts. Stub until run records are
// persisted (roadmap milestone 3).
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
