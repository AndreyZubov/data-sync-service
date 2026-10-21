package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"

	"github.com/AndreyZubov/data-sync-service/internal/store"
	"github.com/AndreyZubov/data-sync-service/internal/syncer"
)

// writeJSON renders v as a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError renders an error message as a JSON response.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
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

// handleExport starts an export run of {object} and answers 202 Accepted with
// the queued run. The query parameter full=true ignores the watermark and
// re-reads every record.
func (s *Server) handleExport(w http.ResponseWriter, r *http.Request) {
	object, ok := s.resolveObject(w, r)
	if !ok {
		return
	}
	full := false
	if v := r.URL.Query().Get("full"); v != "" {
		var err error
		if full, err = strconv.ParseBool(v); err != nil {
			writeError(w, http.StatusBadRequest, "full must be true or false")
			return
		}
	}

	run, err := s.runs.StartExport(r.Context(), object, store.Manual, full)
	var inProgress *store.RunInProgressError
	switch {
	case err == nil:
		w.Header().Set("Location", "/runs/"+strconv.FormatInt(run.ID, 10))
		writeJSON(w, http.StatusAccepted, newRunResponse(run))
	case errors.As(err, &inProgress):
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":  "an export of this object is already in progress",
			"run_id": inProgress.RunID,
		})
	case errors.Is(err, syncer.ErrShuttingDown):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	default:
		s.log.Error("start export", zap.String("object", object), zap.Error(err))
		writeError(w, http.StatusInternalServerError, "could not start the export")
	}
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

// handleRun returns a run's status and record counts.
func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || id < 1 {
		writeError(w, http.StatusBadRequest, "run id must be a positive integer")
		return
	}
	run, err := s.runs.GetRun(r.Context(), id)
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, newRunResponse(run))
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "run not found")
	default:
		s.log.Error("get run", zap.Int64("run_id", id), zap.Error(err))
		writeError(w, http.StatusInternalServerError, "could not read the run")
	}
}

// handleHealthz reports liveness — the process is up.
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
