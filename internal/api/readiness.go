package api

import (
	"context"
	"net/http"
	"time"

	"go.uber.org/zap"
)

// readinessCheckTimeout bounds each check, so one hanging dependency cannot
// stall the probe beyond what orchestrators wait for.
const readinessCheckTimeout = 5 * time.Second

// ReadinessCheck reports whether a dependency the service needs is usable.
type ReadinessCheck struct {
	// Name identifies the check in the /readyz response and in logs.
	Name string
	// Check returns nil when the dependency is usable.
	Check func(ctx context.Context) error
}

type readinessResponse struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

// handleReadyz reports readiness: 200 when every check passes, 503 otherwise.
// The response names failed checks but not their errors, which may describe
// internal systems; the errors are logged instead.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	resp := readinessResponse{Status: "ready"}
	status := http.StatusOK
	if len(s.checks) > 0 {
		resp.Checks = make(map[string]string, len(s.checks))
	}

	for _, c := range s.checks {
		ctx, cancel := context.WithTimeout(r.Context(), readinessCheckTimeout)
		err := c.Check(ctx)
		cancel()
		if err != nil {
			s.log.Warn("readiness check failed", zap.String("check", c.Name), zap.Error(err))
			resp.Checks[c.Name] = "fail"
			resp.Status = "not ready"
			status = http.StatusServiceUnavailable
			continue
		}
		resp.Checks[c.Name] = "ok"
	}

	writeJSON(w, status, resp)
}
