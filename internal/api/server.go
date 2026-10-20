// Package api exposes the HTTP control surface for the sync service.
package api

import (
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// RequestTimeout bounds how long a single control API request may run before
// the client receives 504 Gateway Timeout.
const RequestTimeout = 60 * time.Second

// Server wires the chi router and its dependencies.
type Server struct {
	log *zap.Logger
	// accessLog records one entry per request. Stack traces are disabled: at
	// error level they would only show the middleware chain, never the cause.
	accessLog *zap.Logger
	router    chi.Router
	metrics   *httpMetrics
	// objects maps lower-cased object names to their configured spelling;
	// Salesforce API names are case-insensitive.
	objects map[string]string
}

// NewServer builds the router with control, health, and metrics endpoints.
//
// HTTP metrics are registered on reg, and /metrics serves everything reg
// gathers. Registering two servers on the same registry panics.
// syncObjects lists the Salesforce objects the control API accepts; requests
// for any other object are answered with 404 Not Found.
func NewServer(log *zap.Logger, reg *prometheus.Registry, syncObjects []string) *Server {
	s := &Server{
		log:       log,
		accessLog: log.WithOptions(zap.AddStacktrace(zapcore.FatalLevel + 1)),
		metrics:   newHTTPMetrics(reg),
		objects:   make(map[string]string, len(syncObjects)),
	}
	for _, name := range syncObjects {
		s.objects[strings.ToLower(name)] = name
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	// middleware.RealIP is deliberately not used: it trusts client-supplied
	// X-Forwarded-For / X-Real-IP headers and is spoofable unless a trusted
	// proxy sets them (GHSA-3fxj-6jh8-hvhx).
	// observe sits outside Recoverer so recovered panics are logged and
	// counted with the 500 status Recoverer writes.
	r.Use(s.observe)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(RequestTimeout))

	// Sync control — stubs until the pipelines land.
	r.Post("/sync/{object}/export", s.handleExport)
	r.Post("/sync/{object}/import", s.handleImport)
	r.Get("/runs/{id}", s.handleRun)

	// Health & metrics.
	r.Get("/healthz", s.handleHealthz)
	r.Get("/readyz", s.handleReadyz)
	r.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))

	s.router = r
	return s
}

// Handler exposes the router as an http.Handler.
func (s *Server) Handler() http.Handler { return s.router }
