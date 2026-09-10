// Package api exposes the HTTP control surface for the sync service.
package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// Server wires the chi router and its dependencies.
type Server struct {
	log    *zap.Logger
	router chi.Router
}

// NewServer builds the router with control, health, and metrics endpoints.
func NewServer(log *zap.Logger) *Server {
	s := &Server{log: log}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	// middleware.RealIP is deliberately not used: it trusts client-supplied
	// X-Forwarded-For / X-Real-IP headers and is spoofable unless a trusted
	// proxy sets them (GHSA-3fxj-6jh8-hvhx).
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))

	// Sync control (§8) — stubs until the pipelines land.
	r.Post("/sync/{object}/export", s.handleExport)
	r.Post("/sync/{object}/import", s.handleImport)
	r.Get("/runs/{id}", s.handleRun)

	// Health & metrics.
	r.Get("/healthz", s.handleHealthz)
	r.Get("/readyz", s.handleReadyz)
	r.Handle("/metrics", promhttp.Handler())

	s.router = r
	return s
}

// Handler exposes the router as an http.Handler.
func (s *Server) Handler() http.Handler { return s.router }
