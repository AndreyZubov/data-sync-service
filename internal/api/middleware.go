package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// unmatchedRoute labels requests that hit no route, so arbitrary client paths
// cannot inflate metric cardinality.
const unmatchedRoute = "unmatched"

// probeRoutes are polled continuously by orchestrators and scrapers; their
// successful requests are logged at debug level to keep info logs readable.
var probeRoutes = map[string]bool{
	"/healthz": true,
	"/readyz":  true,
	"/metrics": true,
}

// httpMetrics holds the Prometheus collectors for the control API.
type httpMetrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func newHTTPMetrics(reg prometheus.Registerer) *httpMetrics {
	m := &httpMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Control API requests by method, route pattern, and status code.",
		}, []string{"method", "route", "status"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Control API request latency by method and route pattern.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route"}),
	}
	reg.MustRegister(m.requests, m.duration)
	return m
}

// observe logs each request and records its metrics once the response is done.
func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

		next.ServeHTTP(ww, r)

		elapsed := time.Since(start)
		status := ww.Status()
		if status == 0 {
			// The handler wrote no header; net/http sends 200 in that case.
			status = http.StatusOK
		}
		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			route = unmatchedRoute
		}

		s.metrics.requests.WithLabelValues(r.Method, route, strconv.Itoa(status)).Inc()
		s.metrics.duration.WithLabelValues(r.Method, route).Observe(elapsed.Seconds())

		level := zapcore.InfoLevel
		switch {
		case status >= http.StatusInternalServerError:
			level = zapcore.ErrorLevel
		case probeRoutes[route] && status < http.StatusBadRequest:
			level = zapcore.DebugLevel
		}
		s.accessLog.Log(level, "http request",
			zap.String("request_id", middleware.GetReqID(r.Context())),
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.String("route", route),
			zap.Int("status", status),
			zap.Int("bytes", ww.BytesWritten()),
			zap.Duration("duration", elapsed),
		)
	})
}
