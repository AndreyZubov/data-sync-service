// Command syncd runs the Salesforce ⇄ Postgres bulk data sync service.
package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"go.uber.org/zap"

	"github.com/AndreyZubov/data-sync-service/internal/api"
	"github.com/AndreyZubov/data-sync-service/internal/config"
	"github.com/AndreyZubov/data-sync-service/internal/logging"
	"github.com/AndreyZubov/data-sync-service/internal/salesforce"
	"github.com/AndreyZubov/data-sync-service/internal/store"
	"github.com/AndreyZubov/data-sync-service/internal/syncer"
)

// version is stamped at build time via -ldflags "-X main.version=...".
var version = "dev"

// HTTP server timeouts. writeTimeout must outlast api.RequestTimeout, otherwise
// the connection is closed before a timed-out request receives its 504.
const (
	readHeaderTimeout = 10 * time.Second
	readTimeout       = 30 * time.Second
	writeTimeout      = api.RequestTimeout + 5*time.Second
	idleTimeout       = 120 * time.Second
)

func main() {
	if err := run(); err != nil {
		// Logger may not exist yet; stderr is the fallback.
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	log, err := logging.New(cfg.LogLevel)
	if err != nil {
		return err
	}
	defer func() { _ = log.Sync() }()

	log.Info("starting", zap.String("service", "syncd"), zap.String("version", version))

	// Listen for termination signals; the context is cancelled on the first one.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Binding before serving turns a busy or invalid address into a startup
	// error instead of a failure reported from a background goroutine.
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.HTTPAddr, err)
	}

	a, err := newApp(ctx, cfg, log, salesforce.NewHTTPClient())
	if err != nil {
		_ = ln.Close()
		return err
	}
	defer a.store.Close()

	if err := a.orch.Schedule(cfg.SyncObjects); err != nil {
		_ = ln.Close()
		return err
	}
	return serve(ctx, ln, a.handler, cfg.ShutdownTimeout, log, a.orch.Shutdown)
}

// app holds the wired service.
type app struct {
	handler http.Handler
	orch    *syncer.Orchestrator
	store   *store.Store
}

// newApp migrates the database and wires the service's dependencies. sfHTTP
// sends all requests to Salesforce. The caller must close the returned store.
func newApp(ctx context.Context, cfg *config.Config, log *zap.Logger, sfHTTP *http.Client) (*app, error) {
	if err := store.Migrate(cfg.DatabaseURL); err != nil {
		return nil, fmt.Errorf("migrate database: %w", err)
	}
	// Every worker of every concurrent run may hold a connection, and the
	// control API and run bookkeeping need a few more.
	st, err := store.Open(ctx, cfg.DatabaseURL, poolSize(cfg.MaxConcurrentJobs*cfg.WorkerCount+4))
	if err != nil {
		return nil, err
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	tokens := salesforce.NewClientCredentials(salesforce.ClientCredentialsConfig{
		LoginURL:     cfg.SFLoginURL,
		ClientID:     cfg.SFClientID,
		ClientSecret: salesforce.Secret(cfg.SFClientSecret),
		TokenTTL:     cfg.SFTokenTTL,
		HTTPClient:   sfHTTP,
	})
	sf := salesforce.NewClient(salesforce.ClientConfig{
		Tokens:     tokens,
		APIVersion: cfg.SFAPIVersion,
		HTTPClient: sfHTTP,
		Logger:     log,
		RateLimit:  float64(cfg.APIRateLimit),
		Registerer: reg,
	})

	orch := syncer.New(syncer.Config{
		Salesforce:        sf,
		Store:             st,
		Workers:           cfg.WorkerCount,
		MaxConcurrentRuns: cfg.MaxConcurrentJobs,
		ImportMaxRecords:  cfg.ImportMaxRecords,
		ImportMaxAttempts: cfg.ImportMaxAttempts,
		APIUsageThreshold: float64(cfg.APIUsageThreshold) / 100,
		Logger:            log,
		Registerer:        reg,
	})
	if err := orch.RecoverInterruptedRuns(ctx); err != nil {
		st.Close()
		return nil, err
	}

	handler := api.NewServer(log, reg, api.Options{
		SyncObjects: cfg.ObjectNames(),
		Runs:        orch,
		Outbox:      st,
		ReadinessChecks: []api.ReadinessCheck{
			{
				// The token is cached, so a passing probe costs no Salesforce call;
				// an API request here would spend the org's daily quota on every probe.
				Name: "salesforce",
				Check: func(ctx context.Context) error {
					_, err := tokens.Token(ctx)
					return err
				},
			},
			{Name: "database", Check: st.Ping},
		},
	}).Handler()

	return &app{handler: handler, orch: orch, store: st}, nil
}

// poolSize converts a connection count to the pool's type, saturating
// instead of overflowing.
func poolSize(n int) int32 {
	return int32(min(n, math.MaxInt32)) //nolint:gosec // bounded by the min above
}

// serve runs handler on ln until ctx is cancelled or the server fails. On
// cancellation or failure it stops accepting connections, lets in-flight
// requests finish, and then calls each of stop in order, all within
// shutdownTimeout. serve takes ownership of ln.
func serve(
	ctx context.Context, ln net.Listener, handler http.Handler, shutdownTimeout time.Duration, log *zap.Logger,
	stop ...func(context.Context) error,
) error {
	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}

	// Run the HTTP server and surface its error through a channel.
	serverErr := make(chan error, 1)
	go func() {
		log.Info("http server listening", zap.String("addr", ln.Addr().String()))
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	var errs []error
	select {
	case err := <-serverErr:
		errs = append(errs, fmt.Errorf("http server: %w", err))
	case <-ctx.Done():
		log.Info("shutdown signal received, draining")
	}

	// ctx may already be cancelled, so draining needs a context that keeps its
	// values but drops the cancellation, bounded by its own deadline. The stop
	// functions run even after a server failure, so background work is never
	// abandoned mid-write.
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		errs = append(errs, fmt.Errorf("graceful shutdown: %w", err))
	}
	for _, fn := range stop {
		if err := fn(shutdownCtx); err != nil {
			errs = append(errs, fmt.Errorf("graceful shutdown: %w", err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	log.Info("shutdown complete")
	return nil
}
