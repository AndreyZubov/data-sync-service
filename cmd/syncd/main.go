// Command syncd runs the Salesforce ⇄ Postgres bulk data sync service.
package main

import (
	"context"
	"errors"
	"fmt"
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

	return serve(ctx, ln, newHandler(cfg, log, salesforce.NewHTTPClient()), cfg.ShutdownTimeout, log)
}

// newHandler wires the service's dependencies into the control API. sfHTTP
// sends all requests to Salesforce.
func newHandler(cfg *config.Config, log *zap.Logger, sfHTTP *http.Client) http.Handler {
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

	return api.NewServer(log, reg, api.Options{
		SyncObjects: cfg.ObjectNames(),
		ReadinessChecks: []api.ReadinessCheck{{
			// The token is cached, so a passing probe costs no Salesforce call;
			// an API request here would spend the org's daily quota on every probe.
			Name: "salesforce",
			Check: func(ctx context.Context) error {
				_, err := tokens.Token(ctx)
				return err
			},
		}},
	}).Handler()
}

// serve runs handler on ln until ctx is cancelled or the server fails. On
// cancellation it stops accepting connections and lets in-flight requests
// finish for up to shutdownTimeout. serve takes ownership of ln.
func serve(ctx context.Context, ln net.Listener, handler http.Handler, shutdownTimeout time.Duration, log *zap.Logger) error {
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

	select {
	case err := <-serverErr:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
		log.Info("shutdown signal received, draining")
	}

	// ctx is already cancelled, so draining needs a context that keeps its
	// values but drops the cancellation, bounded by its own deadline.
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	log.Info("shutdown complete")
	return nil
}
