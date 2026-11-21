// Command api serves the REST API.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"time"

	"github.com/ayzyk/order-pipeline/internal/httpapi"
	"github.com/ayzyk/order-pipeline/internal/order"
	"github.com/ayzyk/order-pipeline/internal/platform"
)

func main() {
	log := platform.Logger("api")
	cfg, err := platform.LoadConfig()
	if err != nil {
		log.Error("bad config", "err", err)
		os.Exit(1)
	}

	ctx, stop := platform.SignalContext()
	defer stop()

	pool, err := platform.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("database", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpapi.New(order.NewStore(pool), log),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.HTTPAddr)
		errCh <- server.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Error("server failed", "err", err)
			os.Exit(1)
		}
	case <-ctx.Done():
	}

	// Kubernetes removes the pod from the Service and sends SIGTERM at about the same
	// time, so running requests get a few seconds to finish.
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error("shutdown", "err", err)
	}
}
