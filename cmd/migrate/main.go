// Command migrate applies the database migrations and exits.
// In Kubernetes it runs as an init container of the api pods.
package main

import (
	"context"
	"os"
	"time"

	"github.com/ayzyk/order-pipeline/internal/platform"
	"github.com/ayzyk/order-pipeline/migrations"
)

func main() {
	log := platform.Logger("migrate")
	cfg, err := platform.LoadConfig()
	if err != nil {
		log.Error("bad config", "err", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	pool, err := platform.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("database", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	// the database may still be starting, so try a few times
	delay := time.Second
	for {
		err = migrations.Apply(ctx, pool)
		if err == nil {
			log.Info("migrations applied")
			return
		}
		if ctx.Err() != nil {
			log.Error("migrations failed", "err", err)
			os.Exit(1)
		}
		log.Warn("database not ready, retrying", "err", err, "retry_in", delay.String())
		time.Sleep(delay)
		delay = min(delay*2, 10*time.Second)
	}
}
