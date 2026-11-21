// Command relay sends events from the outbox table to Kafka.
package main

import (
	"os"

	"github.com/ayzyk/order-pipeline/internal/kafkabus"
	"github.com/ayzyk/order-pipeline/internal/outbox"
	"github.com/ayzyk/order-pipeline/internal/platform"
)

func main() {
	log := platform.Logger("relay")
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

	if err := kafkabus.WaitForTopics(ctx, cfg.KafkaBrokers, cfg.Topic, cfg.DLQTopic); err != nil {
		log.Error("topics", "err", err)
		os.Exit(1)
	}

	writer := kafkabus.NewWriter(cfg.KafkaBrokers, cfg.Topic)
	defer writer.Close()

	log.Info("relay started", "topic", cfg.Topic)
	outbox.NewRelay(pool, kafkabus.Publisher{Writer: writer}, log).Run(ctx)
	log.Info("relay stopped")
}
