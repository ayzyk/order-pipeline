// Command worker reads order events and reserves stock.
package main

import (
	"os"

	"github.com/ayzyk/order-pipeline/internal/fulfillment"
	"github.com/ayzyk/order-pipeline/internal/kafkabus"
	"github.com/ayzyk/order-pipeline/internal/platform"
)

const consumerGroup = "fulfillment"

func main() {
	log := platform.Logger("worker")
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

	reader := kafkabus.NewReader(cfg.KafkaBrokers, cfg.Topic, consumerGroup)
	defer reader.Close() // leaves the group, so the partitions move to other workers right away
	dlq := kafkabus.NewWriter(cfg.KafkaBrokers, cfg.DLQTopic)
	defer dlq.Close()

	log.Info("worker started", "topic", cfg.Topic, "group", consumerGroup)
	fulfillment.NewConsumer(reader, dlq, fulfillment.NewProcessor(pool), log).Run(ctx)
	log.Info("worker stopped")
}
