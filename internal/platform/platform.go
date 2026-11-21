// Package platform has the small pieces every binary needs: config, logger, database.
package platform

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	DatabaseURL  string
	KafkaBrokers []string
	Topic        string
	DLQTopic     string
	HTTPAddr     string
}

func LoadConfig() (Config, error) {
	cfg := Config{
		DatabaseURL:  os.Getenv("DATABASE_URL"),
		KafkaBrokers: splitList(env("KAFKA_BROKERS", "localhost:9092")),
		Topic:        env("ORDERS_TOPIC", "orders.events"),
		DLQTopic:     env("ORDERS_DLQ_TOPIC", "orders.events.dlq"),
		HTTPAddr:     env("HTTP_ADDR", ":8080"),
	}
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is not set")
	}
	return cfg, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func Logger(service string) *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", service)
}

// SignalContext is cancelled on Ctrl+C and on SIGTERM, which is what Kubernetes sends.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
}

// Connect does not fail if the database is not up yet, pgxpool connects lazily.
// This matters in docker compose and Kubernetes, where containers start in any order.
func Connect(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	return pgxpool.NewWithConfig(ctx, cfg)
}

// NewUUID returns a random version 4 UUID, enough here to not pull in a library.
func NewUUID() string {
	var b [16]byte
	rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
