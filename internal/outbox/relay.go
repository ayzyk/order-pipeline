// Package outbox moves saved events from the outbox table to Kafka.
package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayzyk/order-pipeline/internal/events"
)

type Publisher interface {
	Publish(ctx context.Context, msgs []events.Message) error
}

type Relay struct {
	pool      *pgxpool.Pool
	publisher Publisher
	log       *slog.Logger

	BatchSize    int
	PollInterval time.Duration
}

func NewRelay(pool *pgxpool.Pool, publisher Publisher, log *slog.Logger) *Relay {
	return &Relay{pool: pool, publisher: publisher, log: log, BatchSize: 100, PollInterval: 500 * time.Millisecond}
}

// Run sends batches until ctx is cancelled. After an error it waits and tries again,
// so a restart of Kafka or Postgres does not need a restart of the relay.
func (r *Relay) Run(ctx context.Context) error {
	backoff := r.PollInterval
	for {
		n, err := r.PublishBatch(ctx)
		if ctx.Err() != nil {
			return nil
		}

		wait := r.PollInterval
		switch {
		case err != nil:
			r.log.Error("publish batch failed", "err", err, "retry_in", backoff.String())
			wait = backoff
			backoff = min(backoff*2, 30*time.Second)
		case n == r.BatchSize:
			wait = 0 // there is probably more, do not sleep
			backoff = r.PollInterval
		default:
			backoff = r.PollInterval
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
	}
}

// PublishBatch sends one batch and returns how many events were sent.
//
// The rows stay locked while they are sent. If the publish fails, the transaction
// rolls back and the same events are sent next time. If the relay dies after the
// publish but before the commit, the events are sent twice. That is why consumers
// have to deduplicate by event id: this is "at least once" delivery.
func (r *Relay) PublishBatch(ctx context.Context) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id, aggregate_id::text, payload
		FROM outbox
		WHERE published_at IS NULL
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, r.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("select outbox: %w", err)
	}

	var ids []int64
	var msgs []events.Message
	for rows.Next() {
		var id int64
		var key string
		var payload []byte
		if err := rows.Scan(&id, &key, &payload); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
		msgs = append(msgs, events.Message{Key: key, Value: payload})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(msgs) == 0 {
		return 0, nil
	}

	if err := r.publisher.Publish(ctx, msgs); err != nil {
		return 0, fmt.Errorf("publish: %w", err)
	}

	if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
		return 0, fmt.Errorf("mark published: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(msgs), nil
}

// Pending is used by tests and could back a metric: how many events wait to be sent.
func Pending(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}) (int, error) {
	var n int
	err := q.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n)
	return n, err
}
