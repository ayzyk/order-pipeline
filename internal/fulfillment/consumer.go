package fulfillment

import (
	"context"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

type Reader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

type MessageProcessor interface {
	Process(ctx context.Context, value []byte) (Result, error)
}

type Consumer struct {
	reader    Reader
	dlq       Writer
	processor MessageProcessor
	log       *slog.Logger

	RetryDelay time.Duration
}

func NewConsumer(reader Reader, dlq Writer, processor MessageProcessor, log *slog.Logger) *Consumer {
	return &Consumer{reader: reader, dlq: dlq, processor: processor, log: log, RetryDelay: time.Second}
}

// Run reads messages until ctx is cancelled.
//
// The offset is committed only after the database transaction. If the worker dies
// in between, Kafka gives the message again and processed_events catches the repeat.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.log.Error("fetch failed", "err", err)
			if !sleep(ctx, c.RetryDelay) {
				return nil
			}
			continue
		}

		if !c.handle(ctx, msg) {
			return nil
		}
		if err := c.reader.CommitMessages(ctx, msg); err != nil && ctx.Err() == nil {
			// Not fatal: the message comes again and is skipped as a duplicate.
			c.log.Error("commit failed", "offset", msg.Offset, "err", err)
		}
	}
}

// handle returns false only when ctx is cancelled.
// A temporary error (database down) is retried on the same message forever:
// skipping it would lose an order, and the next messages can wait.
func (c *Consumer) handle(ctx context.Context, msg kafka.Message) bool {
	delay := c.RetryDelay
	for {
		result, err := c.processor.Process(ctx, msg.Value)
		switch {
		case err == nil:
			c.log.Info("message processed", "partition", msg.Partition, "offset", msg.Offset, "result", string(result))
			return true
		case IsPermanent(err):
			c.sendToDLQ(ctx, msg, err)
			return true
		case ctx.Err() != nil:
			return false
		}

		c.log.Error("processing failed, will retry", "offset", msg.Offset, "err", err, "retry_in", delay.String())
		if !sleep(ctx, delay) {
			return false
		}
		delay = min(delay*2, 30*time.Second)
	}
}

func (c *Consumer) sendToDLQ(ctx context.Context, msg kafka.Message, reason error) {
	dead := kafka.Message{
		Key:   msg.Key,
		Value: msg.Value,
		Headers: append(msg.Headers,
			kafka.Header{Key: "error", Value: []byte(reason.Error())},
			kafka.Header{Key: "source-topic", Value: []byte(msg.Topic)},
		),
	}
	if err := c.dlq.WriteMessages(ctx, dead); err != nil {
		// Losing a broken message is better than blocking the whole partition on it.
		c.log.Error("could not write to dead letter topic, dropping message", "offset", msg.Offset, "err", err)
		return
	}
	c.log.Warn("message sent to dead letter topic", "offset", msg.Offset, "reason", reason.Error())
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
