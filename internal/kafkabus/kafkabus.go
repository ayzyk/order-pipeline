// Package kafkabus connects the services to Kafka with segmentio/kafka-go.
package kafkabus

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/ayzyk/order-pipeline/internal/events"
)

const Partitions = 3

func NewWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // same key, same partition
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
	}
}

// Publisher adapts a kafka.Writer to the outbox.Publisher interface.
type Publisher struct {
	Writer *kafka.Writer
}

func (p Publisher) Publish(ctx context.Context, msgs []events.Message) error {
	out := make([]kafka.Message, len(msgs))
	for i, m := range msgs {
		out[i] = kafka.Message{Key: []byte(m.Key), Value: m.Value}
	}
	return p.Writer.WriteMessages(ctx, out...)
}

func NewReader(brokers []string, topic, group string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        group,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        500 * time.Millisecond,
		CommitInterval: 0, // commit synchronously, after the database transaction
		StartOffset:    kafka.FirstOffset,
	})
}

// EnsureTopics creates the topics if they do not exist yet. Auto creation would
// give every topic one partition, and then only one worker could read it.
func EnsureTopics(ctx context.Context, brokers []string, topics ...string) error {
	var lastErr error
	for _, broker := range brokers {
		if err := createTopics(ctx, broker, topics); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return lastErr
}

func createTopics(ctx context.Context, broker string, topics []string) error {
	dialer := &kafka.Dialer{Timeout: 5 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", broker)
	if err != nil {
		return fmt.Errorf("dial %s: %w", broker, err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("find controller: %w", err)
	}
	ctrl, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("dial controller: %w", err)
	}
	defer ctrl.Close()

	configs := make([]kafka.TopicConfig, len(topics))
	for i, t := range topics {
		configs[i] = kafka.TopicConfig{Topic: t, NumPartitions: Partitions, ReplicationFactor: 1}
	}
	err = ctrl.CreateTopics(configs...)
	if err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
		return fmt.Errorf("create topics: %w", err)
	}
	return nil
}

// WaitForTopics retries EnsureTopics until Kafka is up or ctx is cancelled.
func WaitForTopics(ctx context.Context, brokers []string, topics ...string) error {
	delay := time.Second
	for {
		err := EnsureTopics(ctx, brokers, topics...)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("kafka is not available: %w", err)
		case <-time.After(delay):
		}
		delay = min(delay*2, 15*time.Second)
	}
}
