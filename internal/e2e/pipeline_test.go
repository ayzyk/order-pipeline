// Package e2e runs the whole pipeline against real Postgres and Kafka:
// order -> outbox -> relay -> Kafka -> worker -> confirmed.
package e2e

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ayzyk/order-pipeline/internal/events"
	"github.com/ayzyk/order-pipeline/internal/fulfillment"
	"github.com/ayzyk/order-pipeline/internal/kafkabus"
	"github.com/ayzyk/order-pipeline/internal/order"
	"github.com/ayzyk/order-pipeline/internal/outbox"
	"github.com/ayzyk/order-pipeline/internal/testdb"
)

func TestPipeline(t *testing.T) {
	brokersEnv := os.Getenv("TEST_KAFKA_BROKERS")
	if brokersEnv == "" {
		t.Skip("TEST_KAFKA_BROKERS is not set")
	}
	brokers := strings.Split(brokersEnv, ",")
	pool := testdb.Pool(t, "e2e")
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// a fresh topic, so old messages from earlier runs do not get in the way
	suffix := fmt.Sprint(time.Now().UnixNano())
	topic, dlqTopic := "orders.e2e."+suffix, "orders.e2e.dlq."+suffix
	if err := kafkabus.WaitForTopics(ctx, brokers, topic, dlqTopic); err != nil {
		t.Fatal(err)
	}

	writer := kafkabus.NewWriter(brokers, topic)
	defer writer.Close()
	dlq := kafkabus.NewWriter(brokers, dlqTopic)
	defer dlq.Close()
	reader := kafkabus.NewReader(brokers, topic, "e2e-"+suffix)
	defer reader.Close()

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go outbox.NewRelay(pool, kafkabus.Publisher{Writer: writer}, log).Run(runCtx)
	go fulfillment.NewConsumer(reader, dlq, fulfillment.NewProcessor(pool), log).Run(runCtx)

	store := order.NewStore(pool)
	enough := testdb.Product(t, pool, 500, 2)
	scarce := testdb.Product(t, pool, 500, 1)

	ok, _, err := store.Create(ctx, testdb.Key(t), "a", order.CreateRequest{CustomerID: "c-1", Items: []events.Item{{SKU: enough, Quantity: 2}}})
	if err != nil {
		t.Fatal(err)
	}
	tooMuch, _, err := store.Create(ctx, testdb.Key(t), "b", order.CreateRequest{CustomerID: "c-2", Items: []events.Item{{SKU: scarce, Quantity: 5}}})
	if err != nil {
		t.Fatal(err)
	}

	waitForStatus(t, ctx, store, ok.ID, "confirmed")
	waitForStatus(t, ctx, store, tooMuch.ID, "rejected")

	if testdb.Stock(t, pool, enough) != 0 || testdb.Stock(t, pool, scarce) != 1 {
		t.Errorf("stock: %d and %d, want 0 and 1", testdb.Stock(t, pool, enough), testdb.Stock(t, pool, scarce))
	}
}

func waitForStatus(t *testing.T, ctx context.Context, store *order.Store, id, want string) {
	t.Helper()
	for {
		o, err := store.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if o.Status == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("order %s is %s, waited for %s", id, o.Status, want)
		case <-time.After(100 * time.Millisecond):
		}
	}
}
