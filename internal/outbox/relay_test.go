package outbox_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/ayzyk/order-pipeline/internal/events"
	"github.com/ayzyk/order-pipeline/internal/order"
	"github.com/ayzyk/order-pipeline/internal/outbox"
	"github.com/ayzyk/order-pipeline/internal/testdb"
)

type fakePublisher struct {
	mu   sync.Mutex
	err  error
	sent []events.Message
}

func (f *fakePublisher) Publish(ctx context.Context, msgs []events.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, msgs...)
	return nil
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// The outbox table is shared, so the tests first send whatever earlier tests left.
func drain(t *testing.T, r *outbox.Relay) {
	t.Helper()
	for {
		n, err := r.PublishBatch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return
		}
	}
}

func createOrders(t *testing.T, store *order.Store, sku string, n int) []string {
	t.Helper()
	var ids []string
	for i := 0; i < n; i++ {
		o, _, err := store.Create(context.Background(), testdb.Key(t), "hash", order.CreateRequest{
			CustomerID: "c-1",
			Items:      []events.Item{{SKU: sku, Quantity: 1}},
		})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, o.ID)
	}
	return ids
}

func TestRelayPublishesInOrder(t *testing.T) {
	pool := testdb.Pool(t, "outbox")
	pub := &fakePublisher{}
	relay := outbox.NewRelay(pool, pub, quiet)
	drain(t, relay)
	pub.sent = nil

	ids := createOrders(t, order.NewStore(pool), testdb.Product(t, pool, 100, 100), 5)

	n, err := relay.PublishBatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 || len(pub.sent) != 5 {
		t.Fatalf("published %d, sent %d, want 5", n, len(pub.sent))
	}
	for i, msg := range pub.sent {
		if msg.Key != ids[i] {
			t.Errorf("message %d has key %s, want %s", i, msg.Key, ids[i])
		}
	}

	// nothing is sent twice
	if n, err := relay.PublishBatch(context.Background()); err != nil || n != 0 {
		t.Errorf("second batch: n = %d, err = %v", n, err)
	}
}

func TestRelayKeepsEventsWhenKafkaFails(t *testing.T) {
	pool := testdb.Pool(t, "outbox")
	pub := &fakePublisher{}
	relay := outbox.NewRelay(pool, pub, quiet)
	drain(t, relay)

	createOrders(t, order.NewStore(pool), testdb.Product(t, pool, 100, 100), 3)

	pub.err = errors.New("kafka is down")
	if _, err := relay.PublishBatch(context.Background()); err == nil {
		t.Fatal("expected an error")
	}
	pending, err := outbox.Pending(context.Background(), pool)
	if err != nil {
		t.Fatal(err)
	}
	if pending != 3 {
		t.Errorf("pending = %d after a failed publish, want 3", pending)
	}

	pub.err = nil
	if n, err := relay.PublishBatch(context.Background()); err != nil || n != 3 {
		t.Errorf("after recovery: n = %d, err = %v", n, err)
	}
}

// Two relays at the same time must not send the same event: SKIP LOCKED.
func TestTwoRelaysDoNotDuplicate(t *testing.T) {
	pool := testdb.Pool(t, "outbox")
	pub := &fakePublisher{}
	drain(t, outbox.NewRelay(pool, pub, quiet))
	pub.sent = nil

	createOrders(t, order.NewStore(pool), testdb.Product(t, pool, 100, 1000), 60)

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		relay := outbox.NewRelay(pool, pub, quiet)
		relay.BatchSize = 7
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				n, err := relay.PublishBatch(context.Background())
				if err != nil {
					t.Error(err)
					return
				}
				if n == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()

	seen := map[string]bool{}
	for _, msg := range pub.sent {
		if seen[string(msg.Value)] {
			t.Fatalf("event sent twice: %s", msg.Key)
		}
		seen[string(msg.Value)] = true
	}
	if len(seen) != 60 {
		t.Errorf("sent %d events, want 60", len(seen))
	}
}
