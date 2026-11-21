package order_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/ayzyk/order-pipeline/internal/events"
	"github.com/ayzyk/order-pipeline/internal/order"
	"github.com/ayzyk/order-pipeline/internal/testdb"
)

func TestCreateSavesOrderAndEvent(t *testing.T) {
	pool := testdb.Pool(t, "order")
	store := order.NewStore(pool)
	ctx := context.Background()
	keyboard := testdb.Product(t, pool, 1000, 10)
	mouse := testdb.Product(t, pool, 250, 10)

	o, replayed, err := store.Create(ctx, testdb.Key(t), "hash", order.CreateRequest{
		CustomerID: "c-1",
		Items:      []events.Item{{SKU: keyboard, Quantity: 1}, {SKU: mouse, Quantity: 2}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if replayed {
		t.Error("new order must not be a replay")
	}
	if o.Status != "pending" || o.TotalCents != 1500 || len(o.Items) != 2 {
		t.Errorf("order = %+v", o)
	}

	// the event is in the outbox, written in the same transaction
	var payload []byte
	err = pool.QueryRow(ctx, `SELECT payload FROM outbox WHERE aggregate_id = $1`, o.ID).Scan(&payload)
	if err != nil {
		t.Fatal(err)
	}
	var env events.Envelope
	if err := json.Unmarshal(payload, &env); err != nil {
		t.Fatal(err)
	}
	var data events.OrderCreated
	if err := json.Unmarshal(env.Data, &data); err != nil {
		t.Fatal(err)
	}
	if env.Type != events.TypeOrderCreated || data.OrderID != o.ID || data.TotalCents != 1500 {
		t.Errorf("event = %+v, data = %+v", env, data)
	}
}

func TestCreateIsIdempotent(t *testing.T) {
	pool := testdb.Pool(t, "order")
	store := order.NewStore(pool)
	ctx := context.Background()
	sku := testdb.Product(t, pool, 100, 10)
	key := testdb.Key(t)
	req := order.CreateRequest{CustomerID: "c-1", Items: []events.Item{{SKU: sku, Quantity: 1}}}

	first, _, err := store.Create(ctx, key, "hash-a", req)
	if err != nil {
		t.Fatal(err)
	}
	second, replayed, err := store.Create(ctx, key, "hash-a", req)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed || second.ID != first.ID {
		t.Errorf("second call: replayed = %v, id = %s, want %s", replayed, second.ID, first.ID)
	}

	var events int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE aggregate_id = $1`, first.ID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 1 {
		t.Errorf("%d events for one order, want 1", events)
	}

	if _, _, err := store.Create(ctx, key, "hash-b", req); !errors.Is(err, order.ErrIdempotencyMismatch) {
		t.Errorf("other request with the same key: err = %v", err)
	}
}

func TestCreateUnknownSKU(t *testing.T) {
	pool := testdb.Pool(t, "order")
	store := order.NewStore(pool)
	sku := testdb.Product(t, pool, 100, 10)

	_, _, err := store.Create(context.Background(), testdb.Key(t), "hash", order.CreateRequest{
		CustomerID: "c-1",
		Items:      []events.Item{{SKU: sku, Quantity: 1}, {SKU: "NO-SUCH-SKU", Quantity: 1}},
	})
	if !errors.Is(err, order.ErrUnknownSKU) {
		t.Errorf("err = %v, want ErrUnknownSKU", err)
	}
}

func TestGetNotFound(t *testing.T) {
	pool := testdb.Pool(t, "order")
	_, err := order.NewStore(pool).Get(context.Background(), "00000000-0000-4000-8000-000000000000")
	if !errors.Is(err, order.ErrNotFound) {
		t.Errorf("err = %v", err)
	}
}
