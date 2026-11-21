package fulfillment_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayzyk/order-pipeline/internal/events"
	"github.com/ayzyk/order-pipeline/internal/fulfillment"
	"github.com/ayzyk/order-pipeline/internal/order"
	"github.com/ayzyk/order-pipeline/internal/platform"
	"github.com/ayzyk/order-pipeline/internal/testdb"
)

// newOrder creates an order and returns the event the relay would send for it.
func newOrder(t *testing.T, pool *pgxpool.Pool, items ...events.Item) (string, []byte) {
	t.Helper()
	o, _, err := order.NewStore(pool).Create(context.Background(), testdb.Key(t), "hash", order.CreateRequest{
		CustomerID: "c-1",
		Items:      items,
	})
	if err != nil {
		t.Fatal(err)
	}
	var payload []byte
	err = pool.QueryRow(context.Background(), `SELECT payload FROM outbox WHERE aggregate_id = $1`, o.ID).Scan(&payload)
	if err != nil {
		t.Fatal(err)
	}
	return o.ID, payload
}

func status(t *testing.T, pool *pgxpool.Pool, id string) string {
	t.Helper()
	o, err := order.NewStore(pool).Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return o.Status
}

func TestConfirmTakesStock(t *testing.T) {
	pool := testdb.Pool(t, "fulfillment")
	a, b := testdb.Product(t, pool, 100, 5), testdb.Product(t, pool, 100, 5)
	id, msg := newOrder(t, pool, events.Item{SKU: a, Quantity: 2}, events.Item{SKU: b, Quantity: 5})

	result, err := fulfillment.NewProcessor(pool).Process(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
	if result != fulfillment.ResultConfirmed || status(t, pool, id) != "confirmed" {
		t.Errorf("result = %s, status = %s", result, status(t, pool, id))
	}
	if testdb.Stock(t, pool, a) != 3 || testdb.Stock(t, pool, b) != 0 {
		t.Errorf("stock: %d and %d, want 3 and 0", testdb.Stock(t, pool, a), testdb.Stock(t, pool, b))
	}
}

func TestRejectTakesNothing(t *testing.T) {
	pool := testdb.Pool(t, "fulfillment")
	a, b := testdb.Product(t, pool, 100, 5), testdb.Product(t, pool, 100, 1)
	id, msg := newOrder(t, pool, events.Item{SKU: a, Quantity: 2}, events.Item{SKU: b, Quantity: 3})

	result, err := fulfillment.NewProcessor(pool).Process(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
	if result != fulfillment.ResultRejected || status(t, pool, id) != "rejected" {
		t.Errorf("result = %s, status = %s", result, status(t, pool, id))
	}
	// all or nothing: the first product must not be taken either
	if testdb.Stock(t, pool, a) != 5 || testdb.Stock(t, pool, b) != 1 {
		t.Errorf("stock changed on a rejected order")
	}
}

func TestDuplicateMessageIsIgnored(t *testing.T) {
	pool := testdb.Pool(t, "fulfillment")
	sku := testdb.Product(t, pool, 100, 10)
	_, msg := newOrder(t, pool, events.Item{SKU: sku, Quantity: 3})
	p := fulfillment.NewProcessor(pool)

	if _, err := p.Process(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	result, err := p.Process(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
	if result != fulfillment.ResultDuplicate {
		t.Errorf("result = %s, want duplicate", result)
	}
	if testdb.Stock(t, pool, sku) != 7 {
		t.Errorf("stock = %d, the duplicate took stock again", testdb.Stock(t, pool, sku))
	}
}

// Ten orders want the last three items at the same time.
func TestConcurrentOrdersDoNotOversell(t *testing.T) {
	pool := testdb.Pool(t, "fulfillment")
	sku := testdb.Product(t, pool, 100, 3)
	var msgs [][]byte
	for i := 0; i < 10; i++ {
		_, msg := newOrder(t, pool, events.Item{SKU: sku, Quantity: 1})
		msgs = append(msgs, msg)
	}

	p := fulfillment.NewProcessor(pool)
	results := make(chan fulfillment.Result, len(msgs))
	var wg sync.WaitGroup
	for _, msg := range msgs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := p.Process(context.Background(), msg)
			if err != nil {
				t.Error(err)
			}
			results <- r
		}()
	}
	wg.Wait()
	close(results)

	confirmed := 0
	for r := range results {
		if r == fulfillment.ResultConfirmed {
			confirmed++
		}
	}
	if confirmed != 3 || testdb.Stock(t, pool, sku) != 0 {
		t.Errorf("confirmed %d orders, stock %d; want 3 and 0", confirmed, testdb.Stock(t, pool, sku))
	}
}

func TestBadMessages(t *testing.T) {
	pool := testdb.Pool(t, "fulfillment")
	p := fulfillment.NewProcessor(pool)

	envelope := func(eventType string, data any) []byte {
		raw, _ := json.Marshal(data)
		b, _ := json.Marshal(events.Envelope{ID: platform.NewUUID(), Type: eventType, OccurredAt: time.Now(), Data: raw})
		return b
	}

	bad := map[string][]byte{
		"not json":      []byte("hello"),
		"no id":         []byte(`{"type":"order.created","data":{}}`),
		"no items":      envelope(events.TypeOrderCreated, events.OrderCreated{OrderID: platform.NewUUID()}),
		"unknown order": envelope(events.TypeOrderCreated, events.OrderCreated{OrderID: platform.NewUUID(), Items: []events.Item{{SKU: testdb.Product(t, pool, 1, 1), Quantity: 1}}}),
	}
	for name, msg := range bad {
		t.Run(name, func(t *testing.T) {
			_, err := p.Process(context.Background(), msg)
			if !errors.Is(err, fulfillment.ErrBadMessage) || !fulfillment.IsPermanent(err) {
				t.Errorf("err = %v, want a permanent ErrBadMessage", err)
			}
		})
	}

	result, err := p.Process(context.Background(), envelope("order.shipped", map[string]string{}))
	if err != nil || result != fulfillment.ResultSkipped {
		t.Errorf("unknown event type: result = %s, err = %v", result, err)
	}
}
