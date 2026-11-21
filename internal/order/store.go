// Package order creates and reads orders.
package order

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayzyk/order-pipeline/internal/events"
	"github.com/ayzyk/order-pipeline/internal/platform"
)

var (
	ErrNotFound            = errors.New("order not found")
	ErrUnknownSKU          = errors.New("unknown sku")
	ErrIdempotencyMismatch = errors.New("idempotency key was already used with another request")
)

type Item struct {
	SKU        string `json:"sku"`
	Quantity   int    `json:"quantity"`
	PriceCents int64  `json:"price_cents"`
}

type Order struct {
	ID           string    `json:"id"`
	CustomerID   string    `json:"customer_id"`
	Status       string    `json:"status"`
	TotalCents   int64     `json:"total_cents"`
	RejectReason string    `json:"reject_reason,omitempty"`
	Items        []Item    `json:"items"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type CreateRequest struct {
	CustomerID string
	Items      []events.Item
}

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Create saves the order and its order.created event in one transaction.
// replayed is true when the key was used before and the old order is returned.
func (s *Store) Create(ctx context.Context, key, requestHash string, req CreateRequest) (o Order, replayed bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Order{}, false, err
	}
	defer tx.Rollback(ctx)

	// Prices come from the database. A client must not be able to send its own price.
	prices, err := loadPrices(ctx, tx, req.Items)
	if err != nil {
		return Order{}, false, err
	}
	var total int64
	for _, item := range req.Items {
		total += prices[item.SKU] * int64(item.Quantity)
	}

	id := platform.NewUUID()
	var inserted bool
	err = tx.QueryRow(ctx, `
		INSERT INTO orders (id, customer_id, total_cents, idempotency_key, request_hash)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING true`, id, req.CustomerID, total, key, requestHash,
	).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		return s.replay(ctx, tx, key, requestHash)
	}
	if err != nil {
		return Order{}, false, fmt.Errorf("insert order: %w", err)
	}

	for _, item := range req.Items {
		_, err := tx.Exec(ctx, `INSERT INTO order_items (order_id, sku, quantity, price_cents) VALUES ($1, $2, $3, $4)`,
			id, item.SKU, item.Quantity, prices[item.SKU])
		if err != nil {
			return Order{}, false, fmt.Errorf("insert item: %w", err)
		}
	}

	if err := addEvent(ctx, tx, id, req, total); err != nil {
		return Order{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Order{}, false, err
	}

	o, err = s.Get(ctx, id)
	return o, false, err
}

func loadPrices(ctx context.Context, tx pgx.Tx, items []events.Item) (map[string]int64, error) {
	skus := make([]string, 0, len(items))
	for _, item := range items {
		skus = append(skus, item.SKU)
	}
	rows, err := tx.Query(ctx, `SELECT sku, price_cents FROM products WHERE sku = ANY($1)`, skus)
	if err != nil {
		return nil, fmt.Errorf("load prices: %w", err)
	}
	defer rows.Close()

	prices := map[string]int64{}
	for rows.Next() {
		var sku string
		var price int64
		if err := rows.Scan(&sku, &price); err != nil {
			return nil, err
		}
		prices[sku] = price
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, sku := range skus {
		if _, ok := prices[sku]; !ok {
			return nil, fmt.Errorf("%w: %s", ErrUnknownSKU, sku)
		}
	}
	return prices, nil
}

func addEvent(ctx context.Context, tx pgx.Tx, orderID string, req CreateRequest, total int64) error {
	data, err := json.Marshal(events.OrderCreated{
		OrderID:    orderID,
		CustomerID: req.CustomerID,
		Items:      req.Items,
		TotalCents: total,
	})
	if err != nil {
		return err
	}
	eventID := platform.NewUUID()
	payload, err := json.Marshal(events.Envelope{
		ID:         eventID,
		Type:       events.TypeOrderCreated,
		OccurredAt: time.Now().UTC(),
		Data:       data,
	})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox (event_id, event_type, aggregate_id, payload) VALUES ($1, $2, $3, $4)`,
		eventID, events.TypeOrderCreated, orderID, payload)
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}

func (s *Store) replay(ctx context.Context, tx pgx.Tx, key, requestHash string) (Order, bool, error) {
	var id, hash string
	err := tx.QueryRow(ctx, `SELECT id, request_hash FROM orders WHERE idempotency_key = $1`, key).Scan(&id, &hash)
	if err != nil {
		return Order{}, false, fmt.Errorf("select existing order: %w", err)
	}
	if hash != requestHash {
		return Order{}, false, ErrIdempotencyMismatch
	}
	o, err := s.Get(ctx, id)
	return o, true, err
}

func (s *Store) Get(ctx context.Context, id string) (Order, error) {
	var o Order
	var reason *string
	err := s.pool.QueryRow(ctx, `
		SELECT id, customer_id, status, total_cents, reject_reason, created_at, updated_at
		FROM orders WHERE id = $1`, id,
	).Scan(&o.ID, &o.CustomerID, &o.Status, &o.TotalCents, &reason, &o.CreatedAt, &o.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Order{}, ErrNotFound
	}
	if err != nil {
		return Order{}, fmt.Errorf("select order: %w", err)
	}
	if reason != nil {
		o.RejectReason = *reason
	}

	rows, err := s.pool.Query(ctx, `SELECT sku, quantity, price_cents FROM order_items WHERE order_id = $1 ORDER BY sku`, id)
	if err != nil {
		return Order{}, fmt.Errorf("select items: %w", err)
	}
	o.Items, err = pgx.CollectRows(rows, pgx.RowToStructByPos[Item])
	if err != nil {
		return Order{}, fmt.Errorf("select items: %w", err)
	}
	return o, nil
}

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}
