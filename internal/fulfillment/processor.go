// Package fulfillment reserves stock for new orders.
package fulfillment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayzyk/order-pipeline/internal/events"
)

// ErrBadMessage means the message can never be processed. Retrying will not
// help, so the consumer sends it to the dead letter topic.
var ErrBadMessage = errors.New("bad message")

type Result string

const (
	ResultConfirmed Result = "confirmed"
	ResultRejected  Result = "rejected"
	ResultDuplicate Result = "duplicate"
	ResultSkipped   Result = "skipped"
)

type Processor struct {
	pool *pgxpool.Pool
}

func NewProcessor(pool *pgxpool.Pool) *Processor {
	return &Processor{pool: pool}
}

func (p *Processor) Process(ctx context.Context, value []byte) (Result, error) {
	var env events.Envelope
	if err := json.Unmarshal(value, &env); err != nil {
		return "", fmt.Errorf("%w: %v", ErrBadMessage, err)
	}
	if env.ID == "" || env.Type == "" {
		return "", fmt.Errorf("%w: envelope without id or type", ErrBadMessage)
	}
	if env.Type != events.TypeOrderCreated {
		return ResultSkipped, nil // other services may add event types later
	}

	var data events.OrderCreated
	if err := json.Unmarshal(env.Data, &data); err != nil {
		return "", fmt.Errorf("%w: %v", ErrBadMessage, err)
	}
	if data.OrderID == "" || len(data.Items) == 0 {
		return "", fmt.Errorf("%w: order.created without order id or items", ErrBadMessage)
	}
	return p.reserve(ctx, env.ID, data)
}

// reserve takes the stock for all items or for none of them.
// Remembering the event id happens in the same transaction, so a repeated
// message can not reserve the stock twice.
func (p *Processor) reserve(ctx context.Context, eventID string, data events.OrderCreated) (Result, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, eventID)
	if err != nil {
		return "", fmt.Errorf("remember event: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ResultDuplicate, nil
	}

	skus := make([]string, 0, len(data.Items))
	for _, item := range data.Items {
		skus = append(skus, item.SKU)
	}
	// Locked in sku order, so two orders with the same products can not deadlock.
	rows, err := tx.Query(ctx, `SELECT sku, stock FROM products WHERE sku = ANY($1) ORDER BY sku FOR UPDATE`, skus)
	if err != nil {
		return "", fmt.Errorf("lock products: %w", err)
	}
	stock := map[string]int{}
	for rows.Next() {
		var sku string
		var n int
		if err := rows.Scan(&sku, &n); err != nil {
			rows.Close()
			return "", err
		}
		stock[sku] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return "", err
	}

	reason := ""
	for _, item := range data.Items {
		if stock[item.SKU] < item.Quantity {
			reason = fmt.Sprintf("not enough %s in stock: want %d, have %d", item.SKU, item.Quantity, stock[item.SKU])
			break
		}
	}

	result := ResultConfirmed
	if reason != "" {
		result = ResultRejected
	} else {
		for _, item := range data.Items {
			if _, err := tx.Exec(ctx, `UPDATE products SET stock = stock - $1 WHERE sku = $2`, item.Quantity, item.SKU); err != nil {
				return "", fmt.Errorf("take stock: %w", err)
			}
		}
	}

	// the status only moves forward from pending
	tag, err = tx.Exec(ctx, `
		UPDATE orders SET status = $1, reject_reason = NULLIF($2, ''), updated_at = now()
		WHERE id = $3 AND status = 'pending'`, string(result), reason, data.OrderID)
	if err != nil {
		return "", fmt.Errorf("update order: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return "", fmt.Errorf("%w: order %s does not exist or is not pending", ErrBadMessage, data.OrderID)
	}

	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return result, nil
}

// IsPermanent reports whether retrying the message makes no sense.
func IsPermanent(err error) bool {
	return errors.Is(err, ErrBadMessage)
}
