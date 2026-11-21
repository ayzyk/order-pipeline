// Package events describes the messages that go through Kafka.
package events

import (
	"encoding/json"
	"time"
)

const TypeOrderCreated = "order.created"

// Envelope is the same for every event, so a consumer can read the type
// before it knows how to parse the data.
type Envelope struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	OccurredAt time.Time       `json:"occurred_at"`
	Data       json.RawMessage `json:"data"`
}

type OrderCreated struct {
	OrderID    string `json:"order_id"`
	CustomerID string `json:"customer_id"`
	Items      []Item `json:"items"`
	TotalCents int64  `json:"total_cents"`
}

type Item struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

// Message is what goes to the broker. Key is the order id: Kafka keeps the order
// of messages with the same key, so all events of one order stay in sequence.
type Message struct {
	Key   string
	Value []byte
}
