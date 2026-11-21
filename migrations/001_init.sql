CREATE TABLE products (
    sku         TEXT PRIMARY KEY,
    name        TEXT   NOT NULL,
    price_cents BIGINT NOT NULL CHECK (price_cents > 0),
    stock       INT    NOT NULL CHECK (stock >= 0)
);

-- demo catalog for docker compose and kind
INSERT INTO products (sku, name, price_cents, stock) VALUES
    ('KEYBOARD-01', 'Mechanical keyboard', 890000, 20),
    ('MOUSE-01',    'Wireless mouse',      249000, 50),
    ('ESP32-DEV',   'ESP32 dev board',      69000, 5);

CREATE TABLE orders (
    id              UUID PRIMARY KEY,
    customer_id     TEXT        NOT NULL CHECK (customer_id <> ''),
    status          TEXT        NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'confirmed', 'rejected')),
    total_cents     BIGINT      NOT NULL CHECK (total_cents > 0),
    idempotency_key TEXT        NOT NULL UNIQUE,
    request_hash    TEXT        NOT NULL,
    reject_reason   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    order_id    UUID   NOT NULL REFERENCES orders (id),
    sku         TEXT   NOT NULL REFERENCES products (sku),
    quantity    INT    NOT NULL CHECK (quantity > 0),
    price_cents BIGINT NOT NULL CHECK (price_cents > 0),
    PRIMARY KEY (order_id, sku)
);

-- Transactional outbox: the event is saved in the same transaction as the order,
-- the relay sends it to Kafka later. So an order can never exist without its event.
CREATE TABLE outbox (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id     UUID        NOT NULL UNIQUE,
    event_type   TEXT        NOT NULL,
    aggregate_id UUID        NOT NULL,
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

-- Kafka delivers at least once, the worker remembers what it has already done.
CREATE TABLE processed_events (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
