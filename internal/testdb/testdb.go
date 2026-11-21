// Package testdb gives integration tests a migrated database and unique products.
package testdb

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayzyk/order-pipeline/migrations"
)

var (
	counter  atomic.Int64
	resetMu  sync.Mutex
	prepared = map[string]bool{}
)

// Pool skips the test when TEST_DATABASE_URL is not set.
//
// go test runs packages in parallel, and the outbox table is shared by design:
// the relay test would send events created by the worker test. So every package
// gets its own schema, created fresh once per test run.
func Pool(t *testing.T, schema string) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	schema = "test_" + schema

	resetMu.Lock()
	if !prepared[schema] {
		admin, err := pgxpool.New(ctx, url)
		if err != nil {
			resetMu.Unlock()
			t.Fatal(err)
		}
		_, err = admin.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %[1]s CASCADE; CREATE SCHEMA %[1]s", schema))
		admin.Close()
		if err != nil {
			resetMu.Unlock()
			t.Fatal(err)
		}
		prepared[schema] = true
	}
	resetMu.Unlock()

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return pool
}

// Product inserts a product with a unique sku, so tests running against the same
// database never touch each other's stock.
func Product(t *testing.T, pool *pgxpool.Pool, priceCents int64, stock int) string {
	t.Helper()
	sku := fmt.Sprintf("TEST-%d-%d-%d", os.Getpid(), time.Now().UnixNano(), counter.Add(1))
	_, err := pool.Exec(context.Background(),
		`INSERT INTO products (sku, name, price_cents, stock) VALUES ($1, 'test product', $2, $3)`, sku, priceCents, stock)
	if err != nil {
		t.Fatal(err)
	}
	return sku
}

// Key returns a unique idempotency key.
func Key(t *testing.T) string {
	return fmt.Sprintf("%s-%d-%d", t.Name(), time.Now().UnixNano(), counter.Add(1))
}

func Stock(t *testing.T, pool *pgxpool.Pool, sku string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT stock FROM products WHERE sku = $1`, sku).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
