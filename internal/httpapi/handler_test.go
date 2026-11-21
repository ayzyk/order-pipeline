package httpapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ayzyk/order-pipeline/internal/events"
	"github.com/ayzyk/order-pipeline/internal/order"
)

type fakeOrders struct {
	err      error
	replayed bool
	pingErr  error
	gotKey   string
	gotHash  string
}

func (f *fakeOrders) Create(ctx context.Context, key, hash string, req order.CreateRequest) (order.Order, bool, error) {
	f.gotKey, f.gotHash = key, hash
	return order.Order{ID: "11111111-1111-4111-8111-111111111111", Status: "pending"}, f.replayed, f.err
}

func (f *fakeOrders) Get(ctx context.Context, id string) (order.Order, error) {
	return order.Order{ID: id}, f.err
}

func (f *fakeOrders) Ping(ctx context.Context) error { return f.pingErr }

func serve(f *fakeOrders, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	New(f, slog.New(slog.NewTextHandler(io.Discard, nil))).ServeHTTP(rec, req)
	return rec
}

const validBody = `{"customer_id":"c-1","items":[{"sku":"A","quantity":2}]}`

var withKey = map[string]string{"Idempotency-Key": "key-1"}

func TestCreateOrderStatusCodes(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		replayed bool
		want     int
	}{
		{"accepted", nil, false, http.StatusAccepted},
		{"replay", nil, true, http.StatusOK},
		{"unknown sku", order.ErrUnknownSKU, false, http.StatusUnprocessableEntity},
		{"key reused", order.ErrIdempotencyMismatch, false, http.StatusConflict},
		{"database error", errors.New("connection refused"), false, http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := serve(&fakeOrders{err: tt.err, replayed: tt.replayed}, "POST", "/orders", validBody, withKey)
			if rec.Code != tt.want {
				t.Errorf("status = %d, want %d", rec.Code, tt.want)
			}
			if strings.Contains(rec.Body.String(), "connection refused") {
				t.Error("internal error leaked to the client")
			}
		})
	}
}

func TestCreateOrderSetsLocation(t *testing.T) {
	rec := serve(&fakeOrders{}, "POST", "/orders", validBody, withKey)
	if got := rec.Header().Get("Location"); got != "/orders/11111111-1111-4111-8111-111111111111" {
		t.Errorf("Location = %q", got)
	}
}

func TestCreateOrderValidation(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		headers map[string]string
	}{
		{"no idempotency key", validBody, nil},
		{"broken json", `{`, withKey},
		{"unknown field", `{"customer_id":"c","items":[{"sku":"A","quantity":1}],"price":1}`, withKey},
		{"no customer", `{"items":[{"sku":"A","quantity":1}]}`, withKey},
		{"no items", `{"customer_id":"c","items":[]}`, withKey},
		{"zero quantity", `{"customer_id":"c","items":[{"sku":"A","quantity":0}]}`, withKey},
		{"huge quantity", `{"customer_id":"c","items":[{"sku":"A","quantity":100000}]}`, withKey},
		{"empty sku", `{"customer_id":"c","items":[{"sku":"","quantity":1}]}`, withKey},
		{"same sku twice", `{"customer_id":"c","items":[{"sku":"A","quantity":1},{"sku":"A","quantity":1}]}`, withKey},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if rec := serve(&fakeOrders{}, "POST", "/orders", tt.body, tt.headers); rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", rec.Code)
			}
		})
	}
}

func TestHashIgnoresItemOrder(t *testing.T) {
	a := createRequest{CustomerID: "c", Items: []events.Item{{SKU: "A", Quantity: 1}, {SKU: "B", Quantity: 2}}}
	b := createRequest{CustomerID: "c", Items: []events.Item{{SKU: "B", Quantity: 2}, {SKU: "A", Quantity: 1}}}
	c := createRequest{CustomerID: "c", Items: []events.Item{{SKU: "A", Quantity: 1}, {SKU: "B", Quantity: 3}}}
	if hashRequest(a) != hashRequest(b) {
		t.Error("same items in another order should give the same hash")
	}
	if hashRequest(a) == hashRequest(c) {
		t.Error("different quantity should give another hash")
	}
}

func TestGetOrder(t *testing.T) {
	if rec := serve(&fakeOrders{}, "GET", "/orders/not-a-uuid", "", nil); rec.Code != http.StatusBadRequest {
		t.Errorf("bad id: status = %d", rec.Code)
	}
	if rec := serve(&fakeOrders{err: order.ErrNotFound}, "GET", "/orders/11111111-1111-4111-8111-111111111111", "", nil); rec.Code != http.StatusNotFound {
		t.Errorf("missing order: status = %d", rec.Code)
	}
	if rec := serve(&fakeOrders{}, "GET", "/orders/11111111-1111-4111-8111-111111111111", "", nil); rec.Code != http.StatusOK {
		t.Errorf("existing order: status = %d", rec.Code)
	}
}

func TestProbes(t *testing.T) {
	down := &fakeOrders{pingErr: errors.New("down")}
	if rec := serve(down, "GET", "/healthz", "", nil); rec.Code != http.StatusOK {
		t.Errorf("healthz must not depend on the database, got %d", rec.Code)
	}
	if rec := serve(down, "GET", "/readyz", "", nil); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("readyz with the database down: got %d", rec.Code)
	}
	if rec := serve(&fakeOrders{}, "GET", "/readyz", "", nil); rec.Code != http.StatusOK {
		t.Errorf("readyz: got %d", rec.Code)
	}
}
