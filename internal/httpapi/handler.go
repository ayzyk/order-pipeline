// Package httpapi is the REST API of the order service.
package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"sort"
	"time"

	"github.com/ayzyk/order-pipeline/internal/events"
	"github.com/ayzyk/order-pipeline/internal/order"
)

const (
	maxBodyBytes = 1 << 16
	maxItems     = 50
	maxQuantity  = 1000
)

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

type Orders interface {
	Create(ctx context.Context, key, requestHash string, req order.CreateRequest) (order.Order, bool, error)
	Get(ctx context.Context, id string) (order.Order, error)
	Ping(ctx context.Context) error
}

type handler struct {
	orders Orders
	log    *slog.Logger
}

func New(orders Orders, log *slog.Logger) http.Handler {
	h := &handler{orders: orders, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.healthz)
	mux.HandleFunc("GET /readyz", h.readyz)
	mux.HandleFunc("POST /orders", h.createOrder)
	mux.HandleFunc("GET /orders/{id}", h.getOrder)
	return h.logRequests(mux)
}

// healthz only says the process is alive. It must not check the database:
// if Postgres is down, restarting every api pod would not help.
func (h *handler) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyz takes the pod out of the Service while the database is unreachable.
func (h *handler) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := h.orders.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database is not reachable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type createRequest struct {
	CustomerID string        `json:"customer_id"`
	Items      []events.Item `json:"items"`
}

func (h *handler) createOrder(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 200 {
		writeError(w, http.StatusBadRequest, "Idempotency-Key header is required (max 200 characters)")
		return
	}

	var req createRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if msg := validate(req); msg != "" {
		writeError(w, http.StatusBadRequest, msg)
		return
	}

	o, replayed, err := h.orders.Create(r.Context(), key, hashRequest(req), order.CreateRequest{
		CustomerID: req.CustomerID,
		Items:      req.Items,
	})
	switch {
	case errors.Is(err, order.ErrUnknownSKU):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	case errors.Is(err, order.ErrIdempotencyMismatch):
		writeError(w, http.StatusConflict, err.Error())
		return
	case err != nil:
		h.internalError(w, r, err)
		return
	}

	w.Header().Set("Location", "/orders/"+o.ID)
	if replayed {
		writeJSON(w, http.StatusOK, o)
		return
	}
	// 202: the order is saved, but stock is reserved later by the worker
	writeJSON(w, http.StatusAccepted, o)
}

func validate(req createRequest) string {
	if req.CustomerID == "" || len(req.CustomerID) > 100 {
		return "customer_id is required (max 100 characters)"
	}
	if len(req.Items) == 0 || len(req.Items) > maxItems {
		return "items must have between 1 and 50 entries"
	}
	seen := map[string]bool{}
	for _, item := range req.Items {
		if item.SKU == "" {
			return "every item needs a sku"
		}
		if item.Quantity < 1 || item.Quantity > maxQuantity {
			return "quantity must be between 1 and 1000"
		}
		if seen[item.SKU] {
			return "each sku can appear only once"
		}
		seen[item.SKU] = true
	}
	return ""
}

// hashRequest makes the same order give the same hash even if items come in another order.
func hashRequest(req createRequest) string {
	items := append([]events.Item(nil), req.Items...)
	sort.Slice(items, func(i, j int) bool { return items[i].SKU < items[j].SKU })
	data, _ := json.Marshal(createRequest{CustomerID: req.CustomerID, Items: items})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (h *handler) getOrder(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !uuidPattern.MatchString(id) {
		writeError(w, http.StatusBadRequest, "order id must be a UUID")
		return
	}
	o, err := h.orders.Get(r.Context(), id)
	if errors.Is(err, order.ErrNotFound) {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	if err != nil {
		h.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, o)
}

func (h *handler) internalError(w http.ResponseWriter, r *http.Request, err error) {
	h.log.Error("request failed", "method", r.Method, "path", r.URL.Path, "err", err)
	writeError(w, http.StatusInternalServerError, "internal error")
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(status int) {
	s.status = status
	s.ResponseWriter.WriteHeader(status)
}

func (h *handler) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
			return // probes every few seconds would flood the log
		}
		h.log.Info("request", "method", r.Method, "path", r.URL.Path, "status", rec.status,
			"duration_ms", time.Since(start).Milliseconds())
	})
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
