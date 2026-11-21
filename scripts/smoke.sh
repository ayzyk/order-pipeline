#!/usr/bin/env bash
# Creates an order through the API and waits until the worker confirms it.
# Usage: scripts/smoke.sh [base url]
set -euo pipefail

BASE_URL="${1:-http://localhost:8080}"
KEY="smoke-$(date +%s)-$RANDOM"

echo "waiting for the api at $BASE_URL"
for _ in $(seq 1 60); do
  curl -fs "$BASE_URL/readyz" > /dev/null && break
  sleep 2
done

response=$(curl -fsS -X POST "$BASE_URL/orders" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $KEY" \
  -d '{"customer_id":"smoke-test","items":[{"sku":"MOUSE-01","quantity":1}]}')
echo "created: $response"

id=$(echo "$response" | sed -E 's/.*"id":"([^"]+)".*/\1/')

for _ in $(seq 1 60); do
  status=$(curl -fsS "$BASE_URL/orders/$id" | sed -E 's/.*"status":"([^"]+)".*/\1/')
  echo "order $id: $status"
  if [ "$status" = "confirmed" ]; then
    echo "ok"
    exit 0
  fi
  if [ "$status" = "rejected" ]; then
    echo "order was rejected"
    exit 1
  fi
  sleep 2
done

echo "order was not confirmed in time"
exit 1
