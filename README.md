# order-pipeline

An order service split into three small Go programs that talk through Kafka:
a REST API takes orders, a relay publishes events, a worker reserves stock.
It runs with docker compose and on Kubernetes, and CI deploys it to a real kind cluster on every push.

I built it to learn the problems that only show up once a service talks to a message broker:
what if Kafka is down, what if a message comes twice, what if a pod is killed in the middle of a request.

Stack: Go, PostgreSQL, Kafka ([kafka-go](https://github.com/segmentio/kafka-go)), Docker, Kubernetes (kustomize), GitHub Actions.

## How it works

```
  POST /orders
       │
       ▼
  ┌─────────┐   one transaction    ┌──────────────────────┐
  │   api   │ ───────────────────► │ orders + outbox rows │  PostgreSQL
  └─────────┘                      └──────────────────────┘
                                              │ SELECT ... FOR UPDATE SKIP LOCKED
                                              ▼
                                         ┌─────────┐
                                         │  relay  │
                                         └─────────┘
                                              │ key = order id
                                              ▼
                                     Kafka: orders.events (3 partitions)
                                              │ consumer group "fulfillment"
                                              ▼
                                         ┌─────────┐   bad message   ┌──────────────────────┐
                                         │ worker  │ ──────────────► │ orders.events.dlq    │
                                         └─────────┘                 └──────────────────────┘
                                              │ reserve stock, set status
                                              ▼
                                  orders.status = confirmed / rejected
```

1. `POST /orders` saves the order **and** an `order.created` event in the outbox table in one transaction, and answers `202 Accepted`.
2. The relay reads unsent events and publishes them to Kafka. The order id is the message key, so events of one order stay in one partition, in order.
3. The worker takes the stock for all items or none, and sets the order to `confirmed` or `rejected`.
4. The client polls `GET /orders/{id}` to see the result.

## Problems and how they are solved

| Problem | Solution |
| --- | --- |
| The order is saved but publishing to Kafka fails, the event is lost | **Transactional outbox**: the event is a row in the same transaction as the order |
| Writing to the database and to Kafka can not be one transaction | The relay sends from the outbox later and retries until Kafka is back |
| Kafka delivers a message twice (relay crashed after sending) | The worker stores the event id in `processed_events` in the same transaction as the stock change |
| Worker crashes after the database commit but before the Kafka commit | Offsets are committed only after the transaction, the repeat is caught as a duplicate |
| Two orders want the last item at the same time | Products are locked with `SELECT ... FOR UPDATE` in sku order, no oversell, no deadlock |
| A broken message blocks the partition forever | It goes to a dead letter topic with the error in a header |
| Postgres is down for a minute | Temporary errors are retried with backoff on the same message, nothing is skipped |
| The client retries `POST /orders` after a timeout | `Idempotency-Key` header, same key and body returns the same order, another body gives `409` |
| The client sends its own price | Prices are read from the database |

## Run with docker compose

```bash
make up      # postgres, kafka, migrations, api, relay and two workers
make smoke   # creates an order and waits until it is confirmed
make test    # unit and integration tests against the compose services
make down
```

```bash
curl -X POST localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-1' \
  -d '{"customer_id":"c-1","items":[{"sku":"MOUSE-01","quantity":1}]}'
# 202 {"id":"2c37e9fa-...","status":"pending","total_cents":249000,...}

curl localhost:8080/orders/2c37e9fa-...
# {"id":"2c37e9fa-...","status":"confirmed",...}
```

The demo catalog has `KEYBOARD-01`, `MOUSE-01` and `ESP32-DEV` (only 5 in stock, so ordering 99 of them shows a rejected order).

## Run on Kubernetes

```bash
make kind-up       # local cluster with kind
make kind-deploy   # build the image, load it into kind, apply the manifests
kubectl -n order-pipeline port-forward svc/api 8080:80
make smoke
```

```
deploy/k8s/
├── base/            the app: api, relay, worker
└── overlays/kind/   base + throwaway Postgres and Kafka for local use
```

What is in the manifests and why:

- **One image, three Deployments.** The binaries share code, so they share an image and only the command differs.
- **Migrations in an init container** of the api. Two api pods start at the same time, a Postgres advisory lock makes sure only one migrates.
- **Separate probes.** `/healthz` (liveness) does not touch the database, otherwise a database outage would restart every pod and make things worse. `/readyz` (readiness) does, so a pod without a database gets no traffic.
- **Graceful shutdown.** A `preStop` sleep gives the Service time to stop routing to the pod before SIGTERM, then the server finishes running requests. The worker closes its Kafka reader on SIGTERM so its partitions move to another worker right away.
- **Relay: 1 replica, `Recreate` strategy.** Two relays would never send an event twice (`SKIP LOCKED`), but could reorder events of one order.
- **Worker: 3 replicas** for 3 partitions. A fourth would sit idle in the consumer group.
- **HPA and PodDisruptionBudget** for the api.
- **Locked down pods**: non-root, read-only root filesystem, no capabilities, distroless image.

## Tests

- Handler tests with a fake store: validation, status codes, probes, idempotency hash.
- Integration tests against real Postgres: idempotency, unknown products, the outbox, two relays at once, ten orders racing for three items, duplicate and broken messages.
- Consumer tests with a fake Kafka reader: retries before commit, dead letter topic.
- An end-to-end test against real Postgres **and** Kafka: order → outbox → relay → Kafka → worker → confirmed.
- CI builds the image, deploys everything to a kind cluster and runs the smoke test against it.

Every test package gets its own Postgres schema, because `go test` runs packages in parallel and they would
otherwise steal each other's outbox events. (That one took me a while to understand.)

## Known limitations

- The relay polls the table. `LISTEN/NOTIFY` or Debezium (CDC) would cut the delay.
- Published outbox rows are never deleted, a cleanup job is missing.
- No authentication.
- No metrics yet, Prometheus would be the next step. Right now there are only JSON logs.
- Kafka and Postgres in the kind overlay have no persistence, they are only for local runs.
- If a worker is stuck on a temporary error, the whole partition waits. That is on purpose (no lost orders), but it needs an alert.
