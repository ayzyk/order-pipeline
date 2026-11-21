TEST_DATABASE_URL ?= postgres://orders:orders@localhost:5436/orders?sslmode=disable
TEST_KAFKA_BROKERS ?= localhost:9094

.PHONY: up down smoke test kind-up kind-deploy kind-down

up:
	docker compose up -d --build

down:
	docker compose down -v

smoke:
	./scripts/smoke.sh http://localhost:8080

# needs `make up` first: the tests use Postgres and Kafka from docker compose
test:
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" TEST_KAFKA_BROKERS="$(TEST_KAFKA_BROKERS)" go test -race -count=1 ./...

kind-up:
	kind create cluster --name orders

kind-deploy:
	docker build -t order-pipeline:dev .
	kind load docker-image order-pipeline:dev --name orders
	kubectl apply -k deploy/k8s/overlays/kind
	kubectl -n order-pipeline rollout status deploy/api --timeout=300s

kind-down:
	kind delete cluster --name orders
