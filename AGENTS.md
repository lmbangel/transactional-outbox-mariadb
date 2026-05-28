# Outbox Pattern — Supply Chain Sales Order Flow

## Project overview

Build a focused demonstration of the **transactional outbox pattern** applied to a
supply chain sales order flow, written in Go. The project is intentionally scoped
to v1: polling-based outbox relay with MariaDB and RabbitMQ. CDC and Kafka are
documented as a future upgrade path but are NOT in scope here.

The goal is a clean, well-documented reference implementation that runs end-to-end
with a single `docker compose up`.

---

## Tech stack

| Concern | Choice |
|---|---|
| Language | Go (latest stable) |
| Database | MariaDB 10.11 (Docker) |
| Migrations | `goose` |
| Message broker | RabbitMQ 3 with management plugin (Docker) |
| Outbox mechanism | Polling (time.Ticker) |
| Containerisation | Docker Compose |

---

## Repo structure

```
/
├── CLAUDE.md
├── README.md
├── docker-compose.yml
├── .env.example
├── Makefile
│
├── db/
│   └── migrations/
│       ├── 00001_create_orders.sql
│       └── 00002_create_outbox.sql
│
├── internal/
│   ├── db/
│   │   └── connection.go          # MariaDB connection + ping
│   ├── models/
│   │   ├── order.go               # Order struct
│   │   └── outbox.go              # OutboxEvent struct
│   └── broker/
│       └── rabbitmq.go            # RabbitMQ connection + publish helper
│
├── services/
│   ├── order-service/
│   │   ├── main.go                # HTTP server entry point
│   │   ├── handler.go             # POST /orders handler
│   │   └── repository.go         # Transactional write: orders + outbox
│   │
│   ├── outbox-relay/
│   │   ├── main.go                # Entry point, starts ticker loop
│   │   └── relay.go               # Poll → publish → mark published
│   │
│   ├── inventory-service/
│   │   ├── main.go
│   │   └── consumer.go            # Subscribes to OrderPlaced, reserves stock
│   │
│   ├── notification-service/
│   │   ├── main.go
│   │   └── consumer.go            # Subscribes to OrderPlaced, logs email send
│   │
│   └── fulfillment-service/
│       ├── main.go
│       └── consumer.go            # Subscribes to OrderPlaced, logs shipment schedule
│
└── go.work                        # Go workspace (all services in one repo)
```

---

## Bash commands

```bash
# Start all services
docker compose up --build

# Run DB migrations
make migrate-up

# Run migrations down
make migrate-down

# Run all tests
make test

# Run a specific service locally (example)
go run ./services/order-service

# Lint
golangci-lint run ./...

# Build all binaries
make build
```

---

## Database schema

### orders table

```sql
CREATE TABLE orders (
    id           CHAR(36)     NOT NULL PRIMARY KEY,
    customer_id  CHAR(36)     NOT NULL,
    product_sku  VARCHAR(100) NOT NULL,
    quantity     INT          NOT NULL,
    status       VARCHAR(50)  NOT NULL DEFAULT 'PENDING',
    created_at   DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);
```

### outbox table

```sql
CREATE TABLE outbox (
    id            CHAR(36)     NOT NULL PRIMARY KEY,
    event_type    VARCHAR(100) NOT NULL,
    aggregate_id  CHAR(36)     NOT NULL,
    payload       JSON         NOT NULL,
    created_at    DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    published_at  DATETIME(6)  NULL DEFAULT NULL
);
```

`published_at IS NULL` means the event is pending. The relay sets it on success.

---

## Core implementation rules

### Transactional write (the whole point of the pattern)

The order-service MUST write to both `orders` and `outbox` inside a **single
database transaction**. If either insert fails, both roll back. This is the
guarantee the outbox pattern provides — never use two separate DB calls.

```go
// repository.go — pseudocode outline
func (r *Repository) CreateOrder(ctx context.Context, order Order) error {
    tx, err := r.db.BeginTx(ctx, nil)
    // INSERT INTO orders ...
    // INSERT INTO outbox ...
    return tx.Commit()
}
```

### Outbox relay polling loop

Use `time.Ticker` with a configurable interval (default 5 seconds, set via env).
The relay MUST use `SELECT ... FOR UPDATE SKIP LOCKED` to be safe for multiple
relay instances (even if v1 runs only one).

```go
// relay.go — pseudocode outline
func (r *Relay) poll(ctx context.Context) {
    // SELECT id, event_type, aggregate_id, payload
    // FROM outbox
    // WHERE published_at IS NULL
    // ORDER BY created_at ASC
    // LIMIT 10
    // FOR UPDATE SKIP LOCKED

    // for each row:
    //   publish to RabbitMQ exchange
    //   UPDATE outbox SET published_at = NOW() WHERE id = ?
}
```

Process and mark events one at a time inside the loop — do not batch-mark.
If publishing fails, log the error and leave `published_at` as NULL so it
retries on the next tick.

### RabbitMQ topology

- **Exchange**: `supply_chain` (type: `topic`, durable: true)
- **Routing key**: `order.placed` (use `event_type` field lowercased + dotted)
- Each consumer service declares its own queue and binds to the exchange
- Queues are durable so messages survive RabbitMQ restarts

### Event payload shape

```json
{
  "event_id": "uuid",
  "event_type": "OrderPlaced",
  "aggregate_id": "order-uuid",
  "occurred_at": "2025-01-01T12:00:00Z",
  "data": {
    "customer_id": "uuid",
    "product_sku": "SKU-001",
    "quantity": 3,
    "status": "PENDING"
  }
}
```

---

## Environment variables

All services read from environment variables. Provide an `.env.example` at the root.

| Variable | Default | Used by |
|---|---|---|
| `DB_DSN` | `root:secret@tcp(mariadb:3306)/supplychain` | all services |
| `RABBITMQ_URL` | `amqp://guest:guest@rabbitmq:5672/` | relay + consumers |
| `ORDER_SERVICE_PORT` | `8080` | order-service |
| `RELAY_POLL_INTERVAL_SECONDS` | `5` | outbox-relay |
| `RELAY_BATCH_SIZE` | `10` | outbox-relay |

---

## Go conventions

- Use `database/sql` with `github.com/go-sql-driver/mysql` (compatible with MariaDB)
- Use `github.com/rabbitmq/amqp091-go` for RabbitMQ
- Use `github.com/pressly/goose/v3` for migrations
- Use `github.com/google/uuid` for UUID generation
- Structured logging with `log/slog` (stdlib, no external logger needed for v1)
- No global state — pass dependencies via struct fields (constructor injection)
- Return errors, never panic in business logic
- Context must be threaded through all DB and broker calls
- Keep each service's `main.go` thin — wire dependencies, start server/loop, handle
  OS signals for graceful shutdown

---

## Makefile targets to implement

```makefile
.PHONY: up down build test migrate-up migrate-down lint

up:
	docker compose up --build

down:
	docker compose down -v

build:
	go build ./...

test:
	go test ./... -v -race

migrate-up:
	goose -dir db/migrations mysql "$$DB_DSN" up

migrate-down:
	goose -dir db/migrations mysql "$$DB_DSN" down

lint:
	golangci-lint run ./...
```

---

## docker-compose.yml services to include

1. `mariadb` — image `mariadb:10.11`, exposes port 3306, health check on `mysqladmin ping`
2. `rabbitmq` — image `rabbitmq:3-management`, exposes 5672 and 15672 (management UI)
3. `migrate` — runs `goose` migrations on startup, depends on `mariadb` being healthy
4. `order-service` — depends on `migrate` completing
5. `outbox-relay` — depends on `mariadb` and `rabbitmq` being healthy
6. `inventory-service` — depends on `rabbitmq`
7. `notification-service` — depends on `rabbitmq`
8. `fulfillment-service` — depends on `rabbitmq`

Use `depends_on` with `condition: service_healthy` where health checks are defined.

---

## README structure to write

The README must cover these sections in order:

1. **The problem** — explain dual-write failure without the outbox pattern (1 short paragraph)
2. **How the outbox pattern solves it** — the single-transaction guarantee
3. **Architecture diagram** — embed as a Mermaid diagram (flowchart TD)
4. **Sales order flow walkthrough** — numbered steps from POST /orders to consumers
5. **Getting started** — prerequisites, clone, `docker compose up`, test the endpoint
6. **Project structure** — annotated directory tree
7. **Design decisions** — polling vs CDC, at-least-once delivery, idempotency note,
   why MariaDB
8. **Roadmap** — v2 CDC + Kafka upgrade path (brief, link to a ROADMAP.md)
9. **Contributing** — brief

---

## What is explicitly OUT OF SCOPE for v1

Do not implement these — note them in the README as future work:

- CDC / Debezium connector
- Kafka (RabbitMQ only for v1)
- Dead-letter queues
- Retry backoff on the relay
- Consumer idempotency keys (mention the pattern, don't implement)
- Authentication on the order-service endpoint
- Metrics / tracing

---

## Build order

Implement in this sequence to allow incremental testing at each step:

1. `docker-compose.yml` with MariaDB + RabbitMQ + health checks
2. DB migrations (goose) — orders + outbox tables
3. `internal/db` — connection helper
4. `internal/broker` — RabbitMQ connection + publish
5. `internal/models` — Order and OutboxEvent structs
6. `order-service` — POST /orders with transactional write
7. `outbox-relay` — polling loop + publish + mark published
8. `inventory-service` consumer (simplest — just log the event)
9. `notification-service` consumer
10. `fulfillment-service` consumer
11. Makefile
12. README.md

Test end-to-end after step 7 before building consumers — confirm events flow from
HTTP request through DB through relay into RabbitMQ using the management UI at
http://localhost:15672.

---

## Verification checklist

Before considering v1 complete:

- [ ] `docker compose up --build` starts all services with no errors
- [ ] `POST /orders` returns 201 with order ID
- [ ] Row appears in `orders` table
- [ ] Row appears in `outbox` table with `published_at = NULL`
- [ ] Within ~10 seconds `published_at` is set by the relay
- [ ] All three consumer services log receipt of the `OrderPlaced` event
- [ ] Killing and restarting the relay does not duplicate events (SKIP LOCKED)
- [ ] All tests pass with `make test`