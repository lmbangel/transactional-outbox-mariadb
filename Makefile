.PHONY: up down build test migrate-up migrate-down lint clean

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

clean:
	go clean ./...
	docker compose down -v --remove-orphans
