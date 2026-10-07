.PHONY: test bench build run load up chaos

test:
	go vet ./... && go test -race -count=1 ./...

bench:
	go test -bench . -run xxx ./internal/limiter

build:
	go build -o bin/ ./cmd/...

run:
	go run ./cmd/server -rate 100 -burst 200

load:
	go run ./cmd/loadtest -url http://localhost:8080 -c 64 -d 15s

up:
	docker compose up -d --build

chaos:
	./scripts/chaos.sh
