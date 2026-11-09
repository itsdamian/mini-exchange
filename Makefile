.PHONY: build test test-full lint bench bench-report up down smoke

build:
	go build ./...

# Fast: -short trims the 1M-op differential test to 100k. Random order, race detector.
test:
	go test -race -shuffle=on -short ./...

# Everything, including the full 1M-op differential test.
test-full:
	go test -race -shuffle=on ./...

lint:
	go vet ./...
	golangci-lint run

# Go micro-benchmarks (ns/op, allocs/op).
bench:
	go test -run '^$$' -bench . -benchmem -benchtime 2s ./internal/engine

# Reproducible latency distribution; see scripts/bench.sh and BENCHMARKS.md.
bench-report:
	./scripts/bench.sh

up:
	docker compose -f deploy/docker-compose.yml up -d --build --wait

down:
	docker compose -f deploy/docker-compose.yml down -v

smoke:
	curl -fsS localhost:$${GATEWAY_PORT:-18080}/healthz && curl -fsS localhost:$${GATEWAY_PORT:-18080}/metrics | grep -q go_goroutines && echo "smoke ok"
