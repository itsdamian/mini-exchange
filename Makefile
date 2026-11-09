.PHONY: build test test-full lint proto bench bench-report bench-recovery loadtest chaos up down smoke

build:
	go build ./...

# Fast: -short trims the 1M-op differential test to 100k and skips the Kafka
# (Docker) integration tests. Random order, race detector.
test:
	go test -race -shuffle=on -short ./...

# Everything, including the full 1M-op differential test and the Kafka
# integration tests (needs Docker; they start and kill a real broker).
test-full:
	go test -race -shuffle=on ./...

lint:
	go vet ./...
	golangci-lint run

# Regenerate gen/ from proto/ (needs buf, protoc-gen-go, protoc-gen-go-grpc on PATH).
proto:
	buf lint
	buf generate

# Go micro-benchmarks (ns/op, allocs/op).
bench:
	go test -run '^$$' -bench . -benchmem -benchtime 2s ./internal/engine

# Reproducible latency distribution; see scripts/bench.sh and BENCHMARKS.md.
bench-report:
	./scripts/bench.sh

# Recovery-time benchmark: replay of a 1M-event log from Kafka (needs Docker).
bench-recovery:
	./scripts/bench-recovery.sh

# Chaos tests against real processes and the compose Kafka (see docs/runbook.md).
chaos:
	./scripts/chaos/engine-kill.sh
	./scripts/chaos/kafka-kill.sh

# End-to-end open-loop load test (local gateway + loadgen); see scripts/loadtest.sh.
loadtest:
	./scripts/loadtest.sh

up:
	docker compose -f deploy/docker-compose.yml up -d --build --wait

down:
	docker compose -f deploy/docker-compose.yml down -v

smoke:
	curl -fsS localhost:$${GATEWAY_PORT:-18080}/healthz && curl -fsS localhost:$${GATEWAY_PORT:-18080}/metrics | grep -q go_goroutines && echo "smoke ok"
