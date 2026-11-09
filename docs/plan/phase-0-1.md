# Plan: phases 0 and 1

## Phase 0: scaffold (done)
- [x] Repo, Go module, `.gitignore`, Makefile
- [x] Empty gateway service: `/healthz`, `/readyz`, `/metrics`, graceful shutdown
- [x] Dockerfile (multi-stage, multi-arch capable, distroless, non-root)
- [x] docker compose: Kafka (KRaft), Redis 7, PostgreSQL 17, Prometheus, gateway
- [x] CI: vet, staticcheck, golangci-lint, `-race -shuffle=on` tests, Trivy, gitleaks
- Acceptance: `docker compose up` brings everything up (verified: all services healthy, Prometheus `up{job="gateway"} = 1`); CI config in place, first GitHub run pending until the repo has a remote.

## Phase 1: engine core (done)
- [x] Types, events, reject reasons, determinism hash
- [x] Order book: slab + intrusive FIFO lists + sorted price levels
- [x] Matching: LIMIT GTC/IOC, MARKET, place, cancel, partial fills, STP cancel newest, typed rejects
- [x] Naive reference implementation
- [x] Unit tests per rule; rapid property tests; differential test (1M ops)
- [x] In-process benchmarks and `enginebench` (HdrHistogram), `BENCHMARKS.md`
- [x] ADRs 0001 to 0004
- Acceptance (FR-1 and engine target in 5.1): met, except strict zero allocations in the deep-book scenario (documented in `BENCHMARKS.md`).
