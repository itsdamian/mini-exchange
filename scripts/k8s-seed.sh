#!/usr/bin/env bash
# Fund accounts 1..16 in Redis and in the ledger (the funds the load generator uses).
# Run it once per cluster: it adds to the Redis balances every time.
set -euo pipefail
source "$(dirname "$0")/k8s-lib.sh"
need kubectl
kubectl -n mx delete job seed-redis seed-ledger --ignore-not-found >/dev/null
kubectl apply -f deploy/k8s/jobs/seed.yaml
kubectl -n mx wait --for=condition=complete job/seed-redis job/seed-ledger --timeout=180s
kubectl -n mx logs job/seed-redis
kubectl -n mx logs job/seed-ledger | tail -3
