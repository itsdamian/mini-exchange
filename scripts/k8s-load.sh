#!/usr/bin/env bash
# Run the load generator inside the cluster: RATE=2000 DURATION=120s scripts/k8s-load.sh
set -euo pipefail
source "$(dirname "$0")/k8s-lib.sh"
need kubectl
RATE="${RATE:-2000}"; DURATION="${DURATION:-120s}"
kubectl -n mx delete job loadgen --ignore-not-found >/dev/null
sed -e "s/@RATE@/${RATE}/" -e "s/@DURATION@/${DURATION}/" deploy/k8s/jobs/load.yaml | kubectl apply -f -
echo "load: ${RATE} orders/s for ${DURATION} (+5 s warm-up); watch Grafana http://localhost:13000"
kubectl -n mx wait --for=condition=complete job/loadgen --timeout=$(( $(printf '%s' "$DURATION" | tr -dc 0-9) + 300 ))s
kubectl -n mx logs job/loadgen | tail -12
