#!/usr/bin/env bash
# One command from nothing to the whole system on a local k3d cluster:
#   builds the images, creates the cluster "mini-exchange", installs Argo CD, publishes the working
#   tree to an in-cluster git server, and lets Argo CD deploy Strimzi (Kafka), CloudNativePG,
#   kube-prometheus-stack and mini-exchange itself. It waits until everything is Healthy.
# Needs: docker, k3d, kubectl, helm, git, internet access (images and Helm charts).
set -euo pipefail
source "$(dirname "$0")/k8s-lib.sh"
for t in docker k3d kubectl helm git; do need "$t"; done

# The whole stack needs about 4 GB of the Docker VM. Refuse rather than start something that gets
# other containers OOM-killed.
avail="$(docker run --rm alpine sh -c "awk '/MemAvailable/ {print int(\$2/1024)}' /proc/meminfo" 2>/dev/null || echo 0)"
if [ "${avail:-0}" -lt "${MIN_FREE_MB:-4500}" ] && [ -z "${FORCE:-}" ]; then
  echo "Only ${avail} MB of memory is available in the Docker VM (need about 4500). Stop other" >&2
  echo "clusters or containers, or give Docker more memory, or run with FORCE=1." >&2
  exit 1
fi

echo "== building images"
for c in "${IMAGES[@]}"; do
  docker build -q -t "mx/$c:dev" --build-arg CMD="$c" . >/dev/null
done

echo "== cluster"
if ! cluster_exists; then
  k3d cluster create --config deploy/k3d/cluster.yaml --kubeconfig-update-default=false --kubeconfig-switch-context=false
fi
k3d kubeconfig get "$CLUSTER" >"$KUBECONFIG"
for c in "${IMAGES[@]}"; do k3d image import "mx/$c:dev" -c "$CLUSTER" >/dev/null; done
kubectl wait --for=condition=Ready node --all --timeout=180s

echo "== in-cluster git server"
kubectl apply -f deploy/platform/gitserver.yaml
kubectl -n gitserver rollout status deploy/gitserver --timeout=180s
scripts/k8s-publish.sh

echo "== Argo CD"
helm repo add argo https://argoproj.github.io/argo-helm >/dev/null 2>&1 || true
helm repo update argo >/dev/null
helm upgrade --install argocd argo/argo-cd --version 10.9.5 -n argocd --create-namespace \
  -f deploy/platform/values/argocd.yaml --wait --timeout 10m
kubectl apply -f deploy/argocd/root.yaml

echo "== waiting for Argo CD to deploy everything (first run pulls many images: 10 to 20 minutes)"
deadline=$(( $(date +%s) + ${TIMEOUT:-1800} ))
while :; do
  apps="$(kubectl -n argocd get applications -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.sync.status}{" "}{.status.health.status}{"\n"}{end}' 2>/dev/null || true)"
  total="$(printf '%s\n' "$apps" | grep -c . || true)"
  ok="$(printf '%s\n' "$apps" | grep -c ' Synced Healthy$' || true)"
  printf '\r%s of %s applications Synced and Healthy ' "$ok" "$total"
  if [ "$total" -ge 5 ] && [ "$ok" = "$total" ]; then break; fi
  if [ "$(date +%s)" -gt "$deadline" ]; then echo; echo "timed out; state:"; printf '%s\n' "$apps"; exit 1; fi
  sleep 10
done
echo

pass="$(kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' 2>/dev/null | base64 -d || true)"
cat <<MSG

mini-exchange is up on the k3d cluster "$CLUSTER".
  Grafana      http://localhost:13000   (admin / mx-local-dev)  dashboard "Mini Exchange"
  Prometheus   http://localhost:19090
  Argo CD      http://localhost:18800   (admin / ${pass:-see: kubectl -n argocd get secret argocd-initial-admin-secret})
  gateway gRPC localhost:15051    HTTP http://localhost:18180/readyz
  market data  ws://localhost:18183/ws

kubectl:  export KUBECONFIG=$KUBECONFIG
next:     scripts/k8s-seed.sh     fund accounts 1..16 in Redis and the ledger
          scripts/k8s-load.sh     drive load so the dashboards show something
          scripts/k8s-test.sh     check the deployment
          scripts/k8s-down.sh     delete the cluster
MSG
