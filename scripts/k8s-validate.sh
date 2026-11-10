#!/usr/bin/env bash
# Offline checks of the Kubernetes and monitoring files; no cluster needed (CI runs it).
#   - the Kustomize overlay builds
#   - the alert rules are valid and pass their promtool unit tests
#   - every mx_ metric used by the dashboard and the alerts is defined in the Go code
#   - every image the manifests use is one scripts/k8s-up.sh builds
set -euo pipefail
cd "$(dirname "$0")/.."
fail=0
err() { echo "FAIL: $*" >&2; fail=1; }

echo "== kustomize build"
out="$(mktemp)"; trap 'rm -f "$out" /tmp/mx-rules.yaml' EXIT
kustomize build deploy/k8s/overlays/k3d >"$out" || err "kustomize build failed"
for k in StatefulSet Deployment NetworkPolicy PrometheusRule PodMonitor Kafka Cluster ValidatingAdmissionPolicy; do
  grep -q "^kind: $k\$" "$out" || err "no $k in the rendered manifests"
done

echo "== alert rules (promtool)"
sed -n '/^  groups:/,$p' deploy/k8s/base/observability/alerts.yaml | sed 's/^  //' >/tmp/mx-rules.yaml
pt() { docker run --rm -v /tmp/mx-rules.yaml:/tmp/mx-rules.yaml -v "$PWD/deploy/k8s/base/observability/alerts.test.yaml":/t/alerts.test.yaml \
  --entrypoint promtool prom/prometheus:v3.15.0 "$@"; }
pt check rules /tmp/mx-rules.yaml >/dev/null || err "promtool check rules failed"
pt test rules /t/alerts.test.yaml || err "alert unit tests failed"

echo "== metrics used by the dashboard and alerts exist in the code"
defined="$(grep -rhoE '"mx_[a-z_]+"' internal cmd --include='*.go' | tr -d '"' | sort -u)"
used="$( (cat deploy/k8s/base/observability/dashboard.json deploy/k8s/base/observability/alerts.yaml) | grep -oE 'mx_[a-z_]+' | sed -E 's/_(bucket|count|sum)$//' | sort -u)"
for m in $used; do
  echo "$defined" | grep -qx "$m" || err "metric $m is used in the dashboard or alerts but not defined in the Go code"
done

echo "== images"
for img in $(grep -oE 'image: mx/[a-z]+:dev' "$out" deploy/k8s/jobs/*.yaml | sed 's/.*mx\///; s/:dev//' | sort -u); do
  grep -q "IMAGES=.*\b$img\b" scripts/k8s-lib.sh || err "image mx/$img:dev is not built by k8s-up.sh"
done

python3 -c "import json;json.load(open('deploy/k8s/base/observability/dashboard.json'))" || err "dashboard.json is not valid JSON"
[ "$fail" = 0 ] && echo "k8s files OK"
exit "$fail"
