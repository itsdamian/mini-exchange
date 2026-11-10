#!/usr/bin/env bash
# Checks a running cluster (scripts/k8s-up.sh), after scripts/k8s-seed.sh and scripts/k8s-load.sh.
# Prints one line per check and exits non-zero if any failed.
set -uo pipefail
source "$(dirname "$0")/k8s-lib.sh"
need kubectl; need curl
fail=0
ok()  { echo "PASS  $*"; }
bad() { echo "FAIL  $*"; fail=1; }
check() { local name="$1"; shift; if "$@" >/dev/null 2>&1; then ok "$name"; else bad "$name"; fi; }
prom() { curl -fsS --max-time 10 -G "http://localhost:19090/api/v1/query" --data-urlencode "query=$1"; }
graf() { curl -fsS --max-time 10 -u admin:mx-local-dev "http://localhost:13000$1"; }
nonempty() { grep -q '"result":\[{'; }

echo "-- GitOps"
apps="$(kubectl -n argocd get applications -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.sync.status}{" "}{.status.health.status}{"\n"}{end}')"
for a in root strimzi cnpg kube-prometheus-stack mini-exchange; do
  echo "$apps" | grep -q "^$a Synced Healthy$" && ok "Argo CD application $a is Synced and Healthy" || bad "Argo CD application $a: $(echo "$apps" | grep "^$a " || echo missing)"
done

echo "-- workloads"
for w in sts/gateway sts/redis deploy/ledger deploy/marketdata; do
  check "$w is ready" kubectl -n mx rollout status "$w" --timeout=60s
done
check "Kafka is Ready" kubectl -n mx wait kafka/mx-kafka --for=condition=Ready --timeout=60s
check "PostgreSQL cluster is healthy" kubectl -n mx wait cluster/mx-pg --for=condition=Ready --timeout=60s
check "gateway answers on the host port" curl -fsS --max-time 5 http://localhost:18180/readyz
check "gateway has no restarts" test "$(kubectl -n mx get pod gateway-0 -o jsonpath='{.status.containerStatuses[0].restartCount}')" = 0

echo "-- policy"
check "the engine cannot be scaled to 2 replicas" bash -c "! kubectl -n mx scale sts/gateway --replicas=2 2>&1 | tee /dev/stderr | grep -q 'scaled'"
check "gateway still has 1 replica" test "$(kubectl -n mx get sts gateway -o jsonpath='{.spec.replicas}')" = 1
probe() { kubectl run "probe-$RANDOM" -n "$1" --rm -i --restart=Never --image=busybox:1.37 --labels="$3" --command -- sh -c "nc -z -w 3 $2" >/dev/null 2>&1; }
check "default-deny: a stranger pod in mx cannot reach Redis" bash -c "$(declare -f probe); ! probe mx redis.mx.svc 6379 x=y"
check "default-deny: a pod in another namespace cannot reach PostgreSQL" bash -c "$(declare -f probe); ! probe default mx-pg-rw.mx.svc 5432 x=y"
check "default-deny: a pod in another namespace cannot reach Kafka" bash -c "$(declare -f probe); ! probe default mx-kafka-kafka-bootstrap.mx.svc 9092 x=y"

echo "-- observability"
up_count() { prom 'count(up{namespace="mx", pod=~"gateway-.*|ledger-.*|marketdata-.*"} == 1)' | python3 -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(r[0]["value"][1] if r else 0)'; }
[ "$(up_count)" = 3 ] && ok "Prometheus scrapes gateway, ledger and marketdata (3 targets up)" || bad "Prometheus scrapes $(up_count) of 3 targets"
check "latency histogram has data (mx_gateway_request_seconds)" bash -c "$(declare -f prom nonempty); prom 'histogram_quantile(0.99, sum by (le) (rate(mx_gateway_request_seconds_bucket[5m])))' | nonempty"
check "engine match-time histogram has data" bash -c "$(declare -f prom nonempty); prom 'histogram_quantile(0.99, sum by (le) (rate(mx_engine_match_seconds_bucket[5m])))' | nonempty"
check "ledger lag histogram has data" bash -c "$(declare -f prom nonempty); prom 'histogram_quantile(0.99, sum by (le) (rate(mx_ledger_settle_lag_seconds_bucket[5m])))' | nonempty"
check "alert rules are loaded in Prometheus" bash -c "curl -fsS http://localhost:19090/api/v1/rules | grep -q MxLedgerDeadLetter"
check "Grafana has the Mini Exchange dashboard" bash -c "$(declare -f graf); graf '/api/search?query=Mini%20Exchange' | grep -q mini-exchange"
# Run the latency panel's query through Grafana's own datasource: the dashboard shows data, not just exists.
q='histogram_quantile(0.99, sum by (le) (rate(mx_gateway_request_seconds_bucket{rpc=~".*PlaceOrder"}[5m])))'
check "Grafana's Prometheus datasource returns the PlaceOrder latency" bash -c "$(declare -f graf nonempty); graf \"/api/datasources/proxy/uid/prometheus/api/v1/query?query=\$(python3 -c 'import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1]))' '$q')\" | nonempty"

echo "-- recovery: delete the gateway pod"
seq() { prom 'max(mx_engine_last_sequence)' | grep -o '"value":\[[^,]*,"[0-9]*"' | grep -o '[0-9]*"$' | tr -d '"'; }
before="$(seq)"
kubectl -n mx delete pod gateway-0 --wait=true >/dev/null
check "gateway comes back ready from its snapshot and the log" kubectl -n mx wait --for=condition=Ready pod/gateway-0 --timeout=300s
sleep 20 # one Prometheus scrape of the new pod
after="$(seq)"
if [ -n "$before" ] && [ -n "$after" ] && [ "$after" -ge "$before" ]; then ok "the engine sequence continued ($before -> $after), no restart from 1"; else bad "engine sequence before=$before after=$after"; fi

exit "$fail"
