# Plan: phase 7 (Kubernetes and observability)

- [x] k3d cluster `mini-exchange` of its own (own kubeconfig, never the default context), pinned k3s
- [x] Argo CD (app of apps) deploying Strimzi, CloudNativePG, kube-prometheus-stack and the application in sync-wave order; an in-cluster git server so Argo CD deploys the local working tree without GitHub credentials
- [x] Kustomize base and k3d overlay: Kafka (Strimzi KRaft), PostgreSQL (CloudNativePG), Redis, gateway (StatefulSet, one replica, snapshot volume), ledger, marketdata, probes, resources, non-root and read-only containers
- [x] NetworkPolicy default-deny with the connections the system needs; ValidatingAdmissionPolicy that refuses a second engine
- [x] Observability: PodMonitor, Grafana dashboard as code, alert rules with promtool unit tests; offline validation script and a CI job
- [x] `scripts/k8s-up.sh` (one command), `k8s-seed.sh`, `k8s-load.sh`, `k8s-test.sh`, `k8s-down.sh`
- [x] Deployed on the cluster and checked with `k8s-test.sh`: 28 checks pass (see BENCHMARKS.md phase 7)
- [x] ADR 0012, README, runbook
- Acceptance: one-command local cluster deploy; Grafana shows the latency histograms.
