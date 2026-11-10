# ADR 0012: Kubernetes, GitOps and observability on a local k3d cluster

Status: accepted for phase 7

## Context
The phase 7 acceptance is a one-command local cluster deploy and a Grafana that shows the latency histograms. The stack follows the author's other project (`cube_test`): k3d, Argo CD, Strimzi, CloudNativePG, kube-prometheus-stack, NetworkPolicy default-deny. The versions that project already runs are reused (k3s v1.35.5, Strimzi 1.2.0 with Kafka 4.3.1, CloudNativePG 0.29.1, kube-prometheus-stack 91.8.2, Argo CD 10.9.5).

## Decision

**A cluster of its own that touches nothing else.** `k3d cluster create` runs with `updateDefaultKubeconfig: false`; the scripts write `.k8s/kubeconfig` and export it, so `~/.kube/config` and the current kubectl context are never changed and another k3d cluster on the machine is left alone. Host ports (13000 Grafana, 19090 Prometheus, 18800 Argo CD, 15051/18180 gateway, 18183 market data) are chosen not to collide with the other cluster's.

**GitOps from the local working tree.** The repository is private, and putting a GitHub token into a cluster to let Argo CD read it is a credential I did not want to create. Instead `scripts/k8s-publish.sh` commits the working tree (committed or not) into a bare repository served by a small `git daemon` pod, and Argo CD reads `git://gitserver.gitserver.svc`. Argo CD's own git client does not speak "dumb" HTTP (the first attempt failed with `unexpected EOF` listing refs) and the `alpine/git` image has no `http-backend`, hence the git protocol, read-only. Consequence: what runs is what is in the working tree after `k8s-publish.sh`, not what is on GitHub; the repository lives in an `emptyDir` and must be published again if that pod restarts. A deployment from GitHub would change only the `repoURL` fields.

**App of apps with ordered waves.** `root` deploys Strimzi, CloudNativePG and kube-prometheus-stack (waves -20 and -19) and then `mini-exchange` (wave 0). A custom health check on the Application resource (taken from `cube_test`) makes the root wait for each child to be Healthy, otherwise waves only order creation and the application would sync before the operators' CRDs exist. Kafka, PostgreSQL and Redis are in wave -1 inside the application.

**Argo CD cannot see when dependencies accept connections.** The Strimzi and CloudNativePG custom resources report Healthy early, and a new pod can be cut off for its first seconds by the network policy controller. The first deployment had gateway, ledger and marketdata crash once at start (`connection refused` to Kafka and PostgreSQL). Each now has an init container (`busybox nc`) that waits for its dependencies; after that, zero restarts.

**One engine, enforced.** The gateway is a StatefulSet with one replica (a replacement is stop-then-start, never side by side, and the snapshot volume follows it). A `ValidatingAdmissionPolicy` rejects `replicas > 1`, including the `scale` subresource, because two engines would both claim the log's sequence numbers. It has no CPU limit (throttling would show as latency) and `GOMAXPROCS=2` because the engine and dispatcher spin-wait.

**NetworkPolicy default-deny** in the `mx` namespace, with explicit rules: DNS, Prometheus scrape from `monitoring`, clients to the gateway and market data, and per-workload egress (gateway to Redis and Kafka, ledger to PostgreSQL and Kafka, market data to Kafka, the one-off Jobs). Strimzi generates the policies of its Kafka listeners; the listener is configured with `networkPolicyPeers: [podSelector: {}]` so it does not open Kafka to every namespace (policies are additive). The PostgreSQL pod needs egress to the Kubernetes API for the CloudNativePG instance manager. Verified by tests: a stranger pod cannot reach Redis, and a pod in another namespace cannot reach PostgreSQL or Kafka.

**Observability as code.** One PodMonitor for every pod labelled `mx.io/metrics`; a Grafana dashboard JSON loaded by the sidecar from a labelled ConfigMap (latency histograms first: PlaceOrder p50/p99/p99.9, engine queue wait and match time, event publish lag, then settlement lag, health counters that should be zero, market data, pod memory and CPU); 13 alert rules, each for a failure this system can have, thresholds from BENCHMARKS.md, with `promtool` unit tests. `scripts/k8s-validate.sh` (also a CI job) builds the overlay, runs the rule tests and checks that every `mx_` metric in the dashboard and alerts is defined in the Go code, so a renamed metric cannot silently empty a panel.

**Trimmed for a laptop.** No Alertmanager (the rules show as firing in Prometheus and Grafana; nothing is routed), no node exporter, no metrics-server, no Traefik, no service load balancer, no Strimzi entity operator (the applications create their topics).

**No Sealed Secrets in this phase.** The requirements list them. There is no hand-managed secret to seal: CloudNativePG generates the database password into `mx-pg-app`, Redis has no password (access is limited by the NetworkPolicy), and the Grafana admin password is a local-development value kept in the values file on purpose, like the compose database password. A real deployment would need all three handled properly.

## Consequences and limits
- **The stack needs about 5 GiB** of the Docker VM (measured), more than I estimated. `scripts/k8s-up.sh` refuses to start below 4.5 GB available unless `FORCE=1`, because starting it next to another large cluster got other containers close to being OOM-killed.
- **Kafka here is 4.3.1, the tests and compose use 3.9.0.** The client (franz-go) is the same; the integration tests do not run against the cluster's broker.
- **The in-cluster load generator is not comparable to the localhost benchmarks**: it runs in a pod on the same 10 cores as the whole stack, through the k3d network. The numbers in BENCHMARKS.md phase 7 only show that the histograms have data.
- **Not verified here**: reconciliation (`ledger reconcile`) against the cluster's Kafka, PostgreSQL and Redis (the Jobs' network policy does not allow it and no NodePort exposes them); the alert rules firing on a real fault (they are unit-tested, not triggered); the dashboard rendered in a browser (its queries are checked through Grafana's datasource, not by looking at it).
- **Single node, no HA**: one Kafka broker, one PostgreSQL instance, one Redis; a node loss loses availability. Not a production topology.
