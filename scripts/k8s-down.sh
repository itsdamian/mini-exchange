#!/usr/bin/env bash
# Delete the cluster and everything in it (volumes included).
set -euo pipefail
source "$(dirname "$0")/k8s-lib.sh"
need k3d
cluster_exists && k3d cluster delete "$CLUSTER"
rm -f "$KUBECONFIG"
