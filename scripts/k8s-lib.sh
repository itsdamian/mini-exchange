# Shared by the k8s-*.sh scripts. Source it, do not run it.
cd "$(dirname "${BASH_SOURCE[0]}")/.."
CLUSTER=mini-exchange
# A kubeconfig of its own: this never touches ~/.kube/config or the current kubectl context, so
# another cluster on the machine (another k3d cluster, say) is left alone.
export KUBECONFIG="$PWD/.k8s/kubeconfig"
mkdir -p .k8s
IMAGES=(gateway ledger marketdata loadgen risktool)

need() { command -v "$1" >/dev/null || { echo "missing tool: $1" >&2; exit 2; }; }
cluster_exists() { k3d cluster list "$CLUSTER" >/dev/null 2>&1; }
