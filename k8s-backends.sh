#!/bin/bash
# Deploy Postgres (CloudNativePG) and Cassandra to the current k8s context,
# then expose both via NodePort so they're reachable from localhost.
#
# Usage:
#   source ./k8s-backends.sh            # deploy (idempotent) + export env vars
#   source ./k8s-backends.sh --env      # export env vars only (assumes all is up)
#   ./k8s-backends.sh --recreate        # tear down and redeploy from scratch
#   ./k8s-backends.sh --down            # tear down everything

set -euo pipefail

NS=bangfs
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="$SCRIPT_DIR/k8s"

_export_env() {
    NODE_IP=$(kubectl get nodes \
        -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
    export POSTGRES_HOST="$NODE_IP"
    export POSTGRES_PORT=30432
    export POSTGRES_USER=bangfs
    export POSTGRES_PASSWORD=bangfs
    export POSTGRES_DB=bangfs
    export CASSANDRA_HOSTS="$NODE_IP"
    export CASSANDRA_PORT=30942
    echo "POSTGRES_HOST=$POSTGRES_HOST POSTGRES_PORT=$POSTGRES_PORT POSTGRES_USER=$POSTGRES_USER POSTGRES_PASSWORD=$POSTGRES_PASSWORD POSTGRES_DB=$POSTGRES_DB"
    echo "CASSANDRA_HOSTS=$CASSANDRA_HOSTS CASSANDRA_PORT=$CASSANDRA_PORT"
}

if [ "${1:-}" = "--env" ]; then
    _export_env
    return 0 2>/dev/null || exit 0
fi

if [ "${1:-}" = "--recreate" ]; then
    bash "$0" --down
elif [ "${1:-}" = "--down" ]; then
    helm uninstall cnpg -n $NS         2>/dev/null || true
    helm uninstall cnpg -n cnpg-system 2>/dev/null || true
    kubectl get crd -o name | grep '\.cnpg\.io' | xargs -r kubectl delete --ignore-not-found
    kubectl delete svc bangfs-pg-nodeport    -n default --ignore-not-found 2>/dev/null || true
    kubectl delete svc bangfs-cass-cassandra -n default --ignore-not-found 2>/dev/null || true
    kubectl delete namespace $NS         --ignore-not-found --wait
    kubectl delete namespace cnpg-system --ignore-not-found --wait

    echo -n ">> verifying clean..."
    for i in $(seq 1 30); do
        if ! kubectl get namespace $NS &>/dev/null && \
           ! kubectl get namespace cnpg-system &>/dev/null; then
            echo " ok"; exit 0
        fi
        echo -n "."; sleep 2
    done
    echo ""; echo "ERROR: namespaces still exist" >&2
    kubectl get namespace $NS cnpg-system 2>/dev/null >&2; exit 1
fi

kubectl create namespace $NS 2>/dev/null || true

# --- Helm repos ---
helm repo add cnpg https://cloudnative-pg.github.io/charts 2>/dev/null || true
helm repo update

# --- CloudNativePG operator ---
helm upgrade --install cnpg cnpg/cloudnative-pg --namespace $NS --wait
kubectl rollout status deployment/cnpg-cloudnative-pg -n $NS --timeout=120s

# --- Apply manifests ---
kubectl apply -n $NS -f "$K8S_DIR/postgres.yaml"
kubectl apply -n $NS -f "$K8S_DIR/cassandra.yaml"

# --- Wait ---
echo ">> waiting for postgres (2 instances)..."
kubectl wait cluster/bangfs-pg -n $NS --for=condition=Ready --timeout=300s

echo ">> waiting for cassandra (3 nodes)..."
kubectl wait pod -n $NS -l app=bangfs-cassandra --for=condition=Ready \
    --timeout=600s

_export_env
echo ""
echo "backends ready  (node $POSTGRES_HOST)"
echo "  postgres:   $POSTGRES_HOST:$POSTGRES_PORT  (1 primary + 1 standby)"
echo "  cassandra:  $CASSANDRA_HOSTS:$CASSANDRA_PORT  (3 nodes, RF=3 recommended)"
echo ""
echo "source this script to get env vars in current shell"
echo "tear down with: $0 --down"
