#!/bin/bash

# Initialize Riak bucket types for BangFS
# Usage: ./init-riak-buckets.sh <container-name> [namespace]
# Example: ./init-riak-buckets.sh riak-bangfs myfs

set -e

# BANGFS_NAMESPACE="${1:-test}"
# K8S_NAMESPACE="${2:-bangfs}"
BANGFS_NAMESPACE=${BANGFS_NAMESPACE:?"please set BANGFS_NAMESPACE"}
K8S_SERVICE="${BANGFS_K8S_RIAK_SERVICE:-riak}"
K8S_NAMESPACE="${BANGFS_K8S_NAMESPACE:-bangfs}"

echo "Initializing BangFS bucket types with prefix: $BANGFS_NAMESPACE"
echo "Kubernetes namespace: $K8S_NAMESPACE"
echo "Kubernetes service: $K8S_SERVICE"

create_bucket_type() {
    local name="$1"
    local props="$2"

    echo " ==== Creating bucket type '$name'... ===="
    kubectl exec -n "$K8S_NAMESPACE" -it svc/"$K8S_SERVICE" -- riak-admin bucket-type create "$name" "$props"
    kubectl exec -n "$K8S_NAMESPACE" -it svc/"$K8S_SERVICE" -- riak-admin bucket-type activate "$name"
}

# For single-node testing, use n_val=1, w=1, r=1
# For production, increase n_val and use consistent=true with proper quorum
# create_bucket_type "${BANGFS_NAMESPACE}_bangfs_metadata" '{"props":{"n_val":1,"w":1,"r":1}}'
create_bucket_type "${BANGFS_NAMESPACE}_bangfs_metadata" '{"props": {"consistent": true}}'
create_bucket_type "${BANGFS_NAMESPACE}_bangfs_chunks" '{"props":{"n_val":3,"w":1,"r":1}}'

echo "Bucket types initialized successfully!"
