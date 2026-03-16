#!/bin/bash

# Initialize Riak bucket types for BangFS (Docker)
# Usage: ./init-riak-buckets-docker.sh [namespace] [container-name]
# Example: ./init-riak-buckets-docker.sh myfs riak-bangfs

set -e

if [[ -z "$BANGFS_NAMESPACE" ]]; then BANGFS_NAMESPACE="${1:-test}"; fi
if [[ -z "$RIAK_CONTAINER" ]]; then RIAK_CONTAINER="${2:-riak}"; fi

echo "Initializing BangFS bucket types with prefix: $BANGFS_NAMESPACE"
echo "Docker container: $RIAK_CONTAINER"

create_bucket_type() {
    local name="$1"
    local props="$2"

    echo " ==== Creating bucket type '$name'... ===="
    docker exec "$RIAK_CONTAINER" riak-admin bucket-type create "$name" "$props"
    docker exec "$RIAK_CONTAINER" riak-admin bucket-type activate "$name"
}

# Single-node testing, uses n_val=3, w=3, r=1, allow_mult=false to check read-after-write from a single client
# For production, use consistent=true with proper quorum
create_bucket_type "${BANGFS_NAMESPACE}_bangfs_metadata" '{"props":{"n_val":3,"w":3,"r":1}}'
create_bucket_type "${BANGFS_NAMESPACE}_bangfs_chunks" '{"props":{"n_val":1,"w":1,"r":1,"allow_mult":false}}'

echo "Bucket types initialized successfully!"
