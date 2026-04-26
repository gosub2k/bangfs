#!/bin/bash
# Start Postgres + Cassandra in Docker, run integration tests, then stop them.
# Usage: ./integration-test.sh [--no-stop]
set -euo pipefail

PG_CONTAINER=bangfs-pg
CASS_CONTAINER=bangfs-cass

stop() { docker rm -f $PG_CONTAINER $CASS_CONTAINER 2>/dev/null || true; }
[ "${1:-}" = "--no-stop" ] || trap stop EXIT

docker run -d --name $PG_CONTAINER \
    -e POSTGRES_USER=bangfs -e POSTGRES_PASSWORD=bangfs -e POSTGRES_DB=bangfs \
    -p 5432:5432 postgres:16

docker run -d --name $CASS_CONTAINER \
    -p 9042:9042 cassandra:4

echo -n "waiting for postgres"
until docker exec $PG_CONTAINER pg_isready -U bangfs -q 2>/dev/null; do
    echo -n "."; sleep 2
done
echo " ready"

echo -n "waiting for cassandra"
until docker exec $CASS_CONTAINER cqlsh -e "describe cluster" &>/dev/null; do
    echo -n "."; sleep 4
done
echo " ready"

export POSTGRES_HOST=127.0.0.1 POSTGRES_USER=bangfs POSTGRES_PASSWORD=bangfs POSTGRES_DB=bangfs
export CASSANDRA_HOSTS=127.0.0.1

make integration-test
