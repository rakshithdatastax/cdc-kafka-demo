#!/usr/bin/env bash
#
# Live-demo helper for schema evolution: alters ks1.loadgen with a new column, then gets
# out of the way -- watch it land in the Schema Evolution tab yourself, on stage.
#
# Usage: ./demo-schema-evolution.sh [column_name] [column_type]
#   column_name defaults to a timestamp-suffixed name (always safe to re-run)
#   column_type defaults to int
#
# Examples:
#   ./demo-schema-evolution.sh                          # demo_schema_evolution_<ts> int
#   ./demo-schema-evolution.sh customer_email text       # custom name, custom type
#   ./demo-schema-evolution.sh launch_ts timestamp

set -euo pipefail

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
    sed -n '2,/^set -euo pipefail/p' "$0" | sed '$d' | sed 's/^#//'
    exit 0
fi

CTX="${CTX:-teleport-dev-dev.aws.astrastreaming-datastax-us-east-2-dataplane}"
CASSANDRA_POD="${CASSANDRA_POD:-dev-cassandra-dc1-rack1-sts-0}"
RUN_ID=$(date +%s)
NEW_COLUMN="${1:-demo_schema_evolution_${RUN_ID}}"
COLUMN_TYPE="${2:-int}"

pause() {
    echo ""
    echo ">>> $1"
    read -rp "    Press Enter once ready... " _
}

# The ALTER only actually registers a new schema version once a real mutation flows through
# with the new column -- loadgen is what generates that traffic. If it's scaled to 0 (it was,
# once, which is exactly what this check exists for), nothing will ever show up in the tab.
require_loadgen_running() {
    local ready
    ready=$(kubectl --context "$CTX" -n cassandra get deploy loadgen -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "")
    if [ -z "$ready" ] || [ "$ready" -lt 1 ]; then
        echo "loadgen has no ready replicas -- it needs to be running and writing for this demo" >&2
        echo "to work (the schema won't register until a mutation actually uses it). Fix with:" >&2
        echo "  kubectl --context $CTX -n cassandra scale deploy/loadgen --replicas=1" >&2
        exit 1
    fi
}

cassandra_cql() {
    kubectl --context "$CTX" -n cassandra exec "$CASSANDRA_POD" -c cassandra -- cqlsh \
        -u "$CASSANDRA_USERNAME" -p "$CASSANDRA_PASSWORD" -e "$1"
}

echo "== Step 1: fetch Cassandra credentials =="
CASSANDRA_USERNAME=$(kubectl --context "$CTX" -n cassandra get secret dev-cassandra-superuser -o jsonpath='{.data.username}' | base64 -d)
CASSANDRA_PASSWORD=$(kubectl --context "$CTX" -n cassandra get secret dev-cassandra-superuser -o jsonpath='{.data.password}' | base64 -d)
require_loadgen_running

echo ""
echo "== Step 2: baseline =="
echo "  ks1.loadgen currently has $(cassandra_cql "DESCRIBE TABLE ks1.loadgen;" | grep -cE "^\s+\w+ (int|text)") columns."

pause "Open the Schema Evolution tab now if you haven't already, so the before-state is visible on screen."

echo ""
echo "== Step 3: alter the table live =="
echo "  ALTER TABLE ks1.loadgen ADD ${NEW_COLUMN} ${COLUMN_TYPE};"
cassandra_cql "ALTER TABLE ks1.loadgen ADD ${NEW_COLUMN} ${COLUMN_TYPE};"
echo ""
echo ">>> Done. Give it a few seconds for loadgen's next write to carry the new column through,"
echo "    then click Refresh on the Schema Evolution tab -- ${NEW_COLUMN} should show up as a"
echo "    new field in the latest version, with the Cassandra columns panel agreeing on the left."
