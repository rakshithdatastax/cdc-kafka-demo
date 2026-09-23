#!/usr/bin/env bash

set -euo pipefail

CTX="${CTX:-teleport-dev-dev.aws.astrastreaming-datastax-us-east-2-dataplane}"
APP_URL="${APP_URL:-http://localhost:8090}"
RUN_ID=$(date +%s)
DEMO_ID_1="demo-${RUN_ID}-1"
DEMO_ID_2="demo-${RUN_ID}-2"

pause() {
    echo ""
    echo ">>> $1"
    read -rp "    Press Enter once done... " _
}

require_app() {
    if ! curl -sf "$APP_URL/demo/table1/status" >/dev/null 2>&1; then
        echo "The app doesn't seem to be up at $APP_URL. Start it with ./run.sh in another terminal, then re-run this script." >&2
        exit 1
    fi
}

echo "== Step 1: seed fresh demo rows into ks1.table1 ($DEMO_ID_1, $DEMO_ID_2) =="
require_app
curl -sf -X POST "$APP_URL/demo/insert?id=${DEMO_ID_1}&name=BackfillDemo1&age=1" >/dev/null
curl -sf -X POST "$APP_URL/demo/insert?id=${DEMO_ID_2}&name=BackfillDemo2&age=2" >/dev/null
echo "Inserted."

echo ""
echo "== Step 2: baseline validate (should already be identical -- normal CDC flow) =="
curl -s "$APP_URL/demo/table1/validate"; echo

echo ""
echo "== Step 3: delete + recreate data-ks1.table1 to simulate lost topic history =="
kubectl --context "$CTX" -n kafka exec dev-kafka-0 -- /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --delete --topic data-ks1.table1
kubectl --context "$CTX" -n kafka exec dev-kafka-0 -- /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --topic data-ks1.table1 \
    --partitions 4 --replication-factor 3 --config min.insync.replicas=2
echo "Topic recreated (empty)."

pause "Restart the app now (Ctrl+C then ./run.sh in its terminal) so it re-consumes the now-empty topic from scratch."

require_app
echo ""
echo "== Step 4: confirm the gap =="
BEFORE=$(curl -s "$APP_URL/demo/table1/validate")
echo "$BEFORE"
if echo "$BEFORE" | grep -q '"identical":true'; then
    echo "WARNING: already identical -- the gap didn't show up. Give the app a few more seconds and re-check with:" >&2
    echo "  curl -s $APP_URL/demo/table1/validate" >&2
fi

pause "Ready to run the backfill CLI against ks1.table1. Press Enter to continue."

echo ""
echo "Step 5: run the backfill CLI (fresh scratch dir: run $RUN_ID) "
kubectl --context "$CTX" -n cassandra exec cdc-admin -- mkdir -p "/tmp/table1-backfill-$RUN_ID/data" "/tmp/table1-backfill-$RUN_ID/logs"
kubectl --context "$CTX" -n cassandra exec cdc-admin -- bash -c "echo 'bootstrapServers=dev-kafka.kafka.svc.cluster.local:9092' > /tmp/table1-backfill-$RUN_ID/kafka.properties"


CASSANDRA_PASSWORD=$(kubectl --context "$CTX" -n cassandra get secret dev-cassandra-superuser -o jsonpath='{.data.password}' | base64 -d)

kubectl --context "$CTX" -n cassandra exec cdc-admin -- java -jar /backfill-cli.jar \
    --platform KAFKA \
    --kafka-config-file "/tmp/table1-backfill-$RUN_ID/kafka.properties" \
    --data-dir "/tmp/table1-backfill-$RUN_ID/data" \
    --dsbulk-log-dir "/tmp/table1-backfill-$RUN_ID/logs" \
    --export-host dev-cassandra-dc1-service.cassandra.svc.cluster.local \
    --export-consistency LOCAL_QUORUM \
    --keyspace ks1 --table table1 \
    --export-username dev-cassandra-superuser \
    --export-password="$CASSANDRA_PASSWORD"
unset CASSANDRA_PASSWORD

echo ""
echo " Step 6: confirm the gap is closed "
RESULT=""
for _ in $(seq 1 15); do
    RESULT=$(curl -s "$APP_URL/demo/table1/validate")
    if echo "$RESULT" | grep -q '"identical":true'; then
        echo "$RESULT"
        echo ""
        echo "Backfill closed the gap -- identical: true."
        exit 0
    fi
    sleep 2
done
echo "$RESULT"
echo "Still not identical after ~30s -- give it a bit longer and re-check manually:" >&2
echo "  curl -s $APP_URL/demo/table1/validate" >&2
exit 1
