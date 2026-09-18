#!/usr/bin/env bash
# Kills anything run.sh may have left behind: the app, its Gradle daemon, and
# every port-forward it started. Safe to run even if nothing is running, and
# safe to run even if run.sh's own Ctrl+C cleanup already handled it --
# this is a net for when it didn't (terminal closed, machine slept, etc).
set -uo pipefail

killed_anything=false

kill_matching() {
    local pattern="$1" label="$2"
    local pids
    pids=$(pgrep -f "$pattern" 2>/dev/null || true)
    if [ -n "$pids" ]; then
        echo "Stopping $label (pid(s): $pids)"
        # shellcheck disable=SC2086
        kill $pids 2>/dev/null || true
        killed_anything=true
    fi
}

kill_matching "cdc-kafka-demo.*GradleWrapperMain bootRun" "the app (bootRun)"
kill_matching "port-forward.*dev-cassandra-dc1-service" "Cassandra port-forward"
kill_matching "port-forward.*schema-registry" "schema-registry port-forward"
kill_matching "port-forward.*pod/dev-kafka-" "Kafka broker port-forwards"

if [ "$killed_anything" = false ]; then
    echo "Nothing running -- already clean."
fi

rm -f /tmp/cdc-kafka-demo-pf-*.log /tmp/cdc-kafka-demo-app.log
