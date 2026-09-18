#!/usr/bin/env bash
# Sets up every port-forward this app needs, pulls the Cassandra superuser
# password, runs the app in the foreground, and tears everything down on
# exit (Ctrl+C included).
#
# Kafka needs one port-forward PER BROKER POD, not one to the Service: each
# broker advertises itself as "<pod>.dev-kafka.kafka.svc.cluster.local:9092"
# (cluster-internal DNS), and once the client's initial bootstrap connection
# succeeds it tries to open DIRECT connections to that address for every
# subsequent partition fetch. A single svc/dev-kafka port-forward only
# tunnels the bootstrap call; those advertised names still fail to resolve
# locally, so results end up silently incomplete depending on which broker
# happens to lead which partition. Binding each broker's port-forward to
# its own loopback alias (127.0.0.2/.3/.4) and pointing /etc/hosts at them
# makes those advertised names resolve locally too. Traffic to 127.0.0.0/8
# routes to loopback with no setup on macOS/Linux, but macOS additionally
# refuses to let a process *bind a listener* to a loopback address other
# than 127.0.0.1 until that address is added as an interface alias -- see
# check_loopback_aliases below.
set -euo pipefail

CTX="${CTX:-teleport-dev-dev.aws.astrastreaming-datastax-us-east-2-dataplane}"
KAFKA_BROKERS=(dev-kafka-0 dev-kafka-1 dev-kafka-2)
KAFKA_LOOPBACK_IPS=(127.0.0.2 127.0.0.3 127.0.0.4)

check_hosts_entries() {
    local missing=()
    for broker in "${KAFKA_BROKERS[@]}"; do
        local fqdn="${broker}.dev-kafka.kafka.svc.cluster.local"
        if ! grep -q "$fqdn" /etc/hosts 2>/dev/null; then
            missing+=("$fqdn")
        fi
    done
    if [ "${#missing[@]}" -gt 0 ]; then
        echo "Missing /etc/hosts entries for the Kafka brokers' advertised names." >&2
        echo "Add these lines to /etc/hosts (needs sudo), then re-run:" >&2
        echo "" >&2
        for i in "${!KAFKA_BROKERS[@]}"; do
            echo "  ${KAFKA_LOOPBACK_IPS[$i]}  ${KAFKA_BROKERS[$i]}.dev-kafka.kafka.svc.cluster.local" >&2
        done
        echo "" >&2
        echo "One-liner:" >&2
        printf "  sudo sh -c 'cat >> /etc/hosts <<EOF\n" >&2
        for i in "${!KAFKA_BROKERS[@]}"; do
            echo "${KAFKA_LOOPBACK_IPS[$i]}  ${KAFKA_BROKERS[$i]}.dev-kafka.kafka.svc.cluster.local" >&2
        done
        printf "EOF'\n" >&2
        exit 1
    fi
}

check_loopback_aliases() {
    if [ "$(uname)" != "Darwin" ]; then
        return 0
    fi
    local missing=()
    for ip in "${KAFKA_LOOPBACK_IPS[@]}"; do
        if ! ifconfig lo0 | grep -q "inet $ip "; then
            missing+=("$ip")
        fi
    done
    if [ "${#missing[@]}" -gt 0 ]; then
        echo "Missing loopback aliases on lo0 for: ${missing[*]}" >&2
        echo "macOS won't let a process bind a listener to a 127.0.0.0/8 address" >&2
        echo "other than 127.0.0.1 until it's added as an interface alias. Run:" >&2
        echo "" >&2
        for ip in "${missing[@]}"; do
            echo "  sudo ifconfig lo0 alias $ip up" >&2
        done
        echo "" >&2
        echo "(These reset on reboot, not persistent -- re-run if you restart your Mac.)" >&2
        exit 1
    fi
}

PF_PIDS=()

cleanup() {
    echo ""
    echo "Stopping port-forwards..."
    for pid in "${PF_PIDS[@]:-}"; do
        kill "$pid" >/dev/null 2>&1 || true
    done
}
trap cleanup EXIT INT TERM

start_port_forward() {
    local namespace="$1" target="$2" ports="$3" label="$4" address="${5:-}"
    local addr_flag=()
    if [ -n "$address" ]; then
        addr_flag=(--address "$address")
    fi
    kubectl --context "$CTX" -n "$namespace" port-forward "${addr_flag[@]}" "$target" "$ports" \
        > "/tmp/cdc-kafka-demo-pf-${label}.log" 2>&1 &
    PF_PIDS+=("$!")
}

wait_for_port() {
    local address="$1" port="$2" label="$3"
    for _ in $(seq 1 30); do
        if nc -z "$address" "$port" >/dev/null 2>&1; then
            echo "  $label ready on $address:$port"
            return 0
        fi
        sleep 1
    done
    echo "  $label did not come up on $address:$port in time -- check /tmp/cdc-kafka-demo-pf-*.log" >&2
    exit 1
}

check_hosts_entries
check_loopback_aliases

echo "Starting port-forwards against $CTX..."
start_port_forward cassandra svc/dev-cassandra-dc1-service 9042:9042 cassandra
start_port_forward kafka svc/schema-registry 8080:8080 schema-registry
for i in "${!KAFKA_BROKERS[@]}"; do
    start_port_forward kafka "pod/${KAFKA_BROKERS[$i]}" 9092:9092 "kafka-${i}" "${KAFKA_LOOPBACK_IPS[$i]}"
done

wait_for_port localhost 9042 Cassandra
wait_for_port localhost 8080 "Schema registry"
for i in "${!KAFKA_BROKERS[@]}"; do
    wait_for_port "${KAFKA_LOOPBACK_IPS[$i]}" 9092 "Kafka broker ${KAFKA_BROKERS[$i]}"
done

echo "Fetching Cassandra superuser password..."
export CASSANDRA_PASSWORD
CASSANDRA_PASSWORD=$(kubectl --context "$CTX" -n cassandra get secret dev-cassandra-superuser -o jsonpath='{.data.password}' | base64 -d)

echo "Starting the app on localhost:8090 (Ctrl+C stops everything)..."
./gradlew bootRun
