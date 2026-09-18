# cdc-kafka-demo

A small Spring Boot app for exercising the Cassandra CDC → Kafka pipeline
(`dev-cassandra` / `dev-kafka` on `datastax-dev-aws-us-east-2`) from your local
machine, without hand-typing `cqlsh`/`kafka-console-consumer` commands each time.

## One-time setup: /etc/hosts

Kafka's brokers advertise themselves by pod DNS name
(`dev-kafka-{0,1,2}.dev-kafka.kafka.svc.cluster.local:9092`), which only
resolves inside the cluster. The Kafka client uses that name directly for
every per-partition fetch — not just the initial connection — so a single
`port-forward svc/dev-kafka` isn't enough; you need one port-forward *per
broker pod*, each on its own loopback alias, with `/etc/hosts` pointing the
broker's advertised name at that alias. `run.sh` checks for this and prints
the exact lines if they're missing, but you can add them up front:

```bash
sudo sh -c 'cat >> /etc/hosts <<EOF
127.0.0.2  dev-kafka-0.dev-kafka.kafka.svc.cluster.local
127.0.0.3  dev-kafka-1.dev-kafka.kafka.svc.cluster.local
127.0.0.4  dev-kafka-2.dev-kafka.kafka.svc.cluster.local
EOF'
```

(127.0.0.2/.3/.4 are loopback addresses — macOS/Linux route all of
127.0.0.0/8 to localhost with no extra network setup needed.)

Cassandra doesn't need this: its driver tolerates only one reachable node
(the others just log connection warnings) since that one node coordinates
with its peers over the cluster's own internal network on your behalf.

## Run

```bash
./run.sh
```

Starts Cassandra's and the schema registry's port-forwards, one port-forward
per Kafka broker pod (bound to the loopback aliases above), fetches the
Cassandra superuser password, and runs the app in the foreground on
`localhost:8090`. Ctrl+C stops the app and tears down every port-forward.
Uses Teleport context `teleport-dev-dev.aws.astrastreaming-datastax-us-east-2-dataplane`
by default — override with `CTX=... ./run.sh` if needed.

If Ctrl+C doesn't clean up properly (terminal closed, laptop slept, etc.),
run `./cleanup.sh` — it kills the app, its Gradle daemon, and every
port-forward `run.sh` might have started. Safe to run anytime, including
when nothing is running.

### Manual alternative

If you'd rather manage the port-forwards yourself (e.g. keep them up across
several `bootRun` restarts):

```bash
CTX=teleport-dev-dev.aws.astrastreaming-datastax-us-east-2-dataplane

kubectl --context $CTX -n cassandra port-forward svc/dev-cassandra-dc1-service 9042:9042
kubectl --context $CTX -n kafka      port-forward svc/schema-registry         8080:8080
kubectl --context $CTX -n kafka      port-forward --address 127.0.0.2 pod/dev-kafka-0 9092:9092
kubectl --context $CTX -n kafka      port-forward --address 127.0.0.3 pod/dev-kafka-1 9092:9092
kubectl --context $CTX -n kafka      port-forward --address 127.0.0.4 pod/dev-kafka-2 9092:9092

export CASSANDRA_PASSWORD=$(kubectl --context $CTX -n cassandra get secret dev-cassandra-superuser -o jsonpath='{.data.password}' | base64 -d)

./gradlew bootRun
```

## Endpoints

```bash
# 1. Create ks1.table1 with cdc=true
curl -X POST localhost:8090/demo/create-table

# 2. Insert a row
curl -X POST "localhost:8090/demo/insert?id=1&name=Rakshith&age=33"

# 3. Check Kafka
curl localhost:8090/demo/kafka/events   
curl localhost:8090/demo/kafka/data   

# 4. Schema evolution: add a column and set it
curl -X POST "localhost:8090/demo/alter?id=1&email=alice@example.com"
curl localhost:8090/demo/kafka/data     # new message + new schema version should appear
curl -X POST "localhost:8090/demo/alter-column?id=1&column=phone&value=555-0100"
curl -X POST "localhost:8090/demo/alter-column?id=1&column=address&value=221B+Baker+St"
curl http://localhost:8080/apis/ccompat/v7/subjects/data-ks1.table1-value/versions
# -> [1,2,3,4] -- one new version per column added

# 5. Delete a row
curl -X DELETE localhost:8090/demo/delete/1
curl localhost:8090/demo/kafka/data     # tombstone (value: null) for id=1

# Anytime: see current Cassandra state
curl localhost:8090/demo/select

# Wipe every row (start fresh) -- see note below
curl -X DELETE localhost:8090/demo/truncate
```

`/demo/kafka/*` reads every message currently on the topic from the beginning
using a fresh consumer group each call, so it's safe to call repeatedly.

`/demo/truncate` clears the table in Cassandra, but `TRUNCATE` doesn't go
through the normal per-row mutation path, so it produces **no** CDC events —
`/demo/kafka/*` will still show every row ever inserted, even after
truncating. There's no way to clear individual messages out of a Kafka topic
short of deleting and recreating it, which this app doesn't do since that
topic is shared infrastructure, not owned by this demo.
