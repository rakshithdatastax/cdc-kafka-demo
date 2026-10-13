package com.example.loadgen;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.auth.ProgrammaticPlainTextAuthProvider;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class LoadGenMain {

    /**
     * Controls which operations are active during a run.
     *
     * S1  - Steady-state scatter updates only. Baseline for latency / throughput measurement.
     * S2  - Scatter updates + schema evolution (ALTER TABLE ADD extra_N).
     * S3  - Scatter updates + periodic deletes + reinserts (tombstone / resurrection path).
     * S4  - Same as S1. Run repeatedly at increasing TARGET_UPS to find the saturation point.
     * S5  - Bulk insert burst: floods BULK_INSERT_COUNT rows before entering steady-state updates.
     * S6  - Wide rows: column values are text padded to VALUE_SIZE_BYTES bytes.
     * S7  - TTL writes: every insert and update carries USING TTL TTL_SECONDS. Cell expiry itself is
     *       not a write, so CDC is expected to see only the TTL-bearing mutations (not the expiry).
     * ALL - All behaviours active simultaneously (original default behaviour).
     */
    enum Scenario { S1, S2, S3, S4, S5, S6, S7, ALL }

    public static void main(String[] args) throws Exception {
        Config config = Config.fromEnv();
        config.print();

        CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(config.contactPoint, config.port))
                .withLocalDatacenter(config.localDc)
                .withAuthProvider(new ProgrammaticPlainTextAuthProvider(config.username, config.password))
                .build();
        Runtime.getRuntime().addShutdownHook(new Thread(session::close));

        LoadGen loadGen = new LoadGen(session, config);
        loadGen.setup();
        loadGen.run();
    }

    static class Config {
        String contactPoint;
        int port;
        String localDc;
        String username;
        String password;
        String keyspace;
        String table;
        int numPks;
        double targetUps;
        int numColumns;
        int alterIntervalSeconds;
        int maxAlters;
        int deleteIntervalSeconds;
        int deletesPerInterval;
        double minLiveFraction;
        int reinsertIntervalSeconds;
        int reinsertsPerInterval;

        static final long MAX_ROW_BYTES = 800_000;

        Scenario scenario;
        int bulkInsertCount; // S5: rows to insert as fast as possible before steady state. 0 = disabled.
        int valueSizeBytes;  // S6: when > 0, columns are text padded to this many bytes. 0 = int columns (default).
        int ttlSeconds;      // S7: when > 0, all inserts use USING TTL <n>. 0 = no TTL (default).

        static Config fromEnv() {
            Config c = new Config();
            c.contactPoint = env("CASSANDRA_CONTACT_POINT", "dev-cassandra-dc1-service.cassandra.svc.cluster.local");
            c.port = Integer.parseInt(env("CASSANDRA_PORT", "9042"));
            c.localDc = env("CASSANDRA_LOCAL_DC", "dc1");
            c.username = env("CASSANDRA_USERNAME", "dev-cassandra-superuser");
            c.password = requireEnv("CASSANDRA_PASSWORD");
            c.keyspace = env("KEYSPACE", "ks1");
            c.table = env("TABLE", "loadgen");
            c.numPks = Integer.parseInt(env("NUM_PKS", "1000"));
            c.targetUps = Double.parseDouble(env("TARGET_UPS", "50"));
            c.numColumns = Integer.parseInt(env("NUM_COLUMNS", "5"));
            c.alterIntervalSeconds = Integer.parseInt(env("ALTER_INTERVAL_SECONDS", "120"));
            c.maxAlters = Integer.parseInt(env("MAX_ALTERS", "3"));
            c.deleteIntervalSeconds = Integer.parseInt(env("DELETE_INTERVAL_SECONDS", "30"));
            c.deletesPerInterval = Integer.parseInt(env("DELETES_PER_INTERVAL", "1"));
            c.minLiveFraction = Double.parseDouble(env("MIN_LIVE_FRACTION", "0.5"));
            c.reinsertIntervalSeconds = Integer.parseInt(env("REINSERT_INTERVAL_SECONDS", "45"));
            c.reinsertsPerInterval = Integer.parseInt(env("REINSERTS_PER_INTERVAL", "1"));
            c.scenario = Scenario.valueOf(env("SCENARIO", "ALL"));
            c.bulkInsertCount = Integer.parseInt(env("BULK_INSERT_COUNT", "0"));
            c.valueSizeBytes = Integer.parseInt(env("VALUE_SIZE_BYTES", "0"));
            c.ttlSeconds = Integer.parseInt(env("TTL_SECONDS", "0"));
            c.validate();
            return c;
        }

        // Fail fast: S5/S6/S7 are meaningless without their parameter and would silently
        // behave like S1, producing a benchmark that looks valid but isn't.
        private void validate() {
            if (scenario == Scenario.S5 && bulkInsertCount <= 0) {
                throw new IllegalStateException("SCENARIO=S5 requires BULK_INSERT_COUNT > 0");
            }
            if (scenario == Scenario.S6 && valueSizeBytes <= 0) {
                throw new IllegalStateException("SCENARIO=S6 requires VALUE_SIZE_BYTES > 0");
            }
            if (scenario == Scenario.S7 && ttlSeconds <= 0) {
                throw new IllegalStateException("SCENARIO=S7 requires TTL_SECONDS > 0");
            }
            // A full-row write (initial insert, reinsert) is one CDC event of ~numColumns * valueSizeBytes.
            // Stay under the default Kafka message.max.bytes (~1 MB) so S6 measures CDC, not a size rejection.
            if (wideRows() && valueSizeBytes > 0 && (long) numColumns * valueSizeBytes > MAX_ROW_BYTES) {
                throw new IllegalStateException("NUM_COLUMNS * VALUE_SIZE_BYTES = "
                        + ((long) numColumns * valueSizeBytes) + " exceeds " + MAX_ROW_BYTES
                        + " bytes (default Kafka message limit is ~1 MB)");
            }
        }

        void print() {
            System.out.printf(
                    "[loadgen] scenario=%s contactPoint=%s:%d localDc=%s keyspace=%s table=%s numPks=%d targetUps=%.1f "
                            + "numColumns=%d alterEvery=%ds(max %d) deleteEvery=%ds(x%d, floor=%.0f%%) "
                            + "reinsertEvery=%ds(x%d) bulkInsertCount=%d valueSizeBytes=%d ttlSeconds=%d%n",
                    scenario, contactPoint, port, localDc, keyspace, table, numPks, targetUps, numColumns,
                    alterIntervalSeconds, maxAlters, deleteIntervalSeconds, deletesPerInterval, minLiveFraction * 100,
                    reinsertIntervalSeconds, reinsertsPerInterval, bulkInsertCount, valueSizeBytes, ttlSeconds);
        }

        boolean alterEnabled()    { return scenario == Scenario.S2 || scenario == Scenario.ALL; }
        boolean deleteEnabled()   { return scenario == Scenario.S3 || scenario == Scenario.ALL; }
        boolean reinsertEnabled() { return scenario == Scenario.S3 || scenario == Scenario.ALL; }
        boolean bulkEnabled()     { return scenario == Scenario.S5 || scenario == Scenario.ALL; }
        boolean wideRows()        { return scenario == Scenario.S6 || scenario == Scenario.ALL; }
        boolean ttlEnabled()      { return scenario == Scenario.S7 || scenario == Scenario.ALL; }

        private static String env(String name, String def) {
            String v = System.getenv(name);
            return (v == null || v.isEmpty()) ? def : v;
        }

        private static String requireEnv(String name) {
            String v = System.getenv(name);
            if (v == null || v.isEmpty()) {
                throw new IllegalStateException("Required environment variable " + name + " is not set");
            }
            return v;
        }
    }

    static class LoadGen {
        private final CqlSession session;
        private final Config config;
        private final List<String> columnNames = new ArrayList<>();
        private final int[] updateCounts;
        private final int[][] columnValues;
        private final boolean[] deleted;
        private final int[] generation;
        private final Random random = ThreadLocalRandom.current();

        private PreparedStatement insertStmt;
        private PreparedStatement deleteStmt;
        private final List<PreparedStatement> updateStmtByColumn = new ArrayList<>();

        // S5: cap on concurrent async inserts during the bulk burst
        private static final int BULK_MAX_IN_FLIGHT = 128;

        // S6: pre-built padding string reused for every wide-row write
        private String paddingValue;

        private long totalInserts;
        private long totalUpdates;
        private long totalAlters;
        private long totalDeletes;
        private long totalReinserts;
        private int liveCount;

        // S6: true when columns are text padded to VALUE_SIZE_BYTES
        private boolean isWide() {
            return config.wideRows() && config.valueSizeBytes > 0;
        }

        // CQL type for scenario columns: text for S6, int otherwise
        private String columnType() {
            return isWide() ? "text" : "int";
        }

        // S7: UPDATE takes its TTL before SET, so every update statement is built here
        private PreparedStatement prepareUpdate(String qualifiedTable, String col) {
            String ttl = config.ttlEnabled() && config.ttlSeconds > 0 ? " USING TTL " + config.ttlSeconds : "";
            return session.prepare("UPDATE " + qualifiedTable + ttl + " SET " + col + " = ? WHERE id = ?");
        }

        // Binds the prepared insert. The statement was prepared with numColumns placeholders
        // in wide mode (before any extra_N columns were discovered), so bind exactly that many.
        private BoundStatement bindInsert(String pk) {
            if (!isWide()) {
                return insertStmt.bind(pk);
            }
            Object[] values = new Object[1 + config.numColumns];
            values[0] = pk;
            for (int i = 1; i < values.length; i++) {
                values[i] = paddingValue;
            }
            return insertStmt.bind(values);
        }

        LoadGen(CqlSession session, Config config) {
            this.session = session;
            this.config = config;
            this.updateCounts = new int[config.numPks];
            this.columnValues = new int[config.numPks][config.numColumns + config.maxAlters];
            this.deleted = new boolean[config.numPks];
            this.generation = new int[config.numPks];
            this.liveCount = config.numPks;
            for (int i = 0; i < config.numColumns; i++) {
                columnNames.add("c" + i);
            }
            // S6: build the padding string once upfront
            if (isWide()) {
                paddingValue = "x".repeat(config.valueSizeBytes);
            }
        }

        void setup() {
            String qualifiedTable = config.keyspace + "." + config.table;
            session.execute("CREATE KEYSPACE IF NOT EXISTS " + config.keyspace
                    + " WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3}");

            // S6: text columns when wide-row scenario, otherwise int (original behaviour)
            String colType = columnType();
            StringBuilder createTable = new StringBuilder("CREATE TABLE IF NOT EXISTS " + qualifiedTable + " (id text PRIMARY KEY");
            for (String col : columnNames) {
                createTable.append(", ").append(col).append(" ").append(colType);
            }
            createTable.append(") WITH cdc = true");
            session.execute(createTable.toString());

            StringBuilder insertCql = new StringBuilder("INSERT INTO " + qualifiedTable + " (id");
            StringBuilder placeholders = new StringBuilder("?");
            // S6: text columns need a bound padding value; int columns keep the literal 0
            String columnPlaceholder = isWide() ? ", ?" : ", 0";
            for (String col : columnNames) {
                insertCql.append(", ").append(col);
                placeholders.append(columnPlaceholder);
            }
            insertCql.append(") VALUES (").append(placeholders).append(")");
            // S7: bake USING TTL into the prepared statement so every insert auto-expires
            if (config.ttlEnabled() && config.ttlSeconds > 0) {
                insertCql.append(" USING TTL ").append(config.ttlSeconds);
            }
            insertStmt = session.prepare(insertCql.toString());

            deleteStmt = session.prepare("DELETE FROM " + qualifiedTable + " WHERE id = ?");

            discoverExistingAlterColumns(qualifiedTable);

            for (String col : columnNames) {
                updateStmtByColumn.add(prepareUpdate(qualifiedTable, col));
            }

            System.out.println("[loadgen] inserting " + config.numPks + " rows into " + qualifiedTable + "...");
            for (int i = 0; i < config.numPks; i++) {
                session.execute(bindInsert(pkFor(i)));
                totalInserts++;
            }
            System.out.println("[loadgen] initial insert complete: " + totalInserts + " rows");

            // S5: bulk insert burst before entering the steady-state loop
            if (config.bulkEnabled() && config.bulkInsertCount > 0) {
                doBulkInsertBurst(qualifiedTable);
            }
        }

        // S5: insert BULK_INSERT_COUNT rows with "bulk-N" PKs as fast as possible (no rate limit).
        // These PKs are outside 0..numPks-1 so they never interfere with scatter-update
        // tracking, delete, or reinsert logic. The burst creates a backlog spike so we can
        // measure how fast each CDC agent (Kafka vs Pulsar) drains and catches up.
        //
        // Uses executeAsync with at most BULK_MAX_IN_FLIGHT outstanding requests, so the write
        // rate is not bounded by a single synchronous round-trip.

        private void doBulkInsertBurst(String qualifiedTable) {
            System.out.printf("[loadgen] S5 bulk insert burst: inserting %d rows into %s (max %d in flight)...%n",
                    config.bulkInsertCount, qualifiedTable, BULK_MAX_IN_FLIGHT);
            Semaphore inFlight = new Semaphore(BULK_MAX_IN_FLIGHT);
            AtomicLong failures = new AtomicLong();
            long start = System.currentTimeMillis();
            try {
                for (int i = 0; i < config.bulkInsertCount; i++) {
                    // bind before acquiring so a bind failure never holds a permit
                    BoundStatement stmt = bindInsert("bulk-" + i);
                    inFlight.acquire();
                    CompletionStage<AsyncResultSet> pending;
                    try {
                        pending = session.executeAsync(stmt);
                    } catch (RuntimeException e) {
                        // executeAsync threw synchronously, so no callback will ever release this permit
                        failures.incrementAndGet();
                        inFlight.release();
                        continue;
                    }
                    pending.whenComplete((rs, err) -> {
                        if (err != null) {
                            failures.incrementAndGet();
                        }
                        inFlight.release();
                    });
                }
                // wait for every outstanding request to finish
                inFlight.acquire(BULK_MAX_IN_FLIGHT);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted during S5 bulk insert burst", e);
            }
            long elapsed = System.currentTimeMillis() - start;
            long succeeded = config.bulkInsertCount - failures.get();
            System.out.printf("[loadgen] S5 bulk insert burst complete: %d ok, %d failed in %dms (%.0f rows/s)%n",
                    succeeded, failures.get(), elapsed, succeeded * 1000.0 / Math.max(elapsed, 1));
        }

        private void discoverExistingAlterColumns(String qualifiedTable) {
            var tableMeta = session.getMetadata()
                    .getKeyspace(config.keyspace)
                    .flatMap(ks -> ks.getTable(config.table));
            if (tableMeta.isEmpty()) {
                return;
            }
            List<String> existingExtras = new ArrayList<>();
            for (ColumnMetadata col : tableMeta.get().getColumns().values()) {
                String name = col.getName().asInternal();
                if (name.startsWith("extra_")) {
                    existingExtras.add(name);
                }
            }
            existingExtras.sort(Comparator.comparingInt(name -> Integer.parseInt(name.substring("extra_".length()))));
            columnNames.addAll(existingExtras);
            totalAlters = existingExtras.size();
            if (totalAlters > 0) {
                System.out.println("[loadgen] found " + totalAlters
                        + " pre-existing schema-evolution column(s) from a prior run on " + qualifiedTable
                        + ": " + existingExtras);
            }
        }

        void run() throws InterruptedException {
            long periodNanos = (long) (1_000_000_000.0 / config.targetUps);
            long nextTick = System.nanoTime();
            long nextAlterAt = System.currentTimeMillis() + config.alterIntervalSeconds * 1000L;
            long nextDeleteAt = System.currentTimeMillis() + config.deleteIntervalSeconds * 1000L;
            long nextReinsertAt = System.currentTimeMillis() + config.reinsertIntervalSeconds * 1000L;
            long nextLogAt = System.currentTimeMillis() + 10_000L;
            long opsSinceLastLog = 0;

            while (true) {
                doScatterUpdate();
                opsSinceLastLog++;

                long now = System.currentTimeMillis();
                if (config.alterEnabled() && totalAlters < config.maxAlters && now >= nextAlterAt) {
                    doAlter();
                    nextAlterAt = now + config.alterIntervalSeconds * 1000L;
                }
                if (config.deleteEnabled() && liveCount > config.numPks * config.minLiveFraction && now >= nextDeleteAt) {
                    doDeletes();
                    nextDeleteAt = now + config.deleteIntervalSeconds * 1000L;
                }
                if (config.reinsertEnabled() && liveCount < config.numPks && now >= nextReinsertAt) {
                    doReinserts();
                    nextReinsertAt = now + config.reinsertIntervalSeconds * 1000L;
                }
                if (now >= nextLogAt) {
                    double actualUps = opsSinceLastLog / 10.0;
                    System.out.printf("[loadgen] %s scenario=%s inserts=%d updates=%d alters=%d deletes=%d reinserts=%d live=%d columns=%d actualUps=%.1f%n",
                            Instant.now(), config.scenario, totalInserts, totalUpdates, totalAlters, totalDeletes,
                            totalReinserts, liveCount, columnNames.size(), actualUps);
                    opsSinceLastLog = 0;
                    nextLogAt = now + 10_000L;
                }

                nextTick += periodNanos;
                long sleepNanos = nextTick - System.nanoTime();
                if (sleepNanos > 0) {
                    Thread.sleep(sleepNanos / 1_000_000, (int) (sleepNanos % 1_000_000));
                } else {
                    nextTick = System.nanoTime();
                }
            }
        }

        private void doScatterUpdate() {
            int pk = pickLivePk();
            if (pk < 0) {
                return;
            }
            int columnIndex = updateCounts[pk] % columnNames.size();
            // S6: update with a uniquely-suffixed padding string; otherwise increment int
            Object newValue;
            if (isWide()) {
                newValue = paddingValue + "-" + updateCounts[pk];
            } else {
                newValue = ++columnValues[pk][columnIndex];
            }
            session.execute(updateStmtByColumn.get(columnIndex).bind(newValue, pkFor(pk)));
            updateCounts[pk]++;
            totalUpdates++;
        }

        private void doAlter() {
            String newColumn = "extra_" + (totalAlters + 1);
            // S6: new columns must also be text when running wide-row scenario
            String colType = columnType();
            session.execute("ALTER TABLE " + config.keyspace + "." + config.table
                    + " ADD " + newColumn + " " + colType);
            updateStmtByColumn.add(prepareUpdate(config.keyspace + "." + config.table, newColumn));
            columnNames.add(newColumn);
            totalAlters++;
            System.out.println("[loadgen] schema evolution: added column " + newColumn + " (" + colType + ")");
        }

        private void doDeletes() {
            for (int i = 0; i < config.deletesPerInterval; i++) {
                int pk = pickLivePk();
                if (pk < 0) {
                    return;
                }
                session.execute(deleteStmt.bind(pkFor(pk)));
                deleted[pk] = true;
                liveCount--;
                totalDeletes++;
            }
        }

        private void doReinserts() {
            for (int i = 0; i < config.reinsertsPerInterval; i++) {
                int pk = pickDeletedPk();
                if (pk < 0) {
                    return;
                }
                generation[pk]++;
                int baseValue = generation[pk] * 1000;

                StringBuilder insertCql = new StringBuilder(
                        "INSERT INTO " + config.keyspace + "." + config.table + " (id");
                StringBuilder values = new StringBuilder("'" + pkFor(pk) + "'");
                for (int col = 0; col < columnNames.size(); col++) {
                    insertCql.append(", ").append(columnNames.get(col));
                    if (isWide()) {
                        // S6: text column, so the value must be a quoted string literal
                        values.append(", '").append(paddingValue).append("'");
                    } else {
                        values.append(", ").append(baseValue);
                        columnValues[pk][col] = baseValue;
                    }
                }
                insertCql.append(") VALUES (").append(values).append(")");
                // S7: reinserts carry the same TTL as every other write
                if (config.ttlEnabled() && config.ttlSeconds > 0) {
                    insertCql.append(" USING TTL ").append(config.ttlSeconds);
                }
                session.execute(insertCql.toString());

                updateCounts[pk] = 0;
                deleted[pk] = false;
                liveCount++;
                totalReinserts++;
            }
        }

        private int pickDeletedPk() {
            if (liveCount >= config.numPks) {
                return -1;
            }
            for (int attempt = 0; attempt < 100; attempt++) {
                int candidate = random.nextInt(config.numPks);
                if (deleted[candidate]) {
                    return candidate;
                }
            }
            for (int i = 0; i < config.numPks; i++) {
                if (deleted[i]) {
                    return i;
                }
            }
            return -1;
        }

        private int pickLivePk() {
            if (liveCount == 0) {
                return -1;
            }
            for (int attempt = 0; attempt < 100; attempt++) {
                int candidate = random.nextInt(config.numPks);
                if (!deleted[candidate]) {
                    return candidate;
                }
            }
            // Fallback if random probing keeps missing (heavily thinned key space).
            for (int i = 0; i < config.numPks; i++) {
                if (!deleted[i]) {
                    return i;
                }
            }
            return -1;
        }

        private String pkFor(int index) {
            return "pk-" + index;
        }
    }
}
