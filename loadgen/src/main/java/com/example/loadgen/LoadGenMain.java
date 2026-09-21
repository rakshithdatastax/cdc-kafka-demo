package com.example.loadgen;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.auth.ProgrammaticPlainTextAuthProvider;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class LoadGenMain {

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
            return c;
        }

        void print() {
            System.out.printf(
                    "[loadgen] contactPoint=%s:%d localDc=%s keyspace=%s table=%s numPks=%d targetUps=%.1f "
                            + "numColumns=%d alterEvery=%ds(max %d) deleteEvery=%ds(x%d, floor=%.0f%%) "
                            + "reinsertEvery=%ds(x%d)%n",
                    contactPoint, port, localDc, keyspace, table, numPks, targetUps, numColumns,
                    alterIntervalSeconds, maxAlters, deleteIntervalSeconds, deletesPerInterval, minLiveFraction * 100,
                    reinsertIntervalSeconds, reinsertsPerInterval);
        }

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

        private long totalInserts;
        private long totalUpdates;
        private long totalAlters;
        private long totalDeletes;
        private long totalReinserts;
        private int liveCount;

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
        }

        void setup() {
            String qualifiedTable = config.keyspace + "." + config.table;
            session.execute("CREATE KEYSPACE IF NOT EXISTS " + config.keyspace
                    + " WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3}");

            StringBuilder createTable = new StringBuilder("CREATE TABLE IF NOT EXISTS " + qualifiedTable + " (id text PRIMARY KEY");
            for (String col : columnNames) {
                createTable.append(", ").append(col).append(" int");
            }
            createTable.append(") WITH cdc = true");
            session.execute(createTable.toString());

            StringBuilder insertCql = new StringBuilder("INSERT INTO " + qualifiedTable + " (id");
            StringBuilder placeholders = new StringBuilder("?");
            for (String col : columnNames) {
                insertCql.append(", ").append(col);
                placeholders.append(", 0");
            }
            insertCql.append(") VALUES (").append(placeholders).append(")");
            insertStmt = session.prepare(insertCql.toString());

            deleteStmt = session.prepare("DELETE FROM " + qualifiedTable + " WHERE id = ?");

            for (String col : columnNames) {
                updateStmtByColumn.add(session.prepare(
                        "UPDATE " + qualifiedTable + " SET " + col + " = ? WHERE id = ?"));
            }

            System.out.println("[loadgen] inserting " + config.numPks + " rows into " + qualifiedTable + "...");
            for (int i = 0; i < config.numPks; i++) {
                session.execute(insertStmt.bind(pkFor(i)));
                totalInserts++;
            }
            System.out.println("[loadgen] initial insert complete: " + totalInserts + " rows");
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
                if (totalAlters < config.maxAlters && now >= nextAlterAt) {
                    doAlter();
                    nextAlterAt = now + config.alterIntervalSeconds * 1000L;
                }
                if (liveCount > config.numPks * config.minLiveFraction && now >= nextDeleteAt) {
                    doDeletes();
                    nextDeleteAt = now + config.deleteIntervalSeconds * 1000L;
                }
                if (liveCount < config.numPks && now >= nextReinsertAt) {
                    doReinserts();
                    nextReinsertAt = now + config.reinsertIntervalSeconds * 1000L;
                }
                if (now >= nextLogAt) {
                    double actualUps = opsSinceLastLog / 10.0;
                    System.out.printf("[loadgen] %s inserts=%d updates=%d alters=%d deletes=%d reinserts=%d live=%d columns=%d actualUps=%.1f%n",
                            Instant.now(), totalInserts, totalUpdates, totalAlters, totalDeletes, totalReinserts,
                            liveCount, columnNames.size(), actualUps);
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
            int newValue = ++columnValues[pk][columnIndex];
            session.execute(updateStmtByColumn.get(columnIndex).bind(newValue, pkFor(pk)));
            updateCounts[pk]++;
            totalUpdates++;
        }

        private void doAlter() {
            String newColumn = "extra_" + (totalAlters + 1);
            session.execute("ALTER TABLE " + config.keyspace + "." + config.table + " ADD " + newColumn + " int");
            updateStmtByColumn.add(session.prepare(
                    "UPDATE " + config.keyspace + "." + config.table + " SET " + newColumn + " = ? WHERE id = ?"));
            columnNames.add(newColumn);
            totalAlters++;
            System.out.println("[loadgen] schema evolution: added column " + newColumn);
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
                    values.append(", ").append(baseValue);
                    columnValues[pk][col] = baseValue;
                }
                insertCql.append(") VALUES (").append(values).append(")");
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
