package com.example.loadgen;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.auth.ProgrammaticPlainTextAuthProvider;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/**
 * EventThroughputGenMain generates a configurable target throughput (n events/sec) against a
 * CDC-enabled Cassandra table, with a configurable insert/delete/update mix, and reports the
 * achieved rate.
 * Out of n events:
 *   - x% are INSERT events
 *   - y% are DELETE events
 *   - z% are UPDATE events
 * 
 * All parameters (n, x, y, z, connection info, initial dataset) are configurable via environment variables.
 */
public class EventThroughputGenMain {

    public static void main(String[] args) throws Exception {
        Config config = Config.fromEnv();
        config.print();

        CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(config.contactPoint, config.port))
                .withLocalDatacenter(config.localDc)
                .withAuthProvider(new ProgrammaticPlainTextAuthProvider(config.username, config.password))
                .build();

        ThroughputGen throughputGen = new ThroughputGen(session, config);
        Thread runnerThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[event-throughput-gen] shutdown signal received, stopping throughput generator...");
            throughputGen.stop();
            runnerThread.interrupt();
            try {
                runnerThread.join(5000);
            } catch (InterruptedException ignored) {
            }
            throughputGen.drainInFlight(5000);
            session.close();
            System.out.println("[event-throughput-gen] session closed cleanly");
        }));

        throughputGen.setup();
        throughputGen.run();
    }

    public static class Config {
        public String contactPoint;
        public int port;
        public String localDc;
        public String username;
        public String password;
        public String keyspace;
        public String table;
        public String keyPrefix;
        public int replicationFactor;
        public int initialRows;
        public int numColumns;
        public int maxInFlight;
        public long maxCatchupMs;
        public boolean trackLiveKeys;

        // n = events per second
        public double eventsPerSecond;

        // x, y, z = percentages
        public double insertPct;
        public double deletePct;
        public double updatePct;

        // Normalized fractions [0.0, 1.0]
        public double insertFraction;
        public double deleteFraction;
        public double updateFraction;

        public static Config fromEnv() {
            Config c = new Config();
            c.contactPoint = env("CASSANDRA_CONTACT_POINT", "dev-cassandra-dc1-service.cassandra.svc.cluster.local");
            c.port = parseIntEnv("CASSANDRA_PORT", "9042");
            c.localDc = env("CASSANDRA_LOCAL_DC", "dc1");
            c.username = env("CASSANDRA_USERNAME", "dev-cassandra-superuser");
            c.password = requireEnv("CASSANDRA_PASSWORD");
            c.keyspace = env("KEYSPACE", "ks1");
            c.table = env("TABLE", "loadgen");
            c.keyPrefix = env("KEY_PREFIX", UUID.randomUUID().toString().replace("-", "").substring(0, 8));
            c.replicationFactor = parseIntEnv("REPLICATION_FACTOR", "3");
            if (c.replicationFactor <= 0) {
                throw new IllegalArgumentException("REPLICATION_FACTOR must be > 0, got: " + c.replicationFactor);
            }
            c.initialRows = parseIntEnv("NUM_PKS", "1000");
            c.numColumns = parseIntEnv("NUM_COLUMNS", "5");
            c.maxInFlight = parseIntEnv("MAX_IN_FLIGHT", "256");
            if (c.maxInFlight <= 0) {
                throw new IllegalArgumentException("MAX_IN_FLIGHT must be > 0, got: " + c.maxInFlight);
            }
            c.maxCatchupMs = parseLongEnv("MAX_CATCHUP_MS", "500");
            if (c.maxCatchupMs < 0) {
                throw new IllegalArgumentException("MAX_CATCHUP_MS must be >= 0, got: " + c.maxCatchupMs);
            }

            // n: events per second
            c.eventsPerSecond = parseDoubleEnv("EVENTS_PER_SECOND", "50");
            if (c.eventsPerSecond <= 0) {
                throw new IllegalArgumentException("EVENTS_PER_SECOND must be > 0, got: " + c.eventsPerSecond);
            }

            // x, y, z: percentages
            c.insertPct = parseDoubleEnv("INSERT_PCT", "60");
            if (c.insertPct < 0) {
                throw new IllegalArgumentException("INSERT_PCT must be >= 0, got: " + c.insertPct);
            }

            c.deletePct = parseDoubleEnv("DELETE_PCT", "10");
            if (c.deletePct < 0) {
                throw new IllegalArgumentException("DELETE_PCT must be >= 0, got: " + c.deletePct);
            }

            c.updatePct = parseDoubleEnv("UPDATE_PCT", "30");
            if (c.updatePct < 0) {
                throw new IllegalArgumentException("UPDATE_PCT must be >= 0, got: " + c.updatePct);
            }

            double totalPct = c.insertPct + c.deletePct + c.updatePct;
            if (totalPct <= 0) {
                throw new IllegalArgumentException(
                        "Sum of INSERT_PCT (" + c.insertPct + "), DELETE_PCT (" + c.deletePct
                                + "), and UPDATE_PCT (" + c.updatePct + ") must be > 0");
            }

            if (Math.abs(totalPct - 100.0) > 0.001) {
                System.out.printf(
                        "[event-throughput-gen] WARNING: Configured percentages (INSERT_PCT=%.1f + DELETE_PCT=%.1f + UPDATE_PCT=%.1f = %.1f) do not sum to 100%%. Values will be normalized.%n",
                        c.insertPct, c.deletePct, c.updatePct, totalPct);
            }

            // Normalize fractions in case percentages do not sum exactly to 100
            c.insertFraction = c.insertPct / totalPct;
            c.deleteFraction = c.deletePct / totalPct;
            c.updateFraction = c.updatePct / totalPct;

            c.trackLiveKeys = (c.deletePct > 0 || c.updatePct > 0);

            return c;
        }

        public void print() {
            System.out.printf(
                    "[event-throughput-gen] contactPoint=%s:%d localDc=%s keyspace=%s table=%s keyPrefix=%s replicationFactor=%d initialRows=%d numColumns=%d maxInFlight=%d maxCatchup=%dms trackLiveKeys=%b%n"
                            + "[event-throughput-gen] rate (n)=%.1f events/sec | insert (x)=%.1f%% (norm=%.2f%%) | delete (y)=%.1f%% (norm=%.2f%%) | update (z)=%.1f%% (norm=%.2f%%)%n",
                    contactPoint, port, localDc, keyspace, table, keyPrefix, replicationFactor, initialRows, numColumns, maxInFlight, maxCatchupMs, trackLiveKeys,
                    eventsPerSecond, insertPct, insertFraction * 100, deletePct, deleteFraction * 100, updatePct, updateFraction * 100);
            if (!trackLiveKeys) {
                System.out.println("[event-throughput-gen] NOTE: Live-key tracking disabled (insert-only workload: DELETE_PCT=0 and UPDATE_PCT=0). Memory usage is O(1).");
            }
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

        private static double parseDoubleEnv(String name, String def) {
            String val = env(name, def);
            try {
                return Double.parseDouble(val);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid numeric value for environment variable " + name + ": \"" + val + "\"", e);
            }
        }

        private static int parseIntEnv(String name, String def) {
            String val = env(name, def);
            try {
                return Integer.parseInt(val);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid numeric value for environment variable " + name + ": \"" + val + "\"", e);
            }
        }

        private static long parseLongEnv(String name, String def) {
            String val = env(name, def);
            try {
                return Long.parseLong(val);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid numeric value for environment variable " + name + ": \"" + val + "\"", e);
            }
        }
    }

    public static class ThroughputGen {
        enum OpType {
            INSERT, DELETE, UPDATE
        }

        private final CqlSession session;
        private final Config config;
        private final List<String> columnNames = new ArrayList<>();
        private final Semaphore inFlight;

        // Key tracking for fast O(1) random selection, insertion, and deletion
        private final List<String> liveKeys = new ArrayList<>();
        private final Map<String, Integer> keyToIndex = new HashMap<>();
        private final AtomicLong keySequence = new AtomicLong(0);

        private PreparedStatement insertStmt;
        private PreparedStatement deleteStmt;
        private final List<PreparedStatement> updateStmtByColumn = new ArrayList<>();

        private volatile boolean running = true;
        private final Map<String, Long> lastErrorLogTimes = new HashMap<>();

        private long issued = 0;
        private long insertsIssued = 0;
        private long deletesIssued = 0;
        private long updatesIssued = 0;

        private final LongAdder totalEvents = new LongAdder();
        private final LongAdder totalInserts = new LongAdder();
        private final LongAdder totalDeletes = new LongAdder();
        private final LongAdder totalUpdates = new LongAdder();
        private final LongAdder totalErrors = new LongAdder();
        private final LongAdder totalBehindSchedule = new LongAdder();
        private final LongAdder totalDroppedTicks = new LongAdder();
        private final LongAdder totalBlockedOnInFlight = new LongAdder();
        private final LongAdder totalBlockedTimeNanos = new LongAdder();
        private final AtomicLong errorsSinceLastLog = new AtomicLong(0);
        private final AtomicLong opsSinceLastLog = new AtomicLong(0);
        private final AtomicLong attemptedInsertsSinceLastLog = new AtomicLong(0);
        private final AtomicLong attemptedDeletesSinceLastLog = new AtomicLong(0);
        private final AtomicLong attemptedUpdatesSinceLastLog = new AtomicLong(0);
        private final AtomicLong behindScheduleSinceLastLog = new AtomicLong(0);
        private final AtomicLong droppedTicksSinceLastLog = new AtomicLong(0);
        private final AtomicLong blockedOnInFlightSinceLastLog = new AtomicLong(0);
        private final AtomicLong blockedTimeNanosSinceLastLog = new AtomicLong(0);

        public ThroughputGen(CqlSession session, Config config) {
            this.session = session;
            this.config = config;
            this.inFlight = new Semaphore(config.maxInFlight);
            for (int i = 0; i < config.numColumns; i++) {
                columnNames.add("c" + i);
            }
        }

        public void setup() {
            String qualifiedTable = config.keyspace + "." + config.table;
            session.execute("CREATE KEYSPACE IF NOT EXISTS " + config.keyspace
                    + " WITH replication = {'class': 'NetworkTopologyStrategy', '" + config.localDc + "': " + config.replicationFactor + "}");

            StringBuilder createTable = new StringBuilder("CREATE TABLE IF NOT EXISTS " + qualifiedTable + " (id text PRIMARY KEY");
            for (String col : columnNames) {
                createTable.append(", ").append(col).append(" int");
            }
            createTable.append(") WITH cdc = true");
            session.execute(createTable.toString());

            session.execute("ALTER TABLE " + qualifiedTable + " WITH cdc = true");
            System.out.println("[event-throughput-gen] ensured CDC is enabled on " + qualifiedTable);

            // Prepare INSERT statement
            StringBuilder insertCql = new StringBuilder("INSERT INTO ").append(qualifiedTable).append(" (id");
            StringBuilder placeholders = new StringBuilder("?"); // what will be the id value
            for (String col : columnNames) {
                insertCql.append(", ").append(col);
                placeholders.append(", ?");
            }
            insertCql.append(") VALUES (").append(placeholders).append(")");
            insertStmt = session.prepare(insertCql.toString());

            // Prepare DELETE statement
            deleteStmt = session.prepare("DELETE FROM " + qualifiedTable + " WHERE id = ?");

            // Prepare UPDATE statements per column
            for (String col : columnNames) {
                updateStmtByColumn.add(session.prepare(
                        "UPDATE " + qualifiedTable + " SET " + col + " = ? WHERE id = ?"));
            }

            // Prime initial rows
            System.out.println("[event-throughput-gen] inserting " + config.initialRows + " initial rows into " + qualifiedTable + "...");
            int initialInserts = 0;
            for (int i = 0; i < config.initialRows; i++) {
                String pk = config.keyPrefix + "-pk-" + keySequence.getAndIncrement();
                Object[] values = new Object[1 + columnNames.size()];
                values[0] = pk;
                for (int col = 0; col < columnNames.size(); col++) {
                    values[1 + col] = 0;
                }
                try {
                    session.execute(insertStmt.bind(values));
                    if (config.trackLiveKeys) {
                        addLiveKey(pk);
                    }
                    initialInserts++;
                } catch (Exception e) {
                    handleError(e, "initial_insert");
                }
            }
            System.out.println("[event-throughput-gen] initial insert complete: " + initialInserts + " rows live");
        }

        public void run() {
            long periodNanos = (long) (1_000_000_000.0 / config.eventsPerSecond);
            long maxCatchupNanos = config.maxCatchupMs * 1_000_000L;
            long nextTick = System.nanoTime();
            long lastLogNanos = System.nanoTime();
            long nextLogAt = System.currentTimeMillis() + 10_000L;

            while (running && !Thread.interrupted()) {
                OpType targetOp = pickNextOp();
                OpType executed = switch (targetOp) {
                    case INSERT -> doInsert();
                    case DELETE -> doDelete();
                    case UPDATE -> doUpdate();
                };

                switch (executed) {
                    case INSERT -> attemptedInsertsSinceLastLog.incrementAndGet();
                    case DELETE -> attemptedDeletesSinceLastLog.incrementAndGet();
                    case UPDATE -> attemptedUpdatesSinceLastLog.incrementAndGet();
                }

                totalEvents.increment();
                opsSinceLastLog.incrementAndGet();

                long now = System.currentTimeMillis();
                if (now >= nextLogAt) {
                    long nowNanos = System.nanoTime();
                    double elapsedSec = (nowNanos - lastLogNanos) / 1_000_000_000.0;
                    long opsInterval = opsSinceLastLog.getAndSet(0);
                    long insertsInterval = attemptedInsertsSinceLastLog.getAndSet(0);
                    long deletesInterval = attemptedDeletesSinceLastLog.getAndSet(0);
                    long updatesInterval = attemptedUpdatesSinceLastLog.getAndSet(0);
                    long errorsInterval = errorsSinceLastLog.getAndSet(0);
                    long behindInterval = behindScheduleSinceLastLog.getAndSet(0);
                    long droppedInterval = droppedTicksSinceLastLog.getAndSet(0);
                    long blockedInterval = blockedOnInFlightSinceLastLog.getAndSet(0);
                    long blockedTimeMsInterval = blockedTimeNanosSinceLastLog.getAndSet(0) / 1_000_000L;
                    long totalBlockedTimeMs = totalBlockedTimeNanos.sum() / 1_000_000L;

                    double actualEps = elapsedSec > 0 ? (opsInterval / elapsedSec) : 0;
                    double actualInsertPct = opsInterval > 0 ? (insertsInterval * 100.0 / opsInterval) : 0;
                    double actualDeletePct = opsInterval > 0 ? (deletesInterval * 100.0 / opsInterval) : 0;
                    double actualUpdatePct = opsInterval > 0 ? (updatesInterval * 100.0 / opsInterval) : 0;
                    String liveStr = config.trackLiveKeys ? String.valueOf(getLiveKeyCount()) : "n/a";

                    System.out.printf(
                            "[event-throughput-gen] %s totalEvents=%d inserts=%d deletes=%d updates=%d live=%s errors=%d(interval=%d) behind=%d(interval=%d) droppedTicks=%d(interval=%d) blockedInFlight=%d(interval=%d, %dms/intvl, %dms/tot) inFlight=%d | actualRate=%.1f eps (target=%.1f) | attempted: insert=%.1f%% delete=%.1f%% update=%.1f%%%n",
                            Instant.now(), totalEvents.sum(), totalInserts.sum(), totalDeletes.sum(), totalUpdates.sum(), liveStr,
                            totalErrors.sum(), errorsInterval, totalBehindSchedule.sum(), behindInterval,
                            totalDroppedTicks.sum(), droppedInterval,
                            totalBlockedOnInFlight.sum(), blockedInterval, blockedTimeMsInterval, totalBlockedTimeMs,
                            (config.maxInFlight - inFlight.availablePermits()),
                            actualEps, config.eventsPerSecond, actualInsertPct, actualDeletePct, actualUpdatePct);

                    lastLogNanos = nowNanos;
                    nextLogAt = now + 10_000L;
                }

                nextTick += periodNanos;
                long nowNanos = System.nanoTime();
                long sleepNanos = nextTick - nowNanos;
                if (sleepNanos > 0) {
                    LockSupport.parkNanos(sleepNanos);
                } else {
                    totalBehindSchedule.increment();
                    behindScheduleSinceLastLog.incrementAndGet();
                    if (nowNanos - nextTick > maxCatchupNanos) {
                        long targetNextTick = nowNanos - maxCatchupNanos;
                        long skipped = (targetNextTick - nextTick) / periodNanos;
                        if (skipped > 0) {
                            totalDroppedTicks.add(skipped);
                            droppedTicksSinceLastLog.addAndGet(skipped);
                            nextTick += skipped * periodNanos;
                        }
                    }
                }
            }
        }

        public void stop() {
            this.running = false;
        }

        public void drainInFlight(long timeoutMillis) {
            try {
                boolean drained = inFlight.tryAcquire(config.maxInFlight, timeoutMillis, TimeUnit.MILLISECONDS);
                if (drained) {
                    inFlight.release(config.maxInFlight);
                    System.out.println("[event-throughput-gen] successfully drained all in-flight requests");
                } else {
                    System.out.printf("[event-throughput-gen] drain timed out with %d in-flight requests still pending%n",
                            config.maxInFlight - inFlight.availablePermits());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private void handleError(Throwable t, String op) {
            if (t instanceof CompletionException && t.getCause() != null) {
                t = t.getCause();
            } else if (t instanceof ExecutionException && t.getCause() != null) {
                t = t.getCause();
            }
            totalErrors.increment();
            errorsSinceLastLog.incrementAndGet();
            String errorType;
            if (t instanceof WriteTimeoutException) {
                errorType = "WriteTimeoutException";
            } else if (t instanceof UnavailableException) {
                errorType = "UnavailableException";
            } else if (t instanceof OverloadedException) {
                errorType = "OverloadedException";
            } else if (t instanceof DriverException) {
                errorType = "DriverException(" + t.getClass().getSimpleName() + ")";
            } else {
                errorType = "UNEXPECTED(" + t.getClass().getSimpleName() + ")";
            }
            long now = System.currentTimeMillis();
            synchronized (lastErrorLogTimes) {
                Long lastLog = lastErrorLogTimes.get(errorType);
                if (lastLog == null || now - lastLog >= 10_000L) {
                    lastErrorLogTimes.put(errorType, now);
                    System.err.printf("[event-throughput-gen] %s error during %s: %s - %s%n",
                            Instant.now(), op, errorType, t.getMessage());
                }
            }
        }

        OpType pickNextOp() {
            issued++;
            double insertDeficit = config.insertFraction * issued - insertsIssued;
            double deleteDeficit = config.deleteFraction * issued - deletesIssued;
            double updateDeficit = config.updateFraction * issued - updatesIssued;

            OpType chosen = OpType.INSERT;
            double maxDeficit = insertDeficit;

            if (deleteDeficit > maxDeficit) {
                maxDeficit = deleteDeficit;
                chosen = OpType.DELETE;
            }
            if (updateDeficit > maxDeficit) {
                maxDeficit = updateDeficit;
                chosen = OpType.UPDATE;
            }

            switch (chosen) {
                case INSERT -> insertsIssued++;
                case DELETE -> deletesIssued++;
                case UPDATE -> updatesIssued++;
            }
            return chosen;
        }

        private void acquireInFlightPermit() {
            if (inFlight.tryAcquire()) {
                return;
            }
            totalBlockedOnInFlight.increment();
            blockedOnInFlightSinceLastLog.incrementAndGet();
            long startBlock = System.nanoTime();
            inFlight.acquireUninterruptibly();
            long blockedNanos = System.nanoTime() - startBlock;
            totalBlockedTimeNanos.add(blockedNanos);
            blockedTimeNanosSinceLastLog.addAndGet(blockedNanos);
        }

        private OpType doInsert() {
            String pk = config.keyPrefix + "-pk-" + keySequence.getAndIncrement();
            Object[] values = new Object[1 + columnNames.size()];
            values[0] = pk;
            for (int col = 0; col < columnNames.size(); col++) {
                values[1 + col] = ThreadLocalRandom.current().nextInt(1000);
            }
            acquireInFlightPermit();
            try {
                session.executeAsync(insertStmt.bind(values)).whenComplete((rs, ex) -> {
                    try {
                        if (ex != null) {
                            handleError(ex, "insert");
                        } else {
                            if (config.trackLiveKeys) {
                                addLiveKey(pk);
                            }
                            totalInserts.increment();
                        }
                    } finally {
                        inFlight.release();
                    }
                });
            } catch (Throwable t) {
                inFlight.release();
                handleError(t, "insert");
            }
            return OpType.INSERT;
        }

        private OpType doDelete() {
            String pk = takeRandomLiveKey();
            if (pk == null) {
                // If no live keys currently available, perform an insert to keep load moving
                return doInsert();
            }
            acquireInFlightPermit();
            try {
                session.executeAsync(deleteStmt.bind(pk)).whenComplete((rs, ex) -> {
                    try {
                        if (ex != null) {
                            addLiveKey(pk);
                            handleError(ex, "delete");
                        } else {
                            totalDeletes.increment();
                        }
                    } finally {
                        inFlight.release();
                    }
                });
            } catch (Throwable t) {
                inFlight.release();
                addLiveKey(pk);
                handleError(t, "delete");
            }
            return OpType.DELETE;
        }

        private OpType doUpdate() {
            String pk = pickRandomLiveKey();
            if (pk == null) {
                // If no live keys currently available, perform an insert
                return doInsert();
            }
            int colIndex = ThreadLocalRandom.current().nextInt(columnNames.size());
            int newValue = ThreadLocalRandom.current().nextInt(100000);
            acquireInFlightPermit();
            try {
                session.executeAsync(updateStmtByColumn.get(colIndex).bind(newValue, pk)).whenComplete((rs, ex) -> {
                    try {
                        if (ex != null) {
                            handleError(ex, "update");
                        } else {
                            totalUpdates.increment();
                        }
                    } finally {
                        inFlight.release();
                    }
                });
            } catch (Throwable t) {
                inFlight.release();
                handleError(t, "update");
            }
            return OpType.UPDATE;
        }

        private synchronized void addLiveKey(String pk) {
            if (!keyToIndex.containsKey(pk)) {
                keyToIndex.put(pk, liveKeys.size());
                liveKeys.add(pk);
            }
        }

        private synchronized String takeRandomLiveKey() {
            if (liveKeys.isEmpty()) {
                return null;
            }
            int index = ThreadLocalRandom.current().nextInt(liveKeys.size());
            int lastIndex = liveKeys.size() - 1;
            String pk = liveKeys.get(index);
            keyToIndex.remove(pk);
            if (index != lastIndex) {
                String lastKey = liveKeys.get(lastIndex);
                liveKeys.set(index, lastKey);
                keyToIndex.put(lastKey, index);
            }
            liveKeys.remove(lastIndex);
            return pk;
        }

        private synchronized String pickRandomLiveKey() {
            if (liveKeys.isEmpty()) {
                return null;
            }
            int index = ThreadLocalRandom.current().nextInt(liveKeys.size());
            return liveKeys.get(index);
        }

        private synchronized int getLiveKeyCount() {
            return liveKeys.size();
        }
    }
}
