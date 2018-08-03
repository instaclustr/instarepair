package com.instaclustr.instarepair;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;

/**
 * Coordinator for read repairing a node.
 */
public class RepairTask implements Callable<Boolean> {
    private static final Logger logger = LoggerFactory.getLogger(RepairTask.class);

    /**
     * Flag to include if only including the primary token ranges owned by the host.
     */
    private boolean primaryOnly;

    /**
     * Number of parallel repairs.
     */
    private int threads;

    /**
     * Initial number of steps per token range.
     */
    private int steps;

    /**
     * Queue of keyspaces to repair.
     */
    private Queue<KeyspaceMetadata> keyspaces;

    /**
     * Queue of tables to repair.
     */
    private Queue<TableMetadata> tables;

    /**
     * The keyspace currently being repaired.
     */
    private KeyspaceMetadata keyspace;

    /**
     * The table currently being repaired.
     */
    private TableMetadata table;

    /**
     * The ranges to be repaired for the current table.
     */
    private Queue<TokenRange> tableRanges;

    /**
     * Failed ranges per table.
     */
    private Map<TableMetadata, Set<TokenRange>> failedRanges;

    /**
     * Failed tokens per table.
     */
    private Map<TableMetadata, Set<TokenRange>> failedTokens;

    /**
     * Host being repaired.
     */
    private Host host;

    /**
     * Cassandra metadata.
     */
    private Metadata metadata;

    /**
     * Cassandra session.
     */
    private Session session;

    /**
     * Executor of range repairs.
     */
    private ExecutorService executor;

    /**
     * Coordinate running of tasks.
     */
    private TaskCoordinator coordinator = new TaskCoordinator();

    /**
     * Number of retry attempts.
     */
    private final int MAX_RETRY_ATTEMPTS;

    /**
     * Delay between retries when nodes are unavailable.
     */
    private final int RETRY_DELAY_MS;

    /**
     * Cache of PreparedStatement for table.
     */
    private HashMap<TableMetadata, PreparedStatement> statementCache = new HashMap<>();

    /**
     * @param maxRetryAttempts       Maximum number of retries when there are unavailable nodes.
     * @param retryDelayMilliseconds Delay between retries when nodes are unavailable.
     * @param host                   Host to repair.
     * @param metadata               Cassandra metadata.
     * @param session                Cassandra session.
     * @param threads                Number of parallel repairs.
     */
    private RepairTask(int maxRetryAttempts, int retryDelayMilliseconds, Host host, Metadata metadata, Session session, int threads) {
        this.MAX_RETRY_ATTEMPTS = maxRetryAttempts;
        this.RETRY_DELAY_MS = retryDelayMilliseconds;
        this.host = host;
        this.metadata = metadata;
        this.session = session;
        this.threads = threads;
        this.executor = Executors.newFixedThreadPool(threads);
    }

    /**
     * Repair queue of keyspaces.
     *
     * @param maxRetryAttempts       Maximum number of retries when there are unavailable nodes.
     * @param retryDelayMilliseconds Delay between retries when nodes are unavailable.
     * @param host                   Host to repair.
     * @param metadata               Cassandra metadata.
     * @param session                Cassandra session.
     * @param primaryOnly            True if only include primary token ranges owned by the host.
     * @param threads                Number of parallel repairs.
     * @param steps                  Initial steps per token range.
     * @param keyspaces              Queue of keyspaces to repair.
     */
    public RepairTask(int maxRetryAttempts, int retryDelayMilliseconds, Host host, Metadata metadata, Session session, boolean primaryOnly, int threads, int steps, Queue<KeyspaceMetadata> keyspaces) {
        this(maxRetryAttempts, retryDelayMilliseconds, host, metadata, session, threads);
        this.primaryOnly = primaryOnly;
        this.steps = steps;
        this.keyspaces = keyspaces;
        failedRanges = new HashMap<>();
        failedTokens = new HashMap<>();
    }

    /**
     * Repair queue of tables.
     *
     * @param maxRetryAttempts       Maximum number of retries when there are unavailable nodes.
     * @param retryDelayMilliseconds Delay between retries when nodes are unavailable.
     * @param host                   Host to repair.
     * @param metadata               Cassandra metadata.
     * @param session                Cassandra session.
     * @param primaryOnly            Wether only including primary token ranges owned by the host.
     * @param threads                Number of parallel repairs.
     * @param steps                  Initial steps per token range.
     * @param keyspace               Keyspace to repair.
     * @param tables                 Tables to repair.
     */
    public RepairTask(int maxRetryAttempts, int retryDelayMilliseconds, Host host, Metadata metadata, Session session, boolean primaryOnly, int threads, int steps, KeyspaceMetadata keyspace, Queue<TableMetadata> tables) {
        this(maxRetryAttempts, retryDelayMilliseconds, host, metadata, session, threads);
        this.primaryOnly = primaryOnly;
        this.steps = steps;
        keyspaces = new LinkedList<>();
        this.keyspace = keyspace;
        this.tables = tables;
        failedRanges = new HashMap<>();
        failedTokens = new HashMap<>();
    }

    /**
     * Resume a repair.
     *
     * @param maxRetryAttempts       Maximum number of retries when there are unavailable nodes.
     * @param retryDelayMilliseconds Delay between retries when nodes are unavailable.
     * @param host                   Host to repair.
     * @param metadata               Cassandra metadata.
     * @param session                Cassandra session.
     * @param in                     ObjectInputStream with saved repair state.
     * @param threads                Number of parallel repairs.
     * @throws IOException            Any of the usual Input/Output related exceptions.
     * @throws ClassNotFoundException Class of a serialized object cannot be found.
     */
    public RepairTask(int maxRetryAttempts, int retryDelayMilliseconds, Host host, Metadata metadata, Session session, ObjectInputStream in, int threads) throws IOException, ClassNotFoundException {
        this(maxRetryAttempts, retryDelayMilliseconds, host, metadata, session, threads);
        readObject(in);
    }

    /**
     * Write repair state to ObjectOutputStream.
     *
     * @param out ObjectOutputStream to write to.
     * @throws IOException Any of the usual Input/Output related exceptions.
     */
    public void writeObject(ObjectOutputStream out) throws IOException {
        out.writeBoolean(primaryOnly);
        out.writeInt(steps);
        List<String> strKeyspaces = keyspaces.stream().map(KeyspaceMetadata::getName).collect(Collectors.toList());
        out.writeObject(strKeyspaces);
        out.writeUTF(keyspace != null ? keyspace.getName() : "");
        List<String> strTables = tables.stream().map(TableMetadata::getName).collect(Collectors.toList());
        out.writeObject(strTables);
        out.writeUTF(table != null ? table.getName() : "");
        writeRanges(out, tableRanges);

        out.writeInt(failedRanges.size());
        for (Map.Entry<TableMetadata, Set<TokenRange>> entry : failedRanges.entrySet()) {
            TableMetadata table = entry.getKey();
            out.writeUTF(table.getKeyspace().getName());
            out.writeUTF(table.getName());
            writeRanges(out, entry.getValue());
        }

        out.writeInt(failedTokens.size());
        for (Map.Entry<TableMetadata, Set<TokenRange>> entry : failedTokens.entrySet()) {
            TableMetadata table = entry.getKey();
            out.writeUTF(table.getKeyspace().getName());
            out.writeUTF(table.getName());
            writeRanges(out, entry.getValue());
        }
    }

    /**
     * Write collection of TokenRange to ObjectOutputStream.
     *
     * @param out    ObjectOutputStream to write to.
     * @param ranges TokenRanges to write.
     * @throws IOException Any of the usual Input/Output related exceptions.
     */
    private static void writeRanges(ObjectOutputStream out, Collection<TokenRange> ranges) throws IOException {
        if (ranges == null) {
            out.writeInt(0);
            return;
        }
        out.writeInt(ranges.size());
        for (TokenRange range : ranges) {
            out.writeUTF(range.getStart().toString());
            out.writeUTF(range.getEnd().toString());
        }
    }

    /**
     * Read repair state from ObjectInputStream.
     *
     * @param in ObjectInputStream to read from.
     * @throws IOException            Any of the usual Input/Output related exceptions.
     * @throws ClassNotFoundException Class of a serialized object cannot be found.
     */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        this.primaryOnly = in.readBoolean();
        this.steps = in.readInt();
        List<String> strKeyspaces = (List<String>) in.readObject();
        keyspaces = new LinkedList<>();
        for (String keyspace : strKeyspaces) {
            keyspaces.offer(metadata.getKeyspace(keyspace));
        }
        String strKeyspace = in.readUTF();
        keyspace = metadata.getKeyspace(strKeyspace);
        List<String> strTables = (List<String>) in.readObject();
        tables = new LinkedList<>();
        for (String table : strTables) {
            tables.offer(keyspace.getTable(table));
        }
        String strTable = in.readUTF();
        table = !Objects.equals(strTable, "") ? keyspace.getTable(strTable) : null;
        tableRanges = newTokenRangeQueue(readRanges(metadata, in));

        failedRanges = new HashMap<>();
        int noFailedRanges = in.readInt();
        for (int i = 0; i < noFailedRanges; i++) {
            strKeyspace = in.readUTF();
            strTable = in.readUTF();
            TableMetadata table = metadata.getKeyspace(strKeyspace).getTable(strTable);
            failedRanges.put(table, readRanges(metadata, in));
        }

        failedTokens = new HashMap<>();
        int noFailedTokens = in.readInt();
        for (int i = 0; i < noFailedTokens; i++) {
            strKeyspace = in.readUTF();
            strTable = in.readUTF();
            TableMetadata table = metadata.getKeyspace(strKeyspace).getTable(strTable);
            failedTokens.put(table, readRanges(metadata, in));
        }
    }

    private static Set<TokenRange> newTokenRangeSet() {
        return Collections.synchronizedSet(new LinkedHashSet<>());
    }

    private static Set<TokenRange> newTokenRangeSet(Set<TokenRange> tokens) {
        return Collections.synchronizedSet(new LinkedHashSet<>(tokens));
    }

    private static Queue<TokenRange> newTokenRangeQueue(Collection<TokenRange> ranges) {
        return new ConcurrentLinkedQueue<>(ranges);
    }

    /**
     * Read collection of TokenRange to ObjectInputStream.
     *
     * @param in ObjectInputStream to read from.
     * @return Set of TokenRange
     * @throws IOException Any of the usual Input/Output related exceptions.
     */
    private static Set<TokenRange> readRanges(Metadata metadata, ObjectInputStream in) throws IOException {
        int noRanges = in.readInt();
        Set<TokenRange> ranges = newTokenRangeSet();
        for (int i = 0; i < noRanges; i++) {
            Token start = metadata.newToken(in.readUTF());
            Token end = metadata.newToken(in.readUTF());
            ranges.add(metadata.newTokenRange(start, end));
        }
        return ranges;
    }

    /**
     * Get the token ranges to repair for the host.
     *
     * @return Token ranges owned by host for this keyspace.
     */
    private List<TokenRange> getTokenRanges() {
        Stream<TokenRange> ranges = metadata.getTokenRanges(keyspace.getName(), host).stream();
        if (primaryOnly) {
            ranges = ranges.filter(range -> host.getTokens().contains(range.getEnd()));
        }
        List<TokenRange> nodeRanges = ranges.collect(Collectors.toList());
        Collection<Iterable<TokenRange>> stepRanges = new ArrayList<>(nodeRanges.size());
        for (TokenRange nodeRange : nodeRanges) {
            stepRanges.add(Util.split(metadata, nodeRange, steps));
        }
        RoundRobinIterator<TokenRange> iterator = new RoundRobinIterator<>(stepRanges);
        List<TokenRange> tokenRanges = new ArrayList<>(nodeRanges.size() * steps);
        while (iterator.hasNext()) {
            tokenRanges.add(iterator.next());
        }
        return tokenRanges;
    }

    @Override
    public Boolean call() {
        // Grab snapshot of previously failed ranges. This is because we add to the set when repairing.
        Map<TableMetadata, Set<TokenRange>> retryRanges = new HashMap<>(failedRanges);
        for (Map.Entry<TableMetadata, Set<TokenRange>> entry : retryRanges.entrySet()) {
            entry.setValue(newTokenRangeSet(entry.getValue()));
        }
        // Grab snapshot of previously failed ranges. This is because we add to the set when repairing.
        Map<TableMetadata, Set<TokenRange>> retryTokens = new HashMap<>(failedTokens);
        for (Map.Entry<TableMetadata, Set<TokenRange>> entry : retryTokens.entrySet()) {
            entry.setValue(newTokenRangeSet(entry.getValue()));
        }
        boolean isRepaired = true;
        // Resume suspended repair.
        if (table != null) {
            logger.info("Resuming repair of {}.{}", table.getKeyspace().getName(), table.getName());
            isRepaired = repairTable(table, tableRanges);
        }
        // Repair queued tables.
        if (coordinator.isRunning() && keyspace != null) {
            isRepaired = repairTables() && isRepaired;
        }
        // Repair queued keyspaces.
        if (coordinator.isRunning()) {
            isRepaired = repairKeyspaces() && isRepaired;
        }
        // Retry previously failed ranges.
        if (coordinator.isRunning()) {
            Iterator<Map.Entry<TableMetadata, Set<TokenRange>>> it = retryRanges.entrySet().iterator();
            while (coordinator.isRunning() && it.hasNext()) {
                Map.Entry<TableMetadata, Set<TokenRange>> entry = it.next();
                TableMetadata table = entry.getKey();
                Set<TokenRange> ranges = entry.getValue();
                logger.info("Retrying failed ranges for {}.{}", table.getKeyspace().getName(), table.getName());
                isRepaired = repairTable(table, newTokenRangeQueue(ranges)) && isRepaired;
            }
        }
        // Retry previously failed tokens.
        if (coordinator.isRunning()) {
            Iterator<Map.Entry<TableMetadata, Set<TokenRange>>> it = retryTokens.entrySet().iterator();
            while (coordinator.isRunning() && it.hasNext()) {
                Map.Entry<TableMetadata, Set<TokenRange>> entry = it.next();
                TableMetadata table = entry.getKey();
                Set<TokenRange> ranges = entry.getValue();
                logger.info("Retrying failed tokens for {}.{}", table.getKeyspace().getName(), table.getName());
                isRepaired = repairTable(table, newTokenRangeQueue(ranges)) && isRepaired;
            }
        }
        coordinator.shutdownDone();
        return isRepaired;
    }

    /**
     * Repair queued keyspaces.
     *
     * @return true if all keyspaces were repaired.
     */
    private boolean repairKeyspaces() {
        boolean isRepaired = true;
        while (coordinator.isRunning() && (keyspace = keyspaces.poll()) != null) {
            tables = new LinkedList<>(keyspace.getTables());
            isRepaired = repairTables() && isRepaired;
        }
        return isRepaired;
    }

    /**
     * Repair queued tables.
     *
     * @return true if all tables were repaired.
     */
    private boolean repairTables() {
        List<TokenRange> keyspaceTokenRanges = getTokenRanges();

        boolean isRepaired = true;

        while (coordinator.isRunning() && (table = tables.poll()) != null) {
            logger.info("Repairing {}.{}", table.getKeyspace().getName(), table.getName());
            tableRanges = newTokenRangeQueue(keyspaceTokenRanges);
            isRepaired = repairTable(table, tableRanges) && isRepaired;
        }

        return coordinator.isRunning() && isRepaired;
    }

    /**
     * Repair ranges on a table.
     *
     * @param table  Table to repair
     * @param ranges Ranges to repair
     * @return true if all repairs were successful.
     */
    private boolean repairTable(TableMetadata table, Queue<TokenRange> ranges) {
        final PreparedStatement repairStatement = prepare(table);
        final String keyspaceName = table.getKeyspace().getName();
        final String tableName = table.getName();

        // Create tasks to consume the ranges queue and run repair tasks.
        AtomicInteger progress = new AtomicInteger(1);
        final int n = ranges.size();
        Collection<Callable<Boolean>> tasks = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            tasks.add(() -> {
                boolean isTaskRepaired = true;
                while (!ranges.isEmpty()) {
                    TokenRange range = ranges.poll();
                    Set<TokenRange> rangeFailedTokens = new LinkedHashSet<>();
                    Set<TokenRange> rangeFailedRanges = new LinkedHashSet<>();
                    RangeRepairTask task = new RangeRepairTask(
                            coordinator,
                            MAX_RETRY_ATTEMPTS,
                            RETRY_DELAY_MS,
                            metadata,
                            session,
                            table,
                            repairStatement,
                            range,
                            rangeFailedTokens,
                            rangeFailedRanges);
                    logger.info("Repairing {}.{}, range {}/{} ({},{}]", keyspaceName, tableName, progress.getAndIncrement(), n, range.getStart(), range.getEnd());
                    isTaskRepaired = task.call() && isTaskRepaired;
                    // If repair was aborted, re-schedule for next run.
                    if (!coordinator.isRunning()) {
                        ranges.add(range);
                        return false;
                    } else {
                        // Remove the range from failed set.
                        if (failedTokens.containsKey(table)) {
                            failedTokens.get(table).remove(range);
                        }
                        if (failedRanges.containsKey(table)) {
                            failedRanges.get(table).remove(range);
                        }

                        // Add fails from this repair.
                        if (!rangeFailedTokens.isEmpty()) {
                            failedTokens.putIfAbsent(table, newTokenRangeSet());
                            failedTokens.get(table).addAll(rangeFailedTokens);
                        }
                        if (!rangeFailedRanges.isEmpty()) {
                            failedRanges.putIfAbsent(table, newTokenRangeSet());
                            failedRanges.get(table).addAll(rangeFailedRanges);
                        }
                    }
                }
                return isTaskRepaired && coordinator.isRunning();
            });
        }

        // Wait for table to repair.
        try {
            boolean isRepaired = executor.invokeAll(tasks).stream().allMatch(Futures::getUnchecked);
            if (failedTokens.containsKey(table) && failedTokens.get(table).isEmpty()) {
                failedTokens.remove(table);
            }
            if (failedRanges.containsKey(table) && failedRanges.get(table).isEmpty()) {
                failedRanges.remove(table);
            }
            return isRepaired && coordinator.isRunning();
        } catch (InterruptedException e) {
            logger.error("Ignoring interrupt!");
        }

        return false;
    }

    /**
     * Prepare statement for repairing token range for a table.
     *
     * @param table Table to repair
     * @return PreparedStatement that takes token range parameters.
     */
    private PreparedStatement prepare(final TableMetadata table) {
        if (!statementCache.containsKey(table)) {
            List<String> columns = new ArrayList<>(table.getColumns().size() + 1);
            String[] partitionColumns = table.getPartitionKey().stream().map(ColumnMetadata::getName).toArray(String[]::new);
            String tokenKeys = token(partitionColumns);
            columns.add(tokenKeys);
            table.getColumns().stream().map(ColumnMetadata::getName).forEachOrdered(columns::add);
            PreparedStatement statement = session.prepare(QueryBuilder.select(columns.toArray(new String[columns.size()]))
                    .from(table).where(gt(tokenKeys, bindMarker())).and(lte(tokenKeys, bindMarker())));
            statementCache.put(table, statement);
            return statement;
        }
        return statementCache.get(table);
    }

    /**
     * Stop the repair.
     */
    public void shutdown() {
        coordinator.shutdown();
        executor.shutdown();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("keyspaces ");
        sb.append(keyspaces.stream().map(KeyspaceMetadata::getName).collect(Collectors.toList()));

        if (keyspace != null) {
            sb.append("; keyspace = ");
            sb.append(keyspace.getName());
            sb.append(", tables ");
            sb.append(tables.stream().map(TableMetadata::getName).collect(Collectors.toList()));
        }

        if (table != null) {
            sb.append("; table = ");
            sb.append(table.getKeyspace().getName());
            sb.append('.');
            sb.append(table.getName());
            sb.append(", ranges = ");
            sb.append(tableRanges);
        }

        if (!failedRanges.isEmpty()) {
            sb.append("; failed ranges (");
            for (Map.Entry<TableMetadata, Set<TokenRange>> entry : failedRanges.entrySet()) {
                TableMetadata table = entry.getKey();
                Set<TokenRange> ranges = entry.getValue();
                sb.append(table.getKeyspace().getName());
                sb.append('.');
                sb.append(table.getName());
                sb.append(' ');
                sb.append(ranges.stream().map(r -> "(" + r.getStart().toString() + "," + r.getEnd().toString() + "]").collect(Collectors.toList()));
                sb.append(", ");
            }
            sb.append(")");
        }

        if (!failedTokens.isEmpty()) {
            sb.append("; failed tokens (");
            for (Map.Entry<TableMetadata, Set<TokenRange>> entry : failedTokens.entrySet()) {
                TableMetadata table = entry.getKey();
                Set<TokenRange> ranges = entry.getValue();
                sb.append(table.getKeyspace().getName());
                sb.append('.');
                sb.append(table.getName());
                sb.append(' ');
                sb.append(ranges.stream().map(r -> r.getEnd().toString()).collect(Collectors.toList()));
                sb.append(", ");
            }
            sb.append(")");
        }

        return sb.toString();
    }
}
