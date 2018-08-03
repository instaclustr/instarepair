package com.instaclustr.instarepair;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Repair a TokenRange for table.
 */
public class RangeRepairTask implements Callable<Boolean> {
    private static final Logger logger = LoggerFactory.getLogger(RangeRepairTask.class);

    /**
     * Delay between retries when nodes are unavailable.
     */
    private final int RETRY_DELAY_MS;

    /**
     * Number of retry attempts.
     */
    private final int MAX_RETRY_ATTEMPTS;

    private TaskCoordinator coordinator;
    private Metadata metadata;
    private Session session;
    private TableMetadata table;
    private PreparedStatement repairStatement;
    private TokenRange range;
    private Set<TokenRange> failedTokens;
    private Set<TokenRange> failedRanges;

    public RangeRepairTask(TaskCoordinator coordinator,
                           int maxRetryAttempts,
                           int retryDelayMilliseconds,
                           Metadata metadata,
                           Session session,
                           TableMetadata table,
                           PreparedStatement repairStatement,
                           TokenRange range,
                           Set<TokenRange> failedTokens,
                           Set<TokenRange> failedRanges) {
        this.coordinator = coordinator;
        this.MAX_RETRY_ATTEMPTS = maxRetryAttempts;
        this.RETRY_DELAY_MS = retryDelayMilliseconds;
        this.metadata = metadata;
        this.session = session;
        this.table = table;
        this.repairStatement = repairStatement;
        this.range = range;
        this.failedTokens = failedTokens;
        this.failedRanges = failedRanges;
    }

    @Override
    public Boolean call() {
        final String keyspaceName = table.getKeyspace().getName();
        final String tableName = table.getName();

        LinkedList<TokenRange> ranges = new LinkedList<>();
        ranges.add(range);
        boolean isRepaired = true;
        int unavailableRetryNo = 1;
        while (!ranges.isEmpty() && coordinator.isRunning()) {
            TokenRange range = ranges.poll();
            Token lastToken = null;
            Token lastRepairedToken = null;
            Row lastRow = null;
            Statement statement = repairStatement.bind(range.getStart(), range.getEnd());
            try {
                ResultSet rs = session.execute(statement);
                for (Row row : rs) {
                    if (!coordinator.isRunning()) {
                        return false;
                    }
                    Token token = row.getToken(0);
                    if (!token.equals(lastToken)) {
                        lastRepairedToken = lastToken;
                    }
                    lastToken = token;
                    lastRow = row;
                }
                unavailableRetryNo = 1;
            } catch (ReadFailureException|ReadTimeoutException|OperationTimedOutException e) {
                TokenRange retryRange = range;
                if (lastRepairedToken != null) {
                    retryRange = metadata.newTokenRange(lastRepairedToken, range.getEnd());
                }
                List<TokenRange> split = Util.split(metadata, retryRange, 100);
                if (split.size() == 1) {
                    logger.error("Failed to repair {}.{} {}; Last row: {}; Error {} {}", keyspaceName, tableName, range.getEnd(), lastRow, e.getClass().getCanonicalName(), e.getMessage());
                    failedTokens.add(range);
                    isRepaired = false;
                } else {
                    // Retry the step as smaller steps.
                    ranges.addAll(split);
                }
            } catch (UnavailableException|NoHostAvailableException e) {
                unavailableRetryNo++;
                if (unavailableRetryNo <= MAX_RETRY_ATTEMPTS) {
                    long timeout = RETRY_DELAY_MS * (1 << unavailableRetryNo);
                    logger.info("{}.{} {}: Replicas unavailable. Waiting {} ms...", keyspaceName, tableName, range, timeout);
                    coordinator.sleep(RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
                } else {
                    logger.error("Aborting repair due to unavailable hosts!");
                    coordinator.abort();
                    isRepaired = false;
                }
                ranges.addFirst(range);
            } catch (DriverException e) {
                logger.info("Failed range {}.{} {}: {} - {}", keyspaceName, tableName, range, e.getClass().getCanonicalName(), e.getMessage());
                failedRanges.add(range);
                isRepaired = false;
            }
        }
        return isRepaired && ranges.isEmpty() && coordinator.isRunning();
    }
}
