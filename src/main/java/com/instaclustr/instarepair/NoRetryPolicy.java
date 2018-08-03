package com.instaclustr.instarepair;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RetryPolicy;

/**
 * A no retry policy.
 */
public class NoRetryPolicy implements RetryPolicy {
    @Override
    public RetryPolicy.RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
        return RetryPolicy.RetryDecision.rethrow();
    }

    @Override
    public RetryPolicy.RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
        return RetryPolicy.RetryDecision.rethrow();
    }

    @Override
    public RetryPolicy.RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
        return RetryPolicy.RetryDecision.rethrow();
    }

    @Override
    public RetryPolicy.RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, DriverException e, int nbRetry) {
        return RetryPolicy.RetryDecision.rethrow();
    }

    @Override
    public void init(Cluster cluster) {

    }

    @Override
    public void close() {

    }
}
