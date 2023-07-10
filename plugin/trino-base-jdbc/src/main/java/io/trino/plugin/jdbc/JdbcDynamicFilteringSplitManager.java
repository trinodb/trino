/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.airlift.units.Duration.succinctNanos;
import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.dynamicFilteringEnabled;
import static io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties.getDynamicFilteringWaitTimeout;
import static io.trino.spi.connector.ConnectorSplitSource.ConnectorSplitBatch;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Implements waiting for collection of dynamic filters before generating splits from {@link ConnectorSplitManager}.
 * This allows JDBC based connectors to take advantage of dynamic filters during splits generation phase.
 * Implementing this as a wrapper over {@link ConnectorSplitManager} allows this class to be used by JDBC connectors
 * which don't rely on {@link JdbcSplitManager} for splits generation.
 */
public class JdbcDynamicFilteringSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(JdbcDynamicFilteringSplitManager.class);
    private static final ConnectorSplitBatch EMPTY_BATCH = new ConnectorSplitBatch(ImmutableList.of(), false);

    private final ConnectorSplitManager delegateSplitManager;
    private final DynamicFilteringStats stats;

    @Inject
    public JdbcDynamicFilteringSplitManager(
            @ForJdbcDynamicFiltering ConnectorSplitManager delegateSplitManager,
            DynamicFilteringStats stats)
    {
        this.delegateSplitManager = requireNonNull(delegateSplitManager, "delegateSplitManager is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        // JdbcProcedureHandle doesn't support any pushdown operation, so we rely on delegateSplitManager
        if (table instanceof JdbcProcedureHandle) {
            return delegateSplitManager.getSplits(transaction, session, table, dynamicFilter, constraint);
        }

        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        // pushing DF through limit could reduce query performance
        boolean hasLimit = tableHandle.getLimit().isPresent();
        if (dynamicFilter == DynamicFilter.EMPTY || hasLimit || !dynamicFilteringEnabled(session)) {
            return delegateSplitManager.getSplits(transaction, session, table, dynamicFilter, constraint);
        }

        return new DynamicFilteringSplitSource(transaction, session, (JdbcTableHandle) table, dynamicFilter, constraint);
    }

    private class DynamicFilteringSplitSource
            implements ConnectorSplitSource
    {
        private final ConnectorTransactionHandle transaction;
        private final ConnectorSession session;
        private final JdbcTableHandle table;
        private final DynamicFilter dynamicFilter;
        private final Constraint constraint;
        private final long dynamicFilteringTimeoutNanos;
        private final long startNanos;

        @GuardedBy("this")
        private Optional<ConnectorSplitSource> delegateSplitSource = Optional.empty();

        DynamicFilteringSplitSource(
                ConnectorTransactionHandle transaction,
                ConnectorSession session,
                JdbcTableHandle table,
                DynamicFilter dynamicFilter,
                Constraint constraint)
        {
            this.transaction = requireNonNull(transaction, "transaction is null");
            this.session = requireNonNull(session, "session is null");
            this.table = requireNonNull(table, "table is null");
            this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
            this.constraint = requireNonNull(constraint, "constraint is null");
            this.dynamicFilteringTimeoutNanos = (long) getDynamicFilteringWaitTimeout(session).getValue(NANOSECONDS);
            this.startNanos = System.nanoTime();
        }

        @Override
        public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
        {
            long remainingTimeoutNanos = getRemainingTimeoutNanos();
            if (remainingTimeoutNanos > 0 && dynamicFilter.isAwaitable()) {
                log.debug(
                        "Waiting for dynamic filter (query: %s, table: %s, remaining timeout: %s)",
                        session.getQueryId(),
                        table,
                        succinctNanos(remainingTimeoutNanos));
                // wait for dynamic filter and yield
                return dynamicFilter.isBlocked()
                        .thenApply(ignored -> EMPTY_BATCH)
                        .completeOnTimeout(EMPTY_BATCH, remainingTimeoutNanos, NANOSECONDS);
            }

            Duration waitingTime = succinctNanos(System.nanoTime() - startNanos);
            log.debug("Enumerating splits (query %s, table: %s, waiting time: %s, awaitable: %s, completed: %s)",
                    session.getQueryId(),
                    table,
                    waitingTime,
                    dynamicFilter.isAwaitable(),
                    dynamicFilter.isComplete());
            stats.processDynamicFilter(dynamicFilter, waitingTime);
            return getDelegateSplitSource().getNextBatch(maxSize);
        }

        @Override
        public void close()
        {
            getOptionalDelegateSplitSource().ifPresent(ConnectorSplitSource::close);
        }

        @Override
        public boolean isFinished()
        {
            if (getRemainingTimeoutNanos() > 0 && dynamicFilter.isAwaitable()) {
                return false;
            }

            return getDelegateSplitSource().isFinished();
        }

        private long getRemainingTimeoutNanos()
        {
            return dynamicFilteringTimeoutNanos - (System.nanoTime() - startNanos);
        }

        private synchronized ConnectorSplitSource getDelegateSplitSource()
        {
            if (delegateSplitSource.isPresent()) {
                return delegateSplitSource.get();
            }

            delegateSplitSource = Optional.of(delegateSplitManager.getSplits(
                    transaction,
                    session,
                    table.intersectedWithConstraint(dynamicFilter.getCurrentPredicate()),
                    dynamicFilter,
                    constraint));
            return delegateSplitSource.get();
        }

        private synchronized Optional<ConnectorSplitSource> getOptionalDelegateSplitSource()
        {
            return delegateSplitSource;
        }
    }
}
