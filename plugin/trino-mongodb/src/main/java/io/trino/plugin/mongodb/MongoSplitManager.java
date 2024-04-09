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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import java.util.concurrent.CompletableFuture;

import static io.trino.plugin.mongodb.MongoSessionProperties.getDynamicFilteringWaitTimeout;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class MongoSplitManager
        implements ConnectorSplitManager
{
    private static final ConnectorSplitSource.ConnectorSplitBatch EMPTY_BATCH = new ConnectorSplitSource.ConnectorSplitBatch(ImmutableList.of(), false);

    private final MongoServerDetailsProvider serverDetailsProvider;

    @Inject
    public MongoSplitManager(MongoServerDetailsProvider serverDetailsProvider)
    {
        this.serverDetailsProvider = requireNonNull(serverDetailsProvider, "serverDetailsProvider is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        MongoSplit split = new MongoSplit(serverDetailsProvider.getServerAddress());
        return new MongoSplitSource(session, dynamicFilter, new FixedSplitSource(split));
    }

    private static class MongoSplitSource
            implements ConnectorSplitSource
    {
        private final DynamicFilter dynamicFilter;
        private final long startNanos;
        private final long dynamicFilteringTimeoutNanos;

        private final ConnectorSplitSource delegateSplitSource;

        public MongoSplitSource(ConnectorSession session, DynamicFilter dynamicFilter, ConnectorSplitSource delegateSplitSource)
        {
            this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
            this.dynamicFilteringTimeoutNanos = (long) getDynamicFilteringWaitTimeout(session).getValue(NANOSECONDS);
            this.startNanos = System.nanoTime();
            this.delegateSplitSource = requireNonNull(delegateSplitSource, "delegateSplitSource is null");
        }

        @Override
        public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
        {
            long remainingTimeoutNanos = getRemainingTimeoutNanos();
            if (remainingTimeoutNanos > 0 && dynamicFilter.isAwaitable()) {
                // wait for dynamic filter and yield
                return dynamicFilter.isBlocked()
                        .thenApply(ignored -> EMPTY_BATCH)
                        .completeOnTimeout(EMPTY_BATCH, remainingTimeoutNanos, NANOSECONDS);
            }

            return delegateSplitSource.getNextBatch(maxSize);
        }

        @Override
        public void close()
        {
            delegateSplitSource.close();
        }

        @Override
        public boolean isFinished()
        {
            if (getRemainingTimeoutNanos() > 0 && dynamicFilter.isAwaitable()) {
                return false;
            }

            return delegateSplitSource.isFinished();
        }

        private long getRemainingTimeoutNanos()
        {
            return dynamicFilteringTimeoutNanos - (System.nanoTime() - startNanos);
        }
    }
}
