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
package io.trino.plugin.kudu;

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

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.trino.plugin.kudu.KuduSessionProperties.getDynamicFilteringWaitTimeout;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class KuduSplitManager
        implements ConnectorSplitManager
{
    private static final ConnectorSplitSource.ConnectorSplitBatch EMPTY_BATCH = new ConnectorSplitSource.ConnectorSplitBatch(ImmutableList.of(), false);
    private final KuduClientSession clientSession;

    @Inject
    public KuduSplitManager(KuduClientSession clientSession)
    {
        this.clientSession = requireNonNull(clientSession, "clientSession is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        return new KuduDynamicFilteringSplitSource(session, clientSession, dynamicFilter, table);
    }

    private static class KuduDynamicFilteringSplitSource
            implements ConnectorSplitSource
    {
        private final KuduClientSession clientSession;
        private final DynamicFilter dynamicFilter;
        private final ConnectorTableHandle tableHandle;
        private final long dynamicFilteringTimeoutNanos;
        private ConnectorSplitSource delegateSplitSource;
        private final long startNanos;

        private KuduDynamicFilteringSplitSource(
                ConnectorSession connectorSession,
                KuduClientSession clientSession,
                DynamicFilter dynamicFilter,
                ConnectorTableHandle tableHandle)
        {
            this.clientSession = requireNonNull(clientSession, "clientSession is null");
            this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilterFuture is null");
            this.tableHandle = requireNonNull(tableHandle, "splitSourceFuture is null");
            this.dynamicFilteringTimeoutNanos = (long) getDynamicFilteringWaitTimeout(connectorSession).getValue(NANOSECONDS);
            this.startNanos = System.nanoTime();
        }

        @Override
        public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
        {
            CompletableFuture<?> blocked = dynamicFilter.isBlocked();
            long remainingTimeoutNanos = getRemainingTimeoutNanos();
            if (remainingTimeoutNanos > 0 && dynamicFilter.isAwaitable()) {
                // wait for dynamic filter and yield
                return blocked
                        .thenApply(_ -> EMPTY_BATCH)
                        .completeOnTimeout(EMPTY_BATCH, remainingTimeoutNanos, NANOSECONDS);
            }

            if (delegateSplitSource == null) {
                KuduTableHandle handle = (KuduTableHandle) tableHandle;

                List<KuduSplit> splits = clientSession.buildKuduSplits(handle, dynamicFilter);
                delegateSplitSource = new FixedSplitSource(splits);
            }

            return delegateSplitSource.getNextBatch(maxSize);
        }

        @Override
        public void close()
        {
            if (delegateSplitSource != null) {
                delegateSplitSource.close();
            }
        }

        @Override
        public boolean isFinished()
        {
            if (getRemainingTimeoutNanos() > 0 && dynamicFilter.isAwaitable()) {
                return false;
            }

            if (delegateSplitSource != null) {
                return delegateSplitSource.isFinished();
            }

            return false;
        }

        private long getRemainingTimeoutNanos()
        {
            return dynamicFilteringTimeoutNanos - (System.nanoTime() - startNanos);
        }
    }
}
