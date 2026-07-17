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

import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.connector.FixedSplitSource;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.trino.plugin.mongodb.MongoSessionProperties.getDynamicFilteringWaitTimeout;
import static java.util.Objects.requireNonNull;

public class MongoSplitManager
        implements ConnectorSplitManager
{
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
            Set<ColumnHandle> dynamicFilterColumns,
            Constraint constraint)
    {
        long dynamicFilteringWaitTimeoutMillis = getDynamicFilteringWaitTimeout(session).toMillis();
        MongoSplit split = new MongoSplit(serverDetailsProvider.getServerAddress());
        return new MongoSplitSource(dynamicFilteringWaitTimeoutMillis, new FixedSplitSource(split));
    }

    private static class MongoSplitSource
            implements ConnectorSplitSource
    {
        private final long dynamicFilteringWaitTimeoutMillis;
        private final ConnectorSplitSource delegateSplitSource;

        public MongoSplitSource(long dynamicFilteringWaitTimeoutMillis, ConnectorSplitSource delegateSplitSource)
        {
            this.dynamicFilteringWaitTimeoutMillis = dynamicFilteringWaitTimeoutMillis;
            this.delegateSplitSource = requireNonNull(delegateSplitSource, "delegateSplitSource is null");
        }

        @Override
        public long getRequestedDynamicFilterWaitTimeoutMillis()
        {
            return dynamicFilteringWaitTimeoutMillis;
        }

        @Override
        public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot)
        {
            return delegateSplitSource.getNextBatch(maxSize, dynamicFilterSnapshot);
        }

        @Override
        public void close()
        {
            delegateSplitSource.close();
        }

        @Override
        public boolean isFinished()
        {
            return delegateSplitSource.isFinished();
        }
    }
}
