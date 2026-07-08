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
package io.trino.plugin.elasticsearch;

import com.google.inject.Inject;
import io.trino.plugin.elasticsearch.client.ElasticsearchClient;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.elasticsearch.ElasticsearchSessionProperties.getDynamicFilteringWaitTimeout;
import static io.trino.plugin.elasticsearch.ElasticsearchTableHandle.Type.QUERY;
import static java.util.Objects.requireNonNull;

public class ElasticsearchSplitManager
        implements ConnectorSplitManager
{
    private final ElasticsearchClient client;

    @Inject
    public ElasticsearchSplitManager(ElasticsearchClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            Set<ColumnHandle> dynamicFilterColumns,
            Constraint constraint)
    {
        ElasticsearchTableHandle tableHandle = (ElasticsearchTableHandle) table;

        ConnectorSplitSource splitSource;
        if (tableHandle.type().equals(QUERY) || tableHandle.aggregation().isPresent()) {
            // A pushed-down aggregation runs across the whole index in a single request, so a single split suffices
            splitSource = new FixedSplitSource(new ElasticsearchSplit(tableHandle.index(), 0, Optional.empty()));
        }
        else {
            List<ElasticsearchSplit> splits = client.getSearchShards(tableHandle.index()).stream()
                    .map(shard -> new ElasticsearchSplit(shard.index(), shard.id(), shard.address()))
                    .collect(toImmutableList());
            splitSource = new FixedSplitSource(splits);
        }

        return new DynamicFilteringSplitSource(getDynamicFilteringWaitTimeout(session).toMillis(), splitSource);
    }

    private static final class DynamicFilteringSplitSource
            implements ConnectorSplitSource
    {
        private final long dynamicFilteringWaitTimeoutMillis;
        private final ConnectorSplitSource delegateSplitSource;

        private DynamicFilteringSplitSource(long dynamicFilteringWaitTimeoutMillis, ConnectorSplitSource delegateSplitSource)
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
