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
package io.trino.plugin.memory;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.connector.DynamicFilter.NOT_BLOCKED;
import static java.util.Objects.requireNonNull;

public final class MemorySplitManager
        implements ConnectorSplitManager
{
    private final int splitsPerNode;
    private final MemoryMetadata metadata;
    private final boolean enableLazyDynamicFiltering;

    @Inject
    public MemorySplitManager(MemoryConfig config, MemoryMetadata metadata)
    {
        this.splitsPerNode = config.getSplitsPerNode();
        this.metadata = metadata;
        this.enableLazyDynamicFiltering = config.isEnableLazyDynamicFiltering();
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableHandle handle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        MemoryTableHandle table = (MemoryTableHandle) handle;

        List<MemoryDataFragment> dataFragments = metadata.getDataFragments(table.getId());

        int totalRows = 0;

        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

        for (MemoryDataFragment dataFragment : dataFragments) {
            long rows = dataFragment.getRows();
            totalRows += rows;

            if (table.getLimit().isPresent() && totalRows > table.getLimit().getAsLong()) {
                rows -= totalRows - table.getLimit().getAsLong();
                splits.add(new MemorySplit(table.getId(), 0, 1, dataFragment.getHostAddress(), rows, OptionalLong.of(rows)));
                break;
            }

            for (int i = 0; i < splitsPerNode; i++) {
                splits.add(new MemorySplit(table.getId(), i, splitsPerNode, dataFragment.getHostAddress(), rows, OptionalLong.empty()));
            }
        }

        ConnectorSplitSource splitSource = new FixedSplitSource(splits.build());
        if (enableLazyDynamicFiltering) {
            // Needed to avoid scheduling a stage that is waiting for dynamic filters to become available.
            // It makes no difference for pipelined execution where the stages are scheduled eagerly and there's no limit on the number of tasks running in parallel.
            // However in fault tolerant execution if the stage waiting for dynamic filters is scheduled first it may occupy all available slots leaving no resources
            // for the stage that collects dynamic filters to be scheduled effectively creating a deadlock.
            splitSource = new DelayedSplitSource(whenCompleted(dynamicFilter), splitSource);
        }
        return splitSource;
    }

    private static CompletableFuture<?> whenCompleted(DynamicFilter dynamicFilter)
    {
        if (dynamicFilter.isAwaitable()) {
            return dynamicFilter.isBlocked().thenCompose(ignored -> whenCompleted(dynamicFilter));
        }
        return NOT_BLOCKED;
    }

    private static class DelayedSplitSource
            implements ConnectorSplitSource
    {
        private final CompletableFuture<?> delay;
        private final ConnectorSplitSource delegate;

        public DelayedSplitSource(CompletableFuture<?> delay, ConnectorSplitSource delegate)
        {
            this.delay = requireNonNull(delay, "delay is null");
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
        {
            return delay.thenCompose(ignored -> delegate.getNextBatch(maxSize));
        }

        @Override
        public void close()
        {
            delegate.close();
        }

        @Override
        public boolean isFinished()
        {
            if (delay.isDone()) {
                return delegate.isFinished();
            }
            return false;
        }
    }
}
