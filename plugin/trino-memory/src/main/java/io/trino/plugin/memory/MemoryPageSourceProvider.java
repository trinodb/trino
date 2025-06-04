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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeUtils;

import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public final class MemoryPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final MemoryPagesStore pagesStore;
    private final boolean enableLazyDynamicFiltering;

    @Inject
    public MemoryPageSourceProvider(MemoryPagesStore pagesStore, MemoryConfig config)
    {
        this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
        this.enableLazyDynamicFiltering = config.isEnableLazyDynamicFiltering();
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        MemorySplit memorySplit = (MemorySplit) split;
        long tableId = memorySplit.table();
        int partNumber = memorySplit.partNumber();
        int totalParts = memorySplit.totalPartsPerWorker();
        long expectedRows = memorySplit.expectedRows();
        MemoryTableHandle memoryTable = (MemoryTableHandle) table;
        OptionalDouble sampleRatio = memoryTable.sampleRatio();

        int[] columnIndexes = new int[columns.size()];
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        for (int i = 0; i < columns.size(); i++) {
            MemoryColumnHandle column = (MemoryColumnHandle) columns.get(i);
            columnIndexes[i] = column.columnIndex();
            columnTypes.add(column.type());
        }

        List<Page> pages = pagesStore.getPages(
                tableId,
                partNumber,
                totalParts,
                columnIndexes,
                columnTypes.build(),
                expectedRows,
                memorySplit.limit(),
                sampleRatio);

        return new DynamicFilteringPageSource(new FixedPageSource(pages), columns, dynamicFilter, enableLazyDynamicFiltering);
    }

    private static class DynamicFilteringPageSource
            implements ConnectorPageSource
    {
        private final FixedPageSource delegate;
        private final List<ColumnHandle> columns;
        private final DynamicFilter dynamicFilter;
        private final boolean enableLazyDynamicFiltering;
        private long rows;
        private long completedPositions;
        private boolean closed;

        private DynamicFilteringPageSource(FixedPageSource delegate, List<ColumnHandle> columns, DynamicFilter dynamicFilter, boolean enableLazyDynamicFiltering)
        {
            this.delegate = delegate;
            this.columns = columns;
            this.dynamicFilter = dynamicFilter;
            this.enableLazyDynamicFiltering = enableLazyDynamicFiltering;
        }

        @Override
        public long getCompletedBytes()
        {
            return delegate.getCompletedBytes();
        }

        @Override
        public OptionalLong getCompletedPositions()
        {
            return OptionalLong.of(completedPositions);
        }

        @Override
        public long getReadTimeNanos()
        {
            return delegate.getReadTimeNanos();
        }

        @Override
        public boolean isFinished()
        {
            return delegate.isFinished();
        }

        @Override
        public SourcePage getNextSourcePage()
        {
            if (enableLazyDynamicFiltering && dynamicFilter.isAwaitable()) {
                return null;
            }
            TupleDomain<ColumnHandle> predicate = dynamicFilter.getCurrentPredicate();
            if (predicate.isNone()) {
                close();
                return null;
            }
            SourcePage page = delegate.getNextSourcePage();
            if (page == null) {
                return null;
            }
            completedPositions += page.getPositionCount();

            if (!predicate.isAll()) {
                applyFilter(page, predicate.transformKeys(columns::indexOf).getDomains().get());
            }
            rows += page.getPositionCount();
            return page;
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            if (enableLazyDynamicFiltering) {
                return dynamicFilter.isBlocked();
            }
            return NOT_BLOCKED;
        }

        @Override
        public long getMemoryUsage()
        {
            return delegate.getMemoryUsage();
        }

        @Override
        public void close()
        {
            delegate.close();
            closed = true;
        }

        @Override
        public Metrics getMetrics()
        {
            return new Metrics(ImmutableMap.of(
                    "rows", new LongCount(rows),
                    "finished", new LongCount(closed ? 1 : 0),
                    "started", new LongCount(1)));
        }
    }

    private static void applyFilter(SourcePage page, Map<Integer, Domain> domains)
    {
        int[] positions = new int[page.getPositionCount()];
        int length = 0;
        for (int position = 0; position < page.getPositionCount(); ++position) {
            if (positionMatchesPredicate(page, position, domains)) {
                positions[length++] = position;
            }
        }
        page.selectPositions(positions, 0, length);
    }

    private static boolean positionMatchesPredicate(SourcePage page, int position, Map<Integer, Domain> domains)
    {
        for (Map.Entry<Integer, Domain> entry : domains.entrySet()) {
            int channel = entry.getKey();
            Domain domain = entry.getValue();
            Object value = TypeUtils.readNativeValue(domain.getType(), page.getBlock(channel), position);
            if (!domain.includesNullableValue(value)) {
                return false;
            }
        }

        return true;
    }
}
