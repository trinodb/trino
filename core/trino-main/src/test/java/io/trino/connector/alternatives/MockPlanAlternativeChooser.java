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
package io.trino.connector.alternatives;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorAlternativeChooser;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiPredicate;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class MockPlanAlternativeChooser
        implements ConnectorAlternativeChooser
{
    private static final Logger log = Logger.get(MockPlanAlternativeChooser.class);
    private final ConnectorPageSourceProvider delegate;

    public MockPlanAlternativeChooser(ConnectorPageSourceProvider delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Choice chooseAlternative(ConnectorSession session, ConnectorSplit split, List<ConnectorTableHandle> alternatives)
    {
        MockPlanAlternativeSplit planAlternativeSplit = (MockPlanAlternativeSplit) split;

        // 1 is added to the split number in order to choose the non-trivial alternative when there is only one split
        int alternative = (planAlternativeSplit.getSplitNumber() + 1) % alternatives.size();
        ConnectorTableHandle table = alternatives.get(alternative);
        checkArgument(alternative == 0 || table instanceof MockPlanAlternativeTableHandle, "Not the trivial alternative, expected a MockPlanAlternativeTableHandle");
        return new Choice(alternative, (transaction, session1, columns, dynamicFilter, splitAddressEnforced) ->
                createPageSource(transaction, session1, planAlternativeSplit, table, columns, dynamicFilter));
    }

    @Override
    public boolean shouldPerformDynamicRowFiltering()
    {
        return true;
    }

    private ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            MockPlanAlternativeSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        if (table instanceof MockPlanAlternativeTableHandle handle) {
            log.debug("filtering table %s, split %s by mock plan alternative connector. df: %s", table, split, dynamicFilter.getCurrentPredicate());
            int filterColumnIndex = columns.indexOf(handle.filterColumn());
            boolean returnFilterColumn = filterColumnIndex >= 0;
            if (!returnFilterColumn) {
                filterColumnIndex = columns.size();
                columns = ImmutableList.<ColumnHandle>builder()
                        .addAll(columns)
                        .add(handle.filterColumn())
                        .build();
            }
            ConnectorPageSource pageSource = delegate.createPageSource(transaction, session, split.getDelegate(), handle.delegate(), columns, dynamicFilter);
            return new PlanAlternativePageSource(pageSource, handle.filterDefinition().asPredicate(session), filterColumnIndex, returnFilterColumn);
        }
        log.debug("NOT filtering table %s, split %s by mock plan alternative connector. df: %s", table, split, dynamicFilter.getCurrentPredicate());
        return delegate.createPageSource(transaction, session, split.getDelegate(), table, columns, dynamicFilter);
    }

    public static class PlanAlternativePageSource
            implements ConnectorPageSource
    {
        public static final String FILTERED_OUT_POSITIONS = "filteredOutPositions";

        private final ConnectorPageSource delegate;
        private final int filterColumnIndex;
        private final BiPredicate<Block, Integer> filter;
        private long filteredOutPositions;
        private boolean returnFilterColumn;

        public PlanAlternativePageSource(ConnectorPageSource delegate, BiPredicate<Block, Integer> filter, int filterColumnIndex, boolean returnFilterColumn)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.filter = requireNonNull(filter, "filter is null");
            checkArgument(filterColumnIndex >= 0);
            this.filterColumnIndex = filterColumnIndex;
            this.returnFilterColumn = returnFilterColumn;
        }

        @Override
        public Page getNextPage()
        {
            Page page = delegate.getNextPage();
            if (page == null) {
                return null;
            }

            if (page.getPositionCount() == 0) {
                return page;
            }

            Block filterBlock = page.getBlock(filterColumnIndex);
            IntArrayList filteredPositions = new IntArrayList(page.getPositionCount());
            for (int i = 0; i < page.getPositionCount(); i++) {
                if (filter.test(filterBlock, i)) {
                    filteredPositions.add(i);
                }
                else {
                    filteredOutPositions++;
                }
            }

            Page newPage = page.copyPositions(filteredPositions.elements(), 0, filteredPositions.size());
            if (returnFilterColumn) {
                return newPage;
            }
            int[] columns = IntStream.range(0, newPage.getChannelCount())
                    .filter(i -> i != filterColumnIndex)
                    .toArray();
            return newPage.getColumns(columns);
        }

        // just delegation below
        @Override
        public long getCompletedBytes()
        {
            return delegate.getCompletedBytes();
        }

        @Override
        public OptionalLong getCompletedPositions()
        {
            return delegate.getCompletedPositions();
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
        public long getMemoryUsage()
        {
            return delegate.getMemoryUsage();
        }

        @Override
        public void close()
                throws IOException
        {
            delegate.close();
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            return delegate.isBlocked();
        }

        @Override
        public Metrics getMetrics()
        {
            Map<String, Metric<?>> metrics = new HashMap<>(delegate.getMetrics().getMetrics());
            metrics.put(FILTERED_OUT_POSITIONS, new LongCount(filteredOutPositions));

            return new Metrics(metrics);
        }
    }
}
