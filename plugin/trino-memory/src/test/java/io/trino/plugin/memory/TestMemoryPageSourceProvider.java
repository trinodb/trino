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
import io.trino.spi.HostAddress;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

final class TestMemoryPageSourceProvider
{
    @Test
    void testReportsMemoryUsageToMemoryContext()
            throws IOException
    {
        MemoryPagesStore pagesStore = new MemoryPagesStore(new MemoryConfig());
        pagesStore.initialize(0);
        Page page = new Page(createLongSequenceBlock(0, 100));
        pagesStore.add(0L, page);
        MemoryPageSourceProvider pageSourceProvider = new MemoryPageSourceProvider(pagesStore, new MemoryConfig());

        AtomicLong reportedBytes = new AtomicLong(-1);
        try (ConnectorPageSource _ = pageSourceProvider.createPageSource(
                MemoryTransactionHandle.INSTANCE,
                SESSION,
                new MemorySplit(0, 0, 1, HostAddress.fromString("localhost:8080"), 100, OptionalLong.empty()),
                new MemoryTableHandle(0, new SchemaTableName("default", "test"), OptionalLong.empty(), OptionalDouble.empty()),
                Optional.empty(),
                ImmutableList.of(new MemoryColumnHandle(0, "value", BIGINT)),
                DynamicFilter.EMPTY,
                reportedBytes::set)) {
            assertThat(reportedBytes).hasValue(page.getRetainedSizeInBytes());
        }
    }

    @Test
    void testAppliesNarrowingDynamicFilter()
            throws IOException
    {
        MemoryPagesStore pagesStore = new MemoryPagesStore(new MemoryConfig());
        pagesStore.initialize(0);
        pagesStore.add(0L, new Page(createLongSequenceBlock(0, 100)));
        pagesStore.add(0L, new Page(createLongSequenceBlock(100, 200)));
        pagesStore.add(0L, new Page(createLongSequenceBlock(200, 300)));
        pagesStore.add(0L, new Page(createLongSequenceBlock(300, 400)));
        MemoryPageSourceProvider pageSourceProvider = new MemoryPageSourceProvider(pagesStore, new MemoryConfig());

        MemoryColumnHandle column = new MemoryColumnHandle(0, "value", BIGINT);
        MutableDynamicFilter dynamicFilter = new MutableDynamicFilter(column, TupleDomain.all());
        try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                MemoryTransactionHandle.INSTANCE,
                SESSION,
                new MemorySplit(0, 0, 1, HostAddress.fromString("localhost:8080"), 400, OptionalLong.empty()),
                new MemoryTableHandle(0, new SchemaTableName("default", "test"), OptionalLong.empty(), OptionalDouble.empty()),
                Optional.empty(),
                ImmutableList.of(column),
                dynamicFilter,
                _ -> {})) {
            // no filtering
            assertThat(values(pageSource.getNextSourcePage())).hasSize(100).startsWith(0L).endsWith(99L);

            // the same predicate is used for the whole page
            dynamicFilter.predicate = TupleDomain.withColumnDomains(ImmutableMap.of(column, Domain.multipleValues(BIGINT, List.of(5L, 120L, 150L, 250L))));
            assertThat(values(pageSource.getNextSourcePage())).containsExactly(120L, 150L);

            // the same predicate instance is used for the next page
            assertThat(values(pageSource.getNextSourcePage())).containsExactly(250L);

            // narrowed predicate
            dynamicFilter.predicate = TupleDomain.withColumnDomains(ImmutableMap.of(column, Domain.singleValue(BIGINT, 5L)));
            assertThat(values(pageSource.getNextSourcePage())).isEmpty();
            assertThat(pageSource.isFinished()).isTrue();
        }
    }

    private static List<Long> values(SourcePage page)
    {
        ImmutableList.Builder<Long> values = ImmutableList.builder();
        for (int position = 0; position < page.getPositionCount(); position++) {
            values.add(BIGINT.getLong(page.getBlock(0), position));
        }
        return values.build();
    }

    private static class MutableDynamicFilter
            implements DynamicFilter
    {
        private final ColumnHandle column;
        private volatile TupleDomain<ColumnHandle> predicate;

        private MutableDynamicFilter(ColumnHandle column, TupleDomain<ColumnHandle> predicate)
        {
            this.column = column;
            this.predicate = predicate;
        }

        @Override
        public Set<ColumnHandle> getColumnsCovered()
        {
            return Set.of(column);
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            return NOT_BLOCKED;
        }

        @Override
        public boolean isComplete()
        {
            return false;
        }

        @Override
        public boolean isAwaitable()
        {
            return false;
        }

        @Override
        public TupleDomain<ColumnHandle> getCurrentPredicate()
        {
            return predicate;
        }
    }
}
