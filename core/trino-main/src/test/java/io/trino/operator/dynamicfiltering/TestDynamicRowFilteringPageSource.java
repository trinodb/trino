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
package io.trino.operator.dynamicfiltering;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.Futures.getUnchecked;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.unmodifiableFuture;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.operator.dynamicfiltering.DynamicRowFilteringPageSource.EMPTY_PAGE;
import static io.trino.operator.dynamicfiltering.DynamicRowFilteringPageSource.FILTER_INPUT_POSITIONS;
import static io.trino.operator.dynamicfiltering.DynamicRowFilteringPageSource.FILTER_OUTPUT_POSITIONS;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.function.Function.identity;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDynamicRowFilteringPageSource
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final IsolatedBlockFilterFactory BLOCK_FILTER_FACTORY = new IsolatedBlockFilterFactory();

    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(ConnectorPageSource.class, DynamicRowFilteringPageSource.class);
    }

    @Test
    public void testEmptyPageSource()
    {
        DynamicRowFilteringPageSource pageSource = new DynamicRowFilteringPageSource(
                new EmptyPageSource(),
                1,
                Duration.valueOf("0s"),
                0,
                createDynamicPageFilter(
                        getDynamicFilter(TupleDomain.all()),
                        ImmutableList.of()));
        assertThat(pageSource.getNextPage()).isNull();
        assertThat(pageSource.getMetrics().getMetrics()).isEmpty();
    }

    @Test
    public void testEmptyPage()
    {
        DynamicRowFilteringPageSource pageSource = new DynamicRowFilteringPageSource(
                new TestingConnectorPageSource().addPages(ImmutableList.of(new Page(0))),
                1,
                Duration.valueOf("0s"),
                0,
                createDynamicPageFilter(
                        getDynamicFilter(TupleDomain.all()),
                        ImmutableList.of()));
        assertThat(pageSource.getNextPage().getPositionCount()).isEqualTo(0);
        assertThat(pageSource.getMetrics().getMetrics()).isEmpty();
    }

    @Test
    public void testEmptyDynamicFilter()
    {
        List<ColumnHandle> columnHandleList = ImmutableList.of(new TestingColumnHandle("column"));
        TestingConnectorPageSource testingPageSource = new TestingConnectorPageSource()
                .addPages(ImmutableList.of(new Page(createLongSequenceBlock(0, 1024))));
        DynamicRowFilteringPageSource pageSource = new DynamicRowFilteringPageSource(
                testingPageSource,
                1,
                Duration.valueOf("0s"),
                columnHandleList.size(),
                createDynamicPageFilter(DynamicFilter.EMPTY, columnHandleList));
        assertThat(pageSource.getNextPage().getPositionCount()).isEqualTo(1024);
        assertThat(pageSource.getMetrics().getMetrics()).isEmpty();
    }

    @Test
    public void testAllDynamicFilter()
    {
        List<ColumnHandle> columnHandleList = ImmutableList.of(new TestingColumnHandle("column"));
        Page inputPage = new Page(createLongSequenceBlock(0, 1024));
        TestingConnectorPageSource testingPageSource = new TestingConnectorPageSource()
                .addPages(ImmutableList.of(inputPage));
        DynamicRowFilteringPageSource pageSource = new DynamicRowFilteringPageSource(
                testingPageSource,
                1,
                Duration.valueOf("0s"),
                columnHandleList.size(),
                createDynamicPageFilter(
                        getDynamicFilter(TupleDomain.all()),
                        columnHandleList));
        Page outputPage = pageSource.getNextPage();
        assertThat(outputPage).isEqualTo(inputPage);
        assertThat(outputPage.getPositionCount()).isEqualTo(1024);
        assertThat(pageSource.getMetrics().getMetrics()).isEmpty();
    }

    @Test
    public void testNoneDynamicFilter()
    {
        List<ColumnHandle> columnHandleList = ImmutableList.of(new TestingColumnHandle("column"));
        TestingConnectorPageSource testingPageSource = new TestingConnectorPageSource()
                .addPages(ImmutableList.of(new Page(createLongSequenceBlock(0, 1024))));
        DynamicRowFilteringPageSource pageSource = new DynamicRowFilteringPageSource(
                testingPageSource,
                1,
                Duration.valueOf("0s"),
                columnHandleList.size(),
                createDynamicPageFilter(
                        getDynamicFilter(TupleDomain.none()),
                        columnHandleList));
        assertThat(pageSource.getNextPage()).isEqualTo(EMPTY_PAGE);
        assertThat(pageSource.getMetrics().getMetrics())
                .containsAllEntriesOf(ImmutableMap.of(
                        FILTER_INPUT_POSITIONS, new LongCount(1024),
                        FILTER_OUTPUT_POSITIONS, new LongCount(0)));
    }

    @Test
    public void testIneffectiveFilter()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        List<ColumnHandle> columnHandleList = ImmutableList.of(column);
        int pageCount = 3;
        TestingConnectorPageSource testingPageSource = new TestingConnectorPageSource()
                .addPages(generateInputPages(pageCount, 1, 1024));
        DynamicRowFilteringPageSource pageSource = new DynamicRowFilteringPageSource(
                testingPageSource,
                0.9,
                Duration.valueOf("0s"),
                columnHandleList.size(),
                createDynamicPageFilter(
                        getDynamicFilter(TupleDomain.withColumnDomains(ImmutableMap.of(
                                column,
                                getRangePredicate(100, 5000)))),
                        columnHandleList));
        for (int i = 0; i < pageCount - 1; i++) {
            assertThat(pageSource.getNextPage().getPositionCount()).isEqualTo(924);
        }

        // FilterProfiler should turn off row filtering
        assertThat(pageSource.getNextPage().getPositionCount()).isEqualTo(1024);
    }

    @Test
    public void testSingleIneffectiveBlockFilterFirst()
    {
        ColumnHandle columnA = new TestingColumnHandle("columnA");
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        List<ColumnHandle> columnHandleList = List.of(columnA, columnB);
        int pageCount = 3;
        TestingConnectorPageSource testingPageSource = new TestingConnectorPageSource()
                .addPages(generateInputPages(pageCount, 2, 1024));
        DynamicRowFilteringPageSource pageSource = new DynamicRowFilteringPageSource(
                testingPageSource,
                0.9,
                Duration.valueOf("0s"),
                columnHandleList.size(),
                createDynamicPageFilter(
                        getDynamicFilter(TupleDomain.withColumnDomains(ImmutableMap.of(
                                columnA, getRangePredicate(100, 1024),
                                columnB, singleValue(BIGINT, 13L)))),
                        columnHandleList));
        for (int i = 0; i < pageCount - 1; i++) {
            assertThat(pageSource.getNextPage().getPositionCount()).isEqualTo(0);
        }

        // FilterProfiler should turn off row filtering from first block
        assertThat(pageSource.getNextPage().getPositionCount()).isEqualTo(1);
    }

    @Test
    public void testSingleIneffectiveBlockFilterLast()
    {
        ColumnHandle columnA = new TestingColumnHandle("columnA");
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        List<ColumnHandle> columnHandleList = List.of(columnA, columnB);
        int pageCount = 3;
        TestingConnectorPageSource testingPageSource = new TestingConnectorPageSource()
                .addPages(generateInputPages(pageCount, 2, 1024));
        DynamicRowFilteringPageSource pageSource = new DynamicRowFilteringPageSource(
                testingPageSource,
                0.9,
                Duration.valueOf("0s"),
                columnHandleList.size(),
                createDynamicPageFilter(
                        getDynamicFilter(TupleDomain.withColumnDomains(ImmutableMap.of(
                                columnA, getRangePredicate(0, 1024),
                                columnB, getRangePredicate(100, 1024)))),
                        columnHandleList));
        for (int i = 0; i < pageCount - 1; i++) {
            assertThat(pageSource.getNextPage().getPositionCount()).isEqualTo(924);
        }

        // FilterProfiler should turn off row filtering from last block
        assertThat(pageSource.getNextPage().getPositionCount()).isEqualTo(1024);
    }

    @Test
    public void testEffectiveFilter()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        List<ColumnHandle> columnHandleList = ImmutableList.of(column);
        int pageCount = 5;
        TestingConnectorPageSource testingPageSource = new TestingConnectorPageSource()
                .addPages(generateInputPages(pageCount, 1, 1024));
        DynamicRowFilteringPageSource pageSource = new DynamicRowFilteringPageSource(
                testingPageSource,
                0.1,
                Duration.valueOf("0s"),
                columnHandleList.size(),
                createDynamicPageFilter(
                        getDynamicFilter(TupleDomain.withColumnDomains(ImmutableMap.of(
                                column,
                                singleValue(BIGINT, 13L)))),
                        columnHandleList));
        // FilterProfiler should not turn off row filtering
        for (int i = 0; i < pageCount; i++) {
            assertThat(pageSource.getNextPage().getPositionCount()).isEqualTo(1);
        }

        assertThat(pageSource.getCompletedPositions()).isEqualTo(OptionalLong.of(5 * 1024));
        assertThat(pageSource.getMetrics().getMetrics())
                .containsAllEntriesOf(ImmutableMap.of(
                        FILTER_INPUT_POSITIONS, new LongCount(5 * 1024),
                        FILTER_OUTPUT_POSITIONS, new LongCount(5)));
    }

    @Test
    public void testMultipleColumnsShortCircuit()
    {
        ColumnHandle columnA = new TestingColumnHandle("columnA");
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        ColumnHandle columnC = new TestingColumnHandle("columnC");
        List<ColumnHandle> columnHandleList = ImmutableList.of(columnA, columnB, columnC);
        TestingConnectorPageSource testingPageSource = new TestingConnectorPageSource()
                .addPages(generateInputPages(1, 3, 100));
        DynamicRowFilteringPageSource pageSource = new DynamicRowFilteringPageSource(
                testingPageSource,
                1,
                Duration.valueOf("0s"),
                columnHandleList.size(),
                createDynamicPageFilter(
                        getDynamicFilter(TupleDomain.withColumnDomains(ImmutableMap.of(
                                columnA, multipleValues(BIGINT, ImmutableList.of(-10L, 5L, 15L, 35L, 50L, 85L, 95L, 105L)),
                                columnB, singleValue(BIGINT, 0L),
                                columnC, getRangePredicate(150, 250)))),
                        columnHandleList));
        assertThat(pageSource.getNextPage()).isEqualTo(EMPTY_PAGE);
    }

    @Test
    public void testDynamicFilterOnSubsetOfColumns()
    {
        ColumnHandle columnA = new TestingColumnHandle("columnA");
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        ColumnHandle columnC = new TestingColumnHandle("columnC");
        ColumnHandle columnD = new TestingColumnHandle("columnD");
        ColumnHandle columnE = new TestingColumnHandle("columnE");
        List<ColumnHandle> columnHandleList = ImmutableList.of(columnA, columnB, columnC, columnD, columnE);
        int pageCount = 5;
        TestingConnectorPageSource testingPageSource = new TestingConnectorPageSource()
                .addPages(generateInputPages(pageCount, 5, 1024));
        DynamicRowFilteringPageSource pageSource = new DynamicRowFilteringPageSource(
                testingPageSource,
                1,
                Duration.valueOf("0s"),
                columnHandleList.size(),
                createDynamicPageFilter(
                        getDynamicFilter(TupleDomain.withColumnDomains(ImmutableMap.of(
                                columnB, multipleValues(BIGINT, ImmutableList.of(-10L, 5L, 15L, 35L, 50L, 85L, 95L, 105L)),
                                columnD, getRangePredicate(-50, 90)))),
                        columnHandleList));
        for (int i = 0; i < pageCount; i++) {
            assertThat(pageSource.getNextPage().getPositionCount()).isEqualTo(5);
        }
    }

    @Test
    @Timeout(10)
    public void testDynamicFilterBlockingTimeout()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        List<ColumnHandle> columnHandleList = ImmutableList.of(column);
        TestingConnectorPageSource testingPageSource = new TestingConnectorPageSource()
                .addPages(ImmutableList.of(new Page(createLongSequenceBlock(0, 1024))));
        DynamicRowFilteringPageSource pageSource = new DynamicRowFilteringPageSource(
                testingPageSource,
                0.1,
                Duration.valueOf("2s"),
                columnHandleList.size(),
                createDynamicPageFilter(
                        getDynamicFilter(TupleDomain.all(), Duration.valueOf("1h")),
                        columnHandleList));
        assertEventually(() -> {
            CompletableFuture<?> future = pageSource.isBlocked();
            assertThat(future).isCompleted();
            // verify that after timeout we get the future from delegate page source
            assertThat(getUnchecked(future)).isEqualTo(TestingConnectorPageSource.TESTING_FUTURE_VALUE);
        });
    }

    @Test
    public void testDynamicFilterWithDictionariesAndLazyBlocks()
    {
        ColumnHandle columnA = new TestingColumnHandle("columnA");
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        List<ColumnHandle> columnHandleList = ImmutableList.of(columnA, columnB);

        Block longsBlock = createLongsBlock(0, 1, 2, 3);
        Block dictionary = createLongsBlock(0, 1);
        int[] firstPageBlockIds = new int[] {0, 0, 1, 1};
        Page firstPage = new Page(
                longsBlock,
                // top level dictionary block is lazy
                new LazyBlock(4, () -> DictionaryBlock.create(firstPageBlockIds.length, dictionary, firstPageBlockIds)));
        Page secondPage = new Page(longsBlock, longsBlock);

        TestingConnectorPageSource testingPageSource = new TestingConnectorPageSource()
                .addPages(ImmutableList.of(secondPage, firstPage));
        DynamicRowFilteringPageSource pageSource = new DynamicRowFilteringPageSource(
                testingPageSource,
                1,
                Duration.valueOf("0s"),
                columnHandleList.size(),
                createDynamicPageFilter(
                        getDynamicFilter(TupleDomain.withColumnDomains(ImmutableMap.of(
                                columnA, multipleValues(BIGINT, ImmutableList.of(1L, 2L))))),
                        columnHandleList));

        // three pages should be produced
        Page firstOutputPage = pageSource.getNextPage();
        assertThat(firstOutputPage).isNotNull();
        Page secondOutputPage = pageSource.getNextPage();
        assertThat(secondOutputPage).isNotNull();
        assertThat(pageSource.getNextPage()).isNull();

        // all output pages should have two positions
        assertThat(firstOutputPage.getPositionCount()).isEqualTo(2);
        assertThat(secondOutputPage.getPositionCount()).isEqualTo(2);

        // make sure first and third blocks are still lazy
        assertThat(firstOutputPage.getBlock(1)).isInstanceOf(LazyBlock.class);

        // make sure that first and second output block have correct type
        Block firstOutputBlock = ((LazyBlock) firstOutputPage.getBlock(1)).getBlock();
        assertThat(firstOutputBlock).isInstanceOf(DictionaryBlock.class);
        assertThat(firstOutputBlock.isLoaded()).isTrue();

        Block secondOutputBlock = secondOutputPage.getBlock(1);
        assertThat(secondOutputBlock).isInstanceOf(LongArrayBlock.class);
        assertThat(secondOutputBlock.isLoaded()).isTrue();

        // make sure first and third page share same dictionary
        Block firstOutputDictionary = ((DictionaryBlock) firstOutputBlock).getDictionary();

        // assert that output dictionary is same as input dictionary
        assertThat(firstOutputDictionary).isSameAs(dictionary);
    }

    private static DynamicPageFilter createDynamicPageFilter(DynamicFilter dynamicFilter, List<ColumnHandle> columns)
    {
        Map<ColumnHandle, Integer> columnHandleMap = IntStream.range(0, columns.size())
                .boxed()
                .collect(toImmutableMap(columns::get, identity()));
        return new DynamicPageFilter(dynamicFilter, columnHandleMap, TYPE_OPERATORS, BLOCK_FILTER_FACTORY, directExecutor());
    }

    private static class TestingConnectorPageSource
            implements ConnectorPageSource
    {
        static final String TESTING_FUTURE_VALUE = "testing_future_value";

        private List<Page> pages = ImmutableList.of();
        long completedPositions;

        public TestingConnectorPageSource addPages(List<Page> pages)
        {
            this.pages = new ArrayList<>(pages);
            return this;
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public OptionalLong getCompletedPositions()
        {
            return OptionalLong.of(completedPositions);
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public boolean isFinished()
        {
            return false;
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            return completedFuture(TESTING_FUTURE_VALUE);
        }

        @Override
        public Page getNextPage()
        {
            if (pages.isEmpty()) {
                return null;
            }
            Page page = pages.remove(pages.size() - 1);
            completedPositions += page.getPositionCount();
            return page;
        }

        @Override
        public long getMemoryUsage()
        {
            return 0;
        }

        @Override
        public void close()
                throws IOException
        {}
    }

    private static List<Page> generateInputPages(int pages, int blocks, int positionsPerBlock)
    {
        return IntStream.range(0, pages)
                .mapToObj(i -> new Page(IntStream.range(0, blocks)
                        .mapToObj(idx -> createLongSequenceBlock(0, positionsPerBlock))
                        .toArray(Block[]::new)))
                .collect(toImmutableList());
    }

    private static Domain getRangePredicate(long start, long end)
    {
        return Domain.create(ValueSet.ofRanges(Range.range(BIGINT, start, true, end, false)), false);
    }

    private static DynamicFilter getDynamicFilter(TupleDomain<ColumnHandle> tupleDomain)
    {
        return getDynamicFilter(tupleDomain, Duration.valueOf("0s"));
    }

    private static DynamicFilter getDynamicFilter(TupleDomain<ColumnHandle> tupleDomain, Duration blockingTimeout)
    {
        CompletableFuture<?> future;
        if (blockingTimeout.toMillis() > 0) {
            future = unmodifiableFuture(CompletableFuture.runAsync(() -> {
                try {
                    TimeUnit.MILLISECONDS.sleep(blockingTimeout.toMillis());
                }
                catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }));
        }
        else {
            future = completedFuture(null);
        }
        return new DynamicFilter()
        {
            @Override
            public Set<ColumnHandle> getColumnsCovered()
            {
                return tupleDomain.getDomains().orElseThrow().keySet();
            }

            @Override
            public CompletableFuture<?> isBlocked()
            {
                return future;
            }

            @Override
            public boolean isComplete()
            {
                return future.isDone();
            }

            @Override
            public boolean isAwaitable()
            {
                return !future.isDone();
            }

            @Override
            public TupleDomain<ColumnHandle> getCurrentPredicate()
            {
                return tupleDomain;
            }

            @Override
            public OptionalLong getPreferredDynamicFilterTimeout()
            {
                return OptionalLong.of(0L);
            }
        };
    }
}
