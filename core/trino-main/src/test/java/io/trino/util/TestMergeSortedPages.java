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
package io.trino.util;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.PageWithPositionComparator;
import io.trino.operator.SimplePageWithPositionComparator;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessor.ProcessState;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.operator.OperatorAssertion.toMaterializedResult;
import static io.trino.operator.WorkProcessorAssertion.processorFrom;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_FIRST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMergeSortedPages
{
    private static final TypeOperators TYPE_OPERATORS_CACHE = new TypeOperators();
    private static final OrderingCompiler ORDERING_COMPILER = new OrderingCompiler(TYPE_OPERATORS_CACHE);

    @Test
    public void testSingleStream()
    {
        List<Type> types = ImmutableList.of(INTEGER, INTEGER);
        MaterializedResult actual = mergeSortedPages(
                types,
                ImmutableList.of(0, 1),
                ImmutableList.of(ASC_NULLS_FIRST, DESC_NULLS_FIRST),
                ImmutableList.of(
                        rowPagesBuilder(types)
                                .row(1, 4)
                                .row(2, 3)
                                .pageBreak()
                                .row(3, 2)
                                .row(4, 1)
                                .build()));
        MaterializedResult expected = resultBuilder(types)
                .row(1, 4)
                .row(2, 3)
                .row(3, 2)
                .row(4, 1)
                .build();
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    @Test
    public void testSimpleTwoStreams()
    {
        List<Type> types = ImmutableList.of(INTEGER);
        MaterializedResult actual = mergeSortedPages(
                types,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_FIRST),
                ImmutableList.of(
                        rowPagesBuilder(types)
                                .row(1)
                                .row(3)
                                .pageBreak()
                                .row(5)
                                .row(7)
                                .build(),
                        rowPagesBuilder(types)
                                .row(2)
                                .row(4)
                                .pageBreak()
                                .row(6)
                                .row(8)
                                .build()));
        MaterializedResult expected = resultBuilder(types)
                .row(1)
                .row(2)
                .row(3)
                .row(4)
                .row(5)
                .row(6)
                .row(7)
                .row(8)
                .build();
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    @Test
    public void testMultipleStreams()
    {
        List<Type> types = ImmutableList.of(INTEGER, INTEGER, INTEGER);
        MaterializedResult actual = mergeSortedPages(
                types,
                ImmutableList.of(0, 1),
                ImmutableList.of(ASC_NULLS_FIRST, DESC_NULLS_FIRST),
                ImmutableList.of(
                        rowPagesBuilder(types)
                                .row(1, 1, 2)
                                .pageBreak()
                                .pageBreak()
                                .row(8, 1, 1)
                                .row(19, 1, 3)
                                .row(27, 1, 4)
                                .row(41, 2, 5)
                                .pageBreak()
                                .row(55, 1, 2)
                                .row(89, 1, 3)
                                .row(100, 2, 6)
                                .row(100, 2, 8)
                                .row(101, 1, 4)
                                .row(202, 1, 3)
                                .row(399, 2, 2)
                                .pageBreak()
                                .row(400, 1, 1)
                                .row(401, 1, 7)
                                .pageBreak()
                                .row(402, 1, 6)
                                .build(),
                        rowPagesBuilder(types)
                                .pageBreak()
                                .row(2, 1, 2)
                                .row(8, 1, 1)
                                .row(19, 1, 3)
                                .row(25, 1, 4)
                                .row(26, 2, 5)
                                .pageBreak()
                                .row(56, 1, 2)
                                .row(66, 1, 3)
                                .row(77, 1, 4)
                                .row(88, 1, 3)
                                .row(99, 1, 1)
                                .pageBreak()
                                .row(99, 2, 2)
                                .row(100, 1, 7)
                                .build(),
                        rowPagesBuilder(types)
                                .row(8, 1, 1)
                                .row(88, 1, 3)
                                .pageBreak()
                                .row(89, 1, 3)
                                .pageBreak()
                                .row(90, 1, 3)
                                .pageBreak()
                                .row(91, 1, 4)
                                .row(92, 2, 5)
                                .pageBreak()
                                .row(93, 1, 2)
                                .row(94, 1, 3)
                                .row(95, 1, 4)
                                .row(97, 1, 3)
                                .row(98, 2, 2)
                                .row(100, 1, 7)
                                .build()));
        MaterializedResult expected = resultBuilder(types)
                .row(1, 1, 2)
                .row(2, 1, 2)
                .row(8, 1, 1)
                .row(8, 1, 1)
                .row(8, 1, 1)
                .row(19, 1, 3)
                .row(19, 1, 3)
                .row(25, 1, 4)
                .row(26, 2, 5)
                .row(27, 1, 4)
                .row(41, 2, 5)
                .row(55, 1, 2)
                .row(56, 1, 2)
                .row(66, 1, 3)
                .row(77, 1, 4)
                .row(88, 1, 3)
                .row(88, 1, 3)
                .row(89, 1, 3)
                .row(89, 1, 3)
                .row(90, 1, 3)
                .row(91, 1, 4)
                .row(92, 2, 5)
                .row(93, 1, 2)
                .row(94, 1, 3)
                .row(95, 1, 4)
                .row(97, 1, 3)
                .row(98, 2, 2)
                .row(99, 1, 1)
                .row(99, 2, 2)
                .row(100, 2, 6)
                .row(100, 2, 8)
                .row(100, 1, 7)
                .row(100, 1, 7)
                .row(101, 1, 4)
                .row(202, 1, 3)
                .row(399, 2, 2)
                .row(400, 1, 1)
                .row(401, 1, 7)
                .row(402, 1, 6)
                .build();
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    @Test
    public void testEmptyStreams()
    {
        List<Type> types = ImmutableList.of(INTEGER, BIGINT, DOUBLE);
        MaterializedResult actual = mergeSortedPages(
                types,
                ImmutableList.of(0, 1),
                ImmutableList.of(ASC_NULLS_FIRST, ASC_NULLS_FIRST),
                ImmutableList.of(
                        rowPagesBuilder(types)
                                .pageBreak()
                                .pageBreak()
                                .build(),
                        rowPagesBuilder(types)
                                .pageBreak()
                                .build(),
                        rowPagesBuilder(types)
                                .pageBreak()
                                .build(),
                        rowPagesBuilder(types)
                                .build()));
        MaterializedResult expected = resultBuilder(types)
                .build();
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    @Test
    public void testDifferentTypes()
    {
        List<Type> types = ImmutableList.of(DOUBLE, VARCHAR, INTEGER);
        MaterializedResult actual = mergeSortedPages(
                types,
                ImmutableList.of(2, 0, 1),
                ImmutableList.of(DESC_NULLS_LAST, DESC_NULLS_FIRST, ASC_NULLS_FIRST),
                ImmutableList.of(
                        rowPagesBuilder(types)
                                .row(16.0, "a1", 16)
                                .row(8.0, "b1", 16)
                                .pageBreak()
                                .row(4.0, "c1", 16)
                                .row(4.0, "d1", 16)
                                .row(null, "d1", 8)
                                .row(16.0, "a1", 8)
                                .row(16.0, "b1", 8)
                                .row(16.0, "c1", 4)
                                .row(8.0, "d1", 4)
                                .row(16.0, "a1", 2)
                                .row(null, "a1", null)
                                .row(16.0, "a1", null)
                                .build(),
                        rowPagesBuilder(types)
                                .row(15.0, "a2", 17)
                                .row(9.0, "b2", 17)
                                .pageBreak()
                                .row(5.0, "c2", 17)
                                .row(5.0, "d2", 17)
                                .row(null, "d2", 8)
                                .row(17.0, "a0", 8)
                                .row(17.0, "b0", 8)
                                .row(17.0, "c0", 5)
                                .row(9.0, "d0", 5)
                                .row(17.0, "a0", 3)
                                .row(null, "a0", null)
                                .row(17.0, "a0", null)
                                .build()));
        MaterializedResult expected = resultBuilder(types)
                .row(15.0, "a2", 17)
                .row(9.0, "b2", 17)
                .row(5.0, "c2", 17)
                .row(5.0, "d2", 17)
                .row(16.0, "a1", 16)
                .row(8.0, "b1", 16)
                .row(4.0, "c1", 16)
                .row(4.0, "d1", 16)
                .row(null, "d1", 8)
                .row(null, "d2", 8)
                .row(17.0, "a0", 8)
                .row(17.0, "b0", 8)
                .row(16.0, "a1", 8)
                .row(16.0, "b1", 8)
                .row(17.0, "c0", 5)
                .row(9.0, "d0", 5)
                .row(16.0, "c1", 4)
                .row(8.0, "d1", 4)
                .row(17.0, "a0", 3)
                .row(16.0, "a1", 2)
                .row(null, "a0", null)
                .row(null, "a1", null)
                .row(17.0, "a0", null)
                .row(16.0, "a1", null)
                .build();
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    @Test
    public void testSortingYields()
    {
        DriverYieldSignal yieldSignal = new DriverYieldSignal();
        yieldSignal.forceYieldForTesting();

        List<Type> types = ImmutableList.of(INTEGER);
        WorkProcessor<Page> mergedPages = MergeSortedPages.mergeSortedPages(
                ImmutableList.of(WorkProcessor.fromIterable(rowPagesBuilder(types)
                        .row(1)
                        .build())),
                new SimplePageWithPositionComparator(ImmutableList.of(types.get(0)), ImmutableList.of(0), ImmutableList.of(DESC_NULLS_LAST), TYPE_OPERATORS_CACHE),
                ImmutableList.of(0),
                types,
                newSimpleAggregatedMemoryContext().newAggregatedMemoryContext(),
                yieldSignal);

        // yield signal is on
        assertThat(mergedPages.process()).isFalse();
        yieldSignal.resetYieldForTesting();

        // page is produced
        assertThat(mergedPages.process()).isTrue();
        assertThat(mergedPages.isFinished()).isFalse();

        Page page = mergedPages.getResult();
        assertThat(toMaterializedResult(types, ImmutableList.of(page)))
                .containsExactlyElementsOf(resultBuilder(types)
                        .row(1)
                        .build());

        // merge source finished
        assertThat(mergedPages.process()).isTrue();
        assertThat(mergedPages.isFinished()).isTrue();
    }

    @Test
    public void testMergeSortYieldingProgresses()
    {
        DriverYieldSignal yieldSignal = new DriverYieldSignal();
        yieldSignal.forceYieldForTesting();
        Type type = INTEGER;
        WorkProcessor<Page> mergedPages = MergeSortedPages.mergeSortedPages(
                ImmutableList.of(WorkProcessor.fromIterable(rowPagesBuilder(type).build())),
                new SimplePageWithPositionComparator(ImmutableList.of(type), ImmutableList.of(0), ImmutableList.of(DESC_NULLS_LAST), TYPE_OPERATORS_CACHE),
                ImmutableList.of(0),
                ImmutableList.of(type),
                newSimpleAggregatedMemoryContext().newAggregatedMemoryContext(),
                yieldSignal);
        // yield signal is on
        assertThat(mergedPages.process()).isFalse();
        // processor finishes computations (yield signal is still on, but previous process() call yielded)
        assertThat(mergedPages.process()).isTrue();
        assertThat(mergedPages.isFinished()).isTrue();
    }

    @Test
    public void testRandomStreams()
    {
        Random random = new Random(42);
        for (int sourceCount : ImmutableList.of(1, 2, 3, 7, 16, 33)) {
            for (SortOrder sortOrder : SortOrder.values()) {
                // few distinct keys, so that many rows are equal and rows of one source are selected one after another
                assertMergesRandomStreams(random, sourceCount, sortOrder, BIGINT, () -> (long) random.nextInt(5));
                assertMergesRandomStreams(random, sourceCount, sortOrder, BIGINT, random::nextLong);
                // keys that share more bytes than a sort key prefix holds
                assertMergesRandomStreams(random, sourceCount, sortOrder, VARCHAR, () -> "ab".repeat(random.nextInt(7)) + (char) ('a' + random.nextInt(3)));
            }
        }
    }

    private static <T extends Comparable<T>> void assertMergesRandomStreams(Random random, int sourceCount, SortOrder sortOrder, Type keyType, Supplier<T> keys)
    {
        Comparator<T> valueOrder = Comparator.naturalOrder();
        if (!sortOrder.isAscending()) {
            valueOrder = valueOrder.reversed();
        }
        Comparator<T> keyOrder = Comparator.nullsLast(valueOrder);
        if (sortOrder.isNullsFirst()) {
            keyOrder = Comparator.nullsFirst(valueOrder);
        }
        Comparator<KeyAndId<T>> rowOrder = Comparator.comparing(KeyAndId::key, keyOrder);

        List<Type> types = ImmutableList.of(keyType, BIGINT);
        ImmutableList.Builder<List<Page>> sources = ImmutableList.builder();
        List<KeyAndId<T>> expected = new ArrayList<>();
        for (int source = 0; source < sourceCount; source++) {
            List<KeyAndId<T>> rows = new ArrayList<>();
            int rowCount = random.nextInt(200);
            for (int row = 0; row < rowCount; row++) {
                T key = null;
                if (random.nextInt(10) != 0) {
                    key = keys.get();
                }
                rows.add(new KeyAndId<>(key, source * 1000L + row));
            }
            rows.sort(rowOrder);
            expected.addAll(rows);
            sources.add(toPages(random, types, rows));
        }
        // sorting is stable, so equal rows stay in the order of their sources
        expected.sort(rowOrder);

        List<PageWithPositionComparator> comparators = ImmutableList.of(
                new SimplePageWithPositionComparator(ImmutableList.of(keyType), ImmutableList.of(0), ImmutableList.of(sortOrder), TYPE_OPERATORS_CACHE),
                ORDERING_COMPILER.compilePageWithPositionComparator(ImmutableList.of(keyType), ImmutableList.of(0), ImmutableList.of(sortOrder)));
        for (PageWithPositionComparator comparator : comparators) {
            AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newAggregatedMemoryContext();
            WorkProcessor<Page> mergedPages = MergeSortedPages.mergeSortedPages(
                    sources.build().stream()
                            .map(WorkProcessor::fromIterable)
                            .collect(toImmutableList()),
                    comparator,
                    types,
                    memoryContext,
                    new DriverYieldSignal());

            List<KeyAndId<Object>> actual = new ArrayList<>();
            for (Page page : drain(mergedPages)) {
                for (int position = 0; position < page.getPositionCount(); position++) {
                    actual.add(new KeyAndId<>(keyType.getObjectValue(page.getBlock(0), position), BIGINT.getLong(page.getBlock(1), position)));
                }
            }
            assertThat(actual)
                    .as("%s sources of %s sorted %s", sourceCount, keyType, sortOrder)
                    .isEqualTo(expected);
            assertThat(memoryContext.getBytes()).isEqualTo(0L);
        }
    }

    private static <T> List<Page> toPages(Random random, List<Type> types, List<KeyAndId<T>> rows)
    {
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        PageBuilder pageBuilder = new PageBuilder(types);
        int pageSize = 1 + random.nextInt(50);
        for (KeyAndId<T> row : rows) {
            pageBuilder.declarePosition();
            writeKey(types.get(0), pageBuilder.getBlockBuilder(0), row.key());
            BIGINT.writeLong(pageBuilder.getBlockBuilder(1), row.id());
            if (pageBuilder.getPositionCount() == pageSize) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
                if (random.nextInt(4) == 0) {
                    // an empty page between pages with rows
                    pages.add(pageBuilder.build());
                }
            }
        }
        pages.add(pageBuilder.build());
        return pages.build();
    }

    private static void writeKey(Type type, BlockBuilder blockBuilder, Object key)
    {
        switch (key) {
            case null -> blockBuilder.appendNull();
            case Long value -> type.writeLong(blockBuilder, value);
            case String value -> type.writeSlice(blockBuilder, utf8Slice(value));
            default -> throw new IllegalArgumentException("Unsupported key: " + key);
        }
    }

    private record KeyAndId<T>(T key, long id) {}

    @Test
    public void testComparesOncePerTreeLevel()
    {
        int sourceCount = 13;
        int rowsPerSource = 100;
        Random random = new Random(42);
        ImmutableList.Builder<WorkProcessor<Page>> sources = ImmutableList.builder();
        for (int source = 0; source < sourceCount; source++) {
            BlockBuilder values = BIGINT.createBlockBuilder(null, rowsPerSource);
            random.longs(rowsPerSource).sorted().forEach(value -> BIGINT.writeLong(values, value));
            sources.add(WorkProcessor.fromIterable(ImmutableList.of(new Page(values.build()))));
        }

        PageWithPositionComparator comparator = new SimplePageWithPositionComparator(ImmutableList.of(BIGINT), ImmutableList.of(0), ImmutableList.of(ASC_NULLS_FIRST), TYPE_OPERATORS_CACHE);
        AtomicLong comparisons = new AtomicLong();
        List<Page> pages = drain(MergeSortedPages.mergeSortedPages(
                sources.build(),
                (left, leftPosition, right, rightPosition) -> {
                    comparisons.incrementAndGet();
                    return comparator.compareTo(left, leftPosition, right, rightPosition);
                },
                ImmutableList.of(BIGINT),
                newSimpleAggregatedMemoryContext(),
                new DriverYieldSignal()));

        assertThat(pages.stream().mapToInt(Page::getPositionCount).sum()).isEqualTo(sourceCount * rowsPerSource);
        // a match is played in every internal node to build the tree, then every returned row
        // is replaced and only the matches between its leaf and the root are replayed
        int treeHeight = Integer.SIZE - Integer.numberOfLeadingZeros(sourceCount - 1);
        assertThat(comparisons.get()).isLessThanOrEqualTo(sourceCount - 1 + (long) sourceCount * rowsPerSource * treeHeight);
    }

    @Test
    public void testBlockedAndYieldingSources()
    {
        List<Type> types = ImmutableList.of(INTEGER);
        SettableFuture<Void> blocked = SettableFuture.create();
        List<ProcessState<Page>> firstSource = ImmutableList.of(
                ProcessState.ofResult(getOnlyElement(rowPagesBuilder(types).row(1).row(3).build())),
                ProcessState.blocked(blocked),
                ProcessState.ofResult(getOnlyElement(rowPagesBuilder(types).row(5).build())),
                ProcessState.finished());
        List<ProcessState<Page>> secondSource = ImmutableList.of(
                ProcessState.yielded(),
                ProcessState.ofResult(getOnlyElement(rowPagesBuilder(types).row(2).row(4).row(6).build())),
                ProcessState.finished());

        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newAggregatedMemoryContext();
        WorkProcessor<Page> mergedPages = MergeSortedPages.mergeSortedPages(
                ImmutableList.of(processorFrom(firstSource), processorFrom(secondSource)),
                new SimplePageWithPositionComparator(types, ImmutableList.of(0), ImmutableList.of(ASC_NULLS_FIRST), TYPE_OPERATORS_CACHE),
                types,
                memoryContext,
                new DriverYieldSignal());

        // the second source yields before its first page
        assertThat(mergedPages.process()).isFalse();
        assertThat(mergedPages.isBlocked()).isFalse();

        // the first source is blocked after its first page
        assertThat(mergedPages.process()).isFalse();
        assertThat(mergedPages.isBlocked()).isTrue();
        // the page of the second source is still being merged
        assertThat(memoryContext.getBytes()).isGreaterThan(0L);

        blocked.set(null);
        assertThat(mergedPages.isBlocked()).isFalse();
        assertThat(mergedPages.process()).isTrue();
        assertThat(toMaterializedResult(types, ImmutableList.of(mergedPages.getResult())))
                .containsExactlyElementsOf(resultBuilder(types)
                        .row(1)
                        .row(2)
                        .row(3)
                        .row(4)
                        .row(5)
                        .row(6)
                        .build());

        assertThat(mergedPages.process()).isTrue();
        assertThat(mergedPages.isFinished()).isTrue();
        assertThat(memoryContext.getBytes()).isEqualTo(0L);
    }

    @Test
    public void testYieldSignalIsCheckedWhileMerging()
    {
        DriverYieldSignal yieldSignal = new DriverYieldSignal();
        yieldSignal.forceYieldForTesting();

        int rowCount = 10_000;
        BlockBuilder values = BIGINT.createBlockBuilder(null, rowCount);
        for (int row = 0; row < rowCount; row++) {
            BIGINT.writeLong(values, row);
        }
        WorkProcessor<Page> mergedPages = MergeSortedPages.mergeSortedPages(
                ImmutableList.of(WorkProcessor.fromIterable(ImmutableList.of(new Page(values.build())))),
                new SimplePageWithPositionComparator(ImmutableList.of(BIGINT), ImmutableList.of(0), ImmutableList.of(ASC_NULLS_FIRST), TYPE_OPERATORS_CACHE),
                ImmutableList.of(BIGINT),
                newSimpleAggregatedMemoryContext(),
                yieldSignal);

        // the signal stays on, so the merge keeps yielding, but every call after a yield makes progress
        int yields = 0;
        while (!mergedPages.process()) {
            assertThat(mergedPages.isBlocked()).isFalse();
            yields++;
        }
        assertThat(yields).isBetween(2, rowCount / 100);
        assertThat(mergedPages.getResult().getPositionCount()).isEqualTo(rowCount);
    }

    @Test
    public void testPageBreakPredicate()
    {
        List<Type> types = ImmutableList.of(INTEGER);
        List<Integer> testedPositions = new ArrayList<>();
        WorkProcessor<Page> mergedPages = MergeSortedPages.mergeSortedPages(
                ImmutableList.of(
                        WorkProcessor.fromIterable(rowPagesBuilder(types).row(1).row(3).row(5).build()),
                        WorkProcessor.fromIterable(rowPagesBuilder(types).row(2).row(4).build())),
                new SimplePageWithPositionComparator(types, ImmutableList.of(0), ImmutableList.of(ASC_NULLS_FIRST), TYPE_OPERATORS_CACHE),
                ImmutableList.of(0),
                types,
                (pageBuilder, page, position) -> {
                    testedPositions.add(INTEGER.getInt(page.getBlock(0), position));
                    // every preceding row is already in the page builder
                    return pageBuilder.getPositionCount() == 2;
                },
                true,
                newSimpleAggregatedMemoryContext(),
                new DriverYieldSignal());

        assertThat(drain(mergedPages))
                .extracting(Page::getPositionCount)
                .containsExactly(2, 2, 1);
        assertThat(testedPositions).containsExactly(1, 2, 3, 4, 5);
    }

    private static List<Page> drain(WorkProcessor<Page> mergedPages)
    {
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        while (true) {
            assertThat(mergedPages.process()).isTrue();
            if (mergedPages.isFinished()) {
                return pages.build();
            }
            pages.add(mergedPages.getResult());
        }
    }

    private static MaterializedResult mergeSortedPages(
            List<Type> types,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            List<List<Page>> sortedPages)
    {
        List<WorkProcessor<Page>> pageProducers = sortedPages.stream()
                .map(WorkProcessor::fromIterable)
                .collect(toImmutableList());
        List<Type> sortTypes = sortChannels.stream()
                .map(types::get)
                .collect(toImmutableList());
        PageWithPositionComparator comparator = new SimplePageWithPositionComparator(sortTypes, sortChannels, sortOrder, TYPE_OPERATORS_CACHE);

        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext().newAggregatedMemoryContext();
        WorkProcessor<Page> mergedPages = MergeSortedPages.mergeSortedPages(
                pageProducers,
                comparator,
                types,
                memoryContext,
                new DriverYieldSignal());

        assertThat(mergedPages.process()).isTrue();

        if (mergedPages.isFinished()) {
            return toMaterializedResult(types, ImmutableList.of());
        }

        Page page = mergedPages.getResult();
        assertThat(mergedPages.process()).isTrue();
        assertThat(mergedPages.isFinished()).isTrue();
        assertThat(memoryContext.getBytes()).isEqualTo(0L);

        return toMaterializedResult(types, ImmutableList.of(page));
    }
}
