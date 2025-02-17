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
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.PageWithPositionComparator;
import io.trino.operator.SimplePageWithPositionComparator;
import io.trino.operator.WorkProcessor;
import io.trino.spi.Page;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.operator.OperatorAssertion.toMaterializedResult;
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
        MaterializedResult expected = resultBuilder(TEST_SESSION, types)
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
        MaterializedResult expected = resultBuilder(TEST_SESSION, types)
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
        MaterializedResult expected = resultBuilder(TEST_SESSION, types)
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
        MaterializedResult expected = resultBuilder(TEST_SESSION, types)
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
        MaterializedResult expected = resultBuilder(TEST_SESSION, types)
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
                (pageBuilder, pageWithPosition) -> pageBuilder.isFull(),
                false,
                newSimpleAggregatedMemoryContext().newAggregatedMemoryContext(),
                yieldSignal);

        // yield signal is on
        assertThat(mergedPages.process()).isFalse();
        yieldSignal.resetYieldForTesting();

        // page is produced
        assertThat(mergedPages.process()).isTrue();
        assertThat(mergedPages.isFinished()).isFalse();

        Page page = mergedPages.getResult();
        assertThat(toMaterializedResult(TEST_SESSION, types, ImmutableList.of(page)))
                .containsExactlyElementsOf(resultBuilder(TEST_SESSION, types)
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
        List<Type> types = ImmutableList.of(INTEGER);
        WorkProcessor<Page> mergedPages = MergeSortedPages.mergeSortedPages(
                ImmutableList.of(WorkProcessor.fromIterable(rowPagesBuilder(types).build())),
                new SimplePageWithPositionComparator(ImmutableList.of(types.get(0)), ImmutableList.of(0), ImmutableList.of(DESC_NULLS_LAST), TYPE_OPERATORS_CACHE),
                ImmutableList.of(0),
                types,
                (pageBuilder, pageWithPosition) -> pageBuilder.isFull(),
                false,
                newSimpleAggregatedMemoryContext().newAggregatedMemoryContext(),
                yieldSignal);
        // yield signal is on
        assertThat(mergedPages.process()).isFalse();
        // processor finishes computations (yield signal is still on, but previous process() call yielded)
        assertThat(mergedPages.process()).isTrue();
        assertThat(mergedPages.isFinished()).isTrue();
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
            return toMaterializedResult(TEST_SESSION, types, ImmutableList.of());
        }

        Page page = mergedPages.getResult();
        assertThat(mergedPages.process()).isTrue();
        assertThat(mergedPages.isFinished()).isTrue();
        assertThat(memoryContext.getBytes()).isEqualTo(0L);

        return toMaterializedResult(TEST_SESSION, types, ImmutableList.of(page));
    }
}
