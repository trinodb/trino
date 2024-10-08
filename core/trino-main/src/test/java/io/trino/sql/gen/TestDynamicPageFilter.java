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
package io.trino.sql.gen;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.columnar.ColumnarFilterCompiler;
import io.trino.sql.gen.columnar.DynamicPageFilter;
import io.trino.sql.gen.columnar.FilterEvaluator;
import io.trino.sql.planner.CompilerConfig;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.block.BlockAssertions.createBlockOfReals;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createRowBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.block.BlockAssertions.createTypedLongsBlock;
import static io.trino.metadata.FunctionManager.createTestingFunctionManager;
import static io.trino.operator.project.SelectedPositions.positionsRange;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.Domain.onlyNull;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.util.DynamicFiltersTestUtil.TestingDynamicFilter;
import static io.trino.util.DynamicFiltersTestUtil.createDynamicFilterEvaluator;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDynamicPageFilter
{
    private static final ColumnarFilterCompiler COMPILER = new ColumnarFilterCompiler(createTestingFunctionManager(), new CompilerConfig());
    private static final Session SESSION = testSessionBuilder().build();
    private static final FullConnectorSession FULL_CONNECTOR_SESSION = new FullConnectorSession(
            testSessionBuilder().build(),
            ConnectorIdentity.ofUser("test"));

    @Test
    public void testAllPageFilter()
    {
        Page page = new Page(
                createLongsBlock(1L, 2L, null, 5L, null),
                createLongsBlock(null, 102L, 135L, null, 3L));
        FilterEvaluator filterEvaluator = createDynamicFilterEvaluator(TupleDomain.all(), ImmutableMap.of());
        verifySelectedPositions(filterPage(page, filterEvaluator), page.getPositionCount());
    }

    @Test
    public void testNonePageFilter()
    {
        Page page = new Page(
                createLongsBlock(1L, 2L, null, 5L, null),
                createLongsBlock(null, 102L, 135L, null, 3L));
        FilterEvaluator filterEvaluator = createDynamicFilterEvaluator(TupleDomain.none(), ImmutableMap.of());
        verifySelectedPositions(filterPage(page, filterEvaluator), 0);
    }

    @Test
    public void testStringFilter()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        FilterEvaluator filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(ImmutableMap.of(column, onlyNull(VARCHAR))),
                ImmutableMap.of(column, 0));
        Page page = new Page(
                createStringsBlock("ab", "bc", null, "cd", null),
                createStringsBlock(null, "de", "ef", null, "fg"));
        verifySelectedPositions(filterPage(page, filterEvaluator), new int[] {2, 4});

        filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        column,
                        multipleValues(VARCHAR, ImmutableList.of("bc", "cd")))),
                ImmutableMap.of(column, 0));
        verifySelectedPositions(filterPage(page, filterEvaluator), new int[] {1, 3});

        filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        column,
                        Domain.create(ValueSet.of(VARCHAR, utf8Slice("ab")), true))),
                ImmutableMap.of(column, 0));
        verifySelectedPositions(filterPage(page, filterEvaluator), new int[] {0, 2, 4});
    }

    @Test
    public void testLongBlockFilter()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        FilterEvaluator filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(ImmutableMap.of(column, onlyNull(INTEGER))),
                ImmutableMap.of(column, 0));
        Page page = new Page(
                createTypedLongsBlock(INTEGER, 1L, 2L, null, 5L, null),
                createTypedLongsBlock(INTEGER, null, 102L, 135L, null, 3L));
        verifySelectedPositions(filterPage(page, filterEvaluator), new int[] {2, 4});

        filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        column,
                        multipleValues(INTEGER, ImmutableList.of(2L, 3L, 4L, 5L)))),
                ImmutableMap.of(column, 0));
        verifySelectedPositions(filterPage(page, filterEvaluator), new int[] {1, 3});

        filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        column,
                        Domain.create(ValueSet.of(INTEGER, 1L), true))),
                ImmutableMap.of(column, 0));
        verifySelectedPositions(filterPage(page, filterEvaluator), new int[] {0, 2, 4});
    }

    @Test
    public void testStructuralTypeFilter()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        Type rowType = rowType(new RowType.Field(Optional.of("a"), INTEGER), new RowType.Field(Optional.of("b"), DOUBLE));
        RowBlock rowBlock = createRowBlock(
                ImmutableList.of(INTEGER, DOUBLE),
                new Object[] {5, 3.14159265358979}, new Object[] {6, 3.14159265358979}, new Object[] {7, 3.14159265358979});
        Block[] filterBlocks = rowBlock.getFieldBlocks().toArray(new Block[0]);
        FilterEvaluator filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        column,
                        multipleValues(rowType, ImmutableList.of(new SqlRow(0, filterBlocks), new SqlRow(1, filterBlocks))))),
                ImmutableMap.of(column, 0));
        Page page = new Page(rowBlock);
        // Columnar filter evaluation does not support IN on structural types, therefore this is a no-op filter
        // This should change to filter rows when the above is resolved
        verifySelectedPositions(filterPage(page, filterEvaluator), page.getPositionCount());
    }

    @Test
    public void testSelectivePageFilter()
    {
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        FilterEvaluator filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(columnB, multipleValues(BIGINT, ImmutableList.of(-10L, 5L, 15L, 135L, 185L, 250L)))),
                ImmutableMap.of(columnB, 1));

        // page without null
        Page page = new Page(createLongSequenceBlock(0, 101), createLongSequenceBlock(100, 201));
        verifySelectedPositions(filterPage(page, filterEvaluator), new int[] {35, 85});

        // page with null
        page = new Page(
                createLongsBlock(1L, 2L, null, 5L, null),
                createLongsBlock(null, 102L, 135L, null, 3L));
        verifySelectedPositions(filterPage(page, filterEvaluator), new int[] {2});
    }

    @Test
    public void testNonSelectivePageFilter()
    {
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        List<Long> filterValues = LongStream.range(-5, 205).boxed().collect(toImmutableList());
        FilterEvaluator filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(columnB, multipleValues(BIGINT, filterValues))),
                ImmutableMap.of(columnB, 1));

        // page without null
        Page page = new Page(
                createLongSequenceBlock(0, 101),
                createLongSequenceBlock(100, 201));
        verifySelectedPositions(filterPage(page, filterEvaluator), 101);

        // page with null
        page = new Page(
                createLongsBlock(1L, 2L, null, 5L, null),
                createLongsBlock(null, 102L, 135L, null, 3L));
        verifySelectedPositions(filterPage(page, filterEvaluator), new int[] {1, 2, 4});
    }

    @Test
    public void testPageFilterWithPositionsList()
    {
        ColumnHandle columnA = new TestingColumnHandle("columnA");
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        FilterEvaluator filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                columnA, Domain.create(ValueSet.of(BIGINT, 1L, 2L, 3L), true),
                                columnB, Domain.create(ValueSet.of(BIGINT, 1L, 2L, 3L), true))),
                ImmutableMap.of(columnA, 0, columnB, 1));

        // block with nulls is second column (positions list instead of range)
        verifySelectedPositions(
                filterPage(
                        new Page(
                                createLongsBlock(3, 1, 5),
                                createLongsBlock(3L, null, 1L)),
                        filterEvaluator),
                new int[] {0, 1});
    }

    @Test
    public void testPageFilterWithRealNaN()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        FilterEvaluator filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(
                        ImmutableMap.of(
                                column,
                                // Domain cannot contain floating point NaN
                                multipleValues(REAL, ImmutableList.of((long) floatToRawIntBits(32.0f), (long) floatToRawIntBits(54.6f))))),
                ImmutableMap.of(column, 0));

        verifySelectedPositions(
                filterPage(
                        new Page(createBlockOfReals(42.0f, Float.NaN, 32.0f, null, 53.1f)),
                        filterEvaluator),
                new int[] {2});
    }

    @Test
    public void testDynamicFilterUpdates()
    {
        ColumnHandle columnA = new TestingColumnHandle("columnA");
        Symbol symbolA = new Symbol(BIGINT, "A");
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        Symbol symbolB = new Symbol(BIGINT, "B");
        ColumnHandle columnC = new TestingColumnHandle("columnC");
        Symbol symbolC = new Symbol(BIGINT, "C");
        TestingDynamicFilter dynamicFilter = new TestingDynamicFilter(4);
        DynamicPageFilter pageFilter = new DynamicPageFilter(
                PLANNER_CONTEXT,
                SESSION,
                ImmutableMap.of(symbolA, columnA, symbolB, columnB, symbolC, columnC),
                ImmutableMap.of(symbolA, 0, symbolB, 1, symbolC, 2),
                1);
        Page page = new Page(
                createLongSequenceBlock(0, 101),
                createLongSequenceBlock(100, 201),
                createLongSequenceBlock(200, 301));

        FilterEvaluator filterEvaluator = pageFilter.createDynamicPageFilterEvaluator(COMPILER, dynamicFilter).get();
        verifySelectedPositions(filterPage(page, filterEvaluator), 101);

        dynamicFilter.update(TupleDomain.withColumnDomains(
                ImmutableMap.of(columnB, multipleValues(BIGINT, ImmutableList.of(131L, 142L)))));
        filterEvaluator = pageFilter.createDynamicPageFilterEvaluator(COMPILER, dynamicFilter).get();
        verifySelectedPositions(filterPage(page, filterEvaluator), new int[] {31, 42});

        dynamicFilter.update(TupleDomain.all());
        dynamicFilter.update(TupleDomain.withColumnDomains(
                ImmutableMap.of(columnC, singleValue(BIGINT, 231L))));
        filterEvaluator = pageFilter.createDynamicPageFilterEvaluator(COMPILER, dynamicFilter).get();
        verifySelectedPositions(filterPage(page, filterEvaluator), new int[] {31});

        dynamicFilter.update(TupleDomain.all());
        Supplier<FilterEvaluator> filterEvaluatorSupplier = pageFilter.createDynamicPageFilterEvaluator(COMPILER, dynamicFilter);
        verifySelectedPositions(filterPage(page, filterEvaluatorSupplier.get()), new int[] {31});

        assertThat(dynamicFilter.isComplete()).isTrue();
        // After dynamic filter is complete, we should get back the same cached FilterEvaluator supplier
        assertThat(pageFilter.createDynamicPageFilterEvaluator(COMPILER, dynamicFilter)).isEqualTo(filterEvaluatorSupplier);
    }

    @Test
    public void testDifferentDynamicFilterInstances()
    {
        ColumnHandle columnA = new TestingColumnHandle("columnA");
        Symbol symbolA = new Symbol(BIGINT, "A");
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        Symbol symbolB = new Symbol(BIGINT, "B");
        ColumnHandle columnC = new TestingColumnHandle("columnC");
        Symbol symbolC = new Symbol(BIGINT, "C");
        DynamicPageFilter pageFilter = new DynamicPageFilter(
                PLANNER_CONTEXT,
                SESSION,
                ImmutableMap.of(symbolA, columnA, symbolB, columnB, symbolC, columnC),
                ImmutableMap.of(symbolA, 0, symbolB, 1, symbolC, 2),
                1);
        Page page = new Page(
                createLongSequenceBlock(0, 101),
                createLongSequenceBlock(100, 201),
                createLongSequenceBlock(200, 301));

        TestingDynamicFilter dynamicFilter = new TestingDynamicFilter(1);
        dynamicFilter.update(TupleDomain.withColumnDomains(
                ImmutableMap.of(columnB, multipleValues(BIGINT, ImmutableList.of(131L, 142L)))));
        FilterEvaluator filterEvaluator = pageFilter.createDynamicPageFilterEvaluator(COMPILER, dynamicFilter).get();
        verifySelectedPositions(filterPage(page, filterEvaluator), new int[] {31, 42});

        dynamicFilter = new TestingDynamicFilter(1);
        dynamicFilter.update(TupleDomain.all());
        filterEvaluator = pageFilter.createDynamicPageFilterEvaluator(COMPILER, dynamicFilter).get();
        verifySelectedPositions(filterPage(page, filterEvaluator), 101);

        dynamicFilter = new TestingDynamicFilter(1);
        dynamicFilter.update(TupleDomain.withColumnDomains(
                ImmutableMap.of(columnC, singleValue(BIGINT, 231L))));
        Supplier<FilterEvaluator> filterEvaluatorSupplier = pageFilter.createDynamicPageFilterEvaluator(COMPILER, dynamicFilter);
        verifySelectedPositions(filterPage(page, filterEvaluatorSupplier.get()), new int[] {31});

        dynamicFilter = new TestingDynamicFilter(1);
        dynamicFilter.update(TupleDomain.withColumnDomains(
                ImmutableMap.of(columnC, singleValue(BIGINT, 231L))));
        // DynamicFilter instance is different, but the underlying predicate is the same, we should get back the same cached FilterEvaluator supplier
        assertThat(pageFilter.createDynamicPageFilterEvaluator(COMPILER, dynamicFilter)).isEqualTo(filterEvaluatorSupplier);
    }

    @Test
    public void testIneffectiveFilter()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        List<Page> inputPages = generateInputPages(3, 1, 1024);
        FilterEvaluator filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(ImmutableMap.of(column, getRangePredicate(100, 5000))),
                ImmutableMap.of(column, 0),
                0.9);
        assertThat(filterPage(inputPages.get(0), filterEvaluator).size()).isEqualTo(924);
        assertThat(filterPage(inputPages.get(1), filterEvaluator).size()).isEqualTo(924);

        // EffectiveFilterProfiler should turn off row filtering
        assertThat(filterPage(inputPages.get(2), filterEvaluator).size()).isEqualTo(1024);
        assertThat(inputPages.get(2).getBlock(0)).isInstanceOf(LazyBlock.class);
    }

    @Test
    public void testEffectiveFilter()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        List<Page> inputPages = generateInputPages(5, 1, 1024);
        FilterEvaluator filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(ImmutableMap.of(column, singleValue(BIGINT, 13L))),
                ImmutableMap.of(column, 0),
                0.1);
        // EffectiveFilterProfiler should not turn off row filtering
        for (Page inputPage : inputPages) {
            assertThat(filterPage(inputPage, filterEvaluator).size()).isEqualTo(1);
        }
    }

    @Test
    public void testIneffectiveFilterFirst()
    {
        ColumnHandle columnA = new TestingColumnHandle("columnA");
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        List<Page> inputPages = generateInputPages(3, 2, 1024);
        FilterEvaluator filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        columnA, getRangePredicate(100, 1024),
                        columnB, singleValue(BIGINT, 13L))),
                ImmutableMap.of(columnA, 0, columnB, 1),
                0.9);
        assertThat(filterPage(inputPages.get(0), filterEvaluator).size()).isEqualTo(0);
        assertThat(filterPage(inputPages.get(1), filterEvaluator).size()).isEqualTo(0);

        // EffectiveFilterProfiler should turn off row filtering only for the first column filter
        assertThat(filterPage(inputPages.get(2), filterEvaluator).size()).isEqualTo(1);
        assertThat(inputPages.get(2).getBlock(0)).isInstanceOf(LazyBlock.class);
    }

    @Test
    public void testIneffectiveFilterLast()
    {
        ColumnHandle columnA = new TestingColumnHandle("columnA");
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        List<Page> inputPages = generateInputPages(4, 2, 1024);
        FilterEvaluator filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        columnA, getRangePredicate(50, 950),
                        columnB, getRangePredicate(100, 1024))),
                ImmutableMap.of(columnA, 0, columnB, 1),
                0.9);
        assertThat(filterPage(inputPages.get(0), filterEvaluator).size()).isEqualTo(850);
        assertThat(filterPage(inputPages.get(1), filterEvaluator).size()).isEqualTo(850);
        assertThat(filterPage(inputPages.get(2), filterEvaluator).size()).isEqualTo(850);

        // EffectiveFilterProfiler should turn off row filtering only for the last column filter
        assertThat(filterPage(inputPages.get(3), filterEvaluator).size()).isEqualTo(900);
        assertThat(inputPages.get(3).getBlock(1)).isInstanceOf(LazyBlock.class);
    }

    @Test
    public void testMultipleColumnsShortCircuit()
    {
        ColumnHandle columnA = new TestingColumnHandle("columnA");
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        ColumnHandle columnC = new TestingColumnHandle("columnC");
        List<Page> inputPages = generateInputPages(5, 3, 100);
        FilterEvaluator filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        columnA, multipleValues(BIGINT, ImmutableList.of(-10L, 5L, 15L, 35L, 50L, 85L, 95L, 105L)),
                        columnB, singleValue(BIGINT, 0L),
                        columnC, getRangePredicate(150, 250))),
                ImmutableMap.of(columnA, 0, columnB, 1, columnC, 2));
        for (Page inputPage : inputPages) {
            assertThat(filterPage(inputPage, filterEvaluator).size()).isEqualTo(0);
            assertThat(inputPage.getBlock(0)).isNotInstanceOf(LazyBlock.class);
            assertThat(inputPage.getBlock(1)).isNotInstanceOf(LazyBlock.class);
            assertThat(inputPage.getBlock(2)).isInstanceOf(LazyBlock.class);
        }
    }

    @Test
    public void testDynamicFilterOnSubsetOfColumns()
    {
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        ColumnHandle columnD = new TestingColumnHandle("columnD");
        List<Page> inputPages = generateInputPages(5, 5, 1024);
        FilterEvaluator filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        columnB, multipleValues(BIGINT, ImmutableList.of(-10L, 5L, 15L, 35L, 50L, 85L, 95L, 105L)),
                        columnD, getRangePredicate(-50, 90))),
                ImmutableMap.of(columnB, 1, columnD, 3));
        for (Page inputPage : inputPages) {
            assertThat(filterPage(inputPage, filterEvaluator).size()).isEqualTo(5);
            assertThat(inputPage.getBlock(0)).isInstanceOf(LazyBlock.class);
            assertThat(inputPage.getBlock(1)).isNotInstanceOf(LazyBlock.class);
            assertThat(inputPage.getBlock(2)).isInstanceOf(LazyBlock.class);
            assertThat(inputPage.getBlock(3)).isNotInstanceOf(LazyBlock.class);
            assertThat(inputPage.getBlock(4)).isInstanceOf(LazyBlock.class);
        }
    }

    private static SelectedPositions filterPage(Page page, FilterEvaluator filterEvaluator)
    {
        FilterEvaluator.SelectionResult result = filterEvaluator.evaluate(FULL_CONNECTOR_SESSION, positionsRange(0, page.getPositionCount()), page);
        return result.selectedPositions();
    }

    private static void verifySelectedPositions(SelectedPositions selectedPositions, int[] positions)
    {
        assertThat(selectedPositions.isList()).isTrue();
        assertThat(selectedPositions.getOffset()).isEqualTo(0);
        assertThat(selectedPositions.size()).isEqualTo(positions.length);
        assertThat(Arrays.copyOf(selectedPositions.getPositions(), positions.length)).isEqualTo(positions);
    }

    private static void verifySelectedPositions(SelectedPositions selectedPositions, int rangeSize)
    {
        assertThat(selectedPositions.isList()).isFalse();
        assertThat(selectedPositions.getOffset()).isEqualTo(0);
        assertThat(selectedPositions.size()).isEqualTo(rangeSize);
    }

    private static List<Page> generateInputPages(int pages, int blocks, int positionsPerBlock)
    {
        return IntStream.range(0, pages)
                .mapToObj(i -> new Page(IntStream.range(0, blocks)
                        .mapToObj(_ -> new LazyBlock(positionsPerBlock, () -> createLongSequenceBlock(0, positionsPerBlock)))
                        .toArray(Block[]::new)))
                .collect(toImmutableList());
    }

    private static Domain getRangePredicate(long start, long end)
    {
        return Domain.create(ValueSet.ofRanges(Range.range(BIGINT, start, true, end, false)), false);
    }
}
