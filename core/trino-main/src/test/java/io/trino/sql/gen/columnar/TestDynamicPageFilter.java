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
package io.trino.sql.gen.columnar;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.connector.TestingColumnHandle;
import io.trino.operator.TestingSourcePage;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.CompilerConfig;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.type.CharVarcharCoercion;
import io.trino.util.DynamicFiltersTestUtil.TestingDynamicFilter;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.block.BlockAssertions.createBlockOfReals;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createRowBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.block.BlockAssertions.createTypedLongsBlock;
import static io.trino.operator.project.SelectedPositions.positionsRange;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.Domain.onlyNull;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.gen.columnar.FilterEvaluator.createColumnarFilterEvaluator;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.util.DynamicFiltersTestUtil.createDynamicFilterEvaluator;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDynamicPageFilter
{
    private static final ColumnarFilterCompiler COMPILER = new ColumnarFilterCompiler(PLANNER_CONTEXT, new CompilerConfig());
    private static final Session SESSION = testSessionBuilder().build();
    private static final FullConnectorSession FULL_CONNECTOR_SESSION = new FullConnectorSession(
            testSessionBuilder().build(),
            ConnectorIdentity.ofUser("test"));
    private static final ColumnHandle COLUMN = new TestingColumnHandle("column");
    private static final int POSITIONS = 1000;

    private static final List<Type> DISCRETE_SET_TYPES = ImmutableList.of(
            BIGINT,
            INTEGER,
            DATE,
            VARCHAR,
            CharType.createCharType(8),
            DOUBLE,
            REAL,
            DecimalType.createDecimalType(10, 2),
            DecimalType.createDecimalType(30, 2));

    private static final List<Integer> DISCRETE_SET_SIZES = ImmutableList.of(1, 2, 5, 7, 8, 9, 100, 3000);

    @Test
    public void testAllPageFilter()
    {
        SourcePage page = SourcePage.create(new Page(
                createLongsBlock(1L, 2L, null, 5L, null),
                createLongsBlock(null, 102L, 135L, null, 3L)));
        FilterEvaluator filterEvaluator = createDynamicFilterEvaluator(TupleDomain.all(), ImmutableMap.of());
        verifySelectedPositions(filterPage(page, filterEvaluator), page.getPositionCount());
    }

    @Test
    public void testNonePageFilter()
    {
        SourcePage page = SourcePage.create(new Page(
                createLongsBlock(1L, 2L, null, 5L, null),
                createLongsBlock(null, 102L, 135L, null, 3L)));
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
        SourcePage page = SourcePage.create(new Page(
                createStringsBlock("ab", "bc", null, "cd", null),
                createStringsBlock(null, "de", "ef", null, "fg")));
        verifySelectedPositions(filterPage(page, filterEvaluator), new int[] {2, 4});

        filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        column, multipleValues(VARCHAR, ImmutableList.of(utf8Slice("bc"), utf8Slice("cd"))))),
                ImmutableMap.of(column, 0));
        verifySelectedPositions(filterPage(page, filterEvaluator), new int[] {1, 3});

        filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        column, Domain.create(ValueSet.of(VARCHAR, utf8Slice("ab")), true))),
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
        SourcePage page = SourcePage.create(new Page(
                createTypedLongsBlock(INTEGER, 1L, 2L, null, 5L, null),
                createTypedLongsBlock(INTEGER, null, 102L, 135L, null, 3L)));
        verifySelectedPositions(filterPage(page, filterEvaluator), new int[] {2, 4});

        filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        column, multipleValues(INTEGER, ImmutableList.of(2L, 3L, 4L, 5L)))),
                ImmutableMap.of(column, 0));
        verifySelectedPositions(filterPage(page, filterEvaluator), new int[] {1, 3});

        filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        column, Domain.create(ValueSet.of(INTEGER, 1L), true))),
                ImmutableMap.of(column, 0));
        verifySelectedPositions(filterPage(page, filterEvaluator), new int[] {0, 2, 4});

        // not-null filter: Domain.notNull produces $not(IsNull(ref))
        filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(ImmutableMap.of(column, Domain.notNull(INTEGER))),
                ImmutableMap.of(column, 0));
        verifySelectedPositions(filterPage(page, filterEvaluator), new int[] {0, 1, 3});
    }

    @Test
    public void testStructuralTypeFilter()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        Type rowType = rowType(new RowType.Field(Optional.of("a"), INTEGER), new RowType.Field(Optional.of("b"), DOUBLE));
        RowBlock rowBlock = createRowBlock(
                ImmutableList.of(INTEGER, DOUBLE),
                new Object[] {5, 3.14159265358979},
                new Object[] {6, 3.14159265358979},
                new Object[] {7, 3.14159265358979});
        Block[] filterBlocks = rowBlock.getFieldBlocks().toArray(new Block[0]);
        FilterEvaluator filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        column, multipleValues(rowType, ImmutableList.of(new SqlRow(0, filterBlocks), new SqlRow(1, filterBlocks))))),
                ImmutableMap.of(column, 0));
        SourcePage page = SourcePage.create(new Page(rowBlock));
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
        SourcePage page = SourcePage.create(new Page(createLongSequenceBlock(0, 101), createLongSequenceBlock(100, 201)));
        verifySelectedPositions(filterPage(page, filterEvaluator), new int[] {35, 85});

        // page with null
        page = SourcePage.create(new Page(
                createLongsBlock(1L, 2L, null, 5L, null),
                createLongsBlock(null, 102L, 135L, null, 3L)));
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
        SourcePage page = SourcePage.create(new Page(
                createLongSequenceBlock(0, 101),
                createLongSequenceBlock(100, 201)));
        verifySelectedPositions(filterPage(page, filterEvaluator), 101);

        // page with null
        page = SourcePage.create(new Page(
                createLongsBlock(1L, 2L, null, 5L, null),
                createLongsBlock(null, 102L, 135L, null, 3L)));
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
                        SourcePage.create(new Page(
                                createLongsBlock(3, 1, 5),
                                createLongsBlock(3L, null, 1L))),
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
                        SourcePage.create(new Page(createBlockOfReals(42.0f, Float.NaN, 32.0f, null, 53.1f))),
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
                1,
                true);
        SourcePage page = SourcePage.create(new Page(
                createLongSequenceBlock(0, 101),
                createLongSequenceBlock(100, 201),
                createLongSequenceBlock(200, 301)));

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
                1,
                true);
        SourcePage page = SourcePage.create(new Page(
                createLongSequenceBlock(0, 101),
                createLongSequenceBlock(100, 201),
                createLongSequenceBlock(200, 301)));

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
    public void testDynamicFilterSkipsCompilerCache()
    {
        ColumnarFilterCompiler compiler = new ColumnarFilterCompiler(PLANNER_CONTEXT, new CompilerConfig());
        ColumnHandle column = new TestingColumnHandle("column");
        Symbol symbol = new Symbol(BIGINT, "A");
        DynamicPageFilter pageFilter = new DynamicPageFilter(
                PLANNER_CONTEXT,
                SESSION,
                ImmutableMap.of(symbol, column),
                ImmutableMap.of(symbol, 0),
                1,
                true);
        TestingDynamicFilter dynamicFilter = new TestingDynamicFilter(1);
        // 8+ values skip the lookupswitch path so the dynamic filter takes the set-field route
        dynamicFilter.update(TupleDomain.withColumnDomains(
                ImmutableMap.of(column, multipleValues(BIGINT, ImmutableList.of(1L, 3L, 5L, 7L, 9L, 11L, 13L, 15L)))));

        FilterEvaluator filterEvaluator = pageFilter.createDynamicPageFilterEvaluator(compiler, dynamicFilter).get();
        filterPage(SourcePage.create(new Page(createLongSequenceBlock(0, 10))), filterEvaluator);

        assertThat(compiler.getFilterCache().getRequestCount()).isEqualTo(0);
        assertThat(compiler.getFilterCache().getLoadCount()).isEqualTo(0);
    }

    @Test
    public void testVarcharDynamicFilterSkipsCompilerCache()
    {
        ColumnarFilterCompiler compiler = new ColumnarFilterCompiler(PLANNER_CONTEXT, new CompilerConfig());
        ColumnHandle column = new TestingColumnHandle("column");
        Symbol symbol = new Symbol(VARCHAR, "A");
        DynamicPageFilter pageFilter = new DynamicPageFilter(
                PLANNER_CONTEXT,
                SESSION,
                ImmutableMap.of(symbol, column),
                ImmutableMap.of(symbol, 0),
                1,
                true);
        TestingDynamicFilter dynamicFilter = new TestingDynamicFilter(1);
        dynamicFilter.update(TupleDomain.withColumnDomains(
                ImmutableMap.of(column, multipleValues(VARCHAR, ImmutableList.of(utf8Slice("ab"), utf8Slice("cd"), utf8Slice("fg"))))));

        FilterEvaluator filterEvaluator = pageFilter.createDynamicPageFilterEvaluator(compiler, dynamicFilter).get();
        SourcePage page = SourcePage.create(new Page(createStringsBlock("ab", "bc", null, "cd", "de", "fg")));
        verifySelectedPositions(filterPage(page, filterEvaluator), new int[] {0, 3, 5});

        assertThat(compiler.getFilterCache().getRequestCount()).isEqualTo(0);
        assertThat(compiler.getFilterCache().getLoadCount()).isEqualTo(0);
    }

    @Test
    public void testIneffectiveFilter()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        List<TestingSourcePage> inputPages = generateInputPages(3, 1, 1024);
        FilterEvaluator filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(ImmutableMap.of(column, getRangePredicate(100, 5000))),
                ImmutableMap.of(column, 0),
                0.9);
        assertThat(filterPage(inputPages.get(0), filterEvaluator).size()).isEqualTo(924);
        assertThat(filterPage(inputPages.get(1), filterEvaluator).size()).isEqualTo(924);

        // EffectiveFilterProfiler should turn off row filtering
        assertThat(filterPage(inputPages.get(2), filterEvaluator).size()).isEqualTo(1024);
        assertThat(inputPages.get(2).wasLoaded(0)).isFalse();
    }

    @Test
    public void testEffectiveFilter()
    {
        ColumnHandle column = new TestingColumnHandle("column");
        List<TestingSourcePage> inputPages = generateInputPages(5, 1, 1024);
        FilterEvaluator filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(ImmutableMap.of(column, singleValue(BIGINT, 13L))),
                ImmutableMap.of(column, 0),
                0.1);
        // EffectiveFilterProfiler should not turn off row filtering
        for (TestingSourcePage inputPage : inputPages) {
            assertThat(filterPage(inputPage, filterEvaluator).size()).isEqualTo(1);
        }
    }

    @Test
    public void testIneffectiveFilterFirst()
    {
        ColumnHandle columnA = new TestingColumnHandle("columnA");
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        List<TestingSourcePage> inputPages = generateInputPages(3, 2, 1024);
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
        assertThat(inputPages.get(2).wasLoaded(0)).isFalse();
    }

    @Test
    public void testIneffectiveFilterLast()
    {
        ColumnHandle columnA = new TestingColumnHandle("columnA");
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        List<TestingSourcePage> inputPages = generateInputPages(4, 2, 1024);
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
        assertThat(inputPages.get(3).wasLoaded(1)).isFalse();
    }

    @Test
    public void testMultipleColumnsShortCircuit()
    {
        ColumnHandle columnA = new TestingColumnHandle("columnA");
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        ColumnHandle columnC = new TestingColumnHandle("columnC");
        List<TestingSourcePage> inputPages = generateInputPages(5, 3, 100);
        FilterEvaluator filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        columnA, multipleValues(BIGINT, ImmutableList.of(-10L, 5L, 15L, 35L, 50L, 85L, 95L, 105L)),
                        columnB, singleValue(BIGINT, 0L),
                        columnC, getRangePredicate(150, 250))),
                ImmutableMap.of(columnA, 0, columnB, 1, columnC, 2));
        for (TestingSourcePage inputPage : inputPages) {
            assertThat(filterPage(inputPage, filterEvaluator).size()).isEqualTo(0);
            assertThat(inputPage.wasLoaded(0)).isTrue();
            assertThat(inputPage.wasLoaded(1)).isTrue();
            assertThat(inputPage.wasLoaded(2)).isFalse();
        }
    }

    @Test
    public void testDynamicFilterOnSubsetOfColumns()
    {
        ColumnHandle columnB = new TestingColumnHandle("columnB");
        ColumnHandle columnD = new TestingColumnHandle("columnD");
        List<TestingSourcePage> inputPages = generateInputPages(5, 5, 1024);
        FilterEvaluator filterEvaluator = createDynamicFilterEvaluator(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        columnB, multipleValues(BIGINT, ImmutableList.of(-10L, 5L, 15L, 35L, 50L, 85L, 95L, 105L)),
                        columnD, getRangePredicate(-50, 90))),
                ImmutableMap.of(columnB, 1, columnD, 3));
        for (TestingSourcePage inputPage : inputPages) {
            assertThat(filterPage(inputPage, filterEvaluator).size()).isEqualTo(5);
            assertThat(inputPage.wasLoaded(0)).isFalse();
            assertThat(inputPage.wasLoaded(1)).isTrue();
            assertThat(inputPage.wasLoaded(2)).isFalse();
            assertThat(inputPage.wasLoaded(3)).isTrue();
            assertThat(inputPage.wasLoaded(4)).isFalse();
        }
    }

    // The evaluator built from the values of a discrete set has to select the same positions as the one built
    // from the optimized expression the domain translates to, and as the domain itself
    @Test
    public void testDiscreteSets()
    {
        Random random = new Random(1234);
        for (Type type : DISCRETE_SET_TYPES) {
            for (int size : DISCRETE_SET_SIZES) {
                for (boolean nullAllowed : new boolean[] {false, true}) {
                    List<Object> values = randomValues(type, size, random);
                    Domain domain = Domain.create(ValueSet.copyOf(type, values), nullAllowed);
                    assertSelectsSamePositions(type, domain, inputPages(type, values, random));
                }
            }
        }
    }

    @Test
    public void testNearlyContinuousValues()
    {
        Random random = new Random(91011);
        for (Type type : ImmutableList.of(BIGINT, INTEGER, DATE)) {
            List<Object> values = new ArrayList<>();
            for (long i = 1000; i < 1100; i++) {
                if (i != 1050) {
                    values.add(i);
                }
            }
            values.add(1101L);
            Domain domain = Domain.create(ValueSet.copyOf(type, values), false);
            assertSelectsSamePositions(type, domain, inputPages(type, values, random));
        }
    }

    @Test
    public void testExtremeBigintValues()
    {
        Random random = new Random(121314);
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            values.add(Long.MAX_VALUE - i * 3L);
            values.add(Long.MIN_VALUE + i * 3L);
        }
        Domain domain = Domain.create(ValueSet.copyOf(BIGINT, values), false);
        assertSelectsSamePositions(BIGINT, domain, inputPages(BIGINT, values, random));

        // the width of the range of these values does not fit in a long, so they are not a continuous range
        List<Object> overflowing = ImmutableList.of(Long.MIN_VALUE, Long.MIN_VALUE + 1, Long.MIN_VALUE + 2, Long.MIN_VALUE + 3, Long.MAX_VALUE - 3, Long.MAX_VALUE - 2, Long.MAX_VALUE - 1, Long.MAX_VALUE);
        domain = Domain.create(ValueSet.copyOf(BIGINT, overflowing), false);
        assertThat(buildCounts(BIGINT, domain)).isEqualTo(new Counts(1, 0));
        assertSelectsSamePositions(BIGINT, domain, inputPages(BIGINT, overflowing, random));
    }

    @Test
    public void testSelectedDiscreteSetsAreBuiltFromValues()
    {
        Random random = new Random(212223);
        for (Type type : ImmutableList.of(BIGINT, INTEGER, DATE, VARCHAR, CharType.createCharType(8), DecimalType.createDecimalType(10, 2), DecimalType.createDecimalType(30, 2))) {
            // eight values are not compiled to a switch
            assertThat(buildCounts(type, Domain.create(ValueSet.copyOf(type, randomValues(type, 8, random)), false)))
                    .as("%s with 8 values", type)
                    .isEqualTo(new Counts(1, 0));
            assertThat(buildCounts(type, Domain.create(ValueSet.copyOf(type, randomValues(type, 5000, random)), false)))
                    .as("%s with 5000 values", type)
                    .isEqualTo(new Counts(1, 0));
        }
        // a switch is not an IN of a set
        assertThat(buildCounts(BIGINT, Domain.create(ValueSet.of(BIGINT, 10L, 20L, 30L, 5000L), false))).isEqualTo(new Counts(0, 1));
        assertThat(buildCounts(VARCHAR, Domain.create(ValueSet.of(VARCHAR, utf8Slice("a"), utf8Slice("b")), false))).isEqualTo(new Counts(1, 0));
    }

    @Test
    public void testFallbackToExpression()
    {
        Random random = new Random(242526);
        // null is allowed, so the IN is one of the terms of an OR
        assertGeneratedFromExpression(BIGINT, Domain.create(ValueSet.copyOf(BIGINT, randomValues(BIGINT, 100, random)), true));
        // single value
        assertGeneratedFromExpression(BIGINT, Domain.singleValue(BIGINT, 5L));
        assertGeneratedFromExpression(VARCHAR, Domain.singleValue(VARCHAR, utf8Slice("a")));
        // continuous values
        List<Object> continuous = new ArrayList<>();
        for (long i = 0; i < 100; i++) {
            continuous.add(i);
        }
        assertGeneratedFromExpression(BIGINT, Domain.create(ValueSet.copyOf(BIGINT, continuous), false));
        // ranges
        assertGeneratedFromExpression(BIGINT, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 100L, true)), false));
        // floating point
        assertGeneratedFromExpression(DOUBLE, Domain.create(ValueSet.copyOf(DOUBLE, randomValues(DOUBLE, 100, random)), false));
        // all values but null
        assertThat(buildCounts(BIGINT, Domain.notNull(BIGINT))).isEqualTo(new Counts(0, 1));
    }

    @Test
    public void testOptimizerDoesNotChangeEligibleDomains()
    {
        // the reason for the fast path to skip the optimizer
        Random random = new Random(272829);
        DomainTranslator translator = new DomainTranslator(PLANNER_CONTEXT.getMetadata());
        for (Type type : DISCRETE_SET_TYPES) {
            for (int size : DISCRETE_SET_SIZES) {
                List<Object> values = randomValues(type, size, random);
                Domain domain = Domain.create(ValueSet.copyOf(type, values), false);
                Symbol symbol = new Symbol(type, "column");
                Expression expression = translator.toPredicate(getCharVarcharCoercion(SESSION), domain, symbol.toSymbolReference());
                Optional<Expression> optimized = PLANNER_CONTEXT.getExpressionOptimizer().process(expression, SESSION, new SymbolAllocator(ImmutableList.of(symbol)), ImmutableMap.of());
                if (domain.getValues().isDiscreteSet() && domain.getValues().getDiscreteSet().size() > 1 && type != DOUBLE && type != REAL) {
                    assertThat(optimized).as("%s with %s values", type, size).isEmpty();
                }
            }
        }
    }

    private static SelectedPositions filterPage(SourcePage page, FilterEvaluator filterEvaluator)
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

    private static List<TestingSourcePage> generateInputPages(int pages, int blocks, int positionsPerBlock)
    {
        return IntStream.range(0, pages)
                .mapToObj(_ -> new TestingSourcePage(positionsPerBlock, IntStream.range(0, blocks)
                        .mapToObj(_ -> createLongSequenceBlock(0, positionsPerBlock))
                        .toArray(Block[]::new)))
                .collect(toImmutableList());
    }

    private static Domain getRangePredicate(long start, long end)
    {
        return Domain.create(ValueSet.ofRanges(Range.range(BIGINT, start, true, end, false)), false);
    }

    private static void assertGeneratedFromExpression(Type type, Domain domain)
    {
        Counts counts = buildCounts(type, domain);
        assertThat(counts.inSetDynamicFilters()).as("%s", domain).isZero();
        assertThat(counts.expressionFilters()).as("%s", domain).isPositive();
    }

    private record Counts(int inSetDynamicFilters, int expressionFilters) {}

    // number of the filters generated from values and from an expression
    private static Counts buildCounts(Type type, Domain domain)
    {
        RecordingCompiler compiler = new RecordingCompiler();
        createEvaluator(compiler, type, domain);
        return new Counts(compiler.valuesFilters, compiler.expressionFilters);
    }

    private static class RecordingCompiler
            extends ColumnarFilterCompiler
    {
        private int valuesFilters;
        private int expressionFilters;

        private RecordingCompiler()
        {
            super(PLANNER_CONTEXT, 0);
        }

        @Override
        public Optional<Supplier<ColumnarFilter>> generateFilter(CharVarcharCoercion charVarcharCoercion, Expression filter, Map<Symbol, Integer> layout, boolean dynamicFilter)
        {
            expressionFilters++;
            return super.generateFilter(charVarcharCoercion, filter, layout, dynamicFilter);
        }

        @Override
        public Optional<Supplier<ColumnarFilter>> generateInSetDynamicFilter(CharVarcharCoercion charVarcharCoercion, Reference reference, List<Object> distinctNonNullValues, Map<Symbol, Integer> layout)
        {
            Optional<Supplier<ColumnarFilter>> filter = super.generateInSetDynamicFilter(charVarcharCoercion, reference, distinctNonNullValues, layout);
            if (filter.isPresent()) {
                valuesFilters++;
            }
            return filter;
        }
    }

    private static FilterEvaluator createEvaluator(ColumnarFilterCompiler compiler, Type type, Domain domain)
    {
        Symbol symbol = new Symbol(type, "column");
        TestingDynamicFilter dynamicFilter = new TestingDynamicFilter(1);
        dynamicFilter.update(TupleDomain.withColumnDomains(ImmutableMap.of(COLUMN, domain)));
        return new DynamicPageFilter(
                PLANNER_CONTEXT,
                SESSION,
                ImmutableMap.of(symbol, COLUMN),
                ImmutableMap.of(symbol, 0),
                1,
                true)
                .createDynamicPageFilterEvaluator(compiler, dynamicFilter)
                .get();
    }

    // the evaluator of the optimized expression the domain translates to
    private static FilterEvaluator createReferenceEvaluator(Type type, Domain domain)
    {
        Symbol symbol = new Symbol(type, "column");
        Expression expression = new DomainTranslator(PLANNER_CONTEXT.getMetadata()).toPredicate(getCharVarcharCoercion(SESSION), domain, symbol.toSymbolReference());
        expression = PLANNER_CONTEXT.getExpressionOptimizer()
                .process(expression, SESSION, new SymbolAllocator(ImmutableList.of(symbol)), ImmutableMap.of())
                .orElse(expression);
        return createColumnarFilterEvaluator(getCharVarcharCoercion(SESSION), expression, ImmutableMap.of(symbol, 0), new ColumnarFilterCompiler(PLANNER_CONTEXT, 0), true, true)
                .orElseThrow()
                .get();
    }

    private static void assertSelectsSamePositions(Type type, Domain domain, List<Page> pages)
    {
        FilterEvaluator evaluator = createEvaluator(new ColumnarFilterCompiler(PLANNER_CONTEXT, 0), type, domain);
        FilterEvaluator reference = createReferenceEvaluator(type, domain);
        for (Page page : pages) {
            int[] actual = selectedPositions(evaluator, page);
            assertThat(actual).as("%s, %s", type, domain.getValues()).isEqualTo(selectedPositions(reference, page));
            Block block = page.getBlock(0);
            List<Integer> expected = new ArrayList<>();
            for (int position = 0; position < block.getPositionCount(); position++) {
                Object value = block.isNull(position) ? null : readNativeValue(type, block, position);
                if (domain.includesNullableValue(value)) {
                    expected.add(position);
                }
            }
            assertThat(actual).as("%s, %s", type, domain.getValues()).isEqualTo(expected.stream().mapToInt(Integer::intValue).toArray());
        }
    }

    private static int[] selectedPositions(FilterEvaluator evaluator, Page page)
    {
        SelectedPositions selected = filterPage(SourcePage.create(page), evaluator);
        int[] positions = new int[selected.size()];
        for (int i = 0; i < positions.length; i++) {
            positions[i] = selected.isList() ? selected.getPositions()[selected.getOffset() + i] : selected.getOffset() + i;
        }
        return positions;
    }

    // flat, dictionary and run length encoded pages, with members of the set, other values and nulls
    private static List<Page> inputPages(Type type, List<Object> members, Random random)
    {
        List<Object> inputs = new ArrayList<>();
        for (int i = 0; i < POSITIONS; i++) {
            switch (random.nextInt(4)) {
                case 0 -> inputs.add(null);
                case 1 -> inputs.add(randomValues(type, 1, random).get(0));
                default -> inputs.add(members.get(random.nextInt(members.size())));
            }
        }
        Block flat = block(type, inputs);

        int dictionarySize = 50;
        Block dictionary = block(type, inputs.subList(0, dictionarySize));
        int[] ids = new int[POSITIONS];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = random.nextInt(dictionarySize);
        }

        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        pages.add(new Page(flat));
        pages.add(new Page(DictionaryBlock.create(ids.length, dictionary, ids)));
        // the same dictionary again, as the evaluator may remember the last one
        pages.add(new Page(DictionaryBlock.create(ids.length, dictionary, ids)));
        pages.add(new Page(RunLengthEncodedBlock.create(block(type, ImmutableList.of(members.get(0))), POSITIONS)));
        pages.add(new Page(RunLengthEncodedBlock.create(block(type, randomValues(type, 1, new Random(7))), POSITIONS)));
        pages.add(new Page(RunLengthEncodedBlock.create(block(type, singletonList(null)), POSITIONS)));
        return pages.build();
    }

    private static Block block(Type type, List<Object> values)
    {
        BlockBuilder builder = type.createBlockBuilder(null, values.size());
        for (Object value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                writeNativeValue(type, builder, value);
            }
        }
        return builder.build();
    }

    private static List<Object> randomValues(Type type, int count, Random random)
    {
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            values.add(randomValue(type, random));
        }
        return values;
    }

    private static Object randomValue(Type type, Random random)
    {
        if (type == BIGINT) {
            // not close to each other, so that the values are not continuous
            return random.nextLong() >> random.nextInt(40);
        }
        if (type == INTEGER) {
            return (long) random.nextInt();
        }
        if (type == DATE) {
            return (long) random.nextInt(30_000);
        }
        if (type == DOUBLE) {
            return random.nextDouble() * 1000;
        }
        if (type == REAL) {
            return (long) floatToRawIntBits(random.nextFloat() * 1000);
        }
        if (type instanceof DecimalType decimalType) {
            long unscaled = random.nextLong() % 1_000_000_000L;
            if (decimalType.isShort()) {
                return unscaled;
            }
            return Int128.valueOf(unscaled, random.nextLong());
        }
        if (type instanceof CharType) {
            return utf8Slice(Long.toString(random.nextInt(100_000_000), 36));
        }
        return utf8Slice("value-" + Long.toString(random.nextLong() & Long.MAX_VALUE, 36));
    }
}
