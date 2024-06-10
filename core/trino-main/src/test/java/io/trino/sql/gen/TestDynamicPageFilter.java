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
import io.trino.metadata.Metadata;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.sql.gen.columnar.ColumnarFilterCompiler;
import io.trino.sql.gen.columnar.DynamicPageFilter;
import io.trino.sql.gen.columnar.FilterEvaluator;
import io.trino.sql.planner.CompilerConfig;
import io.trino.sql.planner.Symbol;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.LongStream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.concurrent.MoreFutures.unmodifiableFuture;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.block.BlockAssertions.createBlockOfReals;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createRowBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.block.BlockAssertions.createTypedLongsBlock;
import static io.trino.metadata.FunctionManager.createTestingFunctionManager;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
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
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDynamicPageFilter
{
    private static final TypeManager TYPE_MANAGER = new TestingTypeManager();
    private static final Metadata METADATA = createTestMetadataManager();
    private static final ColumnarFilterCompiler COMPILER = new ColumnarFilterCompiler(createTestingFunctionManager(), new CompilerConfig());
    private static final FullConnectorSession FULL_CONNECTOR_SESSION = new FullConnectorSession(
            TestingSession.testSessionBuilder().build(),
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
        RowBlock rowBlock = createRowBlock(ImmutableList.of(INTEGER, DOUBLE), new Object[] {5, 3.14159265358979}, new Object[] {6, 3.14159265358979}, new Object[] {7, 3.14159265358979});
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
                METADATA,
                TYPE_MANAGER,
                dynamicFilter,
                ImmutableMap.of(symbolA, columnA, symbolB, columnB, symbolC, columnC),
                ImmutableMap.of(symbolA, 0, symbolB, 1, symbolC, 2));
        Page page = new Page(
                createLongSequenceBlock(0, 101),
                createLongSequenceBlock(100, 201),
                createLongSequenceBlock(200, 301));

        FilterEvaluator filterEvaluator = pageFilter.createDynamicPageFilterEvaluator(COMPILER).get();
        verifySelectedPositions(filterPage(page, filterEvaluator), 101);

        dynamicFilter.update(TupleDomain.withColumnDomains(
                ImmutableMap.of(columnB, multipleValues(BIGINT, ImmutableList.of(131L, 142L)))));
        filterEvaluator = pageFilter.createDynamicPageFilterEvaluator(COMPILER).get();
        verifySelectedPositions(filterPage(page, filterEvaluator), new int[] {31, 42});

        dynamicFilter.update(TupleDomain.all());
        dynamicFilter.update(TupleDomain.withColumnDomains(
                ImmutableMap.of(columnC, singleValue(BIGINT, 231L))));
        filterEvaluator = pageFilter.createDynamicPageFilterEvaluator(COMPILER).get();
        verifySelectedPositions(filterPage(page, filterEvaluator), new int[] {31});

        dynamicFilter.update(TupleDomain.all());
        Supplier<FilterEvaluator> filterEvaluatorSupplier = pageFilter.createDynamicPageFilterEvaluator(COMPILER);
        verifySelectedPositions(filterPage(page, filterEvaluatorSupplier.get()), new int[] {31});

        assertThat(dynamicFilter.isComplete()).isTrue();
        // After dynamic filter is complete, we should get back the same cached FilterEvaluator supplier
        assertThat(pageFilter.createDynamicPageFilterEvaluator(COMPILER)).isEqualTo(filterEvaluatorSupplier);
    }

    private static FilterEvaluator createDynamicFilterEvaluator(
            TupleDomain<ColumnHandle> tupleDomain,
            Map<ColumnHandle, Integer> channels)
    {
        TestingDynamicFilter dynamicFilter = new TestingDynamicFilter(1);
        dynamicFilter.update(tupleDomain);
        Map<ColumnHandle, Type> types = tupleDomain.getDomains().orElse(ImmutableMap.of())
                .entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getType()));
        int index = 0;
        ImmutableMap.Builder<Symbol, ColumnHandle> columns = ImmutableMap.builder();
        ImmutableMap.Builder<Symbol, Integer> layout = ImmutableMap.builder();
        for (Map.Entry<ColumnHandle, Integer> entry : channels.entrySet()) {
            ColumnHandle column = entry.getKey();
            Symbol symbol = new Symbol(types.get(column), "col" + index++);
            columns.put(symbol, column);
            int channel = entry.getValue();
            layout.put(symbol, channel);
        }
        return new DynamicPageFilter(
                METADATA,
                TYPE_MANAGER,
                dynamicFilter,
                columns.buildOrThrow(),
                layout.buildOrThrow())
                .createDynamicPageFilterEvaluator(COMPILER)
                .get();
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

    private static class TestingDynamicFilter
            implements DynamicFilter
    {
        private CompletableFuture<?> isBlocked;
        private TupleDomain<ColumnHandle> currentPredicate;
        private int futuresLeft;

        private TestingDynamicFilter(int expectedFilters)
        {
            this.futuresLeft = expectedFilters;
            this.isBlocked = expectedFilters == 0 ? NOT_BLOCKED : new CompletableFuture<>();
            this.currentPredicate = TupleDomain.all();
        }

        public void update(TupleDomain<ColumnHandle> predicate)
        {
            futuresLeft -= 1;
            verify(futuresLeft >= 0);
            currentPredicate = currentPredicate.intersect(predicate);
            CompletableFuture<?> currentFuture = isBlocked;
            // create next blocking future (if needed)
            isBlocked = isComplete() ? NOT_BLOCKED : new CompletableFuture<>();
            verify(currentFuture.complete(null));
        }

        @Override
        public Set<ColumnHandle> getColumnsCovered()
        {
            return currentPredicate.getDomains().orElseThrow().keySet();
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            return unmodifiableFuture(isBlocked);
        }

        @Override
        public boolean isComplete()
        {
            return futuresLeft == 0;
        }

        @Override
        public boolean isAwaitable()
        {
            return futuresLeft > 0;
        }

        @Override
        public TupleDomain<ColumnHandle> getCurrentPredicate()
        {
            return currentPredicate;
        }
    }
}
