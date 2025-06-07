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

package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.sql.DynamicFilters;
import io.trino.sql.ir.Cast;
import io.trino.sql.planner.plan.DynamicFilterId;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLocalDynamicFiltersCollector
{
    @Test
    public void testSingleEquality()
    {
        LocalDynamicFiltersCollector collector = new LocalDynamicFiltersCollector(TEST_SESSION);
        DynamicFilterId filterId = new DynamicFilterId("filter");
        collector.register(ImmutableSet.of(filterId));

        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol symbol = symbolAllocator.newSymbol("symbol", BIGINT);
        ColumnHandle column = new TestingColumnHandle("column");
        DynamicFilter filter = createDynamicFilter(
                collector,
                ImmutableList.of(new DynamicFilters.Descriptor(filterId, symbol.toSymbolReference())),
                ImmutableMap.of(symbol, column));

        assertThat(filter.getColumnsCovered())
                .describedAs("columns covered")
                .isEqualTo(Set.of(column));

        // Filter is blocked and not completed.
        CompletableFuture<?> isBlocked = filter.isBlocked();
        assertThat(filter.isComplete()).isFalse();
        assertThat(filter.isAwaitable()).isTrue();
        assertThat(isBlocked.isDone()).isFalse();
        assertThat(filter.getCurrentPredicate()).isEqualTo(TupleDomain.all());

        DynamicFilterDomain domain = DynamicFilterDomain.singleValue(BIGINT, 7L);
        collector.collectDynamicFilterDomains(ImmutableMap.of(filterId, domain));

        // Unblocked and completed.
        assertThat(filter.isComplete()).isTrue();
        assertThat(filter.isAwaitable()).isFalse();
        assertThat(isBlocked.isDone()).isTrue();
        assertThat(filter.getCurrentPredicate())
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(column, domain.toDomain())));
    }

    @Test
    public void testDynamicFilterCoercion()
    {
        LocalDynamicFiltersCollector collector = new LocalDynamicFiltersCollector(TEST_SESSION);
        DynamicFilterId filterId = new DynamicFilterId("filter");
        collector.register(ImmutableSet.of(filterId));

        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol symbol = symbolAllocator.newSymbol("symbol", INTEGER);
        ColumnHandle column = new TestingColumnHandle("column");
        DynamicFilter filter = createDynamicFilter(
                collector,
                ImmutableList.of(new DynamicFilters.Descriptor(filterId, new Cast(symbol.toSymbolReference(), BIGINT))),
                ImmutableMap.of(symbol, column));

        assertThat(filter.getColumnsCovered())
                .describedAs("columns covered")
                .isEqualTo(Set.of(column));

        // Filter is blocked and not completed.
        CompletableFuture<?> isBlocked = filter.isBlocked();
        assertThat(filter.isComplete()).isFalse();
        assertThat(filter.isAwaitable()).isTrue();
        assertThat(isBlocked.isDone()).isFalse();
        assertThat(filter.getCurrentPredicate()).isEqualTo(TupleDomain.all());

        DynamicFilterDomain domain = DynamicFilterDomain.singleValue(BIGINT, 7L);
        collector.collectDynamicFilterDomains(ImmutableMap.of(filterId, domain));

        // Unblocked and completed.
        assertThat(filter.isComplete()).isTrue();
        assertThat(filter.isAwaitable()).isFalse();
        assertThat(isBlocked.isDone()).isTrue();
        assertThat(filter.getCurrentPredicate()).isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(column, Domain.singleValue(INTEGER, 7L))));
    }

    @Test
    public void testDynamicFilterCancellation()
    {
        LocalDynamicFiltersCollector collector = new LocalDynamicFiltersCollector(TEST_SESSION);
        DynamicFilterId filterId = new DynamicFilterId("filter");
        collector.register(ImmutableSet.of(filterId));

        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol symbol = symbolAllocator.newSymbol("symbol", BIGINT);
        ColumnHandle column = new TestingColumnHandle("column");
        DynamicFilter filter = createDynamicFilter(
                collector,
                ImmutableList.of(new DynamicFilters.Descriptor(filterId, symbol.toSymbolReference())),
                ImmutableMap.of(symbol, column));

        // Filter is blocked and not completed.
        CompletableFuture<?> isBlocked = filter.isBlocked();
        assertThat(filter.isComplete()).isFalse();
        assertThat(isBlocked.isDone()).isFalse();
        assertThat(filter.getCurrentPredicate()).isEqualTo(TupleDomain.all());
        // DynamicFilter future cancellation should not affect LocalDynamicFiltersCollector
        assertThat(isBlocked.cancel(false)).isFalse();
        assertThat(isBlocked.isDone()).isFalse();
        assertThat(filter.isComplete()).isFalse();

        DynamicFilterDomain domain = DynamicFilterDomain.singleValue(BIGINT, 7L);
        collector.collectDynamicFilterDomains(ImmutableMap.of(filterId, domain));

        // Unblocked and completed.
        assertThat(filter.isComplete()).isTrue();
        assertThat(isBlocked.isDone()).isTrue();
        assertThat(filter.getCurrentPredicate())
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(column, domain.toDomain())));
    }

    @Test
    public void testMultipleProbeColumns()
    {
        LocalDynamicFiltersCollector collector = new LocalDynamicFiltersCollector(TEST_SESSION);
        DynamicFilterId filterId = new DynamicFilterId("filter");
        collector.register(ImmutableSet.of(filterId));

        // Same build-side column being matched to multiple probe-side columns.
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol symbol1 = symbolAllocator.newSymbol("symbol1", BIGINT);
        Symbol symbol2 = symbolAllocator.newSymbol("symbol2", BIGINT);
        ColumnHandle column1 = new TestingColumnHandle("column1");
        ColumnHandle column2 = new TestingColumnHandle("column2");
        DynamicFilter filter = createDynamicFilter(
                collector,
                ImmutableList.of(
                        new DynamicFilters.Descriptor(filterId, symbol1.toSymbolReference()),
                        new DynamicFilters.Descriptor(filterId, symbol2.toSymbolReference())),
                ImmutableMap.of(symbol1, column1, symbol2, column2));

        assertThat(filter.getColumnsCovered())
                .describedAs("columns covered")
                .isEqualTo(Set.of(column1, column2));

        // Filter is blocked and not completed.
        CompletableFuture<?> isBlocked = filter.isBlocked();
        assertThat(filter.isComplete()).isFalse();
        assertThat(filter.isAwaitable()).isTrue();
        assertThat(isBlocked.isDone()).isFalse();
        assertThat(filter.getCurrentPredicate()).isEqualTo(TupleDomain.all());

        DynamicFilterDomain domain = DynamicFilterDomain.singleValue(BIGINT, 7L);
        collector.collectDynamicFilterDomains(ImmutableMap.of(filterId, domain));

        // Unblocked and completed.
        assertThat(filter.isComplete()).isTrue();
        assertThat(filter.isAwaitable()).isFalse();
        assertThat(isBlocked.isDone()).isTrue();
        assertThat(filter.getCurrentPredicate())
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(column1, domain.toDomain(), column2, domain.toDomain())));
    }

    @Test
    public void testComparison()
    {
        LocalDynamicFiltersCollector collector = new LocalDynamicFiltersCollector(TEST_SESSION);
        DynamicFilterId filterId1 = new DynamicFilterId("filter1");
        DynamicFilterId filterId2 = new DynamicFilterId("filter2");
        collector.register(ImmutableSet.of(filterId1, filterId2));
        SymbolAllocator symbolAllocator = new SymbolAllocator();

        Symbol symbol = symbolAllocator.newSymbol("symbol", BIGINT);
        ColumnHandle column = new TestingColumnHandle("column");
        DynamicFilter filter = createDynamicFilter(
                collector,
                ImmutableList.of(
                        new DynamicFilters.Descriptor(filterId1, symbol.toSymbolReference(), GREATER_THAN),
                        new DynamicFilters.Descriptor(filterId2, symbol.toSymbolReference(), LESS_THAN)),
                ImmutableMap.of(symbol, column));

        assertThat(filter.getColumnsCovered())
                .describedAs("columns covered")
                .isEqualTo(Set.of(column));

        // Filter is blocked and not completed.
        CompletableFuture<?> isBlocked = filter.isBlocked();
        assertThat(filter.isComplete()).isFalse();
        assertThat(isBlocked.isDone()).isFalse();
        assertThat(filter.getCurrentPredicate()).isEqualTo(TupleDomain.all());

        collector.collectDynamicFilterDomains(ImmutableMap.of(
                filterId1, DynamicFilterDomain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L)),
                filterId2, DynamicFilterDomain.multipleValues(BIGINT, ImmutableList.of(4L, 5L, 6L))));

        // Unblocked and completed.
        assertThat(filter.isComplete()).isTrue();
        assertThat(isBlocked.isDone()).isTrue();
        assertThat(filter.getCurrentPredicate()).isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                column,
                Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, false, 6L, false)), false))));
    }

    @Test
    public void testIsNotDistinctFrom()
    {
        LocalDynamicFiltersCollector collector = new LocalDynamicFiltersCollector(TEST_SESSION);
        DynamicFilterId filterId1 = new DynamicFilterId("filter1");
        DynamicFilterId filterId2 = new DynamicFilterId("filter2");
        collector.register(ImmutableSet.of(filterId1, filterId2));
        SymbolAllocator symbolAllocator = new SymbolAllocator();

        Symbol symbol1 = symbolAllocator.newSymbol("symbol1", BIGINT);
        Symbol symbol2 = symbolAllocator.newSymbol("symbol2", BIGINT);
        ColumnHandle column1 = new TestingColumnHandle("column1");
        ColumnHandle column2 = new TestingColumnHandle("column2");
        DynamicFilter filter = createDynamicFilter(
                collector,
                ImmutableList.of(
                        new DynamicFilters.Descriptor(filterId1, symbol1.toSymbolReference(), EQUAL, true),
                        new DynamicFilters.Descriptor(filterId2, symbol2.toSymbolReference(), EQUAL, true)),
                ImmutableMap.of(symbol1, column1, symbol2, column2));

        assertThat(filter.getColumnsCovered())
                .describedAs("columns covered")
                .isEqualTo(Set.of(column1, column2));

        // Filter is blocked and not completed.
        CompletableFuture<?> isBlocked = filter.isBlocked();
        assertThat(filter.isComplete()).isFalse();
        assertThat(isBlocked.isDone()).isFalse();
        assertThat(filter.getCurrentPredicate()).isEqualTo(TupleDomain.all());

        collector.collectDynamicFilterDomains(ImmutableMap.of(
                filterId1, DynamicFilterDomain.multipleValues(BIGINT, ImmutableList.of(4L, 5L, 6L)),
                filterId2, DynamicFilterDomain.none(BIGINT)));

        // Unblocked and completed.
        assertThat(filter.isComplete()).isTrue();
        assertThat(isBlocked.isDone()).isTrue();
        assertThat(filter.getCurrentPredicate()).isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(
                column1, Domain.create(ValueSet.of(BIGINT, 4L, 5L, 6L), true),
                column2, Domain.onlyNull(BIGINT))));
    }

    @Test
    public void testMultipleBuildColumnsSingleProbeColumn()
    {
        LocalDynamicFiltersCollector collector = new LocalDynamicFiltersCollector(TEST_SESSION);
        DynamicFilterId filter1 = new DynamicFilterId("filter1");
        DynamicFilterId filter2 = new DynamicFilterId("filter2");
        collector.register(ImmutableSet.of(filter1));
        collector.register(ImmutableSet.of(filter2));

        // Multiple build-side columns matching the same probe-side column.
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol symbol = symbolAllocator.newSymbol("symbol", BIGINT);
        ColumnHandle column = new TestingColumnHandle("column");
        DynamicFilter filter = createDynamicFilter(
                collector,
                ImmutableList.of(
                        new DynamicFilters.Descriptor(filter1, symbol.toSymbolReference()),
                        new DynamicFilters.Descriptor(filter2, symbol.toSymbolReference())),
                ImmutableMap.of(symbol, column));

        assertThat(filter.getColumnsCovered())
                .describedAs("columns covered")
                .isEqualTo(Set.of(column));

        // Filter is blocking and not completed.
        CompletableFuture<?> isBlocked = filter.isBlocked();
        assertThat(filter.isComplete()).isFalse();
        assertThat(filter.isAwaitable()).isTrue();
        assertThat(isBlocked.isDone()).isFalse();
        assertThat(filter.getCurrentPredicate()).isEqualTo(TupleDomain.all());

        collector.collectDynamicFilterDomains(
                ImmutableMap.of(filter1, DynamicFilterDomain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L))));

        // Unblocked, but not completed.
        assertThat(filter.isComplete()).isFalse();
        assertThat(filter.isAwaitable()).isTrue();
        assertThat(isBlocked.isDone()).isTrue();
        assertThat(filter.getCurrentPredicate()).isEqualTo(TupleDomain.withColumnDomains(
                ImmutableMap.of(column, Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L)))));

        // Create a new blocking future, waiting for next completion.
        isBlocked = filter.isBlocked();
        assertThat(isBlocked.isDone()).isFalse();
        assertThat(filter.isComplete()).isFalse();
        assertThat(filter.isAwaitable()).isTrue();

        collector.collectDynamicFilterDomains(
                ImmutableMap.of(filter2, DynamicFilterDomain.multipleValues(BIGINT, ImmutableList.of(2L, 3L, 4L))));

        // Unblocked and completed.
        assertThat(filter.isComplete()).isTrue();
        assertThat(filter.isAwaitable()).isFalse();
        assertThat(isBlocked.isDone()).isTrue();
        assertThat(filter.getCurrentPredicate()).isEqualTo(TupleDomain.withColumnDomains(
                ImmutableMap.of(column, Domain.multipleValues(BIGINT, ImmutableList.of(2L, 3L)))));
    }

    @Test
    public void testUnusedDynamicFilter()
    {
        LocalDynamicFiltersCollector collector = new LocalDynamicFiltersCollector(TEST_SESSION);
        DynamicFilterId unusedFilterId = new DynamicFilterId("unused");
        DynamicFilterId usedFilterId = new DynamicFilterId("used");
        collector.register(ImmutableSet.of(unusedFilterId));
        collector.register(ImmutableSet.of(usedFilterId));

        // One of the dynamic filters is not used for the the table scan.
        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol usedSymbol = symbolAllocator.newSymbol("used", BIGINT);
        ColumnHandle usedColumn = new TestingColumnHandle("used");
        DynamicFilter filter = createDynamicFilter(
                collector,
                ImmutableList.of(new DynamicFilters.Descriptor(usedFilterId, usedSymbol.toSymbolReference())),
                ImmutableMap.of(usedSymbol, usedColumn));

        // Filter is blocking and not completed.
        CompletableFuture<?> isBlocked = filter.isBlocked();
        assertThat(filter.isComplete()).isFalse();
        assertThat(filter.isAwaitable()).isTrue();
        assertThat(isBlocked.isDone()).isFalse();
        assertThat(filter.getCurrentPredicate()).isEqualTo(TupleDomain.all());

        collector.collectDynamicFilterDomains(ImmutableMap.of(unusedFilterId, DynamicFilterDomain.singleValue(BIGINT, 1L)));

        // This dynamic filter is unused here - has no effect on blocking/completion of the above future.
        assertThat(filter.isComplete()).isFalse();
        assertThat(isBlocked.isDone()).isFalse();
        assertThat(filter.getCurrentPredicate()).isEqualTo(TupleDomain.all());

        collector.collectDynamicFilterDomains(ImmutableMap.of(usedFilterId, DynamicFilterDomain.singleValue(BIGINT, 2L)));

        // Unblocked and completed.
        assertThat(filter.isComplete()).isTrue();
        assertThat(filter.isAwaitable()).isFalse();
        assertThat(isBlocked.isDone()).isTrue();
        assertThat(filter.getCurrentPredicate()).isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(usedColumn, Domain.singleValue(BIGINT, 2L))));
    }

    @Test
    public void testUnregisteredDynamicFilter()
    {
        // One dynamic filter is not collected locally (e.g. due to a distributed join)
        LocalDynamicFiltersCollector collector = new LocalDynamicFiltersCollector(TEST_SESSION);
        DynamicFilterId registeredFilterId = new DynamicFilterId("registered");
        DynamicFilterId unregisteredFilterId = new DynamicFilterId("unregistered");
        collector.register(ImmutableSet.of(registeredFilterId));

        SymbolAllocator symbolAllocator = new SymbolAllocator();
        Symbol registeredSymbol = symbolAllocator.newSymbol("registered", BIGINT);
        Symbol unregisteredSymbol = symbolAllocator.newSymbol("unregistered", BIGINT);
        ColumnHandle registeredColumn = new TestingColumnHandle("registered");
        ColumnHandle unregisteredColumn = new TestingColumnHandle("unregistered");
        DynamicFilter filter = createDynamicFilter(
                collector,
                ImmutableList.of(
                        new DynamicFilters.Descriptor(registeredFilterId, registeredSymbol.toSymbolReference()),
                        new DynamicFilters.Descriptor(unregisteredFilterId, unregisteredSymbol.toSymbolReference())),
                ImmutableMap.of(registeredSymbol, registeredColumn, unregisteredSymbol, unregisteredColumn));

        // Filter is blocked and not completed.
        CompletableFuture<?> isBlocked = filter.isBlocked();
        assertThat(filter.isComplete()).isFalse();
        assertThat(filter.isAwaitable()).isTrue();
        assertThat(isBlocked.isDone()).isFalse();
        assertThat(filter.getCurrentPredicate()).isEqualTo(TupleDomain.all());

        collector.collectDynamicFilterDomains(ImmutableMap.of(registeredFilterId, DynamicFilterDomain.singleValue(BIGINT, 2L)));

        // Unblocked and completed (don't wait for filter2)
        assertThat(filter.isComplete()).isTrue();
        assertThat(filter.isAwaitable()).isFalse();
        assertThat(isBlocked.isDone()).isTrue();
        assertThat(filter.getCurrentPredicate()).isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(registeredColumn, Domain.singleValue(BIGINT, 2L))));
    }

    private DynamicFilter createDynamicFilter(
            LocalDynamicFiltersCollector collector,
            List<DynamicFilters.Descriptor> descriptors,
            Map<Symbol, ColumnHandle> columnsMap)
    {
        return collector.createDynamicFilter(descriptors, columnsMap, PLANNER_CONTEXT);
    }
}
