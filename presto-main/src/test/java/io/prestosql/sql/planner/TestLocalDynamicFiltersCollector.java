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

package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.DynamicFilter;
import io.prestosql.spi.connector.TestingColumnHandle;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.plan.DynamicFilterId;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestLocalDynamicFiltersCollector
{
    @Test
    public void testSingle()
    {
        LocalDynamicFiltersCollector collector = new LocalDynamicFiltersCollector();
        DynamicFilterId filterId = new DynamicFilterId("filter");
        collector.register(ImmutableSet.of(filterId));

        Symbol symbol = new Symbol("symbol");
        ColumnHandle column = new TestingColumnHandle("column");
        DynamicFilter filter = collector.createDynamicFilter(
                ImmutableList.of(new DynamicFilters.Descriptor(filterId, symbol.toSymbolReference())),
                ImmutableMap.of(symbol, column));

        // Filter is blocked and not completed.
        CompletableFuture<?> isBlocked = filter.isBlocked();
        assertFalse(filter.isComplete());
        assertTrue(filter.isAwaitable());
        assertFalse(isBlocked.isDone());
        assertEquals(filter.getCurrentPredicate(), TupleDomain.all());

        Domain domain = Domain.singleValue(BIGINT, 7L);
        collector.collectDynamicFilterDomains(ImmutableMap.of(filterId, domain));

        // Unblocked and completed.
        assertTrue(filter.isComplete());
        assertFalse(filter.isAwaitable());
        assertTrue(isBlocked.isDone());
        assertEquals(filter.getCurrentPredicate(), TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)));
    }

    @Test
    public void testDynamicFilterCancellation()
    {
        LocalDynamicFiltersCollector collector = new LocalDynamicFiltersCollector();
        DynamicFilterId filterId = new DynamicFilterId("filter");
        collector.register(ImmutableSet.of(filterId));

        Symbol symbol = new Symbol("symbol");
        ColumnHandle column = new TestingColumnHandle("column");
        DynamicFilter filter = collector.createDynamicFilter(
                ImmutableList.of(new DynamicFilters.Descriptor(filterId, symbol.toSymbolReference())),
                ImmutableMap.of(symbol, column));

        // Filter is blocked and not completed.
        CompletableFuture<?> isBlocked = filter.isBlocked();
        assertFalse(filter.isComplete());
        assertFalse(isBlocked.isDone());
        assertEquals(filter.getCurrentPredicate(), TupleDomain.all());
        // DynamicFilter future cancellation should not affect LocalDynamicFiltersCollector
        assertFalse(isBlocked.cancel(false));
        assertFalse(isBlocked.isDone());
        assertFalse(filter.isComplete());

        Domain domain = Domain.singleValue(BIGINT, 7L);
        collector.collectDynamicFilterDomains(ImmutableMap.of(filterId, domain));

        // Unblocked and completed.
        assertTrue(filter.isComplete());
        assertTrue(isBlocked.isDone());
        assertEquals(filter.getCurrentPredicate(), TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)));
    }

    @Test
    public void testMultipleProbeColumns()
    {
        LocalDynamicFiltersCollector collector = new LocalDynamicFiltersCollector();
        DynamicFilterId filterId = new DynamicFilterId("filter");
        collector.register(ImmutableSet.of(filterId));

        // Same build-side column being matched to multiple probe-side columns.
        Symbol symbol1 = new Symbol("symbol1");
        Symbol symbol2 = new Symbol("symbol2");
        ColumnHandle column1 = new TestingColumnHandle("column1");
        ColumnHandle column2 = new TestingColumnHandle("column2");
        DynamicFilter filter = collector.createDynamicFilter(
                ImmutableList.of(
                        new DynamicFilters.Descriptor(filterId, symbol1.toSymbolReference()),
                        new DynamicFilters.Descriptor(filterId, symbol2.toSymbolReference())),
                ImmutableMap.of(symbol1, column1, symbol2, column2));

        // Filter is blocked and not completed.
        CompletableFuture<?> isBlocked = filter.isBlocked();
        assertFalse(filter.isComplete());
        assertTrue(filter.isAwaitable());
        assertFalse(isBlocked.isDone());
        assertEquals(filter.getCurrentPredicate(), TupleDomain.all());

        Domain domain = Domain.singleValue(BIGINT, 7L);
        collector.collectDynamicFilterDomains(ImmutableMap.of(filterId, domain));

        // Unblocked and completed.
        assertTrue(filter.isComplete());
        assertFalse(filter.isAwaitable());
        assertTrue(isBlocked.isDone());
        assertEquals(filter.getCurrentPredicate(), TupleDomain.withColumnDomains(ImmutableMap.of(column1, domain, column2, domain)));
    }

    @Test
    public void testMultipleBuildColumnsSingleProbeColumn()
    {
        LocalDynamicFiltersCollector collector = new LocalDynamicFiltersCollector();
        DynamicFilterId filter1 = new DynamicFilterId("filter1");
        DynamicFilterId filter2 = new DynamicFilterId("filter2");
        collector.register(ImmutableSet.of(filter1));
        collector.register(ImmutableSet.of(filter2));

        // Multiple build-side columns matching the same probe-side column.
        Symbol symbol = new Symbol("symbol");
        ColumnHandle column = new TestingColumnHandle("column");
        DynamicFilter filter = collector.createDynamicFilter(
                ImmutableList.of(
                        new DynamicFilters.Descriptor(filter1, symbol.toSymbolReference()),
                        new DynamicFilters.Descriptor(filter2, symbol.toSymbolReference())),
                ImmutableMap.of(symbol, column));

        // Filter is blocking and not completed.
        CompletableFuture<?> isBlocked = filter.isBlocked();
        assertFalse(filter.isComplete());
        assertTrue(filter.isAwaitable());
        assertFalse(isBlocked.isDone());
        assertEquals(filter.getCurrentPredicate(), TupleDomain.all());

        collector.collectDynamicFilterDomains(
                ImmutableMap.of(filter1, Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L))));

        // Unblocked, but not completed.
        assertFalse(filter.isComplete());
        assertTrue(filter.isAwaitable());
        assertTrue(isBlocked.isDone());
        assertEquals(filter.getCurrentPredicate(), TupleDomain.withColumnDomains(
                ImmutableMap.of(column, Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L)))));

        // Create a new blocking future, waiting for next completion.
        isBlocked = filter.isBlocked();
        assertFalse(isBlocked.isDone());
        assertFalse(filter.isComplete());
        assertTrue(filter.isAwaitable());

        collector.collectDynamicFilterDomains(
                ImmutableMap.of(filter2, Domain.multipleValues(BIGINT, ImmutableList.of(2L, 3L, 4L))));

        // Unblocked and completed.
        assertTrue(filter.isComplete());
        assertFalse(filter.isAwaitable());
        assertTrue(isBlocked.isDone());
        assertEquals(filter.getCurrentPredicate(), TupleDomain.withColumnDomains(
                ImmutableMap.of(column, Domain.multipleValues(BIGINT, ImmutableList.of(2L, 3L)))));
    }

    @Test
    public void testUnusedDynamicFilter()
    {
        LocalDynamicFiltersCollector collector = new LocalDynamicFiltersCollector();
        DynamicFilterId unusedFilterId = new DynamicFilterId("unused");
        DynamicFilterId usedFilterId = new DynamicFilterId("used");
        collector.register(ImmutableSet.of(unusedFilterId));
        collector.register(ImmutableSet.of(usedFilterId));

        // One of the dynamic filters is not used for the the table scan.
        Symbol usedSymbol = new Symbol("used");
        ColumnHandle usedColumn = new TestingColumnHandle("used");
        DynamicFilter filter = collector.createDynamicFilter(
                ImmutableList.of(new DynamicFilters.Descriptor(usedFilterId, usedSymbol.toSymbolReference())),
                ImmutableMap.of(usedSymbol, usedColumn));

        // Filter is blocking and not completed.
        CompletableFuture<?> isBlocked = filter.isBlocked();
        assertFalse(filter.isComplete());
        assertTrue(filter.isAwaitable());
        assertFalse(isBlocked.isDone());
        assertEquals(filter.getCurrentPredicate(), TupleDomain.all());

        collector.collectDynamicFilterDomains(ImmutableMap.of(unusedFilterId, Domain.singleValue(BIGINT, 1L)));

        // This dynamic filter is unused here - has no effect on blocking/completion of the above future.
        assertFalse(filter.isComplete());
        assertFalse(isBlocked.isDone());
        assertEquals(filter.getCurrentPredicate(), TupleDomain.all());

        collector.collectDynamicFilterDomains(ImmutableMap.of(usedFilterId, Domain.singleValue(BIGINT, 2L)));

        // Unblocked and completed.
        assertTrue(filter.isComplete());
        assertFalse(filter.isAwaitable());
        assertTrue(isBlocked.isDone());
        assertEquals(filter.getCurrentPredicate(), TupleDomain.withColumnDomains(ImmutableMap.of(usedColumn, Domain.singleValue(BIGINT, 2L))));
    }

    @Test
    public void testUnregisteredDynamicFilter()
    {
        // One dynamic filter is not collected locally (e.g. due to a distributed join)
        LocalDynamicFiltersCollector collector = new LocalDynamicFiltersCollector();
        DynamicFilterId registeredFilterId = new DynamicFilterId("registered");
        DynamicFilterId unregisteredFilterId = new DynamicFilterId("unregistered");
        collector.register(ImmutableSet.of(registeredFilterId));

        Symbol registeredSymbol = new Symbol("registered");
        Symbol unregisteredSymbol = new Symbol("unregistered");
        ColumnHandle registeredColumn = new TestingColumnHandle("registered");
        ColumnHandle unregisteredColumn = new TestingColumnHandle("unregistered");
        DynamicFilter filter = collector.createDynamicFilter(
                ImmutableList.of(
                        new DynamicFilters.Descriptor(registeredFilterId, registeredSymbol.toSymbolReference()),
                        new DynamicFilters.Descriptor(unregisteredFilterId, unregisteredSymbol.toSymbolReference())),
                ImmutableMap.of(registeredSymbol, registeredColumn, unregisteredSymbol, unregisteredColumn));

        // Filter is blocked and not completed.
        CompletableFuture<?> isBlocked = filter.isBlocked();
        assertFalse(filter.isComplete());
        assertTrue(filter.isAwaitable());
        assertFalse(isBlocked.isDone());
        assertEquals(filter.getCurrentPredicate(), TupleDomain.all());

        collector.collectDynamicFilterDomains(ImmutableMap.of(registeredFilterId, Domain.singleValue(BIGINT, 2L)));

        // Unblocked and completed (don't wait for filter2)
        assertTrue(filter.isComplete());
        assertFalse(filter.isAwaitable());
        assertTrue(isBlocked.isDone());
        assertEquals(filter.getCurrentPredicate(), TupleDomain.withColumnDomains(ImmutableMap.of(registeredColumn, Domain.singleValue(BIGINT, 2L))));
    }
}
