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
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Set;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.testing.assertions.Assert.assertEquals;

public class TestLocalDynamicFiltersCollector
{
    @Test
    public void testCollector()
    {
        Symbol symbol = new Symbol("symbol");
        Set<Symbol> probeSymbols = ImmutableSet.of(symbol);

        LocalDynamicFiltersCollector collector = new LocalDynamicFiltersCollector();
        assertEquals(collector.getDynamicFilter(probeSymbols), TupleDomain.all());

        collector.addDynamicFilter(ImmutableMap.of());
        assertEquals(collector.getDynamicFilter(probeSymbols), TupleDomain.all());

        collector.addDynamicFilter(toDomainMap(symbol, 1L, 2L));
        assertEquals(collector.getDynamicFilter(probeSymbols), tupleDomain(symbol, 1L, 2L));

        collector.addDynamicFilter(toDomainMap(symbol, 2L, 3L));
        assertEquals(collector.getDynamicFilter(probeSymbols), tupleDomain(symbol, 2L));

        collector.addDynamicFilter(toDomainMap(symbol, 0L));
        assertEquals(collector.getDynamicFilter(probeSymbols), TupleDomain.none());
    }

    @Test
    public void testCollectorMultipleScans()
    {
        Symbol symbol1 = new Symbol("symbol1");
        Symbol symbol2 = new Symbol("symbol2");
        Set<Symbol> probeSymbols1 = ImmutableSet.of(symbol1);
        Set<Symbol> probeSymbols2 = ImmutableSet.of(symbol2);

        LocalDynamicFiltersCollector collector = new LocalDynamicFiltersCollector();
        assertEquals(collector.getDynamicFilter(probeSymbols1), TupleDomain.all());
        assertEquals(collector.getDynamicFilter(probeSymbols2), TupleDomain.all());

        collector.addDynamicFilter(toDomainMap(symbol1, 1L, 2L));
        collector.addDynamicFilter(toDomainMap(symbol2, 2L, 3L));
        assertEquals(collector.getDynamicFilter(probeSymbols1), tupleDomain(symbol1, 1L, 2L));
        assertEquals(collector.getDynamicFilter(probeSymbols2), tupleDomain(symbol2, 2L, 3L));
        assertEquals(collector.getDynamicFilter(
                ImmutableSet.of(symbol1, symbol2)),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        symbol1, Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L)),
                        symbol2, Domain.multipleValues(BIGINT, ImmutableList.of(2L, 3L)))));

        collector.addDynamicFilter(toDomainMap(symbol1, 0L));
        assertEquals(collector.getDynamicFilter(probeSymbols1), TupleDomain.none());
        assertEquals(collector.getDynamicFilter(probeSymbols2), tupleDomain(symbol2, 2L, 3L));
        assertEquals(collector.getDynamicFilter(ImmutableSet.of(symbol1, symbol2)), TupleDomain.none());
    }

    @Test
    public void testCollectorMultipleScansNone()
    {
        Symbol symbol1 = new Symbol("symbol1");
        Symbol symbol2 = new Symbol("symbol2");
        Set<Symbol> probeSymbols1 = ImmutableSet.of(symbol1);
        Set<Symbol> probeSymbols2 = ImmutableSet.of(symbol2);

        LocalDynamicFiltersCollector collector = new LocalDynamicFiltersCollector();
        assertEquals(collector.getDynamicFilter(probeSymbols1), TupleDomain.all());
        assertEquals(collector.getDynamicFilter(probeSymbols2), TupleDomain.all());

        collector.addDynamicFilter(toDomainMap(symbol1, 1L, 2L));
        collector.addDynamicFilter(toDomainMap(symbol2, 2L, 3L));

        collector.addDynamicFilter(ImmutableMap.of(symbol1, Domain.none(BIGINT)));
        assertEquals(collector.getDynamicFilter(probeSymbols1), TupleDomain.none());
        assertEquals(collector.getDynamicFilter(probeSymbols2), tupleDomain(symbol2, 2L, 3L));
        assertEquals(collector.getDynamicFilter(ImmutableSet.of(symbol1, symbol2)), TupleDomain.none());

        collector.addDynamicFilter(toDomainMap(symbol1, 1L, 2L));
        assertEquals(collector.getDynamicFilter(probeSymbols1), TupleDomain.none());
        assertEquals(collector.getDynamicFilter(probeSymbols2), tupleDomain(symbol2, 2L, 3L));
        assertEquals(collector.getDynamicFilter(ImmutableSet.of(symbol1, symbol2)), TupleDomain.none());
    }

    private TupleDomain<Symbol> tupleDomain(Symbol symbol, Long... values)
    {
        return TupleDomain.withColumnDomains(ImmutableMap.of(symbol, Domain.multipleValues(BIGINT, ImmutableList.copyOf(values))));
    }

    private Map<Symbol, Domain> toDomainMap(Symbol symbol, Long... values)
    {
        return ImmutableMap.of(symbol, Domain.multipleValues(BIGINT, ImmutableList.copyOf(values)));
    }
}
