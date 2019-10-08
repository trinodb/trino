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
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.testing.assertions.Assert.assertEquals;

public class TestLocalDynamicFiltersCollector
{
    @Test
    public void testCollector()
    {
        Symbol symbol = new Symbol("symbol");

        LocalDynamicFiltersCollector collector = new LocalDynamicFiltersCollector();
        assertEquals(collector.getPredicate(), TupleDomain.all());

        collector.intersect(TupleDomain.all());
        assertEquals(collector.getPredicate(), TupleDomain.all());

        collector.intersect(tupleDomain(symbol, 1L, 2L));
        assertEquals(collector.getPredicate(), tupleDomain(symbol, 1L, 2L));

        collector.intersect(tupleDomain(symbol, 2L, 3L));
        assertEquals(collector.getPredicate(), tupleDomain(symbol, 2L));

        collector.intersect(tupleDomain(symbol, 0L));
        assertEquals(collector.getPredicate(), TupleDomain.none());
    }

    private TupleDomain<Symbol> tupleDomain(Symbol symbol, Long... values)
    {
        return TupleDomain.withColumnDomains(ImmutableMap.of(symbol, Domain.multipleValues(BIGINT, ImmutableList.copyOf(values))));
    }
}
