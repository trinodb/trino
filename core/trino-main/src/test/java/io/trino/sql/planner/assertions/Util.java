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
package io.trino.sql.planner.assertions;

import com.google.common.collect.Iterables;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern.Ordering;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;

final class Util
{
    private Util() {}

    static boolean domainsMatch(TupleDomain<Predicate<ColumnHandle>> expected, TupleDomain<ColumnHandle> actual)
    {
        Optional<Map<Predicate<ColumnHandle>, Domain>> expectedDomains = expected.getDomains();
        Optional<Map<ColumnHandle, Domain>> actualDomains = actual.getDomains();

        if (expectedDomains.isPresent() != actualDomains.isPresent()) {
            return false;
        }

        if (expectedDomains.isPresent()) {
            if (expectedDomains.get().size() != actualDomains.get().size()) {
                return false;
            }

            for (Map.Entry<Predicate<ColumnHandle>, Domain> entry : expectedDomains.get().entrySet()) {
                // There should be exactly one column matching the expected column matcher
                ColumnHandle actualColumn = Iterables.getOnlyElement(actualDomains.get().keySet().stream()
                        .filter(x -> entry.getKey().test(x))
                        .collect(toImmutableList()));

                if (!actualDomains.get().get(actualColumn).contains(entry.getValue())) {
                    return false;
                }
            }
        }
        return true;
    }

    static boolean orderingSchemeMatches(List<Ordering> expectedOrderBy, OrderingScheme orderingScheme, SymbolAliases symbolAliases)
    {
        if (expectedOrderBy.size() != orderingScheme.orderBy().size()) {
            return false;
        }

        for (int i = 0; i < expectedOrderBy.size(); ++i) {
            Ordering ordering = expectedOrderBy.get(i);
            Symbol symbol = Symbol.from(symbolAliases.get(ordering.getField()));
            if (!symbol.equals(orderingScheme.orderBy().get(i))) {
                return false;
            }
            if (ordering.getSortOrder() != orderingScheme.ordering(symbol)) {
                return false;
            }
        }

        return true;
    }
}
