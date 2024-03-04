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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.tree.Expression;

import java.util.List;
import java.util.Optional;

import static io.trino.sql.planner.assertions.PlanMatchPattern.toSymbolReferences;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

final class AggregationFunctionProvider
        implements ExpectedValueProvider<AggregationFunction>
{
    private final String name;
    private final boolean distinct;
    private final List<PlanTestSymbol> args;
    private final List<PlanMatchPattern.Ordering> orderBy;
    private final Optional<SymbolAlias> filter;

    public AggregationFunctionProvider(String name, boolean distinct, List<PlanTestSymbol> args, List<PlanMatchPattern.Ordering> orderBy, Optional<SymbolAlias> filter)
    {
        this.name = requireNonNull(name, "name is null");
        this.distinct = distinct;
        this.args = requireNonNull(args, "args is null");
        this.orderBy = requireNonNull(orderBy, "orderBy is null");
        this.filter = requireNonNull(filter, "filter is null");
    }

    @Override
    public String toString()
    {
        return format("%s (%s%s) %s %s",
                name,
                distinct ? "DISTINCT" : "",
                Joiner.on(", ").join(args),
                orderBy.isEmpty() ? "" : " ORDER BY " + Joiner.on(", ").join(orderBy),
                filter.isPresent() ? filter.get().toString() : "");
    }

    @Override
    public AggregationFunction getExpectedValue(SymbolAliases aliases)
    {
        List<Expression> symbolReferences = toSymbolReferences(args, aliases);

        Optional<OrderingScheme> orderByClause = Optional.empty();
        if (!orderBy.isEmpty()) {
            ImmutableList.Builder<Symbol> fields = ImmutableList.builder();
            ImmutableMap.Builder<Symbol, SortOrder> orders = ImmutableMap.builder();

            for (PlanMatchPattern.Ordering ordering : this.orderBy) {
                Symbol symbol = Symbol.from(aliases.get(ordering.getField()));
                fields.add(symbol);
                orders.put(symbol, ordering.getSortOrder());
            }
            orderByClause = Optional.of(new OrderingScheme(fields.build(), orders.buildOrThrow()));
        }

        return new AggregationFunction(name, filter.map(symbol -> symbol.toSymbol(aliases)), orderByClause, distinct, symbolReferences);
    }
}
