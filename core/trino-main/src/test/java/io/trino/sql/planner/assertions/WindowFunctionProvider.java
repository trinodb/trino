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
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.WindowNode;

import java.util.List;
import java.util.Optional;

import static io.trino.sql.planner.assertions.PlanMatchPattern.toSymbolReferences;
import static java.util.Objects.requireNonNull;

final class WindowFunctionProvider
        implements ExpectedValueProvider<WindowFunction>
{
    private final String name;
    private final WindowNode.Frame frame;
    private final List<PlanTestSymbol> args;
    private final List<PlanMatchPattern.Ordering> orderBy;
    private final boolean distinct;

    public WindowFunctionProvider(String name, WindowNode.Frame frame, List<PlanTestSymbol> args, List<PlanMatchPattern.Ordering> orderBy, boolean distinct)
    {
        this.name = requireNonNull(name, "name is null");
        this.frame = requireNonNull(frame, "frame is null");
        this.args = requireNonNull(args, "args is null");
        this.orderBy = ImmutableList.copyOf(orderBy);
        this.distinct = distinct;
    }

    @Override
    public String toString()
    {
        return "%s(%s%s%s) %s".formatted(
                name,
                distinct ? "DISTINCT " : "",
                Joiner.on(", ").join(args),
                orderBy.isEmpty() ? "" : " ORDER BY " + Joiner.on(", ").join(orderBy),
                frame);
    }

    @Override
    public WindowFunction getExpectedValue(SymbolAliases aliases)
    {
        Optional<OrderingScheme> orderingScheme = Optional.empty();
        if (!orderBy.isEmpty()) {
            ImmutableList.Builder<Symbol> fields = ImmutableList.builder();
            ImmutableMap.Builder<Symbol, SortOrder> orders = ImmutableMap.builder();

            for (PlanMatchPattern.Ordering ordering : this.orderBy) {
                Reference reference = aliases.get(ordering.getField());
                Symbol symbol = new Symbol(reference.type(), reference.name());
                fields.add(symbol);
                orders.put(symbol, ordering.getSortOrder());
            }
            orderingScheme = Optional.of(new OrderingScheme(fields.build(), orders.buildOrThrow()));
        }

        return new WindowFunction(name, frame, toSymbolReferences(args, aliases), orderingScheme, distinct);
    }
}
