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

import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.UnnestNode.Mapping;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class UnnestedSymbolMatcher
        implements RvalueMatcher
{
    private final String symbol;
    private final int index;

    public UnnestedSymbolMatcher(String symbol, int index)
    {
        this.symbol = requireNonNull(symbol, "symbol is null");
        checkArgument(index >= 0, "index cannot be negative");
        this.index = index;
    }

    @Override
    public Optional<Symbol> getAssignedSymbol(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        if (!(node instanceof UnnestNode unnestNode)) {
            return Optional.empty();
        }

        Symbol unnestSymbol = Symbol.from(symbolAliases.get(symbol));
        List<Mapping> matches = unnestNode.getMappings().stream()
                .filter(mapping -> mapping.getInput().equals(unnestSymbol))
                .collect(toImmutableList());
        checkState(matches.size() < 2, "alias matching not supported for repeated unnest symbols");
        if (matches.size() == 0) {
            return Optional.empty();
        }

        Mapping mapping = getOnlyElement(matches);

        if (index >= mapping.getOutputs().size()) {
            return Optional.empty();
        }

        return Optional.of(mapping.getOutputs().get(index));
    }

    public String getSymbol()
    {
        return symbol;
    }
}
