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
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SetExpressionMatcher
        implements RvalueMatcher
{
    private final ApplyNode.SetExpression expression;

    SetExpressionMatcher(ApplyNode.SetExpression expression)
    {
        this.expression = requireNonNull(expression, "expression is null");
    }

    @Override
    public Optional<Symbol> getAssignedSymbol(PlanNode node, Session session, Metadata metadata, SymbolAliases aliases)
    {
        if (!(node instanceof ApplyNode applyNode)) {
            return Optional.empty();
        }

        List<Map.Entry<Symbol, ApplyNode.SetExpression>> matches = new ArrayList<>();

        for (Map.Entry<Symbol, ApplyNode.SetExpression> entry : applyNode.getSubqueryAssignments().entrySet()) {
            Map.Entry<Symbol, ApplyNode.SetExpression> match = switch (expression) {
                case ApplyNode.Exists unused when entry.getValue() instanceof ApplyNode.Exists -> entry;
                case ApplyNode.In expected
                        when entry.getValue() instanceof ApplyNode.In actual &&
                        matches(aliases, expected.value(), actual.value()) &&
                        matches(aliases, expected.reference(), actual.reference()) -> entry;
                case ApplyNode.QuantifiedComparison expected
                        when entry.getValue() instanceof ApplyNode.QuantifiedComparison actual &&
                        expected.operator().equals(actual.operator()) &&
                        expected.quantifier().equals(actual.quantifier()) &&
                        matches(aliases, expected.value(), actual.value()) &&
                        matches(aliases, expected.reference(), actual.reference()) -> entry;
                default -> null;
            };

            if (match != null) {
                matches.add(match);
            }
        }

        checkState(matches.size() <= 1, "Ambiguous expression %s matches multiple assignments: %s", expression, matches);

        return matches.stream().map(Map.Entry::getKey).findFirst();
    }

    private boolean matches(SymbolAliases aliases, Symbol expected, Symbol actual)
    {
        return aliases.get(expected.name()).name().equals(actual.name());
    }

    @Override
    public String toString()
    {
        return expression.toString();
    }
}
