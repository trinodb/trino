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

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionFormatter;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ExpressionMatcher
        implements RvalueMatcher
{
    private final String sql;
    private final Expression expression;

    ExpressionMatcher(Expression expression)
    {
        this.expression = requireNonNull(expression, "expression is null");
        this.sql = ExpressionFormatter.formatExpression(expression);
    }

    @Override
    public Optional<Symbol> getAssignedSymbol(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        Optional<Symbol> result = Optional.empty();
        ImmutableList.Builder<Expression> matchesBuilder = ImmutableList.builder();
        Map<Symbol, Expression> assignments = getAssignments(node);

        if (assignments == null) {
            return result;
        }

        ExpressionVerifier verifier = new ExpressionVerifier(symbolAliases);

        for (Map.Entry<Symbol, Expression> assignment : assignments.entrySet()) {
            if (verifier.process(assignment.getValue(), expression)) {
                result = Optional.of(assignment.getKey());
                matchesBuilder.add(assignment.getValue());
            }
        }

        List<Expression> matches = matchesBuilder.build();
        checkState(matches.size() < 2, "Ambiguous expression %s matches multiple assignments: %s", expression, matches);
        return result;
    }

    private static Map<Symbol, Expression> getAssignments(PlanNode node)
    {
        if (node instanceof ProjectNode projectNode) {
            return projectNode.getAssignments().assignments();
        }
        return null;
    }

    @Override
    public String toString()
    {
        return sql;
    }
}
