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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.JoinNode.EquiJoinClause;
import io.prestosql.sql.planner.plan.JoinNode.Type;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.CoalesceExpression;
import io.prestosql.sql.tree.Expression;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.plan.JoinNode.Type.FULL;
import static io.prestosql.sql.planner.plan.Patterns.join;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;

public class SimplifyCoalesceExpressionOverJoin
        implements Rule<ProjectNode>
{
    private static final Capture<JoinNode> CHILD = newCapture();

    private static final Pattern<ProjectNode> PATTERN = project()
            .matching(projectNode -> projectNode.getAssignments().getExpressions().stream()
                    .anyMatch(expression -> expression instanceof CoalesceExpression))
            .with(source().matching(
                    join().capturedAs(CHILD).matching(
                            joinNode -> !joinNode.getType().equals(FULL))));

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ProjectNode node, Captures captures, Context context)
    {
        JoinNode joinNode = captures.get(CHILD);
        ImmutableMap.Builder<Set<Expression>, Expression> builder = ImmutableMap.builder();

        Function<EquiJoinClause, Expression> joinClauseMapper = getEquiJoinClauseMapper(joinNode.getType());

        for (JoinNode.EquiJoinClause joinClause : joinNode.getCriteria()) {
            builder.put(ImmutableSet.of(joinClause.getLeft().toSymbolReference(), joinClause.getRight().toSymbolReference()), joinClauseMapper.apply(joinClause));
        }
        Map<Set<Expression>, Expression> coalesceExpressionMapper = builder.build();

        Assignments translatedAssignments = node.getAssignments().rewrite(expression -> {
            if (expression instanceof CoalesceExpression) {
                CoalesceExpression coalesceExpression = (CoalesceExpression) expression;
                return coalesceExpressionMapper.getOrDefault(ImmutableSet.copyOf(coalesceExpression.getOperands()), coalesceExpression);
            }
            return expression;
        });

        if (node.getAssignments().equals(translatedAssignments)) {
            return Result.empty();
        }
        return Result.ofPlanNode(new ProjectNode(node.getId(), node.getSource(), translatedAssignments));
    }

    private Function<EquiJoinClause, Expression> getEquiJoinClauseMapper(Type type)
    {
        switch (type) {
            case INNER:
            case LEFT:
                return (joinClause) -> joinClause.getLeft().toSymbolReference();
            case RIGHT:
                return (joinClause) -> joinClause.getRight().toSymbolReference();
            default:
                throw new IllegalStateException("No mapper defined for join type: " + type);
        }
    }
}
