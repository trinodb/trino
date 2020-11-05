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

import com.google.common.collect.ImmutableList;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.ExceptNode;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.GenericLiteral;

import static io.prestosql.sql.ExpressionUtils.and;
import static io.prestosql.sql.planner.plan.Patterns.except;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static java.util.Objects.requireNonNull;

/**
 * Converts EXCEPT DISTINCT queries into UNION ALL..GROUP BY...WHERE
 * E.g.:
 * <pre>
 *     SELECT a FROM foo
 *     EXCEPT DISTINCT
 *     SELECT x FROM bar
 * </pre>
 * =>
 * <pre>
 *     SELECT a
 *     FROM
 *     (
 *         SELECT a,
 *         COUNT(foo_marker) AS foo_count,
 *         COUNT(bar_marker) AS bar_count
 *         FROM
 *         (
 *             SELECT a, true as foo_marker, null as bar_marker
 *             FROM foo
 *             UNION ALL
 *             SELECT x, null as foo_marker, true as bar_marker
 *             FROM bar
 *         ) T1
 *     GROUP BY a
 *     ) T2
 *     WHERE foo_count >= 1 AND bar_count = 0;
 * </pre>
 */
public class ImplementExceptDistinctAsUnion
        implements Rule<ExceptNode>
{
    private static final Pattern<ExceptNode> PATTERN = except();
    private final Metadata metadata;

    public ImplementExceptDistinctAsUnion(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<ExceptNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ExceptNode node, Captures captures, Context context)
    {
        SetOperationNodeTranslator translator = new SetOperationNodeTranslator(metadata, context.getSymbolAllocator(), context.getIdAllocator());
        SetOperationNodeTranslator.TranslationResult result = translator.makeSetContainmentPlan(node);

        // except predicate: the row must be present in the first source and absent in all the other sources
        ImmutableList.Builder<Expression> predicatesBuilder = ImmutableList.builder();
        predicatesBuilder.add(new ComparisonExpression(GREATER_THAN_OR_EQUAL, result.getCountSymbols().get(0).toSymbolReference(), new GenericLiteral("BIGINT", "1")));
        for (int i = 1; i < node.getSources().size(); i++) {
            predicatesBuilder.add(new ComparisonExpression(EQUAL, result.getCountSymbols().get(i).toSymbolReference(), new GenericLiteral("BIGINT", "0")));
        }
        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new FilterNode(context.getIdAllocator().getNextId(), result.getPlanNode(), and(predicatesBuilder.build())),
                        Assignments.identity(node.getOutputSymbols())));
    }
}
