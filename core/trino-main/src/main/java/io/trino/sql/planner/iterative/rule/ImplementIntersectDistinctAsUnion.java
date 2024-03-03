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
package io.trino.sql.planner.iterative.rule;

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.IntersectNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GenericLiteral;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.planner.plan.Patterns.Intersect.distinct;
import static io.trino.sql.planner.plan.Patterns.intersect;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static java.util.Objects.requireNonNull;

/**
 * Converts INTERSECT DISTINCT queries into UNION ALL..GROUP BY...WHERE
 * E.g.:
 * <pre>
 *     SELECT a FROM foo
 *     INTERSECT DISTINCT
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
 *     WHERE foo_count >= 1 AND bar_count >= 1;
 * </pre>
 */
public class ImplementIntersectDistinctAsUnion
        implements Rule<IntersectNode>
{
    private static final Pattern<IntersectNode> PATTERN = intersect()
            .with(distinct().equalTo(true));

    private final Metadata metadata;

    public ImplementIntersectDistinctAsUnion(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<IntersectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(IntersectNode node, Captures captures, Context context)
    {
        SetOperationNodeTranslator translator = new SetOperationNodeTranslator(context.getSession(), metadata, context.getSymbolAllocator(), context.getIdAllocator());
        SetOperationNodeTranslator.TranslationResult result = translator.makeSetContainmentPlanForDistinct(node);

        // intersect predicate: the row must be present in every source
        Expression predicate = and(result.getCountSymbols().stream()
                .map(symbol -> new ComparisonExpression(GREATER_THAN_OR_EQUAL, symbol.toSymbolReference(), new GenericLiteral("BIGINT", "1")))
                .collect(toImmutableList()));

        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new FilterNode(context.getIdAllocator().getNextId(), result.getPlanNode(), predicate),
                        Assignments.identity(node.getOutputSymbols())));
    }
}
