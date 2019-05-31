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

import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.IntersectNode;
import io.prestosql.sql.planner.plan.ProjectNode;

import static io.prestosql.sql.ExpressionUtils.and;
import static io.prestosql.sql.planner.plan.Patterns.intersect;

/**
 * Converts INTERSECT queries into UNION ALL..GROUP BY...WHERE
 * E.g.:
 * <pre>
 *     SELECT a FROM foo
 *     INTERSECT
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
public class ImplementIntersectAsUnion
        implements Rule<IntersectNode>
{
    private static final Pattern<IntersectNode> PATTERN = intersect();

    @Override
    public Pattern<IntersectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(IntersectNode node, Captures captures, Context context)
    {
        SetOperationNodeTranslator translator = new SetOperationNodeTranslator(context.getSymbolAllocator(), context.getIdAllocator());
        SetOperationNodeTranslator.TranslationResult result = translator.makeSetContainmentPlan(node);

        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new FilterNode(context.getIdAllocator().getNextId(), result.getPlanNode(), and(result.getPresentExpressions())),
                        Assignments.identity(node.getOutputSymbols())));
    }
}
