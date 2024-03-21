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

import com.google.common.collect.ImmutableList;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ExceptNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.ProjectNode;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.planner.plan.Patterns.Except.distinct;
import static io.trino.sql.planner.plan.Patterns.except;
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
    private static final Pattern<ExceptNode> PATTERN = except()
            .with(distinct().equalTo(true));

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
        SetOperationNodeTranslator translator = new SetOperationNodeTranslator(context.getSession(), metadata, context.getSymbolAllocator(), context.getIdAllocator());
        SetOperationNodeTranslator.TranslationResult result = translator.makeSetContainmentPlanForDistinct(node);

        // except predicate: the row must be present in the first source and absent in all the other sources
        ImmutableList.Builder<Expression> predicatesBuilder = ImmutableList.builder();
        predicatesBuilder.add(new Comparison(GREATER_THAN_OR_EQUAL, result.getCountSymbols().get(0).toSymbolReference(), new Constant(BIGINT, 1L)));
        for (int i = 1; i < node.getSources().size(); i++) {
            predicatesBuilder.add(new Comparison(EQUAL, result.getCountSymbols().get(i).toSymbolReference(), new Constant(BIGINT, 0L)));
        }
        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new FilterNode(context.getIdAllocator().getNextId(), result.getPlanNode(), and(predicatesBuilder.build())),
                        Assignments.identity(node.getOutputSymbols())));
    }
}
