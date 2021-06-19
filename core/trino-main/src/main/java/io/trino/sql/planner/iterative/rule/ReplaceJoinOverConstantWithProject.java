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
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isAtLeastScalar;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isAtMost;
import static io.trino.sql.planner.plan.Patterns.join;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;

/**
 * This rule transforms plans with join where one of the sources is
 * a single-row ValuesNode, and the join condition is `on true`.
 * The JoinNode is replaced with the other join source and a projection
 * which appends constant values from the ValuesNode.
 * This rule is similar to ReplaceRedundantJoinWithSource.
 * <p>
 * Note 1: When transforming an outer join (LEFT, RIGHT or FULL), and an
 * outer source is a single row ValuesNode, it is checked that the other
 * source is not empty. If it is possibly empty, the transformation cannot
 * be done, because the result of the transformation would be possibly
 * empty, while the single constant row should be preserved on output.
 * <p>
 * Note 2: The transformation is valid when the ValuesNode contains
 * non-deterministic expressions. This is because any expression from
 * the ValuesNode can only be used once. Assignments.Builder deduplicates
 * them in case when the JoinNode produces any of the input symbols
 * more than once.
 * <p>
 * Note 3: The transformation is valid when the ValuesNode contains
 * expressions using correlation symbols. They are constant from the
 * perspective of the transformed plan.
 * <p>
 * Transforms:
 * <pre>
 * - join (on true), layout: (a, b, c)
 *    - source (a)
 *    - values
 *      b <- expr1
 *      c <- expr2
 * </pre>
 * into:
 * <pre>
 * - project (a <- a, b <- expr1, c <- expr2)
 *     - source (a)
 * </pre>
 */
public class ReplaceJoinOverConstantWithProject
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join()
            .matching(ReplaceJoinOverConstantWithProject::isUnconditional);

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        if (isAtMost(node.getLeft(), context.getLookup(), 0) || isAtMost(node.getRight(), context.getLookup(), 0)) {
            return Result.empty();
        }

        PlanNode left = context.getLookup().resolve(node.getLeft());
        PlanNode right = context.getLookup().resolve(node.getRight());
        boolean canInlineLeftSource = canInlineJoinSource(left);
        boolean canInlineRightSource = canInlineJoinSource(right);

        switch (node.getType()) {
            case INNER:
                if (canInlineLeftSource) {
                    return Result.ofPlanNode(appendProjection(right, node.getRightOutputSymbols(), left, node.getLeftOutputSymbols(), context.getIdAllocator()));
                }
                if (canInlineRightSource) {
                    return Result.ofPlanNode(appendProjection(left, node.getLeftOutputSymbols(), right, node.getRightOutputSymbols(), context.getIdAllocator()));
                }
                break;
            case LEFT:
                if (canInlineLeftSource && isAtLeastScalar(right, context.getLookup())) {
                    return Result.ofPlanNode(appendProjection(right, node.getRightOutputSymbols(), left, node.getLeftOutputSymbols(), context.getIdAllocator()));
                }
                if (canInlineRightSource) {
                    return Result.ofPlanNode(appendProjection(left, node.getLeftOutputSymbols(), right, node.getRightOutputSymbols(), context.getIdAllocator()));
                }
                break;
            case RIGHT:
                if (canInlineLeftSource) {
                    return Result.ofPlanNode(appendProjection(right, node.getRightOutputSymbols(), left, node.getLeftOutputSymbols(), context.getIdAllocator()));
                }
                if (canInlineRightSource && isAtLeastScalar(left, context.getLookup())) {
                    return Result.ofPlanNode(appendProjection(left, node.getLeftOutputSymbols(), right, node.getRightOutputSymbols(), context.getIdAllocator()));
                }
                break;
            case FULL:
                if (canInlineLeftSource && isAtLeastScalar(right, context.getLookup())) {
                    return Result.ofPlanNode(appendProjection(right, node.getRightOutputSymbols(), left, node.getLeftOutputSymbols(), context.getIdAllocator()));
                }
                if (canInlineRightSource && isAtLeastScalar(left, context.getLookup())) {
                    return Result.ofPlanNode(appendProjection(left, node.getLeftOutputSymbols(), right, node.getRightOutputSymbols(), context.getIdAllocator()));
                }
        }

        return Result.empty();
    }

    private static boolean isUnconditional(JoinNode joinNode)
    {
        return joinNode.getCriteria().isEmpty() &&
                (joinNode.getFilter().isEmpty() || joinNode.getFilter().get().equals(TRUE_LITERAL));
    }

    private boolean canInlineJoinSource(PlanNode source)
    {
        // the case of a source producing no outputs is handled by ReplaceRedundantJoinWithSource rule
        return isSingleConstantRow(source) && !source.getOutputSymbols().isEmpty();
    }

    private boolean isSingleConstantRow(PlanNode node)
    {
        if (!(node instanceof ValuesNode)) {
            return false;
        }

        ValuesNode values = (ValuesNode) node;
        if (values.getRowCount() != 1) {
            return false;
        }

        if (values.getRows().isEmpty()) {
            return true;
        }

        Expression row = getOnlyElement(values.getRows().get());

        return row instanceof Row;
    }

    private ProjectNode appendProjection(PlanNode source, List<Symbol> sourceOutputs, PlanNode constantSource, List<Symbol> constantOutputs, PlanNodeIdAllocator idAllocator)
    {
        ValuesNode values = (ValuesNode) constantSource;
        Row row = (Row) getOnlyElement(values.getRows().get());

        Map<Symbol, Expression> mapping = new HashMap<>();
        for (int i = 0; i < values.getOutputSymbols().size(); i++) {
            mapping.put(values.getOutputSymbols().get(i), row.getItems().get(i));
        }

        Assignments.Builder assignments = Assignments.builder()
                .putIdentities(sourceOutputs);

        constantOutputs.stream()
                .forEach(symbol -> assignments.put(symbol, mapping.get(symbol)));

        return new ProjectNode(idAllocator.getNextId(), source, assignments.build());
    }
}
