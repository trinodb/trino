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

import com.google.common.collect.ImmutableSet;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.List;

import static io.trino.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isAtLeastScalar;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isAtMost;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isScalar;
import static io.trino.sql.planner.plan.Patterns.join;

/**
 * This rule transforms plans with joins, where:
 * - one of the sources is scalar and produces no output symbols.
 * In case of LEFT or RIGHT join, it has to be the inner source.
 * In case of FULL or INNER join, it can be either source.
 * - the other join source is at least scalar (not known to be empty).
 * <p>
 * The join is replaced with the other source and an optional
 * pruning projection.
 * <p>
 * Note: This rule does not transform plans where either join source
 * is empty. Such plans are transformed by RemoveRedundantJoin
 * and ReplaceRedundantJoinWithProject rules.
 */
public class ReplaceRedundantJoinWithSource
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join();

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

        boolean leftSourceScalarWithNoOutputs = node.getLeft().getOutputSymbols().isEmpty() && isScalar(node.getLeft(), context.getLookup());
        boolean rightSourceScalarWithNoOutputs = node.getRight().getOutputSymbols().isEmpty() && isScalar(node.getRight(), context.getLookup());

        switch (node.getType()) {
            case INNER:
                PlanNode source;
                List<Symbol> sourceOutputs;
                if (leftSourceScalarWithNoOutputs) {
                    source = node.getRight();
                    sourceOutputs = node.getRightOutputSymbols();
                }
                else if (rightSourceScalarWithNoOutputs) {
                    source = node.getLeft();
                    sourceOutputs = node.getLeftOutputSymbols();
                }
                else {
                    return Result.empty();
                }

                if (node.getFilter().isPresent()) {
                    source = new FilterNode(context.getIdAllocator().getNextId(), source, node.getFilter().get());
                }

                return Result.ofPlanNode(restrictOutputs(context.getIdAllocator(), source, ImmutableSet.copyOf(sourceOutputs)).orElse(source));
            case LEFT:
                if (rightSourceScalarWithNoOutputs) {
                    return Result.ofPlanNode(restrictOutputs(context.getIdAllocator(), node.getLeft(), ImmutableSet.copyOf(node.getLeftOutputSymbols()))
                            .orElse(node.getLeft()));
                }
                break;
            case RIGHT:
                if (leftSourceScalarWithNoOutputs) {
                    return Result.ofPlanNode(restrictOutputs(context.getIdAllocator(), node.getRight(), ImmutableSet.copyOf(node.getRightOutputSymbols()))
                            .orElse(node.getRight()));
                }
                break;
            case FULL:
                if (leftSourceScalarWithNoOutputs && isAtLeastScalar(node.getRight(), context.getLookup())) {
                    return Result.ofPlanNode(restrictOutputs(context.getIdAllocator(), node.getRight(), ImmutableSet.copyOf(node.getRightOutputSymbols()))
                            .orElse(node.getRight()));
                }
                if (rightSourceScalarWithNoOutputs && isAtLeastScalar(node.getLeft(), context.getLookup())) {
                    return Result.ofPlanNode(restrictOutputs(context.getIdAllocator(), node.getLeft(), ImmutableSet.copyOf(node.getLeftOutputSymbols()))
                            .orElse(node.getLeft()));
                }
        }

        return Result.empty();
    }
}
