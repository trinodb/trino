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
import io.trino.sql.ir.Constant;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.List;

import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isEmpty;
import static io.trino.sql.planner.plan.Patterns.join;

/**
 * This rule is complementary to RemoveRedundantJoin.
 * It transforms plans with outer join where outer source of the join is not empty,
 * and the other source is empty. Outer join is replaced with the outer source and
 * a project which appends nulls as the empty source's outputs.
 */
public class ReplaceRedundantJoinWithProject
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
        Lookup lookup = context.getLookup();
        PlanNode left = node.getLeft();
        PlanNode right = node.getRight();

        return switch (node.getType()) {
            case INNER -> Result.empty();
            case LEFT -> !isEmpty(left, lookup) && isEmpty(right, lookup) ?
                    Result.ofPlanNode(appendNulls(
                            left,
                            node.getLeftOutputSymbols(),
                            node.getRightOutputSymbols(),
                            context.getIdAllocator()
                    )) :
                    Result.empty();
            case RIGHT -> isEmpty(left, lookup) && !isEmpty(right, lookup) ?
                    Result.ofPlanNode(appendNulls(
                            right,
                            node.getRightOutputSymbols(),
                            node.getLeftOutputSymbols(),
                            context.getIdAllocator()
                    )) :
                    Result.empty();
            case FULL -> {
                if (isEmpty(left, lookup) && !isEmpty(right, lookup)) {
                    yield Result.ofPlanNode(appendNulls(
                            right,
                            node.getRightOutputSymbols(),
                            node.getLeftOutputSymbols(),
                            context.getIdAllocator()));
                }
                if (!isEmpty(left, lookup) && isEmpty(right, lookup)) {
                    yield Result.ofPlanNode(appendNulls(
                            left,
                            node.getLeftOutputSymbols(),
                            node.getRightOutputSymbols(),
                            context.getIdAllocator()));
                }
                yield Result.empty();
            }
        };
    }

    private static ProjectNode appendNulls(PlanNode source, List<Symbol> sourceOutputs, List<Symbol> nullSymbols, PlanNodeIdAllocator idAllocator)
    {
        Assignments.Builder assignments = Assignments.builder()
                .putIdentities(sourceOutputs);
        nullSymbols
                .forEach(symbol -> assignments.put(symbol, new Constant(symbol.type(), null)));

        return new ProjectNode(idAllocator.getNextId(), source, assignments.build());
    }
}
