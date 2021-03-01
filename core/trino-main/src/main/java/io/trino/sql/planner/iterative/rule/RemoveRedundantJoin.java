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
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.Type;
import io.trino.sql.planner.plan.ValuesNode;

import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isAtMost;
import static io.trino.sql.planner.plan.Patterns.join;

public class RemoveRedundantJoin
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
        if (canRemoveJoin(
                node.getType(),
                isAtMost(node.getLeft(), context.getLookup(), 0),
                isAtMost(node.getRight(), context.getLookup(), 0))) {
            return Result.ofPlanNode(
                    new ValuesNode(
                            context.getIdAllocator().getNextId(),
                            node.getOutputSymbols(),
                            ImmutableList.of()));
        }

        return Result.empty();
    }

    private boolean canRemoveJoin(Type joinType, boolean isLeftSourceEmpty, boolean isRightSourceEmpty)
    {
        switch (joinType) {
            case INNER:
                return isLeftSourceEmpty || isRightSourceEmpty;
            case LEFT:
                return isLeftSourceEmpty;
            case RIGHT:
                return isRightSourceEmpty;
            case FULL:
                return isLeftSourceEmpty && isRightSourceEmpty;
        }

        return false;
    }
}
