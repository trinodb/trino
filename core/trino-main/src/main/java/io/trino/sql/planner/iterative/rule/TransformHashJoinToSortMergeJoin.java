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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SortMergeJoinNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.SystemSessionProperties.isPreferSortMergeJoin;
import static io.trino.sql.planner.plan.Patterns.join;

public class TransformHashJoinToSortMergeJoin
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join()
            .matching(node -> !node.getCriteria().isEmpty() && node.getDynamicFilters().isEmpty()
                    && node.getDistributionType().stream().allMatch(distributionType -> distributionType.equals(JoinNode.DistributionType.PARTITIONED)));

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isPreferSortMergeJoin(session);
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        if (node.getFilter().isPresent()) {
            PlanNode smj = new SortMergeJoinNode(context.getIdAllocator().getNextId(), node.getType(),
                    node.getLeft(), node.getRight(), node.getCriteria(),
                    node.getLeft().getOutputSymbols(), node.getRight().getOutputSymbols(),
                    Optional.empty(), node.getDistributionType(), ImmutableMap.of(), true, true);
            PlanNode filter = new FilterNode(context.getIdAllocator().getNextId(), smj, node.getFilter().get());
            List<Symbol> output = new ArrayList<>();
            output.addAll(node.getLeftOutputSymbols());
            output.addAll(node.getRightOutputSymbols());

            PlanNode project = new ProjectNode(context.getIdAllocator().getNextId(), filter,
                    Assignments.identity(output));
            return Result.ofPlanNode(project);
        }
        else {
            PlanNode smj = new SortMergeJoinNode(context.getIdAllocator().getNextId(), node.getType(),
                    node.getLeft(), node.getRight(), node.getCriteria(),
                    node.getLeftOutputSymbols(), node.getRightOutputSymbols(),
                    Optional.empty(), node.getDistributionType(), ImmutableMap.of(), true, true);
            return Result.ofPlanNode(smj);
        }
    }
}
