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

import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.GroupIdNode;
import io.prestosql.sql.planner.plan.PlanNode;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.sql.planner.plan.Patterns.groupId;

/**
 * This rule prunes GroupIdNode's aggregationArguments.
 * <p>
 * Transforms:
 * <pre>
 * - Project (a, key_1, key_2, group_id)
 *      - GroupId
 *          grouping sets: ((key_1), (key_2))
 *          aggregation arguments: (a, b)
 *          group id symbol: group_id
 *           - Source (a, b, key_1, key_2)
 * </pre>
 * Into:
 * <pre>
 * - Project (a, key_1, key_2, group_id)
 *      - GroupId
 *          grouping sets: ((key_1), (key_2))
 *          aggregation arguments: (a)
 *          group id symbol: group_id
 *           - Source (a, b, key_1, key_2)
 * </pre>
 * Note: this rule does not prune any grouping symbols.
 * Currently, GroupIdNode is only used in regard to AggregationNode.
 * The presence of an AggregationNode in the plan ensures that
 * the grouping symbols are referenced.
 * This rule could be extended to prune grouping symbols.
 * <p>
 * Note: after pruning an aggregation argument, the child node
 * of the GroupIdNode becomes eligible for symbol pruning.
 * That is performed by the rule PruneGroupIdSourceColumns.
 * </p>
 */
public class PruneGroupIdColumns
        extends ProjectOffPushDownRule<GroupIdNode>
{
    public PruneGroupIdColumns()
    {
        super(groupId());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(
            Context context,
            GroupIdNode groupIdNode,
            Set<Symbol> referencedOutputs)
    {
        List<Symbol> prunedAggregationArguments = groupIdNode.getAggregationArguments().stream()
                .filter(referencedOutputs::contains)
                .collect(toImmutableList());
        if (prunedAggregationArguments.size() == groupIdNode.getAggregationArguments().size()) {
            return Optional.empty();
        }

        return Optional.of(new GroupIdNode(
                groupIdNode.getId(),
                groupIdNode.getSource(),
                groupIdNode.getGroupingSets(),
                groupIdNode.getGroupingColumns(),
                prunedAggregationArguments,
                groupIdNode.getGroupIdSymbol()));
    }
}
