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

import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.Optional;
import java.util.Set;

import static io.trino.sql.planner.plan.Patterns.join;
import static io.trino.util.MoreLists.filteredCopy;

/**
 * Joins support output symbol selection, so absorb any project-off into the node.
 */
public class PruneJoinColumns
        extends ProjectOffPushDownRule<JoinNode>
{
    public PruneJoinColumns()
    {
        super(join());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(Context context, JoinNode joinNode, Set<Symbol> referencedOutputs)
    {
        return Optional.of(
                new JoinNode(
                        joinNode.getId(),
                        joinNode.getType(),
                        joinNode.getLeft(),
                        joinNode.getRight(),
                        joinNode.getCriteria(),
                        filteredCopy(joinNode.getLeftOutputSymbols(), referencedOutputs::contains),
                        filteredCopy(joinNode.getRightOutputSymbols(), referencedOutputs::contains),
                        joinNode.isMaySkipOutputDuplicates(),
                        joinNode.getFilter(),
                        joinNode.getDistributionType(),
                        joinNode.isSpillable(),
                        joinNode.getDynamicFilters(),
                        joinNode.getReorderJoinStatsAndCost()));
    }
}
