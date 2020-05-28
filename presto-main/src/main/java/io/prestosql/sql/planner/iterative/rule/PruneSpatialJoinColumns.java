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
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.SpatialJoinNode;

import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.sql.planner.plan.Patterns.spatialJoin;

/**
 * Spatial joins support output symbol selection, so absorb any project-off into the node.
 */
public class PruneSpatialJoinColumns
        extends ProjectOffPushDownRule<SpatialJoinNode>
{
    public PruneSpatialJoinColumns()
    {
        super(spatialJoin());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(Context context, SpatialJoinNode spatialJoinNode, Set<Symbol> referencedOutputs)
    {
        return Optional.of(new SpatialJoinNode(
                spatialJoinNode.getId(),
                spatialJoinNode.getType(),
                spatialJoinNode.getLeft(),
                spatialJoinNode.getRight(),
                spatialJoinNode.getOutputSymbols().stream()
                        .filter(referencedOutputs::contains)
                        .collect(toImmutableList()),
                spatialJoinNode.getFilter(),
                spatialJoinNode.getLeftPartitionSymbol(),
                spatialJoinNode.getRightPartitionSymbol(),
                spatialJoinNode.getKdbTree()));
    }
}
