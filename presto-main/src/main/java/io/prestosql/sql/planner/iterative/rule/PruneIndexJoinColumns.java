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

import com.google.common.collect.ImmutableSet;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.IndexJoinNode;
import io.prestosql.sql.planner.plan.PlanNode;

import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.sql.planner.iterative.rule.Util.restrictChildOutputs;
import static io.prestosql.sql.planner.plan.Patterns.indexJoin;

public class PruneIndexJoinColumns
        extends ProjectOffPushDownRule<IndexJoinNode>
{
    public PruneIndexJoinColumns()
    {
        super(indexJoin());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(Context context, IndexJoinNode indexJoinNode, Set<Symbol> referencedOutputs)
    {
        ImmutableSet.Builder<Symbol> probeInputs = ImmutableSet.<Symbol>builder()
                .addAll(referencedOutputs)
                .addAll(indexJoinNode.getCriteria().stream()
                        .map(IndexJoinNode.EquiJoinClause::getProbe)
                        .collect(toImmutableList()));
        indexJoinNode.getProbeHashSymbol().ifPresent(probeInputs::add);

        ImmutableSet.Builder<Symbol> indexInputs = ImmutableSet.<Symbol>builder()
                .addAll(referencedOutputs)
                .addAll(indexJoinNode.getCriteria().stream()
                        .map(IndexJoinNode.EquiJoinClause::getIndex)
                        .collect(toImmutableList()));
        indexJoinNode.getIndexHashSymbol().ifPresent(indexInputs::add);

        return restrictChildOutputs(context.getIdAllocator(), indexJoinNode, probeInputs.build(), indexInputs.build());
    }
}
