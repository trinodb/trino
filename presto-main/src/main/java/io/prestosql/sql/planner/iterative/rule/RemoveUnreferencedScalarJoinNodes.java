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

import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.iterative.TraitSet;
import io.prestosql.sql.planner.iterative.trait.CardinalityTrait;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.tree.Expression;

import java.util.Optional;

import static io.prestosql.sql.planner.iterative.trait.CardinalityTrait.CARDINALITY;
import static io.prestosql.sql.planner.plan.Patterns.join;

public class RemoveUnreferencedScalarJoinNodes
        implements Rule<JoinNode>
{
    @Override
    public Pattern<JoinNode> getPattern()
    {
        return join()
                .matching(join -> join.getType() == JoinNode.Type.INNER)
                .matching(join -> join.getCriteria().isEmpty());
    }

    @Override
    public Result apply(JoinNode node, Captures captures, TraitSet traitSet, Context context)
    {
        PlanNode left = node.getLeft();
        PlanNode right = node.getRight();

        if (isUnreferencedScalar(left, context.getLookup())) {
            return Result.ofPlanNode(optionallyWrapWithFilterNode(node.getFilter(), right, context));
        }

        if (isUnreferencedScalar(right, context.getLookup())) {
            return Result.ofPlanNode(optionallyWrapWithFilterNode(node.getFilter(), left, context));
        }

        return Result.empty();
    }

    public PlanNode optionallyWrapWithFilterNode(Optional<Expression> optionalPredicate, PlanNode planNode, Context context)
    {
        return optionalPredicate.map(predicate ->
                (PlanNode) new FilterNode(
                        context.getIdAllocator().getNextId(),
                        planNode,
                        predicate))
                .orElse(planNode);
    }

    private boolean isUnreferencedScalar(PlanNode planNode, Lookup lookup)
    {
        Optional<CardinalityTrait> cardinality = lookup.resolveTrait(planNode, CARDINALITY);
        return planNode.getOutputSymbols().isEmpty() && cardinality.isPresent() && cardinality.get().isScalar();
    }
}
