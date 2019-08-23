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
import com.google.common.collect.Streams;
import io.prestosql.sql.planner.DeterminismEvaluator;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.Expression;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.sql.planner.SymbolsExtractor.extractUnique;

/**
 * Utility class for pushing simple projections through join so that joins can participate
 * in cross join elimination or join reordering.
 */
public final class PushProjectionThroughJoin
{
    public static PlanNode pushProjectionThroughJoin(ProjectNode projectNode, Lookup lookup, PlanNodeIdAllocator planNodeIdAllocator)
    {
        if (!projectNode.getAssignments().getExpressions().stream().allMatch(DeterminismEvaluator::isDeterministic)) {
            return projectNode;
        }

        PlanNode child = lookup.resolve(projectNode.getSource());
        if (!(child instanceof JoinNode)) {
            return projectNode;
        }

        JoinNode joinNode = (JoinNode) child;
        PlanNode leftChild = joinNode.getLeft();
        PlanNode rightChild = joinNode.getRight();

        Assignments.Builder leftAssignmentsBuilder = Assignments.builder();
        Assignments.Builder rightAssignmentsBuilder = Assignments.builder();
        for (Map.Entry<Symbol, Expression> assignment : projectNode.getAssignments().entrySet()) {
            Expression expression = assignment.getValue();
            Set<Symbol> symbols = extractUnique(expression);
            if (leftChild.getOutputSymbols().containsAll(symbols)) {
                // expression is satisfied with left child symbols
                leftAssignmentsBuilder.put(assignment.getKey(), expression);
            }
            else if (rightChild.getOutputSymbols().containsAll(symbols)) {
                // expression is satisfied with right child symbols
                rightAssignmentsBuilder.put(assignment.getKey(), expression);
            }
            else {
                // expression is using symbols from both join sides
                return projectNode;
            }
        }

        // add projections for symbols required by the join itself
        Set<Symbol> joinRequiredSymbols = getJoinRequiredSymbols(joinNode);
        for (Symbol requiredSymbol : joinRequiredSymbols) {
            if (leftChild.getOutputSymbols().contains(requiredSymbol)) {
                leftAssignmentsBuilder.putIdentity(requiredSymbol);
            }
            else {
                checkState(rightChild.getOutputSymbols().contains(requiredSymbol));
                rightAssignmentsBuilder.putIdentity(requiredSymbol);
            }
        }

        Assignments leftAssignments = leftAssignmentsBuilder.build();
        Assignments rightAssignments = rightAssignmentsBuilder.build();
        List<Symbol> outputSymbols = Streams.concat(leftAssignments.getOutputs().stream(), rightAssignments.getOutputs().stream())
                .filter(symbol -> projectNode.getOutputSymbols().contains(symbol))
                .collect(toImmutableList());

        return new JoinNode(
                joinNode.getId(),
                joinNode.getType(),
                new ProjectNode(planNodeIdAllocator.getNextId(), leftChild, leftAssignments),
                new ProjectNode(planNodeIdAllocator.getNextId(), rightChild, rightAssignments),
                joinNode.getCriteria(),
                outputSymbols,
                joinNode.getFilter(),
                joinNode.getLeftHashSymbol(),
                joinNode.getRightHashSymbol(),
                joinNode.getDistributionType(),
                joinNode.isSpillable(),
                joinNode.getDynamicFilters());
    }

    private static Set<Symbol> getJoinRequiredSymbols(JoinNode node)
    {
        return Streams.concat(
                node.getCriteria().stream().map(JoinNode.EquiJoinClause::getLeft),
                node.getCriteria().stream().map(JoinNode.EquiJoinClause::getRight),
                node.getFilter().map(SymbolsExtractor::extractUnique).orElse(ImmutableSet.of()).stream(),
                node.getLeftHashSymbol().map(ImmutableSet::of).orElse(ImmutableSet.of()).stream(),
                node.getRightHashSymbol().map(ImmutableSet::of).orElse(ImmutableSet.of()).stream())
                .collect(toImmutableSet());
    }

    private PushProjectionThroughJoin() {}
}
