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
import com.google.common.collect.Streams;
import io.trino.Session;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.Expression;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.SymbolsExtractor.extractUnique;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;

/**
 * Utility class for pushing projections through inner join so that joins are not separated
 * by a project node and can participate in cross join elimination or join reordering.
 */
public final class PushProjectionThroughJoin
{
    public static Optional<PlanNode> pushProjectionThroughJoin(
            PlannerContext plannerContext,
            ProjectNode projectNode,
            Lookup lookup,
            PlanNodeIdAllocator planNodeIdAllocator,
            Session session,
            TypeAnalyzer typeAnalyzer,
            TypeProvider types)
    {
        if (!projectNode.getAssignments().getExpressions().stream().allMatch(expression -> isDeterministic(expression, plannerContext.getMetadata()))) {
            return Optional.empty();
        }

        PlanNode child = lookup.resolve(projectNode.getSource());
        if (!(child instanceof JoinNode)) {
            return Optional.empty();
        }

        JoinNode joinNode = (JoinNode) child;
        PlanNode leftChild = joinNode.getLeft();
        PlanNode rightChild = joinNode.getRight();

        if (joinNode.getType() != INNER) {
            return Optional.empty();
        }

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
                return Optional.empty();
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
        List<Symbol> leftOutputSymbols = leftAssignments.getOutputs().stream()
                .filter(ImmutableSet.copyOf(projectNode.getOutputSymbols())::contains)
                .collect(toImmutableList());
        List<Symbol> rightOutputSymbols = rightAssignments.getOutputs().stream()
                .filter(ImmutableSet.copyOf(projectNode.getOutputSymbols())::contains)
                .collect(toImmutableList());

        return Optional.of(new JoinNode(
                joinNode.getId(),
                joinNode.getType(),
                inlineProjections(
                        plannerContext,
                        new ProjectNode(planNodeIdAllocator.getNextId(), leftChild, leftAssignments),
                        lookup,
                        session,
                        typeAnalyzer,
                        types),
                inlineProjections(
                        plannerContext,
                        new ProjectNode(planNodeIdAllocator.getNextId(), rightChild, rightAssignments),
                        lookup,
                        session,
                        typeAnalyzer,
                        types),
                joinNode.getCriteria(),
                leftOutputSymbols,
                rightOutputSymbols,
                joinNode.isMaySkipOutputDuplicates(),
                joinNode.getFilter(),
                joinNode.getLeftHashSymbol(),
                joinNode.getRightHashSymbol(),
                joinNode.getDistributionType(),
                joinNode.isSpillable(),
                joinNode.getDynamicFilters(),
                joinNode.getReorderJoinStatsAndCost()));
    }

    private static PlanNode inlineProjections(
            PlannerContext plannerContext,
            ProjectNode parentProjection,
            Lookup lookup,
            Session session,
            TypeAnalyzer typeAnalyzer,
            TypeProvider types)
    {
        PlanNode child = lookup.resolve(parentProjection.getSource());
        if (!(child instanceof ProjectNode)) {
            return parentProjection;
        }
        ProjectNode childProjection = (ProjectNode) child;

        return InlineProjections.inlineProjections(plannerContext, parentProjection, childProjection, session, typeAnalyzer, types)
                .map(node -> inlineProjections(plannerContext, node, lookup, session, typeAnalyzer, types))
                .orElse(parentProjection);
    }

    private static Set<Symbol> getJoinRequiredSymbols(JoinNode node)
    {
        // extract symbols required by the join itself
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
