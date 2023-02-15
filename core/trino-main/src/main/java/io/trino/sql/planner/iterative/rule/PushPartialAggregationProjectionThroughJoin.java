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
import com.google.common.collect.Streams;
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.isPushPartialAggregationThroughJoin;
import static io.trino.sql.planner.iterative.rule.PushPartialAggregationThroughJoin.getJoinRequiredSymbols;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.join;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;

public class PushPartialAggregationProjectionThroughJoin
        implements Rule<AggregationNode>
{
    private static final Capture<ProjectNode> PROJECT_NODE = Capture.newCapture();
    private static final Capture<JoinNode> JOIN_NODE = Capture.newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(PushPartialAggregationThroughJoin::isSupportedAggregationNode)
            .with(source().matching(project().capturedAs(PROJECT_NODE).with(source().matching(join().capturedAs(JOIN_NODE)))));

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isPushPartialAggregationThroughJoin(session);
    }

    @Override
    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
    {
        JoinNode joinNode = captures.get(JOIN_NODE);
        ProjectNode projectNode = captures.get(PROJECT_NODE);

        if (joinNode.getType() != JoinNode.Type.INNER) {
            return Result.empty();
        }

        Assignments projections = projectNode.getAssignments()
                .filter(symbol -> !projectNode.getAssignments().isIdentity(symbol));
        Set<Symbol> projectionInputs = SymbolsExtractor.extractUnique(projections.getExpressions());
        Set<Symbol> aggregationInputs = Streams.concat(
                        aggregationNode.getGroupingKeys().stream(),
                        aggregationNode.getHashSymbol().stream(),
                        aggregationNode.getAggregations().values().stream()
                                .flatMap(aggregation -> SymbolsExtractor.extractUnique(aggregation).stream()))
                .collect(toImmutableSet());

        if (joinNode.getLeft().getOutputSymbols().containsAll(projectionInputs)) {
            return Result.ofPlanNode(aggregationNode.replaceChildren(ImmutableList.of(pushProjectionToLeftChild(joinNode, projections, aggregationInputs, context))));
        }
        if (joinNode.getRight().getOutputSymbols().containsAll(projectionInputs)) {
            return Result.ofPlanNode(aggregationNode.replaceChildren(ImmutableList.of(pushProjectionToRightChild(joinNode, projections, aggregationInputs, context))));
        }

        return Result.empty();
    }

    private PlanNode pushProjectionToLeftChild(JoinNode join, Assignments projections, Set<Symbol> requiredOutputs, Context context)
    {
        Set<Symbol> joinRequiredSymbols = getJoinRequiredSymbols(join);
        Set<Symbol> passThroughSymbols = join.getLeft().getOutputSymbols().stream()
                .filter(symbol -> joinRequiredSymbols.contains(symbol) || requiredOutputs.contains(symbol))
                .collect(toImmutableSet());

        Assignments assignments = Assignments.builder()
                .putAll(projections)
                .putIdentities(passThroughSymbols)
                .build();

        ProjectNode projectNode = new ProjectNode(
                context.getIdAllocator().getNextId(),
                join.getLeft(),
                assignments);

        return updateJoinNode(join, projectNode, join.getRight(), requiredOutputs);
    }

    private PlanNode pushProjectionToRightChild(JoinNode join, Assignments projections, Set<Symbol> requiredOutputs, Context context)
    {
        Set<Symbol> joinRequiredSymbols = getJoinRequiredSymbols(join);
        Set<Symbol> passThroughSymbols = join.getRight().getOutputSymbols().stream()
                .filter(symbol -> joinRequiredSymbols.contains(symbol) || requiredOutputs.contains(symbol))
                .collect(toImmutableSet());

        Assignments assignments = Assignments.builder()
                .putAll(projections)
                .putIdentities(passThroughSymbols)
                .build();

        ProjectNode projectNode = new ProjectNode(
                context.getIdAllocator().getNextId(),
                join.getRight(),
                assignments);

        return updateJoinNode(join, join.getLeft(), projectNode, requiredOutputs);
    }

    private JoinNode updateJoinNode(JoinNode join, PlanNode left, PlanNode right, Set<Symbol> requiredOutputs)
    {
        return new JoinNode(
                join.getId(),
                join.getType(),
                left,
                right,
                join.getCriteria(),
                left.getOutputSymbols().stream()
                        .filter(requiredOutputs::contains)
                        .collect(toImmutableList()),
                right.getOutputSymbols().stream()
                        .filter(requiredOutputs::contains)
                        .collect(toImmutableList()),
                join.isMaySkipOutputDuplicates(),
                join.getFilter(),
                join.getLeftHashSymbol(),
                join.getRightHashSymbol(),
                join.getDistributionType(),
                join.isSpillable(),
                join.getDynamicFilters(),
                join.getReorderJoinStatsAndCost());
    }
}
