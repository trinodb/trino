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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.Rule.Context;
import io.trino.sql.planner.iterative.Rule.Result;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import org.assertj.core.util.VisibleForTesting;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static io.trino.SystemSessionProperties.isPushPartialAggregationThroughJoin;
import static io.trino.sql.planner.iterative.rule.PushProjectionThroughJoin.pushProjectionThroughJoin;
import static io.trino.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.join;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.lang.Double.isNaN;
import static java.util.Objects.requireNonNull;

public class PushPartialAggregationThroughJoin
{
    private static boolean isSupportedAggregationNode(AggregationNode aggregationNode)
    {
        // Don't split streaming aggregations
        if (aggregationNode.isStreamable()) {
            return false;
        }

        if (aggregationNode.getHashSymbol().isPresent()) {
            // TODO: add support for hash symbol in aggregation node
            return false;
        }
        return aggregationNode.getStep() == PARTIAL && aggregationNode.getGroupingSetCount() == 1;
    }

    private final PlannerContext plannerContext;
    private final TypeAnalyzer typeAnalyzer;

    public PushPartialAggregationThroughJoin(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    public Iterable<Rule<?>> rules()
    {
        return ImmutableList.of(
                pushPartialAggregationThroughJoinWithoutProjection(),
                pushPartialAggregationThroughJoinWithProjection());
    }

    @VisibleForTesting
    Rule<?> pushPartialAggregationThroughJoinWithoutProjection()
    {
        return new PushPartialAggregationThroughJoinWithoutProjection();
    }

    @VisibleForTesting
    Rule<?> pushPartialAggregationThroughJoinWithProjection()
    {
        return new PushPartialAggregationThroughJoinWithProjection();
    }

    private class PushPartialAggregationThroughJoinWithoutProjection
            implements Rule<AggregationNode>
    {
        private static final Pattern<AggregationNode> PATTERN_WITHOUT_PROJECTION = aggregation()
                .matching(PushPartialAggregationThroughJoin::isSupportedAggregationNode)
                .with(source().matching(join()));

        @Override
        public Pattern<AggregationNode> getPattern()
        {
            return PATTERN_WITHOUT_PROJECTION;
        }

        @Override
        public boolean isEnabled(Session session)
        {
            return isPushPartialAggregationThroughJoin(session);
        }

        @Override
        public Result apply(AggregationNode node, Captures captures, Context context)
        {
            return applyPushdown(node, context);
        }
    }

    private class PushPartialAggregationThroughJoinWithProjection
            implements Rule<AggregationNode>
    {
        private static final Pattern<AggregationNode> PATTERN_WITH_PROJECTION = aggregation()
                .matching(PushPartialAggregationThroughJoin::isSupportedAggregationNode)
                .with(source().matching(project().with(source().matching(join()))));

        @Override
        public Pattern<AggregationNode> getPattern()
        {
            return PATTERN_WITH_PROJECTION;
        }

        @Override
        public boolean isEnabled(Session session)
        {
            return isPushPartialAggregationThroughJoin(session);
        }

        @Override
        public Result apply(AggregationNode node, Captures captures, Context context)
        {
            ProjectNode projectNode = (ProjectNode) context.getLookup().resolve(node.getSource());
            Optional<PlanNode> joinNodeOptional = pushProjectionThroughJoin(
                    plannerContext, projectNode, context.getLookup(), context.getIdAllocator(), context.getSession(), typeAnalyzer, context.getSymbolAllocator().getTypes());
            if (joinNodeOptional.isEmpty()) {
                return Result.empty();
            }
            return applyPushdown((AggregationNode) node.replaceChildren(ImmutableList.of(joinNodeOptional.get())), context);
        }
    }

    private Result applyPushdown(AggregationNode aggregationNode, Context context)
    {
        JoinNode joinNode = (JoinNode) context.getLookup().resolve(aggregationNode.getSource());

        if (joinNode.getType() != JoinNode.Type.INNER) {
            return Result.empty();
        }

        if (allAggregationsOn(aggregationNode.getAggregations(), joinNode.getLeft().getOutputSymbols())) {
            return pushPartialToLeftChild(aggregationNode, joinNode, context).map(Result::ofPlanNode).orElse(Result.empty());
        }
        if (allAggregationsOn(aggregationNode.getAggregations(), joinNode.getRight().getOutputSymbols())) {
            return pushPartialToRightChild(aggregationNode, joinNode, context).map(Result::ofPlanNode).orElse(Result.empty());
        }

        return Result.empty();
    }

    private static boolean allAggregationsOn(Map<Symbol, Aggregation> aggregations, List<Symbol> symbols)
    {
        Set<Symbol> inputs = aggregations.values().stream()
                .map(SymbolsExtractor::extractAll)
                .flatMap(List::stream)
                .collect(toImmutableSet());
        return symbols.containsAll(inputs);
    }

    private Optional<PlanNode> pushPartialToLeftChild(AggregationNode node, JoinNode child, Context context)
    {
        if (isExpandingJoin(child, child.getLeft(), context)) {
            return Optional.empty();
        }
        Set<Symbol> joinLeftChildSymbols = ImmutableSet.copyOf(child.getLeft().getOutputSymbols());
        List<Symbol> groupingSet = getPushedDownGroupingSet(node, joinLeftChildSymbols, intersection(getJoinRequiredSymbols(child), joinLeftChildSymbols));
        // only push partial aggregation down if pushed grouping set is same or less granular
        if (!ImmutableSet.copyOf(node.getGroupingKeys()).containsAll(ImmutableSet.copyOf(groupingSet))) {
            return Optional.empty();
        }
        AggregationNode pushedAggregation = replaceAggregationSource(node, child.getLeft(), groupingSet);
        return Optional.of(pushPartialToJoin(node, child, pushedAggregation, child.getRight(), context));
    }

    private Optional<PlanNode> pushPartialToRightChild(AggregationNode node, JoinNode child, Context context)
    {
        if (isExpandingJoin(child, child.getRight(), context)) {
            return Optional.empty();
        }
        Set<Symbol> joinRightChildSymbols = ImmutableSet.copyOf(child.getRight().getOutputSymbols());
        List<Symbol> groupingSet = getPushedDownGroupingSet(node, joinRightChildSymbols, intersection(getJoinRequiredSymbols(child), joinRightChildSymbols));
        // only push partial aggregation down if pushed grouping set is same or less granular
        if (!ImmutableSet.copyOf(node.getGroupingKeys()).containsAll(ImmutableSet.copyOf(groupingSet))) {
            return Optional.empty();
        }
        AggregationNode pushedAggregation = replaceAggregationSource(node, child.getRight(), groupingSet);
        return Optional.of(pushPartialToJoin(node, child, child.getLeft(), pushedAggregation, context));
    }

    private boolean isExpandingJoin(JoinNode join, PlanNode source, Context context)
    {
        // Only push aggregation down if pushed aggregation consumes similar number of input rows. Otherwise,
        // there is a possibility that aggregation above join could be more efficient in reducing total number of rows
        double sourceRowCount = context.getStatsProvider().getStats(source).getOutputRowCount();
        double joinRowCount = context.getStatsProvider().getStats(join).getOutputRowCount();
        // Pushing aggregation though filtering join could mean more work for partial aggregation. However,
        // we allow pushing partial aggregations through filtering join because:
        // 1. dynamic filtering should filter unmatched rows at source
        // 2. partial aggregation will adaptively switch off when it's not reducing input rows
        // 3. join operator is not particularly effective at filtering rows
        return isNaN(sourceRowCount) || isNaN(joinRowCount) || joinRowCount > 1.1 * sourceRowCount;
    }

    private Set<Symbol> getJoinRequiredSymbols(JoinNode node)
    {
        return Streams.concat(
                        node.getCriteria().stream().map(JoinNode.EquiJoinClause::getLeft),
                        node.getCriteria().stream().map(JoinNode.EquiJoinClause::getRight),
                        node.getFilter().map(SymbolsExtractor::extractUnique).orElse(ImmutableSet.of()).stream(),
                        node.getLeftHashSymbol().map(ImmutableSet::of).orElse(ImmutableSet.of()).stream(),
                        node.getRightHashSymbol().map(ImmutableSet::of).orElse(ImmutableSet.of()).stream())
                .collect(toImmutableSet());
    }

    private List<Symbol> getPushedDownGroupingSet(AggregationNode aggregation, Set<Symbol> availableSymbols, Set<Symbol> requiredJoinSymbols)
    {
        List<Symbol> groupingSet = aggregation.getGroupingKeys();

        // keep symbols that are directly from the join's child (availableSymbols)
        List<Symbol> pushedDownGroupingSet = groupingSet.stream()
                .filter(availableSymbols::contains)
                .collect(Collectors.toList());

        // add missing required join symbols to grouping set
        Set<Symbol> existingSymbols = new HashSet<>(pushedDownGroupingSet);
        requiredJoinSymbols.stream()
                .filter(existingSymbols::add)
                .forEach(pushedDownGroupingSet::add);

        return pushedDownGroupingSet;
    }

    private AggregationNode replaceAggregationSource(
            AggregationNode aggregation,
            PlanNode source,
            List<Symbol> groupingKeys)
    {
        return AggregationNode.builderFrom(aggregation)
                .setSource(source)
                .setGroupingSets(singleGroupingSet(groupingKeys))
                .setPreGroupedSymbols(ImmutableList.of())
                .build();
    }

    private PlanNode pushPartialToJoin(
            AggregationNode aggregation,
            JoinNode child,
            PlanNode leftChild,
            PlanNode rightChild,
            Context context)
    {
        JoinNode joinNode = new JoinNode(
                child.getId(),
                child.getType(),
                leftChild,
                rightChild,
                child.getCriteria(),
                leftChild.getOutputSymbols(),
                rightChild.getOutputSymbols(),
                child.isMaySkipOutputDuplicates(),
                child.getFilter(),
                child.getLeftHashSymbol(),
                child.getRightHashSymbol(),
                child.getDistributionType(),
                child.isSpillable(),
                child.getDynamicFilters(),
                child.getReorderJoinStatsAndCost());
        return restrictOutputs(context.getIdAllocator(), joinNode, ImmutableSet.copyOf(aggregation.getOutputSymbols())).orElse(joinNode);
    }
}
