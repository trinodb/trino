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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.cost.OptimizerConfig.JoinDistributionType;
import io.trino.cost.CostComparator;
import io.trino.cost.CostProvider;
import io.trino.cost.PlanCostEstimate;
import io.trino.cost.PlanNodeStatsAndCostSummary;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.StatsProvider;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.EqualityInference;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.DistributionType;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SymbolReference;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.powerSet;
import static io.trino.SystemSessionProperties.getJoinDistributionType;
import static io.trino.SystemSessionProperties.getJoinReorderingStrategy;
import static io.trino.SystemSessionProperties.getMaxReorderedJoins;
import static io.trino.cost.OptimizerConfig.JoinReorderingStrategy.AUTOMATIC;
import static io.trino.sql.ExpressionUtils.and;
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.EqualityInference.nonInferrableConjuncts;
import static io.trino.sql.planner.iterative.rule.DetermineJoinDistributionType.canReplicate;
import static io.trino.sql.planner.iterative.rule.PushProjectionThroughJoin.pushProjectionThroughJoin;
import static io.trino.sql.planner.iterative.rule.ReorderJoins.JoinEnumerationResult.INFINITE_COST_RESULT;
import static io.trino.sql.planner.iterative.rule.ReorderJoins.JoinEnumerationResult.UNKNOWN_COST_RESULT;
import static io.trino.sql.planner.iterative.rule.ReorderJoins.MultiJoinNode.toMultiJoinNode;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.Patterns.join;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

public class ReorderJoins
        implements Rule<JoinNode>
{
    private static final Logger log = Logger.get(ReorderJoins.class);

    // We check that join distribution type is absent because we only want
    // to do this transformation once (reordered joins will have distribution type already set).
    private final Pattern<JoinNode> pattern;
    private final TypeAnalyzer typeAnalyzer;

    private final PlannerContext plannerContext;
    private final CostComparator costComparator;

    public ReorderJoins(PlannerContext plannerContext, CostComparator costComparator, TypeAnalyzer typeAnalyzer)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.costComparator = requireNonNull(costComparator, "costComparator is null");
        this.pattern = join().matching(
                joinNode -> joinNode.getDistributionType().isEmpty()
                        && joinNode.getType() == INNER
                        && isDeterministic(joinNode.getFilter().orElse(TRUE_LITERAL), plannerContext.getMetadata()));
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return pattern;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return getJoinReorderingStrategy(session) == AUTOMATIC;
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        // try reorder joins with projection pushdown first
        MultiJoinNode multiJoinNode = toMultiJoinNode(plannerContext, joinNode, context, true, typeAnalyzer);
        JoinEnumerationResult resultWithProjectionPushdown = chooseJoinOrder(multiJoinNode, context);
        if (resultWithProjectionPushdown.getPlanNode().isEmpty()) {
            return Result.empty();
        }

        if (!multiJoinNode.isPushedProjectionThroughJoin()) {
            return Result.ofPlanNode(resultWithProjectionPushdown.getPlanNode().get());
        }

        // try reorder joins without projection pushdown
        multiJoinNode = toMultiJoinNode(plannerContext, joinNode, context, false, typeAnalyzer);
        JoinEnumerationResult resultWithoutProjectionPushdown = chooseJoinOrder(multiJoinNode, context);
        if (resultWithoutProjectionPushdown.getPlanNode().isEmpty()
                || costComparator.compare(context.getSession(), resultWithProjectionPushdown.cost, resultWithoutProjectionPushdown.cost) < 0) {
            return Result.ofPlanNode(resultWithProjectionPushdown.getPlanNode().get());
        }

        return Result.ofPlanNode(resultWithoutProjectionPushdown.getPlanNode().get());
    }

    private JoinEnumerationResult chooseJoinOrder(MultiJoinNode multiJoinNode, Context context)
    {
        JoinEnumerator joinEnumerator = new JoinEnumerator(
                plannerContext.getMetadata(),
                costComparator,
                multiJoinNode.getFilter(),
                context);
        return joinEnumerator.chooseJoinOrder(multiJoinNode.getSources(), multiJoinNode.getOutputSymbols());
    }

    @VisibleForTesting
    static class JoinEnumerator
    {
        private final Metadata metadata;
        private final Session session;
        private final StatsProvider statsProvider;
        private final CostProvider costProvider;
        // Using Ordering to facilitate rule determinism
        private final Ordering<JoinEnumerationResult> resultComparator;
        private final PlanNodeIdAllocator idAllocator;
        private final Expression allFilter;
        private final EqualityInference allFilterInference;
        private final Lookup lookup;
        private final Context context;

        private final Map<Set<PlanNode>, JoinEnumerationResult> memo = new HashMap<>();

        @VisibleForTesting
        JoinEnumerator(Metadata metadata, CostComparator costComparator, Expression filter, Context context)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.context = requireNonNull(context);
            this.session = requireNonNull(context.getSession(), "session is null");
            this.statsProvider = requireNonNull(context.getStatsProvider(), "statsProvider is null");
            this.costProvider = requireNonNull(context.getCostProvider(), "costProvider is null");
            this.resultComparator = costComparator.forSession(session).onResultOf(result -> result.cost);
            this.idAllocator = requireNonNull(context.getIdAllocator(), "idAllocator is null");
            this.allFilter = requireNonNull(filter, "filter is null");
            this.allFilterInference = EqualityInference.newInstance(metadata, filter);
            this.lookup = requireNonNull(context.getLookup(), "lookup is null");
        }

        private JoinEnumerationResult chooseJoinOrder(LinkedHashSet<PlanNode> sources, List<Symbol> outputSymbols)
        {
            context.checkTimeoutNotExhausted();

            Set<PlanNode> multiJoinKey = ImmutableSet.copyOf(sources);
            JoinEnumerationResult bestResult = memo.get(multiJoinKey);
            if (bestResult == null) {
                checkState(sources.size() > 1, "sources size is less than or equal to one");
                ImmutableList.Builder<JoinEnumerationResult> resultBuilder = ImmutableList.builder();
                Set<Set<Integer>> partitions = generatePartitions(sources.size());
                for (Set<Integer> partition : partitions) {
                    JoinEnumerationResult result = createJoinAccordingToPartitioning(sources, outputSymbols, partition);
                    if (result.equals(UNKNOWN_COST_RESULT)) {
                        memo.put(multiJoinKey, result);
                        return result;
                    }
                    if (!result.equals(INFINITE_COST_RESULT)) {
                        resultBuilder.add(result);
                    }
                }

                List<JoinEnumerationResult> results = resultBuilder.build();
                if (results.isEmpty()) {
                    memo.put(multiJoinKey, INFINITE_COST_RESULT);
                    return INFINITE_COST_RESULT;
                }

                bestResult = resultComparator.min(results);
                memo.put(multiJoinKey, bestResult);
            }

            bestResult.planNode.ifPresent(planNode -> log.debug("Least cost join was: %s", planNode));
            return bestResult;
        }

        /**
         * This method generates all the ways of dividing totalNodes into two sets
         * each containing at least one node. It will generate one set for each
         * possible partitioning. The other partition is implied in the absent values.
         * In order not to generate the inverse of any set, we always include the 0th
         * node in our sets.
         *
         * @return A set of sets each of which defines a partitioning of totalNodes
         */
        @VisibleForTesting
        static Set<Set<Integer>> generatePartitions(int totalNodes)
        {
            checkArgument(totalNodes > 1, "totalNodes must be greater than 1");
            Set<Integer> numbers = IntStream.range(0, totalNodes)
                    .boxed()
                    .collect(toImmutableSet());
            return powerSet(numbers).stream()
                    .filter(subSet -> subSet.contains(0))
                    .filter(subSet -> subSet.size() < numbers.size())
                    .collect(toImmutableSet());
        }

        @VisibleForTesting
        JoinEnumerationResult createJoinAccordingToPartitioning(LinkedHashSet<PlanNode> sources, List<Symbol> outputSymbols, Set<Integer> partitioning)
        {
            List<PlanNode> sourceList = ImmutableList.copyOf(sources);
            LinkedHashSet<PlanNode> leftSources = partitioning.stream()
                    .map(sourceList::get)
                    .collect(toCollection(LinkedHashSet::new));
            LinkedHashSet<PlanNode> rightSources = sources.stream()
                    .filter(source -> !leftSources.contains(source))
                    .collect(toCollection(LinkedHashSet::new));
            return createJoin(leftSources, rightSources, outputSymbols);
        }

        private JoinEnumerationResult createJoin(LinkedHashSet<PlanNode> leftSources, LinkedHashSet<PlanNode> rightSources, List<Symbol> outputSymbols)
        {
            Set<Symbol> leftSymbols = leftSources.stream()
                    .flatMap(node -> node.getOutputSymbols().stream())
                    .collect(toImmutableSet());
            Set<Symbol> rightSymbols = rightSources.stream()
                    .flatMap(node -> node.getOutputSymbols().stream())
                    .collect(toImmutableSet());

            List<Expression> joinPredicates = getJoinPredicates(leftSymbols, rightSymbols);
            List<EquiJoinClause> joinConditions = joinPredicates.stream()
                    .filter(JoinEnumerator::isJoinEqualityCondition)
                    .map(predicate -> toEquiJoinClause((ComparisonExpression) predicate, leftSymbols))
                    .collect(toImmutableList());
            if (joinConditions.isEmpty()) {
                return INFINITE_COST_RESULT;
            }
            List<Expression> joinFilters = joinPredicates.stream()
                    .filter(predicate -> !isJoinEqualityCondition(predicate))
                    .collect(toImmutableList());

            Set<Symbol> requiredJoinSymbols = ImmutableSet.<Symbol>builder()
                    .addAll(outputSymbols)
                    .addAll(SymbolsExtractor.extractUnique(joinPredicates))
                    .build();

            JoinEnumerationResult leftResult = getJoinSource(
                    leftSources,
                    requiredJoinSymbols.stream()
                            .filter(leftSymbols::contains)
                            .collect(toImmutableList()));
            if (leftResult.equals(UNKNOWN_COST_RESULT)) {
                return UNKNOWN_COST_RESULT;
            }
            if (leftResult.equals(INFINITE_COST_RESULT)) {
                return INFINITE_COST_RESULT;
            }

            PlanNode left = leftResult.planNode.orElseThrow(() -> new VerifyException("Plan node is not present"));

            JoinEnumerationResult rightResult = getJoinSource(
                    rightSources,
                    requiredJoinSymbols.stream()
                            .filter(rightSymbols::contains)
                            .collect(toImmutableList()));
            if (rightResult.equals(UNKNOWN_COST_RESULT)) {
                return UNKNOWN_COST_RESULT;
            }
            if (rightResult.equals(INFINITE_COST_RESULT)) {
                return INFINITE_COST_RESULT;
            }

            PlanNode right = rightResult.planNode.orElseThrow(() -> new VerifyException("Plan node is not present"));

            List<Symbol> leftOutputSymbols = left.getOutputSymbols().stream()
                    .filter(outputSymbols::contains)
                    .collect(toImmutableList());
            List<Symbol> rightOutputSymbols = right.getOutputSymbols().stream()
                    .filter(outputSymbols::contains)
                    .collect(toImmutableList());

            return setJoinNodeProperties(new JoinNode(
                    idAllocator.getNextId(),
                    INNER,
                    left,
                    right,
                    joinConditions,
                    leftOutputSymbols,
                    rightOutputSymbols,
                    false,
                    joinFilters.isEmpty() ? Optional.empty() : Optional.of(and(joinFilters)),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of(),
                    Optional.empty()));
        }

        private List<Expression> getJoinPredicates(Set<Symbol> leftSymbols, Set<Symbol> rightSymbols)
        {
            ImmutableList.Builder<Expression> joinPredicatesBuilder = ImmutableList.builder();

            // This takes all conjuncts that were part of allFilters that
            // could not be used for equality inference.
            // If they use both the left and right symbols, we add them to the list of joinPredicates
            nonInferrableConjuncts(metadata, allFilter)
                    .map(conjunct -> allFilterInference.rewrite(conjunct, Sets.union(leftSymbols, rightSymbols)))
                    .filter(Objects::nonNull)
                    // filter expressions that contain only left or right symbols
                    .filter(conjunct -> allFilterInference.rewrite(conjunct, leftSymbols) == null)
                    .filter(conjunct -> allFilterInference.rewrite(conjunct, rightSymbols) == null)
                    .forEach(joinPredicatesBuilder::add);

            // create equality inference on available symbols
            // TODO: make generateEqualitiesPartitionedBy take left and right scope
            List<Expression> joinEqualities = allFilterInference.generateEqualitiesPartitionedBy(Sets.union(leftSymbols, rightSymbols)).getScopeEqualities();
            EqualityInference joinInference = EqualityInference.newInstance(metadata, joinEqualities.toArray(new Expression[0]));
            joinPredicatesBuilder.addAll(joinInference.generateEqualitiesPartitionedBy(leftSymbols).getScopeStraddlingEqualities());

            return joinPredicatesBuilder.build();
        }

        private JoinEnumerationResult getJoinSource(LinkedHashSet<PlanNode> nodes, List<Symbol> outputSymbols)
        {
            if (nodes.size() == 1) {
                PlanNode planNode = getOnlyElement(nodes);
                Set<Symbol> scope = ImmutableSet.copyOf(outputSymbols);
                ImmutableList.Builder<Expression> predicates = ImmutableList.builder();
                predicates.addAll(allFilterInference.generateEqualitiesPartitionedBy(scope).getScopeEqualities());
                nonInferrableConjuncts(metadata, allFilter)
                        .map(conjunct -> allFilterInference.rewrite(conjunct, scope))
                        .filter(Objects::nonNull)
                        .forEach(predicates::add);
                Expression filter = combineConjuncts(metadata, predicates.build());
                if (!TRUE_LITERAL.equals(filter)) {
                    planNode = new FilterNode(idAllocator.getNextId(), planNode, filter);
                }
                return createJoinEnumerationResult(planNode);
            }
            return chooseJoinOrder(nodes, outputSymbols);
        }

        private static boolean isJoinEqualityCondition(Expression expression)
        {
            return expression instanceof ComparisonExpression
                    && ((ComparisonExpression) expression).getOperator() == EQUAL
                    && ((ComparisonExpression) expression).getLeft() instanceof SymbolReference
                    && ((ComparisonExpression) expression).getRight() instanceof SymbolReference;
        }

        private static EquiJoinClause toEquiJoinClause(ComparisonExpression equality, Set<Symbol> leftSymbols)
        {
            Symbol leftSymbol = Symbol.from(equality.getLeft());
            Symbol rightSymbol = Symbol.from(equality.getRight());
            EquiJoinClause equiJoinClause = new EquiJoinClause(leftSymbol, rightSymbol);
            return leftSymbols.contains(leftSymbol) ? equiJoinClause : equiJoinClause.flip();
        }

        private JoinEnumerationResult setJoinNodeProperties(JoinNode joinNode)
        {
            if (isAtMostScalar(joinNode.getRight(), lookup)) {
                return createJoinEnumerationResult(joinNode.withDistributionType(REPLICATED));
            }
            if (isAtMostScalar(joinNode.getLeft(), lookup)) {
                return createJoinEnumerationResult(joinNode.flipChildren().withDistributionType(REPLICATED));
            }
            List<JoinEnumerationResult> possibleJoinNodes = getPossibleJoinNodes(joinNode, getJoinDistributionType(session));
            verify(!possibleJoinNodes.isEmpty(), "possibleJoinNodes is empty");
            if (possibleJoinNodes.stream().anyMatch(UNKNOWN_COST_RESULT::equals)) {
                return UNKNOWN_COST_RESULT;
            }
            return resultComparator.min(possibleJoinNodes);
        }

        private List<JoinEnumerationResult> getPossibleJoinNodes(JoinNode joinNode, JoinDistributionType distributionType)
        {
            checkArgument(joinNode.getType() == INNER, "unexpected join node type: %s", joinNode.getType());

            if (joinNode.isCrossJoin()) {
                return getPossibleJoinNodes(joinNode, REPLICATED);
            }

            switch (distributionType) {
                case PARTITIONED:
                    return getPossibleJoinNodes(joinNode, PARTITIONED);
                case BROADCAST:
                    return getPossibleJoinNodes(joinNode, REPLICATED);
                case AUTOMATIC:
                    ImmutableList.Builder<JoinEnumerationResult> result = ImmutableList.builder();
                    result.addAll(getPossibleJoinNodes(joinNode, PARTITIONED));
                    result.addAll(getPossibleJoinNodes(joinNode, REPLICATED, node -> canReplicate(node, context)));
                    return result.build();
            }
            throw new IllegalArgumentException("unexpected join distribution type: " + distributionType);
        }

        private List<JoinEnumerationResult> getPossibleJoinNodes(JoinNode joinNode, DistributionType distributionType)
        {
            return getPossibleJoinNodes(joinNode, distributionType, node -> true);
        }

        private List<JoinEnumerationResult> getPossibleJoinNodes(JoinNode joinNode, DistributionType distributionType, Predicate<JoinNode> isAllowed)
        {
            List<JoinNode> nodes = ImmutableList.of(
                    joinNode.withDistributionType(distributionType),
                    joinNode.flipChildren().withDistributionType(distributionType));
            return nodes.stream().filter(isAllowed).map(this::createJoinEnumerationResult).collect(toImmutableList());
        }

        private JoinEnumerationResult createJoinEnumerationResult(JoinNode joinNode)
        {
            PlanCostEstimate costEstimate = costProvider.getCost(joinNode);
            PlanNodeStatsEstimate statsEstimate = statsProvider.getStats(joinNode);
            return JoinEnumerationResult.createJoinEnumerationResult(
                    Optional.of(joinNode.withReorderJoinStatsAndCost(new PlanNodeStatsAndCostSummary(
                            statsEstimate.getOutputRowCount(),
                            statsEstimate.getOutputSizeInBytes(joinNode.getOutputSymbols(), context.getSymbolAllocator().getTypes()),
                            costEstimate.getCpuCost(),
                            costEstimate.getMaxMemory(),
                            costEstimate.getNetworkCost()))),
                    costEstimate);
        }

        private JoinEnumerationResult createJoinEnumerationResult(PlanNode planNode)
        {
            return JoinEnumerationResult.createJoinEnumerationResult(Optional.of(planNode), costProvider.getCost(planNode));
        }
    }

    /**
     * This class represents a set of inner joins that can be executed in any order.
     */
    @VisibleForTesting
    static class MultiJoinNode
    {
        // Use a linked hash set to ensure optimizer is deterministic
        private final LinkedHashSet<PlanNode> sources;
        private final Expression filter;
        private final List<Symbol> outputSymbols;
        private final boolean pushedProjectionThroughJoin;

        MultiJoinNode(LinkedHashSet<PlanNode> sources, Expression filter, List<Symbol> outputSymbols, boolean pushedProjectionThroughJoin)
        {
            requireNonNull(sources, "sources is null");
            checkArgument(sources.size() > 1, "sources size is <= 1");
            requireNonNull(filter, "filter is null");
            requireNonNull(outputSymbols, "outputSymbols is null");

            this.sources = sources;
            this.filter = filter;
            this.outputSymbols = ImmutableList.copyOf(outputSymbols);
            this.pushedProjectionThroughJoin = pushedProjectionThroughJoin;

            List<Symbol> inputSymbols = sources.stream().flatMap(source -> source.getOutputSymbols().stream()).collect(toImmutableList());
            checkArgument(inputSymbols.containsAll(outputSymbols), "inputs do not contain all output symbols");
        }

        public Expression getFilter()
        {
            return filter;
        }

        public LinkedHashSet<PlanNode> getSources()
        {
            return sources;
        }

        public List<Symbol> getOutputSymbols()
        {
            return outputSymbols;
        }

        public boolean isPushedProjectionThroughJoin()
        {
            return pushedProjectionThroughJoin;
        }

        public static Builder builder()
        {
            return new Builder();
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(sources, ImmutableSet.copyOf(extractConjuncts(filter)), outputSymbols, pushedProjectionThroughJoin);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (!(obj instanceof MultiJoinNode)) {
                return false;
            }

            MultiJoinNode other = (MultiJoinNode) obj;
            return this.sources.equals(other.sources)
                    && ImmutableSet.copyOf(extractConjuncts(this.filter)).equals(ImmutableSet.copyOf(extractConjuncts(other.filter)))
                    && this.outputSymbols.equals(other.outputSymbols)
                    && this.pushedProjectionThroughJoin == other.pushedProjectionThroughJoin;
        }

        static MultiJoinNode toMultiJoinNode(PlannerContext plannerContext, JoinNode joinNode, Context context, boolean pushProjectionsThroughJoin, TypeAnalyzer typeAnalyzer)
        {
            return toMultiJoinNode(
                    plannerContext,
                    joinNode,
                    context.getLookup(),
                    context.getIdAllocator(),
                    getMaxReorderedJoins(context.getSession()),
                    pushProjectionsThroughJoin,
                    context.getSession(),
                    typeAnalyzer,
                    context.getSymbolAllocator().getTypes());
        }

        static MultiJoinNode toMultiJoinNode(
                PlannerContext plannerContext,
                JoinNode joinNode,
                Lookup lookup,
                PlanNodeIdAllocator planNodeIdAllocator,
                int joinLimit,
                boolean pushProjectionsThroughJoin,
                Session session,
                TypeAnalyzer typeAnalyzer,
                TypeProvider types)
        {
            // the number of sources is the number of joins + 1
            return new JoinNodeFlattener(plannerContext, joinNode, lookup, planNodeIdAllocator, joinLimit + 1, pushProjectionsThroughJoin, session, typeAnalyzer, types)
                    .toMultiJoinNode();
        }

        private static class JoinNodeFlattener
        {
            private final PlannerContext plannerContext;
            private final Session session;
            private final TypeAnalyzer typeAnalyzer;
            private final TypeProvider types;
            private final Lookup lookup;
            private final PlanNodeIdAllocator planNodeIdAllocator;

            private final LinkedHashSet<PlanNode> sources = new LinkedHashSet<>();
            private final List<Expression> filters = new ArrayList<>();
            private final List<Symbol> outputSymbols;
            private final boolean pushProjectionsThroughJoin;

            // if projection was pushed through join during join graph flattening?
            private boolean pushedProjectionThroughJoin;

            JoinNodeFlattener(
                    PlannerContext plannerContext,
                    JoinNode node,
                    Lookup lookup,
                    PlanNodeIdAllocator planNodeIdAllocator,
                    int sourceLimit,
                    boolean pushProjectionsThroughJoin,
                    Session session,
                    TypeAnalyzer typeAnalyzer,
                    TypeProvider types)
            {
                this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
                requireNonNull(node, "node is null");
                checkState(node.getType() == INNER, "join type must be INNER");
                this.outputSymbols = node.getOutputSymbols();
                this.lookup = requireNonNull(lookup, "lookup is null");
                this.planNodeIdAllocator = requireNonNull(planNodeIdAllocator, "planNodeIdAllocator is null");
                this.pushProjectionsThroughJoin = pushProjectionsThroughJoin;
                this.session = requireNonNull(session, "session is null");
                this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
                this.types = requireNonNull(types, "types is null");

                flattenNode(node, sourceLimit);
            }

            private void flattenNode(PlanNode node, int limit)
            {
                PlanNode resolved = lookup.resolve(node);

                if (resolved instanceof ProjectNode) {
                    if (!pushProjectionsThroughJoin) {
                        sources.add(node);
                        return;
                    }

                    Optional<PlanNode> rewrittenNode = pushProjectionThroughJoin(plannerContext, (ProjectNode) resolved, lookup, planNodeIdAllocator, session, typeAnalyzer, types);
                    if (rewrittenNode.isEmpty()) {
                        sources.add(node);
                        return;
                    }

                    pushedProjectionThroughJoin = true;
                    flattenNode(rewrittenNode.get(), limit);
                    return;
                }

                // (limit - 2) because you need to account for adding left and right side
                if (!(resolved instanceof JoinNode) || (sources.size() > (limit - 2))) {
                    sources.add(node);
                    return;
                }

                JoinNode joinNode = (JoinNode) resolved;
                if (joinNode.getType() != INNER || !isDeterministic(joinNode.getFilter().orElse(TRUE_LITERAL), plannerContext.getMetadata()) || joinNode.getDistributionType().isPresent()) {
                    sources.add(node);
                    return;
                }

                // we set the left limit to limit - 1 to account for the node on the right
                flattenNode(joinNode.getLeft(), limit - 1);
                flattenNode(joinNode.getRight(), limit);
                joinNode.getCriteria().stream()
                        .map(EquiJoinClause::toExpression)
                        .forEach(filters::add);
                joinNode.getFilter().ifPresent(filters::add);
            }

            MultiJoinNode toMultiJoinNode()
            {
                return new MultiJoinNode(sources, and(filters), outputSymbols, pushedProjectionThroughJoin);
            }
        }

        static class Builder
        {
            private List<PlanNode> sources;
            private Expression filter;
            private List<Symbol> outputSymbols;

            public Builder setSources(PlanNode... sources)
            {
                this.sources = ImmutableList.copyOf(sources);
                return this;
            }

            public Builder setFilter(Expression filter)
            {
                this.filter = filter;
                return this;
            }

            public Builder setOutputSymbols(Symbol... outputSymbols)
            {
                this.outputSymbols = ImmutableList.copyOf(outputSymbols);
                return this;
            }

            public MultiJoinNode build()
            {
                return new MultiJoinNode(new LinkedHashSet<>(sources), filter, outputSymbols, false);
            }
        }
    }

    @VisibleForTesting
    static class JoinEnumerationResult
    {
        static final JoinEnumerationResult UNKNOWN_COST_RESULT = new JoinEnumerationResult(Optional.empty(), PlanCostEstimate.unknown());
        static final JoinEnumerationResult INFINITE_COST_RESULT = new JoinEnumerationResult(Optional.empty(), PlanCostEstimate.infinite());

        private final Optional<PlanNode> planNode;
        private final PlanCostEstimate cost;

        private JoinEnumerationResult(Optional<PlanNode> planNode, PlanCostEstimate cost)
        {
            this.planNode = requireNonNull(planNode, "planNode is null");
            this.cost = requireNonNull(cost, "cost is null");
            checkArgument((cost.hasUnknownComponents() || cost.equals(PlanCostEstimate.infinite())) && planNode.isEmpty()
                            || (!cost.hasUnknownComponents() || !cost.equals(PlanCostEstimate.infinite())) && planNode.isPresent(),
                    "planNode should be present if and only if cost is known");
        }

        public Optional<PlanNode> getPlanNode()
        {
            return planNode;
        }

        public PlanCostEstimate getCost()
        {
            return cost;
        }

        static JoinEnumerationResult createJoinEnumerationResult(Optional<PlanNode> planNode, PlanCostEstimate cost)
        {
            if (cost.hasUnknownComponents()) {
                return UNKNOWN_COST_RESULT;
            }
            if (cost.equals(PlanCostEstimate.infinite())) {
                return INFINITE_COST_RESULT;
            }
            return new JoinEnumerationResult(planNode, cost);
        }
    }
}
