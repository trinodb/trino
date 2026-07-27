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
import io.airlift.log.Logger;
import io.trino.Session;
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
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions.Comparison;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.EqualityInference;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.DistributionType;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongPredicate;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.getJoinDistributionType;
import static io.trino.SystemSessionProperties.getJoinReorderingStrategy;
import static io.trino.SystemSessionProperties.getMaxEnumeratedJoinOrders;
import static io.trino.SystemSessionProperties.getMaxReorderedJoins;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.IrExpressions.matchComparison;
import static io.trino.sql.ir.IrExpressions.mayFail;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.EqualityInference.isInferenceCandidate;
import static io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy.AUTOMATIC;
import static io.trino.sql.planner.SymbolsExtractor.extractAll;
import static io.trino.sql.planner.SymbolsExtractor.extractUnique;
import static io.trino.sql.planner.iterative.rule.DetermineJoinDistributionType.canReplicate;
import static io.trino.sql.planner.iterative.rule.PushProjectionThroughJoin.pushProjectionThroughJoin;
import static io.trino.sql.planner.iterative.rule.ReorderJoins.JoinEnumerationResult.INFINITE_COST_RESULT;
import static io.trino.sql.planner.iterative.rule.ReorderJoins.JoinEnumerationResult.UNKNOWN_COST_RESULT;
import static io.trino.sql.planner.iterative.rule.ReorderJoins.MultiJoinNode.toMultiJoinNode;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.Patterns.join;
import static java.util.Objects.requireNonNull;

public class ReorderJoins
        implements Rule<JoinNode>
{
    private static final Logger log = Logger.get(ReorderJoins.class);

    // Join order enumeration is driven by 64-bit masks over the sources, so at most 63 of them
    // can be reordered as one group. Larger groups are left in the order the query gives them.
    private static final int MAX_ENUMERATED_SOURCES = 63;

    // We check that join distribution type is absent because we only want
    // to do this transformation once (reordered joins will have distribution type already set).
    private final Pattern<JoinNode> pattern;

    private final PlannerContext plannerContext;
    private final CostComparator costComparator;

    public ReorderJoins(PlannerContext plannerContext, CostComparator costComparator)
    {
        this.plannerContext = plannerContext;
        this.costComparator = requireNonNull(costComparator, "costComparator is null");
        this.pattern = join().matching(
                joinNode -> joinNode.getDistributionType().isEmpty()
                        && joinNode.getType() == INNER
                        && isDeterministic(joinNode.getFilter().orElse(TRUE)));
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
        MultiJoinNode multiJoinNode = toMultiJoinNode(joinNode, context, true, plannerContext.getMetadata());
        JoinEnumerationResult resultWithProjectionPushdown = chooseJoinOrder(multiJoinNode, context);
        if (resultWithProjectionPushdown.getPlanNode().isEmpty()) {
            return Result.empty();
        }

        if (!multiJoinNode.isPushedProjectionThroughJoin()) {
            return Result.ofPlanNode(resultWithProjectionPushdown.getPlanNode().get());
        }

        // try reorder joins without projection pushdown
        multiJoinNode = toMultiJoinNode(joinNode, context, false, plannerContext.getMetadata());
        JoinEnumerationResult resultWithoutProjectionPushdown = chooseJoinOrder(multiJoinNode, context);
        if (resultWithoutProjectionPushdown.getPlanNode().isEmpty()
                || costComparator.compare(context.getSession(), resultWithProjectionPushdown.cost, resultWithoutProjectionPushdown.cost) < 0) {
            return Result.ofPlanNode(resultWithProjectionPushdown.getPlanNode().get());
        }

        return Result.ofPlanNode(resultWithoutProjectionPushdown.getPlanNode().get());
    }

    private JoinEnumerationResult chooseJoinOrder(MultiJoinNode multiJoinNode, Context context)
    {
        if (multiJoinNode.getSources().size() > MAX_ENUMERATED_SOURCES) {
            return INFINITE_COST_RESULT;
        }

        JoinEnumerator joinEnumerator = new JoinEnumerator(
                costComparator,
                multiJoinNode.getFilter(),
                multiJoinNode.getSources(),
                context,
                plannerContext);
        return joinEnumerator.choose(multiJoinNode.getOutputSymbols());
    }

    @VisibleForTesting
    static class JoinEnumerator
    {
        // Generating a candidate subgraph is far cheaper than costing the join order it stands
        // for, so the enumeration is allowed this many candidates per join order in the budget
        // before the count gives up.
        private static final long CANDIDATES_PER_PARTITION_LIMIT = 16;

        private final Session session;
        private final StatsProvider statsProvider;
        private final CostProvider costProvider;
        private final PlannerContext plannerContext;
        // Using Ordering to facilitate rule determinism
        private final Ordering<JoinEnumerationResult> resultComparator;
        private final PlanNodeIdAllocator idAllocator;
        private final EqualityInference allFilterInference;
        private final Lookup lookup;
        private final Context context;

        private final Long2ObjectMap<JoinEnumerationResult> memo = new Long2ObjectOpenHashMap<>();
        private final Long2ObjectMap<EqualityInference> joinInferences = new Long2ObjectOpenHashMap<>();
        private final List<Expression> residuals;
        // every symbol the filter mentions, so that a pre-joined source keeps the columns
        // the joins above it still need
        private final Set<Symbol> filterSymbols;
        // sources indexed by their position in the bit masks used to drive the enumeration
        private final List<PlanNode> sources;
        // for each source, the mask of sources it can be joined with directly
        private final long[] neighbors;
        // set when the sources come from the greedy planner, which plans every equality within
        // a source's scope into the source itself, as a join clause or a leaf filter
        private final boolean sourcesPrejoined;

        @VisibleForTesting
        JoinEnumerator(CostComparator costComparator, Expression filter, LinkedHashSet<PlanNode> sources, Context context, PlannerContext plannerContext)
        {
            this.context = requireNonNull(context);
            this.session = requireNonNull(context.getSession(), "session is null");
            this.statsProvider = requireNonNull(context.getStatsProvider(), "statsProvider is null");
            this.costProvider = requireNonNull(context.getCostProvider(), "costProvider is null");
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.resultComparator = costComparator.forSession(session).onResultOf(result -> result.cost);
            this.idAllocator = requireNonNull(context.getIdAllocator(), "idAllocator is null");
            this.lookup = requireNonNull(context.getLookup(), "lookup is null");

            ImmutableList.Builder<Expression> residuals = ImmutableList.builder();
            List<Expression> inferenceCandidates = new ArrayList<>();
            for (Expression conjunct : extractConjuncts(filter)) {
                if (isInferenceCandidate(plannerContext, conjunct) && !mayFail(plannerContext, conjunct)) {
                    inferenceCandidates.add(conjunct);
                }
                else {
                    residuals.add(conjunct);
                }
            }

            this.residuals = residuals.build();
            this.filterSymbols = extractUnique(filter);
            this.allFilterInference = new EqualityInference(plannerContext, inferenceCandidates);
            this.sources = ImmutableList.copyOf(sources);
            checkArgument(this.sources.size() <= MAX_ENUMERATED_SOURCES, "too many sources to enumerate: %s", this.sources.size());
            this.neighbors = buildJoinGraph(this.sources, allFilterInference);
            this.sourcesPrejoined = false;
        }

        /**
         * Creates an enumerator over the simplified sources the greedy planner produced.
         * Everything derived from the filter carries over, but none of the caches do: their keys
         * are masks over the sources, so they mean something else under a different source list.
         */
        private JoinEnumerator(JoinEnumerator parent, List<PlanNode> sources)
        {
            this.context = parent.context;
            this.session = parent.session;
            this.statsProvider = parent.statsProvider;
            this.costProvider = parent.costProvider;
            this.plannerContext = parent.plannerContext;
            this.resultComparator = parent.resultComparator;
            this.idAllocator = parent.idAllocator;
            this.lookup = parent.lookup;
            this.residuals = parent.residuals;
            this.filterSymbols = parent.filterSymbols;
            this.allFilterInference = parent.allFilterInference;
            this.sources = ImmutableList.copyOf(sources);
            this.neighbors = buildJoinGraph(this.sources, allFilterInference);
            this.sourcesPrejoined = true;
        }

        /**
         * Derives the join graph: two sources are adjacent when an equality is known between
         * expressions over their symbols, which is what {@link #createJoin} needs to produce an
         * equi-join clause. Every source referenced by an equivalence class is connected to every
         * other one, because equality is transitive.
         * <p>
         * The graph is deliberately permissive: it connects sources whose symbols merely appear in
         * the same class, even when no plain symbol-to-symbol equality can be derived across a
         * particular cut. Over-approximating only costs a partition that is evaluated and rejected,
         * whereas missing an edge would drop join orders from the search space.
         * <p>
         * No edge is missed because a derived equality only ever equates members of one equality
         * class: whenever {@link #createJoin} can derive an equi-join clause between two sources,
         * some class mentions a symbol of each, and this connects them.
         */
        @VisibleForTesting
        static long[] buildJoinGraph(List<PlanNode> sources, EqualityInference inference)
        {
            Map<Symbol, Integer> sourceIndexes = new HashMap<>();
            for (int index = 0; index < sources.size(); index++) {
                for (Symbol symbol : sources.get(index).getOutputSymbols()) {
                    Integer previous = sourceIndexes.put(symbol, index);
                    checkState(previous == null, "symbol %s is produced by more than one source", symbol);
                }
            }

            long[] neighbors = new long[sources.size()];
            for (Collection<Expression> equalitySet : inference.getEqualitySets()) {
                long referenced = 0;
                for (Expression expression : equalitySet) {
                    for (Symbol symbol : extractUnique(expression)) {
                        Integer index = sourceIndexes.get(symbol);
                        if (index != null) {
                            referenced |= 1L << index;
                        }
                    }
                }

                for (long remaining = referenced; remaining != 0; remaining &= remaining - 1) {
                    int index = Long.numberOfTrailingZeros(remaining);
                    neighbors[index] |= referenced & ~(1L << index);
                }
            }
            return neighbors;
        }

        public JoinEnumerationResult choose(List<Symbol> outputSymbols)
        {
            Set<Symbol> requiredOutputs = ImmutableSet.<Symbol>builder()
                    .addAll(outputSymbols)
                    .addAll(residuals.stream().flatMap(e -> extractAll(e).stream()).toList())
                    .build();

            long budget = getMaxEnumeratedJoinOrders(session);
            JoinEnumerator enumerator = this;
            if (!fitsBudget(sources.size(), neighbors, budget)) {
                Optional<JoinEnumerator> simplified = simplifyToBudget(planGreedily(requiredOutputs), budget);
                if (simplified.isEmpty()) {
                    // nothing to simplify along fits the budget, so the enumeration cannot be kept
                    // within it. Leaving the join order alone beats running until the optimizer
                    // times out.
                    return INFINITE_COST_RESULT;
                }
                enumerator = simplified.get();
            }

            JoinEnumerationResult result = enumerator.chooseJoinOrder(allNodes(enumerator.sources.size()), requiredOutputs);

            if (result.getPlanNode().isPresent()) {
                PlanNode plan = result.getPlanNode().get();

                if (!residuals.isEmpty()) {
                    plan = new FilterNode(idAllocator.getNextId(), result.getPlanNode().get(), combineConjuncts(residuals));
                }

                result = new JoinEnumerationResult(
                        Optional.of(new ProjectNode(
                                idAllocator.getNextId(),
                                plan,
                                Assignments.builder()
                                        .putIdentities(outputSymbols)
                                        .build())),
                        result.getCost());
            }

            return result;
        }

        /**
         * Builds a join order greedily, at every step joining the pair of groups whose join is the
         * cheapest, and returns the intermediate source lists, shortest last.
         * <p>
         * Pre-joining the pairs the greedy order is most confident about is how the search space is
         * cut down when the full enumeration does not fit the budget: it gives up the join orders
         * that separate those pairs, and enumerates everything else exhaustively.
         */
        private List<List<PlanNode>> planGreedily(Set<Symbol> requiredOutputs)
        {
            Set<Symbol> greedyOutputs = ImmutableSet.<Symbol>builder()
                    .addAll(requiredOutputs)
                    .addAll(filterSymbols)
                    .build();

            Map<Long, JoinEnumerationResult> plans = new HashMap<>();
            Map<Long, PlanNode> groupSources = new HashMap<>();
            LongArrayList groups = new LongArrayList();
            for (int index = 0; index < sources.size(); index++) {
                long group = 1L << index;
                JoinEnumerationResult source = getJoinSource(group, restrictTo(greedyOutputs, group));
                if (source.getPlanNode().isEmpty()) {
                    return ImmutableList.of();
                }
                groups.add(group);
                plans.put(group, source);
                groupSources.put(group, source.getPlanNode().get());
            }

            ImmutableList.Builder<List<PlanNode>> simplifications = ImmutableList.builder();
            // the required outputs are deliberately dropped: every group was planned with
            // greedyOutputs restricted to it, which covers anything createJoin can ask for,
            // because the symbols it adds all come from join predicates, and those are a
            // subset of filterSymbols
            SourceResolver resolver = (nodes, _) -> plans.get(nodes);
            // joining a pair only changes the candidates that involve one of its two groups, so the
            // rest are kept rather than re-costed on every step. Groups are disjoint, so the union
            // of a pair identifies it.
            Map<Long, JoinEnumerationResult> candidates = new HashMap<>();
            while (groups.size() > 1) {
                JoinEnumerationResult best = null;
                int bestLeft = -1;
                int bestRight = -1;
                for (int left = 0; left < groups.size(); left++) {
                    long leftNeighborhood = neighborhood(groups.getLong(left), neighbors);
                    for (int right = left + 1; right < groups.size(); right++) {
                        if ((leftNeighborhood & groups.getLong(right)) == 0) {
                            continue;
                        }
                        int leftGroup = left;
                        int rightGroup = right;
                        JoinEnumerationResult candidate = candidates.computeIfAbsent(
                                groups.getLong(left) | groups.getLong(right),
                                pair -> createJoin(groups.getLong(leftGroup), groups.getLong(rightGroup), restrictTo(greedyOutputs, pair), resolver));
                        if (candidate.getPlanNode().isPresent() && (best == null || resultComparator.compare(candidate, best) < 0)) {
                            best = candidate;
                            bestLeft = left;
                            bestRight = right;
                        }
                    }
                }
                if (best == null) {
                    // no pair can be joined, so the greedy order stops here. Whatever it managed to
                    // merge is still usable for simplification.
                    return simplifications.build();
                }

                long merged = groups.getLong(bestLeft) | groups.getLong(bestRight);
                plans.put(merged, best);
                groupSources.put(merged, best.getPlanNode().get());
                // removeLong removes by index, not by value; the higher index goes first so that
                // it is still valid when the lower one is removed
                groups.removeLong(bestRight);
                groups.removeLong(bestLeft);
                groups.add(merged);
                candidates.keySet().removeIf(pair -> (pair & merged) != 0);

                if (groups.size() > 1) {
                    ImmutableList.Builder<PlanNode> simplified = ImmutableList.builder();
                    for (int group = 0; group < groups.size(); group++) {
                        simplified.add(groupSources.get(groups.getLong(group)));
                    }
                    simplifications.add(simplified.build());
                }
            }

            return simplifications.build();
        }

        /**
         * Returns an enumerator over the least simplified of {@code simplifications} whose
         * enumeration fits within {@code budget}. Each one has a pair of sources pre-joined
         * according to the greedy order, so this trades away join orders that the greedy plan
         * considered unpromising rather than truncating the group at an arbitrary point.
         */
        private Optional<JoinEnumerator> simplifyToBudget(List<List<PlanNode>> simplifications, long budget)
        {
            for (List<PlanNode> simplifiedSources : simplifications) {
                JoinEnumerator simplified = new JoinEnumerator(this, simplifiedSources);
                if (fitsBudget(simplifiedSources.size(), simplified.neighbors, budget)) {
                    return Optional.of(simplified);
                }
            }

            // reached only when the greedy order stalled: run to completion it ends at two
            // groups, which always fit. Nothing here can be enumerated within the budget.
            return Optional.empty();
        }

        /**
         * Checks whether enumerating a join graph stays within {@code budget}, without walking
         * the search space when even the densest graph over as many sources would fit.
         */
        private static boolean fitsBudget(int sources, long[] neighbors, long budget)
        {
            return worstCasePartitionCount(sources) <= budget
                    || countPartitions(allNodes(sources), neighbors, budget) <= budget;
        }

        /**
         * The number of partitions {@link #countPartitions} finds for a clique, the densest join
         * graph over the given number of sources: {@code C(n, k) * (2^(k-1) - 1)} summed over
         * every subset size {@code k}, which comes to {@code (3^n - 2^(n+1) + 1) / 2}. No other
         * graph over as many sources counts higher.
         */
        private static long worstCasePartitionCount(int sources)
        {
            if (sources > 39) {
                // 3^40 does not fit in a long
                return Long.MAX_VALUE;
            }
            long powerOfThree = 1;
            for (int i = 0; i < sources; i++) {
                powerOfThree *= 3;
            }
            return (powerOfThree - (2L << sources) + 1) / 2;
        }

        /**
         * Counts the partitions the enumeration would cost, giving up as soon as {@code limit} is
         * exceeded. Counting walks the same subsets as the enumeration but does no costing, so it
         * is orders of magnitude cheaper than finding out by running it.
         * <p>
         * The count also comes out above {@code limit} when finding the partitions takes more
         * than {@link #CANDIDATES_PER_PARTITION_LIMIT} candidate subgraphs per allowed partition.
         * In graphs whose subgraphs mostly have disconnected complements, such as stars, the
         * candidates dominate the enumeration cost even though few of them are partitions, and
         * giving up on them here is what bounds both this count and the enumeration itself.
         */
        @VisibleForTesting
        static long countPartitions(long nodes, long[] neighbors, long limit)
        {
            long[] partitions = new long[1];
            long[] candidates = new long[1];
            countPartitions(nodes, neighbors, limit, new LongOpenHashSet(), partitions, candidates);
            return partitions[0];
        }

        private static boolean countPartitions(long nodes, long[] neighbors, long limit, LongOpenHashSet counted, long[] partitions, long[] candidates)
        {
            if (Long.bitCount(nodes) <= 1 || !counted.add(nodes)) {
                return true;
            }
            return forEachPartitionCandidate(nodes, neighbors, subgraph -> {
                candidates[0]++;
                if (candidates[0] > CANDIDATES_PER_PARTITION_LIMIT * limit) {
                    partitions[0] = Math.max(partitions[0], limit + 1);
                    return false;
                }
                if (!isPartition(subgraph, nodes, neighbors)) {
                    return true;
                }
                partitions[0]++;
                if (partitions[0] > limit) {
                    return false;
                }
                return countPartitions(subgraph, neighbors, limit, counted, partitions, candidates)
                        && countPartitions(nodes & ~subgraph, neighbors, limit, counted, partitions, candidates);
            });
        }

        private Set<Symbol> restrictTo(Set<Symbol> symbols, long nodes)
        {
            Set<Symbol> available = outputSymbols(nodes);
            return symbols.stream()
                    .filter(available::contains)
                    .collect(toImmutableSet());
        }

        private JoinEnumerationResult chooseJoinOrder(long nodes, Set<Symbol> requiredOutputs)
        {
            context.checkTimeoutNotExhausted();

            JoinEnumerationResult bestResult = memo.get(nodes);
            if (bestResult == null) {
                checkState(Long.bitCount(nodes) > 1, "sources size is less than or equal to one");
                ImmutableList.Builder<JoinEnumerationResult> resultBuilder = ImmutableList.builder();
                for (long partition : generatePartitions(nodes, neighbors)) {
                    JoinEnumerationResult result = createJoinAccordingToPartitioning(nodes, requiredOutputs, partition);
                    if (result.equals(UNKNOWN_COST_RESULT)) {
                        memo.put(nodes, result);
                        return result;
                    }
                    if (!result.equals(INFINITE_COST_RESULT)) {
                        resultBuilder.add(result);
                    }
                }

                List<JoinEnumerationResult> results = resultBuilder.build();
                if (results.isEmpty()) {
                    memo.put(nodes, INFINITE_COST_RESULT);
                    return INFINITE_COST_RESULT;
                }

                bestResult = resultComparator.min(results);
                memo.put(nodes, bestResult);
            }

            bestResult.planNode.ifPresent(planNode -> log.debug("Least cost join was: %s", planNode));
            return bestResult;
        }

        /**
         * Generates the ways of dividing {@code nodes} into two sets, each containing at least one
         * node and each connected in the join graph. Only the set containing the lowest node is
         * returned; the other one is implied by the absent nodes.
         * <p>
         * Requiring both sides to be connected does not remove any join order: a set of sources
         * that is disconnected cannot be joined without a cross join at some level, and
         * {@link #createJoin} rejects those with {@link #INFINITE_COST_RESULT}. Skipping them turns
         * enumeration of all {@code 2^n} partitions of every subset into an enumeration of the
         * connected subgraph pairs only, which for anything sparser than a clique is dramatically
         * fewer. A clique — the shape that joining several tables on one shared key produces — is
         * not improved at all, and it is {@link #countPartitions} that keeps it in check.
         * <p>
         * Partitions are returned in ascending mask order, which is the order in which a plain
         * enumeration of all subsets would have evaluated them.
         */
        @VisibleForTesting
        static long[] generatePartitions(long nodes, long[] neighbors)
        {
            LongArrayList partitions = new LongArrayList();
            forEachPartitionCandidate(nodes, neighbors, subgraph -> {
                if (isPartition(subgraph, nodes, neighbors)) {
                    partitions.add(subgraph);
                }
                return true;
            });

            long[] result = partitions.toLongArray();
            Arrays.sort(result);
            return result;
        }

        /**
         * Offers every connected subgraph of {@code nodes} containing its lowest node to
         * {@code consumer}, stopping as soon as the consumer returns {@code false}. These are the
         * candidate halves of the partitions of {@code nodes}: the half with the lowest node is
         * necessarily among them, and {@link #isPartition} tells which candidates qualify.
         */
        private static boolean forEachPartitionCandidate(long nodes, long[] neighbors, LongPredicate consumer)
        {
            checkArgument(Long.bitCount(nodes) > 1, "nodes must contain more than one node");

            long seed = Long.lowestOneBit(nodes);
            if (!consumer.test(seed)) {
                return false;
            }
            return expandConnectedSubgraphs(seed, seed, nodes, neighbors, consumer);
        }

        private static boolean isPartition(long subgraph, long nodes, long[] neighbors)
        {
            long complement = nodes & ~subgraph;
            // the complement is reachable from the subgraph unless nodes itself is disconnected,
            // which can only happen for the complete set of sources
            return complement != 0 && (neighborhood(subgraph, neighbors) & complement) != 0 && isConnected(complement, neighbors);
        }

        /**
         * Grows a connected subgraph by every non-empty subset of its neighborhood, recursively.
         * Nodes already offered as growth candidates are excluded from deeper levels, so that every
         * connected subgraph containing the initial one is produced exactly once.
         */
        private static boolean expandConnectedSubgraphs(long subgraph, long excluded, long nodes, long[] neighbors, LongPredicate consumer)
        {
            long candidates = neighborhood(subgraph, neighbors) & nodes & ~excluded;
            for (long subset = candidates; subset != 0; subset = (subset - 1) & candidates) {
                if (!consumer.test(subgraph | subset)) {
                    return false;
                }
            }
            for (long subset = candidates; subset != 0; subset = (subset - 1) & candidates) {
                if (!expandConnectedSubgraphs(subgraph | subset, excluded | candidates, nodes, neighbors, consumer)) {
                    return false;
                }
            }
            return true;
        }

        private static long neighborhood(long nodes, long[] neighbors)
        {
            long result = 0;
            for (long remaining = nodes; remaining != 0; remaining &= remaining - 1) {
                result |= neighbors[Long.numberOfTrailingZeros(remaining)];
            }
            return result & ~nodes;
        }

        @VisibleForTesting
        static boolean isConnected(long nodes, long[] neighbors)
        {
            long reached = Long.lowestOneBit(nodes);
            long frontier = reached;
            while (frontier != 0) {
                frontier = neighborhood(reached, neighbors) & nodes;
                reached |= frontier;
            }
            return reached == nodes;
        }

        private static long allNodes(int count)
        {
            return (1L << count) - 1;
        }

        /**
         * Produces the plan for a set of sources. The enumeration resolves it recursively, while the
         * greedy planner looks it up among the groups it has already joined.
         */
        @FunctionalInterface
        private interface SourceResolver
        {
            JoinEnumerationResult resolve(long nodes, Set<Symbol> requiredOutputs);
        }

        @VisibleForTesting
        JoinEnumerationResult createJoinAccordingToPartitioning(long nodes, Set<Symbol> requiredOutputs, long partitioning)
        {
            return createJoin(partitioning, nodes & ~partitioning, requiredOutputs);
        }

        private JoinEnumerationResult createJoin(long leftSources, long rightSources, Set<Symbol> requiredOutputs)
        {
            return createJoin(leftSources, rightSources, requiredOutputs, this::getJoinSource);
        }

        private JoinEnumerationResult createJoin(long leftSources, long rightSources, Set<Symbol> requiredOutputs, SourceResolver resolveSource)
        {
            Set<Symbol> leftSymbols = outputSymbols(leftSources);
            Set<Symbol> rightSymbols = outputSymbols(rightSources);

            List<Expression> joinPredicates = getJoinPredicates(leftSources | rightSources, leftSymbols);
            List<EquiJoinClause> joinConditions = joinPredicates.stream()
                    .map(JoinEnumerator::asJoinEqualityCondition)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(equality -> toEquiJoinClause(equality, leftSymbols))
                    .collect(toImmutableList());
            if (joinConditions.isEmpty()) {
                return INFINITE_COST_RESULT;
            }
            List<Expression> joinFilters = joinPredicates.stream()
                    .filter(predicate -> !isJoinEqualityCondition(predicate))
                    .collect(toImmutableList());

            Set<Symbol> requiredJoinSymbols = ImmutableSet.<Symbol>builder()
                    .addAll(requiredOutputs)
                    .addAll(extractUnique(joinPredicates))
                    .build();

            JoinEnumerationResult leftResult = resolveSource.resolve(
                    leftSources,
                    requiredJoinSymbols.stream()
                            .filter(leftSymbols::contains)
                            .collect(toImmutableSet()));
            if (leftResult.equals(UNKNOWN_COST_RESULT)) {
                return UNKNOWN_COST_RESULT;
            }
            if (leftResult.equals(INFINITE_COST_RESULT)) {
                return INFINITE_COST_RESULT;
            }
            PlanNode left = leftResult.planNode.orElseThrow(() -> new VerifyException("Plan node is not present"));

            JoinEnumerationResult rightResult = resolveSource.resolve(
                    rightSources,
                    requiredJoinSymbols.stream()
                            .filter(rightSymbols::contains)
                            .collect(toImmutableSet()));
            if (rightResult.equals(UNKNOWN_COST_RESULT)) {
                return UNKNOWN_COST_RESULT;
            }
            if (rightResult.equals(INFINITE_COST_RESULT)) {
                return INFINITE_COST_RESULT;
            }
            PlanNode right = rightResult.planNode.orElseThrow(() -> new VerifyException("Plan node is not present"));

            List<Symbol> leftOutputSymbols = left.getOutputSymbols().stream()
                    .filter(requiredOutputs::contains)
                    .collect(toImmutableList());
            List<Symbol> rightOutputSymbols = right.getOutputSymbols().stream()
                    .filter(requiredOutputs::contains)
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
                    ImmutableMap.of(),
                    Optional.empty()));
        }

        private List<Expression> getJoinPredicates(long nodes, Set<Symbol> leftSymbols)
        {
            // TODO: make generateEqualitiesPartitionedBy take left and right scope
            return joinInference(nodes).generateEqualitiesPartitionedBy(leftSymbols).getScopeStraddlingEqualities();
        }

        /**
         * The equalities available to join a set of sources depend only on which sources are
         * present, not on how they are split. Every partition of a set would otherwise re-derive
         * them, and building an inference is far from free.
         */
        private EqualityInference joinInference(long nodes)
        {
            return joinInferences.computeIfAbsent(nodes, mask ->
                    new EqualityInference(plannerContext, allFilterInference.generateEqualitiesPartitionedBy(outputSymbols(mask)).getScopeEqualities()));
        }

        private Set<Symbol> outputSymbols(long nodes)
        {
            ImmutableSet.Builder<Symbol> symbols = ImmutableSet.builder();
            for (long remaining = nodes; remaining != 0; remaining &= remaining - 1) {
                symbols.addAll(sources.get(Long.numberOfTrailingZeros(remaining)).getOutputSymbols());
            }
            return symbols.build();
        }

        private JoinEnumerationResult getJoinSource(long nodes, Set<Symbol> requiredOutputs)
        {
            if (Long.bitCount(nodes) == 1) {
                PlanNode planNode = sources.get(Long.numberOfTrailingZeros(nodes));
                if (sourcesPrejoined) {
                    // the greedy planner already planned every equality within this source's scope
                    // into it, so deriving them again would stack a filter that repeats predicates
                    // the source enforces
                    return createJoinEnumerationResult(planNode);
                }
                Set<Symbol> scope = ImmutableSet.copyOf(requiredOutputs);
                Expression filter = combineConjuncts(allFilterInference.generateEqualitiesPartitionedBy(scope).getScopeEqualities());
                if (!TRUE.equals(filter)) {
                    planNode = new FilterNode(idAllocator.getNextId(), planNode, filter);
                }
                return createJoinEnumerationResult(planNode);
            }
            return chooseJoinOrder(nodes, requiredOutputs);
        }

        private static boolean isJoinEqualityCondition(Expression expression)
        {
            return asJoinEqualityCondition(expression).isPresent();
        }

        private static Optional<Comparison.Equal> asJoinEqualityCondition(Expression expression)
        {
            if (matchComparison(expression) instanceof Comparison.Equal(Reference left, Reference right)) {
                return Optional.of(new Comparison.Equal(left, right));
            }
            return Optional.empty();
        }

        private static EquiJoinClause toEquiJoinClause(Comparison.Equal equality, Set<Symbol> leftSymbols)
        {
            Symbol leftSymbol = Symbol.from(equality.left());
            Symbol rightSymbol = Symbol.from(equality.right());
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

            return switch (distributionType) {
                case PARTITIONED -> getPossibleJoinNodes(joinNode, PARTITIONED);
                case BROADCAST -> getPossibleJoinNodes(joinNode, REPLICATED);
                case AUTOMATIC -> ImmutableList.<JoinEnumerationResult>builder()
                        .addAll(getPossibleJoinNodes(joinNode, PARTITIONED))
                        .addAll(getPossibleJoinNodes(joinNode, REPLICATED, node -> canReplicate(node, context)))
                        .build();
            };
        }

        private List<JoinEnumerationResult> getPossibleJoinNodes(JoinNode joinNode, DistributionType distributionType)
        {
            return getPossibleJoinNodes(joinNode, distributionType, _ -> true);
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
                            statsEstimate.getOutputSizeInBytes(joinNode.getOutputSymbols()),
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
            if (!(obj instanceof MultiJoinNode other)) {
                return false;
            }

            return this.sources.equals(other.sources)
                    && ImmutableSet.copyOf(extractConjuncts(this.filter)).equals(ImmutableSet.copyOf(extractConjuncts(other.filter)))
                    && this.outputSymbols.equals(other.outputSymbols)
                    && this.pushedProjectionThroughJoin == other.pushedProjectionThroughJoin;
        }

        static MultiJoinNode toMultiJoinNode(JoinNode joinNode, Context context, boolean pushProjectionsThroughJoin, Metadata metadata)
        {
            return toMultiJoinNode(
                    joinNode,
                    context.getLookup(),
                    context.getIdAllocator(),
                    getMaxReorderedJoins(context.getSession()),
                    pushProjectionsThroughJoin,
                    context.getSession(),
                    metadata);
        }

        static MultiJoinNode toMultiJoinNode(
                JoinNode joinNode,
                Lookup lookup,
                PlanNodeIdAllocator planNodeIdAllocator,
                int joinLimit,
                boolean pushProjectionsThroughJoin,
                Session session,
                Metadata metadata)
        {
            // the number of sources is the number of joins + 1
            return new JoinNodeFlattener(joinNode, lookup, planNodeIdAllocator, joinLimit + 1, pushProjectionsThroughJoin, metadata)
                    .toMultiJoinNode();
        }

        private static class JoinNodeFlattener
        {
            private final Lookup lookup;
            private final PlanNodeIdAllocator planNodeIdAllocator;

            private final LinkedHashSet<PlanNode> sources = new LinkedHashSet<>();
            private final List<Expression> filters = new ArrayList<>();
            private final List<Symbol> outputSymbols;
            private final boolean pushProjectionsThroughJoin;
            private final Metadata metadata;

            // if projection was pushed through join during join graph flattening?
            private boolean pushedProjectionThroughJoin;

            JoinNodeFlattener(
                    JoinNode node,
                    Lookup lookup,
                    PlanNodeIdAllocator planNodeIdAllocator,
                    int sourceLimit,
                    boolean pushProjectionsThroughJoin,
                    Metadata metadata)
            {
                requireNonNull(node, "node is null");
                checkState(node.getType() == INNER, "join type must be INNER");
                this.outputSymbols = node.getOutputSymbols();
                this.lookup = requireNonNull(lookup, "lookup is null");
                this.planNodeIdAllocator = requireNonNull(planNodeIdAllocator, "planNodeIdAllocator is null");
                this.pushProjectionsThroughJoin = pushProjectionsThroughJoin;
                this.metadata = requireNonNull(metadata, "metadata is null");

                flattenNode(node, sourceLimit);
            }

            private void flattenNode(PlanNode node, int limit)
            {
                PlanNode resolved = lookup.resolve(node);

                if (resolved instanceof ProjectNode projectNode) {
                    if (!pushProjectionsThroughJoin) {
                        sources.add(node);
                        return;
                    }

                    Optional<PlanNode> rewrittenNode = pushProjectionThroughJoin(projectNode, lookup, planNodeIdAllocator);
                    if (rewrittenNode.isEmpty()) {
                        sources.add(node);
                        return;
                    }

                    pushedProjectionThroughJoin = true;
                    flattenNode(rewrittenNode.get(), limit);
                    return;
                }

                // (limit - 2) because you need to account for adding left and right side
                if (!(resolved instanceof JoinNode joinNode) || (sources.size() > (limit - 2))) {
                    sources.add(node);
                    return;
                }

                if (joinNode.getType() != INNER || !isDeterministic(joinNode.getFilter().orElse(TRUE)) || joinNode.getDistributionType().isPresent()) {
                    sources.add(node);
                    return;
                }

                // we set the left limit to limit - 1 to account for the node on the right
                flattenNode(joinNode.getLeft(), limit - 1);
                flattenNode(joinNode.getRight(), limit);
                joinNode.getCriteria().stream()
                        .map(clause -> clause.toExpression(metadata))
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
