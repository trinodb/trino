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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Booleans;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Not;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.EffectivePredicateExtractor;
import io.trino.sql.planner.EqualityInference;
import io.trino.sql.planner.IrExpressionInterpreter;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SampleNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.WindowNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.isEnableDynamicFiltering;
import static io.trino.SystemSessionProperties.isPredicatePushdownUseTableProperties;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.sql.DynamicFilters.createDynamicFilterExpression;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.IS_DISTINCT_FROM;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.IrExpressions.mayFail;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.ir.IrUtils.filterDeterministicConjuncts;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.EqualityInference.isInferenceCandidate;
import static io.trino.sql.planner.ExpressionSymbolInliner.inlineSymbols;
import static io.trino.sql.planner.SymbolsExtractor.extractUnique;
import static io.trino.sql.planner.iterative.rule.CanonicalizeExpressionRewriter.canonicalizeExpression;
import static io.trino.sql.planner.iterative.rule.UnwrapCastInComparison.unwrapCasts;
import static io.trino.sql.planner.plan.JoinType.FULL;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.JoinType.RIGHT;
import static java.util.Objects.requireNonNull;

public class PredicatePushDown
        implements PlanOptimizer
{
    private static final Set<Comparison.Operator> DYNAMIC_FILTERING_SUPPORTED_COMPARISONS = ImmutableSet.of(
            EQUAL,
            GREATER_THAN,
            GREATER_THAN_OR_EQUAL,
            LESS_THAN,
            LESS_THAN_OR_EQUAL);

    private final PlannerContext plannerContext;
    private final boolean useTableProperties;
    private final boolean dynamicFiltering;

    public PredicatePushDown(
            PlannerContext plannerContext,
            boolean useTableProperties,
            boolean dynamicFiltering)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.useTableProperties = useTableProperties;
        this.dynamicFiltering = dynamicFiltering;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Context context)
    {
        requireNonNull(plan, "plan is null");

        return SimplePlanRewriter.rewriteWith(
                new Rewriter(context.symbolAllocator(), context.idAllocator(), plannerContext, context.session(), useTableProperties, dynamicFiltering),
                plan,
                TRUE);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Expression>
    {
        private final SymbolAllocator symbolAllocator;
        private final PlanNodeIdAllocator idAllocator;
        private final PlannerContext plannerContext;
        private final Metadata metadata;
        private final Session session;
        private final boolean dynamicFiltering;
        private final EffectivePredicateExtractor effectivePredicateExtractor;

        private Rewriter(
                SymbolAllocator symbolAllocator,
                PlanNodeIdAllocator idAllocator,
                PlannerContext plannerContext,
                Session session,
                boolean useTableProperties,
                boolean dynamicFiltering)
        {
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.metadata = plannerContext.getMetadata();
            this.session = requireNonNull(session, "session is null");
            this.dynamicFiltering = dynamicFiltering;

            this.effectivePredicateExtractor = new EffectivePredicateExtractor(
                    plannerContext,
                    useTableProperties && isPredicatePushdownUseTableProperties(session));
        }

        @Override
        public PlanNode visitPlan(PlanNode node, RewriteContext<Expression> context)
        {
            PlanNode rewrittenNode = context.defaultRewrite(node, TRUE);
            if (!context.get().equals(TRUE)) {
                // Drop in a FilterNode b/c we cannot push our predicate down any further
                rewrittenNode = new FilterNode(idAllocator.getNextId(), rewrittenNode, context.get());
            }
            return rewrittenNode;
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<Expression> context)
        {
            boolean modified = false;
            ImmutableList.Builder<PlanNode> builder = ImmutableList.builder();
            for (int i = 0; i < node.getSources().size(); i++) {
                Map<Symbol, Reference> outputsToInputs = new HashMap<>();
                for (int index = 0; index < node.getInputs().get(i).size(); index++) {
                    outputsToInputs.put(
                            node.getOutputSymbols().get(index),
                            node.getInputs().get(i).get(index).toSymbolReference());
                }

                Expression sourcePredicate = inlineSymbols(outputsToInputs, context.get());
                PlanNode source = node.getSources().get(i);
                PlanNode rewrittenSource = context.rewrite(source, sourcePredicate);
                if (rewrittenSource != source) {
                    modified = true;
                }
                builder.add(rewrittenSource);
            }

            if (modified) {
                return new ExchangeNode(
                        node.getId(),
                        node.getType(),
                        node.getScope(),
                        node.getPartitioningScheme(),
                        builder.build(),
                        node.getInputs(),
                        node.getOrderingScheme());
            }

            return node;
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Expression> context)
        {
            List<Symbol> partitionSymbols = node.getPartitionBy();

            // TODO: This could be broader. We can push down conjucts if they are constant for all rows in a window partition.
            // The simplest way to guarantee this is if the conjucts are deterministic functions of the partitioning symbols.
            // This can leave out cases where they're both functions of some set of common expressions and the partitioning
            // function is injective, but that's a rare case. The majority of window nodes are expected to be partitioned by
            // pre-projected symbols.
            Predicate<Expression> isSupported = conjunct ->
                    isDeterministic(conjunct) &&
                            partitionSymbols.containsAll(extractUnique(conjunct));

            Map<Boolean, List<Expression>> conjuncts = extractConjuncts(context.get()).stream().collect(Collectors.partitioningBy(isSupported));

            PlanNode rewrittenNode = context.defaultRewrite(node, combineConjuncts(conjuncts.get(true)));

            if (!conjuncts.get(false).isEmpty()) {
                rewrittenNode = new FilterNode(idAllocator.getNextId(), rewrittenNode, combineConjuncts(conjuncts.get(false)));
            }

            return rewrittenNode;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Expression> context)
        {
            Set<Symbol> deterministicSymbols = node.getAssignments().entrySet().stream()
                    .filter(entry -> isDeterministic(entry.getValue()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());

            Predicate<Expression> deterministic = conjunct -> deterministicSymbols.containsAll(extractUnique(conjunct));

            Map<Boolean, List<Expression>> conjuncts = extractConjuncts(context.get()).stream().collect(Collectors.partitioningBy(deterministic));

            // Push down conjuncts from the inherited predicate that only depend on deterministic assignments with
            // certain limitations.
            List<Expression> deterministicConjuncts = conjuncts.get(true);

            // We partition the expressions in the deterministicConjuncts into two lists, and only inline the
            // expressions that are in the inlining targets list.
            Map<Boolean, List<Expression>> inlineConjuncts = deterministicConjuncts.stream()
                    .collect(Collectors.partitioningBy(expression -> isInliningCandidate(expression, node)));

            List<Expression> inlinedDeterministicConjuncts = inlineConjuncts.get(true).stream()
                    .map(entry -> inlineSymbols(node.getAssignments().getMap(), entry))
                    .map(conjunct -> canonicalizeExpression(conjunct, plannerContext)) // normalize expressions to a form that unwrapCasts understands
                    .map(conjunct -> unwrapCasts(session, plannerContext, conjunct))
                    .collect(Collectors.toList());

            PlanNode rewrittenNode = context.defaultRewrite(node, combineConjuncts(inlinedDeterministicConjuncts));

            // All deterministic conjuncts that contains non-inlining targets, and non-deterministic conjuncts,
            // if any, will be in the filter node.
            List<Expression> nonInliningConjuncts = inlineConjuncts.get(false);
            nonInliningConjuncts.addAll(conjuncts.get(false));

            if (!nonInliningConjuncts.isEmpty()) {
                rewrittenNode = new FilterNode(idAllocator.getNextId(), rewrittenNode, combineConjuncts(nonInliningConjuncts));
            }

            return rewrittenNode;
        }

        private boolean isInliningCandidate(Expression expression, ProjectNode node)
        {
            // candidate symbols for inlining are
            //   1. references to simple constants or symbol references
            //   2. references to complex expressions that appear only once
            // which come from the node, as opposed to an enclosing scope.
            Set<Symbol> childOutputSet = ImmutableSet.copyOf(node.getOutputSymbols());
            Map<Symbol, Long> dependencies = SymbolsExtractor.extractAll(expression).stream()
                    .filter(childOutputSet::contains)
                    .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

            return dependencies.entrySet().stream()
                    .allMatch(entry -> entry.getValue() == 1
                            || node.getAssignments().get(entry.getKey()) instanceof Constant
                            || node.getAssignments().get(entry.getKey()) instanceof Reference);
        }

        @Override
        public PlanNode visitGroupId(GroupIdNode node, RewriteContext<Expression> context)
        {
            Map<Symbol, Reference> commonGroupingSymbolMapping = node.getGroupingColumns().entrySet().stream()
                    .filter(entry -> node.getCommonGroupingColumns().contains(entry.getKey()))
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().toSymbolReference()));

            Predicate<Expression> pushdownEligiblePredicate = conjunct -> commonGroupingSymbolMapping.keySet().containsAll(extractUnique(conjunct));

            Map<Boolean, List<Expression>> conjuncts = extractConjuncts(context.get()).stream().collect(Collectors.partitioningBy(pushdownEligiblePredicate));

            // Push down conjuncts from the inherited predicate that apply to common grouping symbols
            PlanNode rewrittenNode = context.defaultRewrite(node, inlineSymbols(commonGroupingSymbolMapping, combineConjuncts(conjuncts.get(true))));

            // All other conjuncts, if any, will be in the filter node.
            if (!conjuncts.get(false).isEmpty()) {
                rewrittenNode = new FilterNode(idAllocator.getNextId(), rewrittenNode, combineConjuncts(conjuncts.get(false)));
            }

            return rewrittenNode;
        }

        @Override
        public PlanNode visitMarkDistinct(MarkDistinctNode node, RewriteContext<Expression> context)
        {
            Set<Symbol> pushDownableSymbols = ImmutableSet.copyOf(node.getDistinctSymbols());
            Map<Boolean, List<Expression>> conjuncts = extractConjuncts(context.get()).stream()
                    .collect(Collectors.partitioningBy(conjunct -> pushDownableSymbols.containsAll(extractUnique(conjunct))));

            PlanNode rewrittenNode = context.defaultRewrite(node, combineConjuncts(conjuncts.get(true)));

            if (!conjuncts.get(false).isEmpty()) {
                rewrittenNode = new FilterNode(idAllocator.getNextId(), rewrittenNode, combineConjuncts(conjuncts.get(false)));
            }
            return rewrittenNode;
        }

        @Override
        public PlanNode visitSort(SortNode node, RewriteContext<Expression> context)
        {
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitUnion(UnionNode node, RewriteContext<Expression> context)
        {
            boolean modified = false;
            ImmutableList.Builder<PlanNode> builder = ImmutableList.builder();
            for (int i = 0; i < node.getSources().size(); i++) {
                Expression sourcePredicate = inlineSymbols(node.sourceSymbolMap(i), context.get());
                PlanNode source = node.getSources().get(i);
                PlanNode rewrittenSource = context.rewrite(source, sourcePredicate);
                if (rewrittenSource != source) {
                    modified = true;
                }
                builder.add(rewrittenSource);
            }

            if (modified) {
                return new UnionNode(node.getId(), builder.build(), node.getSymbolMapping(), node.getOutputSymbols());
            }

            return node;
        }

        @Deprecated
        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Expression> context)
        {
            PlanNode rewrittenPlan = context.rewrite(node.getSource(), combineConjuncts(node.getPredicate(), context.get()));
            if (!(rewrittenPlan instanceof FilterNode rewrittenFilterNode)) {
                return rewrittenPlan;
            }

            if (!rewrittenFilterNode.getPredicate().equals(node.getPredicate())
                    || node.getSource() != rewrittenFilterNode.getSource()) {
                return rewrittenPlan;
            }

            return node;
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Expression> context)
        {
            Expression inheritedPredicate = context.get();

            // See if we can rewrite outer joins in terms of a plain inner join
            node = tryNormalizeToOuterToInnerJoin(node, inheritedPredicate);

            Expression leftEffectivePredicate = effectivePredicateExtractor.extract(session, node.getLeft());
            Expression rightEffectivePredicate = effectivePredicateExtractor.extract(session, node.getRight());
            Expression joinPredicate = extractJoinPredicate(node);

            Expression leftPredicate;
            Expression rightPredicate;
            Expression postJoinPredicate;
            Expression newJoinPredicate;

            switch (node.getType()) {
                case INNER -> {
                    InnerJoinPushDownResult innerJoinPushDownResult = processInnerJoin(
                            inheritedPredicate,
                            leftEffectivePredicate,
                            rightEffectivePredicate,
                            joinPredicate,
                            node.getLeft().getOutputSymbols(),
                            node.getRight().getOutputSymbols());
                    leftPredicate = innerJoinPushDownResult.getLeftPredicate();
                    rightPredicate = innerJoinPushDownResult.getRightPredicate();
                    postJoinPredicate = innerJoinPushDownResult.getPostJoinPredicate();
                    newJoinPredicate = innerJoinPushDownResult.getJoinPredicate();
                }
                case LEFT -> {
                    OuterJoinPushDownResult leftOuterJoinPushDownResult = processLimitedOuterJoin(
                            inheritedPredicate,
                            leftEffectivePredicate,
                            rightEffectivePredicate,
                            joinPredicate,
                            node.getLeft().getOutputSymbols(),
                            node.getRight().getOutputSymbols());
                    leftPredicate = leftOuterJoinPushDownResult.getOuterJoinPredicate();
                    rightPredicate = leftOuterJoinPushDownResult.getInnerJoinPredicate();
                    postJoinPredicate = leftOuterJoinPushDownResult.getPostJoinPredicate();
                    newJoinPredicate = leftOuterJoinPushDownResult.getJoinPredicate();
                }
                case RIGHT -> {
                    OuterJoinPushDownResult rightOuterJoinPushDownResult = processLimitedOuterJoin(
                            inheritedPredicate,
                            rightEffectivePredicate,
                            leftEffectivePredicate,
                            joinPredicate,
                            node.getRight().getOutputSymbols(),
                            node.getLeft().getOutputSymbols());
                    leftPredicate = rightOuterJoinPushDownResult.getInnerJoinPredicate();
                    rightPredicate = rightOuterJoinPushDownResult.getOuterJoinPredicate();
                    postJoinPredicate = rightOuterJoinPushDownResult.getPostJoinPredicate();
                    newJoinPredicate = rightOuterJoinPushDownResult.getJoinPredicate();
                }
                case FULL -> {
                    leftPredicate = TRUE;
                    rightPredicate = TRUE;
                    postJoinPredicate = inheritedPredicate;
                    newJoinPredicate = joinPredicate;
                }
                default -> throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }

            newJoinPredicate = simplifyExpression(newJoinPredicate);

            // Create identity projections for all existing symbols
            Assignments.Builder leftProjections = Assignments.builder();
            leftProjections.putAll(node.getLeft()
                    .getOutputSymbols().stream()
                    .collect(toImmutableMap(key -> key, Symbol::toSymbolReference)));

            Assignments.Builder rightProjections = Assignments.builder();
            rightProjections.putAll(node.getRight()
                    .getOutputSymbols().stream()
                    .collect(toImmutableMap(key -> key, Symbol::toSymbolReference)));

            // Create new projections for the new join clauses
            List<JoinNode.EquiJoinClause> equiJoinClauses = new ArrayList<>();
            ImmutableList.Builder<Expression> joinFilterBuilder = ImmutableList.builder();
            for (Expression conjunct : extractConjuncts(newJoinPredicate)) {
                if (joinEqualityExpression(conjunct, node.getLeft().getOutputSymbols(), node.getRight().getOutputSymbols())) {
                    Comparison equality = (Comparison) conjunct;

                    boolean alignedComparison = node.getLeft().getOutputSymbols().containsAll(extractUnique(equality.left()));
                    Expression leftExpression = alignedComparison ? equality.left() : equality.right();
                    Expression rightExpression = alignedComparison ? equality.right() : equality.left();

                    Symbol leftSymbol = symbolForExpression(leftExpression);
                    if (!node.getLeft().getOutputSymbols().contains(leftSymbol)) {
                        leftProjections.put(leftSymbol, leftExpression);
                    }

                    Symbol rightSymbol = symbolForExpression(rightExpression);
                    if (!node.getRight().getOutputSymbols().contains(rightSymbol)) {
                        rightProjections.put(rightSymbol, rightExpression);
                    }

                    equiJoinClauses.add(new JoinNode.EquiJoinClause(leftSymbol, rightSymbol));
                }
                else {
                    joinFilterBuilder.add(conjunct);
                }
            }

            List<Expression> joinFilter = joinFilterBuilder.build();
            DynamicFiltersResult dynamicFiltersResult = createDynamicFilters(node, equiJoinClauses, joinFilter, session, idAllocator);
            Map<DynamicFilterId, Symbol> dynamicFilters = dynamicFiltersResult.getDynamicFilters();
            leftPredicate = combineConjuncts(leftPredicate, combineConjuncts(dynamicFiltersResult.getPredicates()));

            PlanNode leftSource;
            PlanNode rightSource;
            boolean equiJoinClausesUnmodified = ImmutableSet.copyOf(equiJoinClauses).equals(ImmutableSet.copyOf(node.getCriteria()));
            if (!equiJoinClausesUnmodified) {
                leftSource = context.rewrite(new ProjectNode(idAllocator.getNextId(), node.getLeft(), leftProjections.build()), leftPredicate);
                rightSource = context.rewrite(new ProjectNode(idAllocator.getNextId(), node.getRight(), rightProjections.build()), rightPredicate);
            }
            else {
                leftSource = context.rewrite(node.getLeft(), leftPredicate);
                rightSource = context.rewrite(node.getRight(), rightPredicate);
            }

            Optional<Expression> newJoinFilter = Optional.of(combineConjuncts(joinFilter));
            if (newJoinFilter.get().equals(TRUE)) {
                newJoinFilter = Optional.empty();
            }

            if (node.getType() == INNER && newJoinFilter.isPresent() && equiJoinClauses.isEmpty()) {
                // if we do not have any equi conjunct we do not pushdown non-equality condition into
                // inner join, so we plan execution as nested-loops-join followed by filter instead
                // hash join.
                // todo: remove the code when we have support for filter function in nested loop join
                postJoinPredicate = combineConjuncts(postJoinPredicate, newJoinFilter.get());
                newJoinFilter = Optional.empty();
            }

            boolean filtersEquivalent =
                    newJoinFilter.isPresent() == node.getFilter().isPresent() &&
                            (newJoinFilter.isEmpty() || newJoinFilter.get().equals(node.getFilter().get()));

            PlanNode output = node;
            if (leftSource != node.getLeft() ||
                    rightSource != node.getRight() ||
                    !filtersEquivalent ||
                    !dynamicFilters.equals(node.getDynamicFilters()) ||
                    !equiJoinClausesUnmodified) {
                leftSource = new ProjectNode(idAllocator.getNextId(), leftSource, leftProjections.build());
                rightSource = new ProjectNode(idAllocator.getNextId(), rightSource, rightProjections.build());

                output = new JoinNode(
                        node.getId(),
                        node.getType(),
                        leftSource,
                        rightSource,
                        equiJoinClauses,
                        leftSource.getOutputSymbols(),
                        rightSource.getOutputSymbols(),
                        node.isMaySkipOutputDuplicates(),
                        newJoinFilter,
                        node.getLeftHashSymbol(),
                        node.getRightHashSymbol(),
                        node.getDistributionType(),
                        node.isSpillable(),
                        dynamicFilters,
                        node.getReorderJoinStatsAndCost());
            }

            if (!postJoinPredicate.equals(TRUE)) {
                output = new FilterNode(idAllocator.getNextId(), output, postJoinPredicate);
            }

            if (!node.getOutputSymbols().equals(output.getOutputSymbols())) {
                output = new ProjectNode(idAllocator.getNextId(), output, Assignments.identity(node.getOutputSymbols()));
            }

            return output;
        }

        // TODO: collect min/max ranges for inequality dynamic filters (https://github.com/trinodb/trino/issues/5754)
        // TODO: support for complex inequalities, e.g. left < right + 10 (https://github.com/trinodb/trino/issues/5755)
        private DynamicFiltersResult createDynamicFilters(
                JoinNode node,
                List<JoinNode.EquiJoinClause> equiJoinClauses,
                List<Expression> joinFilterClauses,
                Session session,
                PlanNodeIdAllocator idAllocator)
        {
            if ((node.getType() != INNER && node.getType() != RIGHT) || !isEnableDynamicFiltering(session) || !dynamicFiltering) {
                return new DynamicFiltersResult(ImmutableMap.of(), ImmutableList.of());
            }

            List<DynamicFilterExpression> clauses = Streams.concat(
                            equiJoinClauses
                                    .stream()
                                    .map(clause -> new DynamicFilterExpression(
                                            new Comparison(EQUAL, clause.getLeft().toSymbolReference(), clause.getRight().toSymbolReference()))),
                            joinFilterClauses.stream()
                                    .flatMap(Rewriter::tryConvertBetweenIntoComparisons)
                                    .filter(clause -> joinDynamicFilteringExpression(clause, node.getLeft().getOutputSymbols(), node.getRight().getOutputSymbols()))
                                    .map(expression -> {
                                        if (expression instanceof Not notExpression) {
                                            Comparison comparison = (Comparison) notExpression.value();
                                            return new DynamicFilterExpression(new Comparison(EQUAL, comparison.left(), comparison.right()), true);
                                        }
                                        return new DynamicFilterExpression((Comparison) expression);
                                    })
                                    .map(expression -> {
                                        Comparison comparison = expression.getComparison();
                                        Expression leftExpression = comparison.left();
                                        Expression rightExpression = comparison.right();
                                        boolean alignedComparison = node.getLeft().getOutputSymbols().containsAll(extractUnique(leftExpression));
                                        return new DynamicFilterExpression(
                                                new Comparison(
                                                        alignedComparison ? comparison.operator() : comparison.operator().flip(),
                                                        alignedComparison ? leftExpression : rightExpression,
                                                        alignedComparison ? rightExpression : leftExpression),
                                                expression.isNullAllowed());
                                    }))
                    .collect(toImmutableList());

            // New equiJoinClauses could potentially not contain symbols used in current dynamic filters.
            // Since we use PredicatePushdown to push dynamic filters themselves,
            // instead of separate ApplyDynamicFilters rule we derive dynamic filters within PredicatePushdown itself.
            // Even if equiJoinClauses.equals(node.getCriteria), current dynamic filters may not match equiJoinClauses

            // Collect build symbols:
            Set<Symbol> buildSymbols = clauses.stream()
                    .map(DynamicFilterExpression::getComparison)
                    .map(Comparison::right)
                    .map(Symbol::from)
                    .collect(toImmutableSet());

            // Allocate new dynamic filter IDs for each build symbol:
            BiMap<Symbol, DynamicFilterId> buildSymbolToDynamicFilter = HashBiMap.create(node.getDynamicFilters()).inverse();
            for (Symbol buildSymbol : buildSymbols) {
                buildSymbolToDynamicFilter.computeIfAbsent(
                        buildSymbol,
                        key -> new DynamicFilterId("df_" + idAllocator.getNextId().toString()));
            }

            // Multiple probe symbols may depend on a single build symbol / dynamic filter ID:
            List<Expression> predicates = clauses
                    .stream()
                    .map(clause -> {
                        Comparison comparison = clause.getComparison();
                        Expression probeExpression = comparison.left();
                        Symbol buildSymbol = Symbol.from(comparison.right());
                        // we can take type of buildSymbol instead probeExpression as comparison expression must have the same type on both sides
                        Type type = buildSymbol.type();
                        DynamicFilterId id = requireNonNull(buildSymbolToDynamicFilter.get(buildSymbol), () -> "missing dynamic filter for symbol " + buildSymbol);
                        return createDynamicFilterExpression(metadata, id, type, probeExpression, comparison.operator(), clause.isNullAllowed());
                    })
                    .collect(toImmutableList());
            // Return a mapping from build symbols to corresponding dynamic filter IDs:
            return new DynamicFiltersResult(buildSymbolToDynamicFilter.inverse(), predicates);
        }

        private static Stream<Expression> tryConvertBetweenIntoComparisons(Expression clause)
        {
            if (clause instanceof Between between) {
                return Stream.of(
                        new Comparison(GREATER_THAN_OR_EQUAL, between.value(), between.min()),
                        new Comparison(LESS_THAN_OR_EQUAL, between.value(), between.max()));
            }
            return Stream.of(clause);
        }

        private static class DynamicFilterExpression
        {
            private final Comparison comparison;
            private final boolean nullAllowed;

            private DynamicFilterExpression(Comparison comparison)
            {
                this(comparison, false);
            }

            private DynamicFilterExpression(Comparison comparison, boolean nullAllowed)
            {
                this.comparison = requireNonNull(comparison, "comparison is null");
                this.nullAllowed = nullAllowed;
            }

            public Comparison getComparison()
            {
                return comparison;
            }

            public boolean isNullAllowed()
            {
                return nullAllowed;
            }
        }

        private static class DynamicFiltersResult
        {
            private final Map<DynamicFilterId, Symbol> dynamicFilters;
            private final List<Expression> predicates;

            public DynamicFiltersResult(Map<DynamicFilterId, Symbol> dynamicFilters, List<Expression> predicates)
            {
                this.dynamicFilters = ImmutableMap.copyOf(dynamicFilters);
                this.predicates = ImmutableList.copyOf(predicates);
            }

            public Map<DynamicFilterId, Symbol> getDynamicFilters()
            {
                return dynamicFilters;
            }

            public List<Expression> getPredicates()
            {
                return predicates;
            }
        }

        @Override
        public PlanNode visitSpatialJoin(SpatialJoinNode node, RewriteContext<Expression> context)
        {
            Expression inheritedPredicate = context.get();

            // See if we can rewrite left join in terms of a plain inner join
            if (node.getType() == SpatialJoinNode.Type.LEFT && canConvertOuterToInner(node.getRight().getOutputSymbols(), inheritedPredicate)) {
                node = new SpatialJoinNode(node.getId(), SpatialJoinNode.Type.INNER, node.getLeft(), node.getRight(), node.getOutputSymbols(), node.getFilter(), node.getLeftPartitionSymbol(), node.getRightPartitionSymbol(), node.getKdbTree());
            }

            Expression leftEffectivePredicate = effectivePredicateExtractor.extract(session, node.getLeft());
            Expression rightEffectivePredicate = effectivePredicateExtractor.extract(session, node.getRight());
            Expression joinPredicate = node.getFilter();

            Expression leftPredicate;
            Expression rightPredicate;
            Expression postJoinPredicate;
            Expression newJoinPredicate;

            switch (node.getType()) {
                case INNER -> {
                    InnerJoinPushDownResult innerJoinPushDownResult = processInnerJoin(
                            inheritedPredicate,
                            leftEffectivePredicate,
                            rightEffectivePredicate,
                            joinPredicate,
                            node.getLeft().getOutputSymbols(),
                            node.getRight().getOutputSymbols());
                    leftPredicate = innerJoinPushDownResult.getLeftPredicate();
                    rightPredicate = innerJoinPushDownResult.getRightPredicate();
                    postJoinPredicate = innerJoinPushDownResult.getPostJoinPredicate();
                    newJoinPredicate = innerJoinPushDownResult.getJoinPredicate();
                }
                case LEFT -> {
                    OuterJoinPushDownResult leftOuterJoinPushDownResult = processLimitedOuterJoin(
                            inheritedPredicate,
                            leftEffectivePredicate,
                            rightEffectivePredicate,
                            joinPredicate,
                            node.getLeft().getOutputSymbols(),
                            node.getRight().getOutputSymbols());
                    leftPredicate = leftOuterJoinPushDownResult.getOuterJoinPredicate();
                    rightPredicate = leftOuterJoinPushDownResult.getInnerJoinPredicate();
                    postJoinPredicate = leftOuterJoinPushDownResult.getPostJoinPredicate();
                    newJoinPredicate = leftOuterJoinPushDownResult.getJoinPredicate();
                }
                default -> throw new IllegalArgumentException("Unsupported spatial join type: " + node.getType());
            }

            newJoinPredicate = simplifyExpression(newJoinPredicate);
            verify(!newJoinPredicate.equals(Booleans.FALSE), "Spatial join predicate is missing");

            PlanNode leftSource = context.rewrite(node.getLeft(), leftPredicate);
            PlanNode rightSource = context.rewrite(node.getRight(), rightPredicate);

            PlanNode output = node;
            if (leftSource != node.getLeft() ||
                    rightSource != node.getRight() ||
                    !newJoinPredicate.equals(joinPredicate)) {
                // Create identity projections for all existing symbols
                Assignments.Builder leftProjections = Assignments.builder();
                leftProjections.putAll(node.getLeft()
                        .getOutputSymbols().stream()
                        .collect(toImmutableMap(key -> key, Symbol::toSymbolReference)));

                Assignments.Builder rightProjections = Assignments.builder();
                rightProjections.putAll(node.getRight()
                        .getOutputSymbols().stream()
                        .collect(toImmutableMap(key -> key, Symbol::toSymbolReference)));

                leftSource = new ProjectNode(idAllocator.getNextId(), leftSource, leftProjections.build());
                rightSource = new ProjectNode(idAllocator.getNextId(), rightSource, rightProjections.build());

                output = new SpatialJoinNode(
                        node.getId(),
                        node.getType(),
                        leftSource,
                        rightSource,
                        node.getOutputSymbols(),
                        newJoinPredicate,
                        node.getLeftPartitionSymbol(),
                        node.getRightPartitionSymbol(),
                        node.getKdbTree());
            }

            if (!postJoinPredicate.equals(TRUE)) {
                output = new FilterNode(idAllocator.getNextId(), output, postJoinPredicate);
            }

            return output;
        }

        private Symbol symbolForExpression(Expression expression)
        {
            if (expression instanceof Reference) {
                return Symbol.from(expression);
            }

            return symbolAllocator.newSymbol(expression);
        }

        private OuterJoinPushDownResult processLimitedOuterJoin(
                Expression inheritedPredicate,
                Expression outerEffectivePredicate,
                Expression innerEffectivePredicate,
                Expression joinPredicate,
                Collection<Symbol> outerSymbols,
                Collection<Symbol> innerSymbols)
        {
            checkArgument(outerSymbols.containsAll(extractUnique(outerEffectivePredicate)), "outerEffectivePredicate must only contain symbols from outerSymbols");
            checkArgument(innerSymbols.containsAll(extractUnique(innerEffectivePredicate)), "innerEffectivePredicate must only contain symbols from innerSymbols");

            ImmutableList.Builder<Expression> outerPushdownConjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> innerPushdownConjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> postJoinConjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> joinConjuncts = ImmutableList.builder();

            // Strip out non-deterministic conjuncts
            extractConjuncts(inheritedPredicate).stream()
                    .filter(expression -> !isDeterministic(expression))
                    .forEach(postJoinConjuncts::add);
            inheritedPredicate = filterDeterministicConjuncts(inheritedPredicate);

            outerEffectivePredicate = filterDeterministicConjuncts(outerEffectivePredicate);
            innerEffectivePredicate = filterDeterministicConjuncts(innerEffectivePredicate);
            extractConjuncts(joinPredicate).stream()
                    .filter(expression -> !isDeterministic(expression))
                    .forEach(joinConjuncts::add);
            joinPredicate = filterDeterministicConjuncts(joinPredicate);

            // Generate equality inferences
            EqualityInference inheritedInference = new EqualityInference(inheritedPredicate);
            EqualityInference outerInference = new EqualityInference(inheritedPredicate, outerEffectivePredicate);

            Set<Symbol> innerScope = ImmutableSet.copyOf(innerSymbols);
            Set<Symbol> outerScope = ImmutableSet.copyOf(outerSymbols);

            EqualityInference.EqualityPartition equalityPartition = inheritedInference.generateEqualitiesPartitionedBy(outerScope);
            Expression outerOnlyInheritedEqualities = combineConjuncts(equalityPartition.getScopeEqualities());
            EqualityInference potentialNullSymbolInference = new EqualityInference(outerOnlyInheritedEqualities, outerEffectivePredicate, innerEffectivePredicate, joinPredicate);

            // Push outer and join equalities into the inner side. For example:
            // SELECT * FROM nation LEFT OUTER JOIN region ON nation.regionkey = region.regionkey and nation.name = region.name WHERE nation.name = 'blah'

            EqualityInference potentialNullSymbolInferenceWithoutInnerInferred = new EqualityInference(outerOnlyInheritedEqualities, outerEffectivePredicate, joinPredicate);
            innerPushdownConjuncts.addAll(potentialNullSymbolInferenceWithoutInnerInferred.generateEqualitiesPartitionedBy(innerScope).getScopeEqualities());

            // TODO: we can further improve simplifying the equalities by considering other relationships from the outer side
            EqualityInference.EqualityPartition joinEqualityPartition = new EqualityInference(joinPredicate).generateEqualitiesPartitionedBy(innerScope);
            innerPushdownConjuncts.addAll(joinEqualityPartition.getScopeEqualities());
            joinConjuncts.addAll(joinEqualityPartition.getScopeComplementEqualities())
                    .addAll(joinEqualityPartition.getScopeStraddlingEqualities());

            // Add the equalities from the inferences back in
            outerPushdownConjuncts.addAll(equalityPartition.getScopeEqualities());
            postJoinConjuncts.addAll(equalityPartition.getScopeComplementEqualities());
            postJoinConjuncts.addAll(equalityPartition.getScopeStraddlingEqualities());

            // See if we can push inherited predicates down
            EqualityInference.nonInferrableConjuncts(inheritedPredicate).forEach(conjunct -> {
                Expression outerRewritten = outerInference.rewrite(conjunct, outerScope);
                if (outerRewritten != null) {
                    outerPushdownConjuncts.add(outerRewritten);

                    // A conjunct can only be pushed down into an inner side if it can be rewritten in terms of the outer side
                    Expression innerRewritten = potentialNullSymbolInference.rewrite(outerRewritten, innerScope);
                    if (innerRewritten != null) {
                        innerPushdownConjuncts.add(innerRewritten);
                    }
                }
                else {
                    postJoinConjuncts.add(conjunct);
                }
            });

            // See if we can push down any outer effective predicates to the inner side
            EqualityInference.nonInferrableConjuncts(outerEffectivePredicate)
                    .map(conjunct -> potentialNullSymbolInference.rewrite(conjunct, innerScope))
                    .filter(Objects::nonNull)
                    .forEach(innerPushdownConjuncts::add);

            // See if we can push down join predicates to the inner side
            EqualityInference.nonInferrableConjuncts(joinPredicate).forEach(conjunct -> {
                Expression innerRewritten = potentialNullSymbolInference.rewrite(conjunct, innerScope);
                if (innerRewritten != null) {
                    innerPushdownConjuncts.add(innerRewritten);
                }
                else {
                    joinConjuncts.add(conjunct);
                }
            });

            return new OuterJoinPushDownResult(combineConjuncts(outerPushdownConjuncts.build()),
                    combineConjuncts(innerPushdownConjuncts.build()),
                    combineConjuncts(joinConjuncts.build()),
                    combineConjuncts(postJoinConjuncts.build()));
        }

        private static class OuterJoinPushDownResult
        {
            private final Expression outerJoinPredicate;
            private final Expression innerJoinPredicate;
            private final Expression joinPredicate;
            private final Expression postJoinPredicate;

            private OuterJoinPushDownResult(Expression outerJoinPredicate, Expression innerJoinPredicate, Expression joinPredicate, Expression postJoinPredicate)
            {
                this.outerJoinPredicate = outerJoinPredicate;
                this.innerJoinPredicate = innerJoinPredicate;
                this.joinPredicate = joinPredicate;
                this.postJoinPredicate = postJoinPredicate;
            }

            private Expression getOuterJoinPredicate()
            {
                return outerJoinPredicate;
            }

            private Expression getInnerJoinPredicate()
            {
                return innerJoinPredicate;
            }

            public Expression getJoinPredicate()
            {
                return joinPredicate;
            }

            private Expression getPostJoinPredicate()
            {
                return postJoinPredicate;
            }
        }

        private InnerJoinPushDownResult processInnerJoin(
                Expression inheritedPredicate,
                Expression leftEffectivePredicate,
                Expression rightEffectivePredicate,
                Expression joinPredicate,
                Collection<Symbol> leftSymbols,
                Collection<Symbol> rightSymbols)
        {
            checkArgument(leftSymbols.containsAll(extractUnique(leftEffectivePredicate)), "leftEffectivePredicate must only contain symbols from leftSymbols");
            checkArgument(rightSymbols.containsAll(extractUnique(rightEffectivePredicate)), "rightEffectivePredicate must only contain symbols from rightSymbols");

            List<Expression> nonDeterministic = new ArrayList<>();
            List<Expression> candidates = new ArrayList<>();
            List<Expression> residuals = new ArrayList<>();
            List<Expression> mayFail = new ArrayList<>();
            for (Expression predicate : List.of(joinPredicate, inheritedPredicate)) {
                List<Expression> conjuncts = extractConjuncts(predicate);

                for (Expression conjunct : conjuncts) {
                    if (!isDeterministic(conjunct)) {
                        nonDeterministic.add(conjunct);
                    }
                    else if (mayFail(plannerContext, conjunct)) {
                        mayFail.add(conjunct);
                    }
                    else if (isInferenceCandidate(conjunct)) {
                        candidates.add(conjunct);
                    }
                    else {
                        residuals.add(conjunct);
                    }
                }
            }

            List<Expression> leftConjuncts = extractConjuncts(leftEffectivePredicate).stream()
                    .filter(expression -> !mayFail(plannerContext, expression) && isDeterministic(expression))
                    .toList();

            List<Expression> leftCandidates = leftConjuncts.stream()
                    .filter(EqualityInference::isInferenceCandidate)
                    .toList();

            List<Expression> leftResiduals = leftConjuncts.stream()
                    .filter(conjunct -> !isInferenceCandidate(conjunct))
                    .toList();

            List<Expression> rightConjuncts = extractConjuncts(rightEffectivePredicate).stream()
                    .filter(expression -> !mayFail(plannerContext, expression) && isDeterministic(expression))
                    .toList();

            List<Expression> rightCandidates = rightConjuncts.stream()
                    .filter(EqualityInference::isInferenceCandidate)
                    .toList();

            List<Expression> rightResiduals = rightConjuncts.stream()
                    .filter(conjunct -> !isInferenceCandidate(conjunct))
                    .toList();

            ImmutableSet<Symbol> leftScope = ImmutableSet.copyOf(leftSymbols);
            ImmutableSet<Symbol> rightScope = ImmutableSet.copyOf(rightSymbols);

            EqualityInference allInference = new EqualityInference(
                    ImmutableList.<Expression>builder()
                            .addAll(candidates)
                            .addAll(leftCandidates)
                            .addAll(rightCandidates)
                            .build());
            EqualityInference inferenceWithoutLeft = new EqualityInference(
                    ImmutableList.<Expression>builder()
                            .addAll(candidates)
                            .addAll(rightCandidates)
                            .build());
            EqualityInference inferenceWithoutRight = new EqualityInference(
                    ImmutableList.<Expression>builder()
                            .addAll(candidates)
                            .addAll(leftCandidates)
                            .build());

            ImmutableList.Builder<Expression> leftPushDownConjuncts = ImmutableList.<Expression>builder()
                    .addAll(inferenceWithoutLeft.generateEqualitiesPartitionedBy(leftScope).getScopeEqualities())
                    .addAll(rightResiduals.stream()
                            .map(conjunct -> allInference.rewrite(conjunct, leftScope))
                            .filter(Objects::nonNull)
                            .toList());

            ImmutableList.Builder<Expression> rightPushDownConjuncts = ImmutableList.<Expression>builder()
                    .addAll(inferenceWithoutRight.generateEqualitiesPartitionedBy(rightScope).getScopeEqualities())
                    .addAll(leftResiduals.stream()
                            .map(conjunct -> allInference.rewrite(conjunct, rightScope))
                            .filter(Objects::nonNull)
                            .toList());

            ImmutableList.Builder<Expression> joinConjuncts = ImmutableList.<Expression>builder()
                    .addAll(allInference.generateEqualitiesPartitionedBy(leftScope).getScopeStraddlingEqualities())
                    .addAll(nonDeterministic);

            residuals.forEach(conjunct -> {
                Expression leftRewrittenConjunct = allInference.rewrite(conjunct, leftScope);
                if (leftRewrittenConjunct != null) {
                    leftPushDownConjuncts.add(leftRewrittenConjunct);
                }

                Expression rightRewrittenConjunct = allInference.rewrite(conjunct, rightScope);
                if (rightRewrittenConjunct != null) {
                    rightPushDownConjuncts.add(rightRewrittenConjunct);
                }

                // Drop predicate after join only if unable to push down to either side
                if (leftRewrittenConjunct == null && rightRewrittenConjunct == null) {
                    joinConjuncts.add(allInference.rewrite(conjunct, Sets.union(leftScope, rightScope)));
                }
            });

            boolean doNotPush = !combineConjuncts(joinConjuncts.build()).equals(TRUE);
            // attempt to push down the predicates that may fail
            for (Expression conjunct : mayFail) {
                if (doNotPush) {
                    joinConjuncts.add(allInference.rewrite(conjunct, Sets.union(leftScope, rightScope)));
                }
                else {
                    Expression leftRewrittenConjunct = allInference.rewrite(conjunct, leftScope);
                    if (leftRewrittenConjunct != null) {
                        leftPushDownConjuncts.add(leftRewrittenConjunct);
                    }

                    Expression rightRewrittenConjunct = allInference.rewrite(conjunct, rightScope);
                    if (rightRewrittenConjunct != null) {
                        rightPushDownConjuncts.add(rightRewrittenConjunct);
                    }

                    if (leftRewrittenConjunct == null && rightRewrittenConjunct == null) {
                        joinConjuncts.add(allInference.rewrite(conjunct, Sets.union(leftScope, rightScope)));
                        doNotPush = true; // we can't push any of the remaining conjuncts
                    }
                }
            }

            return new InnerJoinPushDownResult(
                    combineConjuncts(leftPushDownConjuncts.build()),
                    combineConjuncts(rightPushDownConjuncts.build()),
                    combineConjuncts(joinConjuncts.build()),
                    TRUE);
        }

        private static class InnerJoinPushDownResult
        {
            private final Expression leftPredicate;
            private final Expression rightPredicate;
            private final Expression joinPredicate;
            private final Expression postJoinPredicate;

            private InnerJoinPushDownResult(Expression leftPredicate, Expression rightPredicate, Expression joinPredicate, Expression postJoinPredicate)
            {
                this.leftPredicate = leftPredicate;
                this.rightPredicate = rightPredicate;
                this.joinPredicate = joinPredicate;
                this.postJoinPredicate = postJoinPredicate;
            }

            private Expression getLeftPredicate()
            {
                return leftPredicate;
            }

            private Expression getRightPredicate()
            {
                return rightPredicate;
            }

            private Expression getJoinPredicate()
            {
                return joinPredicate;
            }

            private Expression getPostJoinPredicate()
            {
                return postJoinPredicate;
            }
        }

        private Expression extractJoinPredicate(JoinNode joinNode)
        {
            ImmutableList.Builder<Expression> builder = ImmutableList.builder();
            for (JoinNode.EquiJoinClause equiJoinClause : joinNode.getCriteria()) {
                builder.add(equiJoinClause.toExpression());
            }
            joinNode.getFilter().ifPresent(builder::add);
            return combineConjuncts(builder.build());
        }

        private JoinNode tryNormalizeToOuterToInnerJoin(JoinNode node, Expression inheritedPredicate)
        {
            checkArgument(EnumSet.of(INNER, RIGHT, LEFT, FULL).contains(node.getType()), "Unsupported join type: %s", node.getType());

            if (node.getType() == JoinType.INNER) {
                return node;
            }

            if (node.getType() == JoinType.FULL) {
                boolean canConvertToLeftJoin = canConvertOuterToInner(node.getLeft().getOutputSymbols(), inheritedPredicate);
                boolean canConvertToRightJoin = canConvertOuterToInner(node.getRight().getOutputSymbols(), inheritedPredicate);
                if (!canConvertToLeftJoin && !canConvertToRightJoin) {
                    return node;
                }
                if (canConvertToLeftJoin && canConvertToRightJoin) {
                    return new JoinNode(
                            node.getId(),
                            INNER,
                            node.getLeft(),
                            node.getRight(),
                            node.getCriteria(),
                            node.getLeftOutputSymbols(),
                            node.getRightOutputSymbols(),
                            node.isMaySkipOutputDuplicates(),
                            node.getFilter(),
                            node.getLeftHashSymbol(),
                            node.getRightHashSymbol(),
                            node.getDistributionType(),
                            node.isSpillable(),
                            node.getDynamicFilters(),
                            node.getReorderJoinStatsAndCost());
                }
                return new JoinNode(
                        node.getId(),
                        canConvertToLeftJoin ? LEFT : RIGHT,
                        node.getLeft(),
                        node.getRight(),
                        node.getCriteria(),
                        node.getLeftOutputSymbols(),
                        node.getRightOutputSymbols(),
                        node.isMaySkipOutputDuplicates(),
                        node.getFilter(),
                        node.getLeftHashSymbol(),
                        node.getRightHashSymbol(),
                        node.getDistributionType(),
                        node.isSpillable(),
                        node.getDynamicFilters(),
                        node.getReorderJoinStatsAndCost());
            }

            if (node.getType() == JoinType.LEFT && !canConvertOuterToInner(node.getRight().getOutputSymbols(), inheritedPredicate) ||
                    node.getType() == JoinType.RIGHT && !canConvertOuterToInner(node.getLeft().getOutputSymbols(), inheritedPredicate)) {
                return node;
            }
            return new JoinNode(
                    node.getId(),
                    JoinType.INNER,
                    node.getLeft(),
                    node.getRight(),
                    node.getCriteria(),
                    node.getLeftOutputSymbols(),
                    node.getRightOutputSymbols(),
                    node.isMaySkipOutputDuplicates(),
                    node.getFilter(),
                    node.getLeftHashSymbol(),
                    node.getRightHashSymbol(),
                    node.getDistributionType(),
                    node.isSpillable(),
                    node.getDynamicFilters(),
                    node.getReorderJoinStatsAndCost());
        }

        private boolean canConvertOuterToInner(List<Symbol> innerSymbolsForOuterJoin, Expression inheritedPredicate)
        {
            Set<Symbol> innerSymbols = ImmutableSet.copyOf(innerSymbolsForOuterJoin);
            for (Expression conjunct : extractConjuncts(inheritedPredicate)) {
                if (isDeterministic(conjunct)) {
                    // Ignore a conjunct for this test if we cannot deterministically get responses from it
                    Expression response = nullInputEvaluator(innerSymbols, conjunct);
                    if (response instanceof Constant constant && (constant.value() == null || Boolean.FALSE.equals(constant.value()))) {
                        // If there is a single conjunct that returns FALSE or NULL given all NULL inputs for the inner side symbols of an outer join
                        // then this conjunct removes all effects of the outer join, and effectively turns this into an equivalent of an inner join.
                        // So, let's just rewrite this join as an INNER join
                        return true;
                    }
                }
            }
            return false;
        }

        // Temporary implementation for joins because the SimplifyExpressions optimizers cannot run properly on join clauses
        private Expression simplifyExpression(Expression expression)
        {
            return new IrExpressionInterpreter(expression, plannerContext, session).optimize();
        }

        /**
         * Evaluates an expression's response to binding the specified input symbols to NULL
         */
        private Expression nullInputEvaluator(Collection<Symbol> nullSymbols, Expression expression)
        {
            return new IrExpressionInterpreter(expression, plannerContext, session)
                    .optimize(symbol -> nullSymbols.contains(symbol) ? Optional.of(new Constant(symbol.type(), null)) : Optional.empty());
        }

        private boolean joinEqualityExpression(Expression expression, Collection<Symbol> leftSymbols, Collection<Symbol> rightSymbols)
        {
            return joinComparisonExpression(expression, leftSymbols, rightSymbols, ImmutableSet.of(EQUAL));
        }

        private boolean joinDynamicFilteringExpression(Expression expression, Collection<Symbol> leftSymbols, Collection<Symbol> rightSymbols)
        {
            Comparison comparison;
            if (expression instanceof Not not) {
                boolean isDistinctFrom = joinComparisonExpression(not.value(), leftSymbols, rightSymbols, ImmutableSet.of(IS_DISTINCT_FROM));
                if (!isDistinctFrom) {
                    return false;
                }
                comparison = (Comparison) not.value();
                Set<Type> expressionTypes = ImmutableSet.of(
                        comparison.left().type(),
                        comparison.right().type());
                // Dynamic filtering is not supported with IS NOT DISTINCT FROM clause on REAL or DOUBLE types to avoid dealing with NaN values
                if (expressionTypes.contains(REAL) || expressionTypes.contains(DOUBLE)) {
                    return false;
                }
            }
            else {
                if (!joinComparisonExpression(expression, leftSymbols, rightSymbols, DYNAMIC_FILTERING_SUPPORTED_COMPARISONS)) {
                    return false;
                }
                comparison = (Comparison) expression;
            }

            // Build side expression must be a symbol reference, since DynamicFilterSourceOperator can only collect column values (not expressions)
            return (comparison.right() instanceof Reference && rightSymbols.contains(Symbol.from(comparison.right())))
                    || (comparison.left() instanceof Reference && rightSymbols.contains(Symbol.from(comparison.left())));
        }

        private boolean joinComparisonExpression(Expression expression, Collection<Symbol> leftSymbols, Collection<Symbol> rightSymbols, Set<Comparison.Operator> operators)
        {
            // At this point in time, our join predicates need to be deterministic
            if (expression instanceof Comparison comparison && isDeterministic(expression)) {
                if (operators.contains(comparison.operator())) {
                    Set<Symbol> symbols1 = extractUnique(comparison.left());
                    Set<Symbol> symbols2 = extractUnique(comparison.right());
                    if (symbols1.isEmpty() || symbols2.isEmpty()) {
                        return false;
                    }
                    return (leftSymbols.containsAll(symbols1) && rightSymbols.containsAll(symbols2)) ||
                            (rightSymbols.containsAll(symbols1) && leftSymbols.containsAll(symbols2));
                }
            }
            return false;
        }

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode node, RewriteContext<Expression> context)
        {
            Expression inheritedPredicate = context.get();
            if (!extractConjuncts(inheritedPredicate).contains(node.getSemiJoinOutput().toSymbolReference())) {
                return visitNonFilteringSemiJoin(node, context);
            }
            return visitFilteringSemiJoin(node, context);
        }

        private PlanNode visitNonFilteringSemiJoin(SemiJoinNode node, RewriteContext<Expression> context)
        {
            Expression inheritedPredicate = context.get();
            List<Expression> sourceConjuncts = new ArrayList<>();
            List<Expression> postJoinConjuncts = new ArrayList<>();

            // TODO: see if there are predicates that can be inferred from the semi join output

            PlanNode rewrittenFilteringSource = context.defaultRewrite(node.getFilteringSource(), TRUE);

            // Push inheritedPredicates down to the source if they don't involve the semi join output
            ImmutableSet<Symbol> sourceScope = ImmutableSet.copyOf(node.getSource().getOutputSymbols());
            EqualityInference inheritedInference = new EqualityInference(inheritedPredicate);
            EqualityInference.nonInferrableConjuncts(inheritedPredicate).forEach(conjunct -> {
                Expression rewrittenConjunct = inheritedInference.rewrite(conjunct, sourceScope);
                // Since each source row is reflected exactly once in the output, ok to push non-deterministic predicates down
                if (rewrittenConjunct != null) {
                    sourceConjuncts.add(rewrittenConjunct);
                }
                else {
                    postJoinConjuncts.add(conjunct);
                }
            });

            // Add the inherited equality predicates back in
            EqualityInference.EqualityPartition equalityPartition = inheritedInference.generateEqualitiesPartitionedBy(sourceScope);
            sourceConjuncts.addAll(equalityPartition.getScopeEqualities());
            postJoinConjuncts.addAll(equalityPartition.getScopeComplementEqualities());
            postJoinConjuncts.addAll(equalityPartition.getScopeStraddlingEqualities());

            PlanNode rewrittenSource = context.rewrite(node.getSource(), combineConjuncts(sourceConjuncts));

            PlanNode output = node;
            if (rewrittenSource != node.getSource() || rewrittenFilteringSource != node.getFilteringSource()) {
                output = new SemiJoinNode(
                        node.getId(),
                        rewrittenSource,
                        rewrittenFilteringSource,
                        node.getSourceJoinSymbol(),
                        node.getFilteringSourceJoinSymbol(),
                        node.getSemiJoinOutput(),
                        node.getSourceHashSymbol(),
                        node.getFilteringSourceHashSymbol(),
                        node.getDistributionType(),
                        Optional.empty());
            }
            if (!postJoinConjuncts.isEmpty()) {
                output = new FilterNode(idAllocator.getNextId(), output, combineConjuncts(postJoinConjuncts));
            }
            return output;
        }

        private PlanNode visitFilteringSemiJoin(SemiJoinNode node, RewriteContext<Expression> context)
        {
            Expression inheritedPredicate = context.get();
            Expression deterministicInheritedPredicate = filterDeterministicConjuncts(inheritedPredicate);
            Expression sourceEffectivePredicate = filterDeterministicConjuncts(effectivePredicateExtractor.extract(session, node.getSource()));
            Expression filteringSourceEffectivePredicate = filterDeterministicConjuncts(effectivePredicateExtractor.extract(session, node.getFilteringSource()));
            Expression joinExpression = new Comparison(
                    EQUAL,
                    node.getSourceJoinSymbol().toSymbolReference(),
                    node.getFilteringSourceJoinSymbol().toSymbolReference());

            List<Symbol> sourceSymbols = node.getSource().getOutputSymbols();
            List<Symbol> filteringSourceSymbols = node.getFilteringSource().getOutputSymbols();

            List<Expression> sourceConjuncts = new ArrayList<>();
            List<Expression> filteringSourceConjuncts = new ArrayList<>();
            List<Expression> postJoinConjuncts = new ArrayList<>();

            // Generate equality inferences
            EqualityInference allInference = new EqualityInference(deterministicInheritedPredicate, sourceEffectivePredicate, filteringSourceEffectivePredicate, joinExpression);
            EqualityInference allInferenceWithoutSourceInferred = new EqualityInference(deterministicInheritedPredicate, filteringSourceEffectivePredicate, joinExpression);
            EqualityInference allInferenceWithoutFilteringSourceInferred = new EqualityInference(deterministicInheritedPredicate, sourceEffectivePredicate, joinExpression);

            // Push inheritedPredicates down to the source if they don't involve the semi join output
            Set<Symbol> sourceScope = ImmutableSet.copyOf(sourceSymbols);
            EqualityInference.nonInferrableConjuncts(inheritedPredicate).forEach(conjunct -> {
                Expression rewrittenConjunct = allInference.rewrite(conjunct, sourceScope);
                // Since each source row is reflected exactly once in the output, ok to push non-deterministic predicates down
                if (rewrittenConjunct != null) {
                    sourceConjuncts.add(rewrittenConjunct);
                }
                else {
                    postJoinConjuncts.add(conjunct);
                }
            });

            // Push inheritedPredicates down to the filtering source if possible
            Set<Symbol> filterScope = ImmutableSet.copyOf(filteringSourceSymbols);
            EqualityInference.nonInferrableConjuncts(deterministicInheritedPredicate).forEach(conjunct -> {
                Expression rewrittenConjunct = allInference.rewrite(conjunct, filterScope);
                // We cannot push non-deterministic predicates to filtering side. Each filtering side row have to be
                // logically reevaluated for each source row.
                if (rewrittenConjunct != null) {
                    filteringSourceConjuncts.add(rewrittenConjunct);
                }
            });

            // move effective predicate conjuncts source <-> filter
            // See if we can push the filtering source effective predicate to the source side
            EqualityInference.nonInferrableConjuncts(filteringSourceEffectivePredicate)
                    .map(conjunct -> allInference.rewrite(conjunct, sourceScope))
                    .filter(Objects::nonNull)
                    .forEach(sourceConjuncts::add);

            // See if we can push the source effective predicate to the filtering source side
            EqualityInference.nonInferrableConjuncts(sourceEffectivePredicate)
                    .map(conjunct -> allInference.rewrite(conjunct, filterScope))
                    .filter(Objects::nonNull)
                    .forEach(filteringSourceConjuncts::add);

            // Add equalities from the inference back in
            sourceConjuncts.addAll(allInferenceWithoutSourceInferred.generateEqualitiesPartitionedBy(sourceScope).getScopeEqualities());
            filteringSourceConjuncts.addAll(allInferenceWithoutFilteringSourceInferred.generateEqualitiesPartitionedBy(filterScope).getScopeEqualities());

            // Add dynamic filtering predicate
            Optional<DynamicFilterId> dynamicFilterId = node.getDynamicFilterId();
            if (dynamicFilterId.isEmpty() && isEnableDynamicFiltering(session) && dynamicFiltering) {
                dynamicFilterId = Optional.of(new DynamicFilterId("df_" + idAllocator.getNextId().toString()));
                Symbol sourceSymbol = node.getSourceJoinSymbol();
                sourceConjuncts.add(createDynamicFilterExpression(
                        metadata,
                        dynamicFilterId.get(),
                        sourceSymbol.type(),
                        sourceSymbol.toSymbolReference(),
                        EQUAL));
            }

            PlanNode rewrittenSource = context.rewrite(node.getSource(), combineConjuncts(sourceConjuncts));
            PlanNode rewrittenFilteringSource = context.rewrite(node.getFilteringSource(), combineConjuncts(filteringSourceConjuncts));

            PlanNode output = node;
            if (rewrittenSource != node.getSource() || rewrittenFilteringSource != node.getFilteringSource() || !dynamicFilterId.equals(node.getDynamicFilterId())) {
                output = new SemiJoinNode(
                        node.getId(),
                        rewrittenSource,
                        rewrittenFilteringSource,
                        node.getSourceJoinSymbol(),
                        node.getFilteringSourceJoinSymbol(),
                        node.getSemiJoinOutput(),
                        node.getSourceHashSymbol(),
                        node.getFilteringSourceHashSymbol(),
                        node.getDistributionType(),
                        dynamicFilterId);
            }
            if (!postJoinConjuncts.isEmpty()) {
                output = new FilterNode(idAllocator.getNextId(), output, combineConjuncts(postJoinConjuncts));
            }
            return output;
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Expression> context)
        {
            if (node.hasEmptyGroupingSet()) {
                // TODO: in case of grouping sets, we should be able to push the filters over grouping keys below the aggregation
                // and also preserve the filter above the aggregation if it has an empty grouping set
                return visitPlan(node, context);
            }

            Expression inheritedPredicate = context.get();

            EqualityInference equalityInference = new EqualityInference(inheritedPredicate);

            List<Expression> pushdownConjuncts = new ArrayList<>();
            List<Expression> postAggregationConjuncts = new ArrayList<>();

            // Strip out non-deterministic conjuncts
            extractConjuncts(inheritedPredicate).stream()
                    .filter(expression -> !isDeterministic(expression))
                    .forEach(postAggregationConjuncts::add);
            inheritedPredicate = filterDeterministicConjuncts(inheritedPredicate);

            // Sort non-equality predicates by those that can be pushed down and those that cannot
            Set<Symbol> groupingKeys = ImmutableSet.copyOf(node.getGroupingKeys());
            EqualityInference.nonInferrableConjuncts(inheritedPredicate).forEach(conjunct -> {
                if (node.getGroupIdSymbol().isPresent() && extractUnique(conjunct).contains(node.getGroupIdSymbol().get())) {
                    // aggregation operator synthesizes outputs for group ids corresponding to the global grouping set (i.e., ()), so we
                    // need to preserve any predicates that evaluate the group id to run after the aggregation
                    // TODO: we should be able to infer if conditions on grouping() correspond to global grouping sets to determine whether
                    // we need to do this for each specific case
                    postAggregationConjuncts.add(conjunct);
                }
                else {
                    Expression rewrittenConjunct = equalityInference.rewrite(conjunct, groupingKeys);
                    if (rewrittenConjunct != null) {
                        pushdownConjuncts.add(rewrittenConjunct);
                    }
                    else {
                        postAggregationConjuncts.add(conjunct);
                    }
                }
            });

            // Add the equality predicates back in
            EqualityInference.EqualityPartition equalityPartition = equalityInference.generateEqualitiesPartitionedBy(groupingKeys);
            pushdownConjuncts.addAll(equalityPartition.getScopeEqualities());
            postAggregationConjuncts.addAll(equalityPartition.getScopeComplementEqualities());
            postAggregationConjuncts.addAll(equalityPartition.getScopeStraddlingEqualities());

            PlanNode rewrittenSource = context.rewrite(node.getSource(), combineConjuncts(pushdownConjuncts));

            PlanNode output = node;
            if (rewrittenSource != node.getSource()) {
                output = AggregationNode.builderFrom(node)
                        .setSource(rewrittenSource)
                        .setPreGroupedSymbols(ImmutableList.of())
                        .build();
            }
            if (!postAggregationConjuncts.isEmpty()) {
                output = new FilterNode(idAllocator.getNextId(), output, combineConjuncts(postAggregationConjuncts));
            }
            return output;
        }

        @Override
        public PlanNode visitUnnest(UnnestNode node, RewriteContext<Expression> context)
        {
            Expression inheritedPredicate = context.get();
            if (node.getJoinType() == RIGHT || node.getJoinType() == FULL) {
                return new FilterNode(idAllocator.getNextId(), node, inheritedPredicate);
            }

            //TODO for LEFT or INNER join type, push down UnnestNode's filter on replicate symbols
            EqualityInference equalityInference = new EqualityInference(inheritedPredicate);

            List<Expression> pushdownConjuncts = new ArrayList<>();
            List<Expression> postUnnestConjuncts = new ArrayList<>();

            // Strip out non-deterministic conjuncts
            extractConjuncts(inheritedPredicate).stream()
                    .filter(expression -> !isDeterministic(expression))
                    .forEach(postUnnestConjuncts::add);
            inheritedPredicate = filterDeterministicConjuncts(inheritedPredicate);

            // Sort non-equality predicates by those that can be pushed down and those that cannot
            Set<Symbol> replicatedSymbols = ImmutableSet.copyOf(node.getReplicateSymbols());
            EqualityInference.nonInferrableConjuncts(inheritedPredicate).forEach(conjunct -> {
                Expression rewrittenConjunct = equalityInference.rewrite(conjunct, replicatedSymbols);
                if (rewrittenConjunct != null) {
                    pushdownConjuncts.add(rewrittenConjunct);
                }
                else {
                    postUnnestConjuncts.add(conjunct);
                }
            });

            // Add the equality predicates back in
            EqualityInference.EqualityPartition equalityPartition = equalityInference.generateEqualitiesPartitionedBy(replicatedSymbols);
            pushdownConjuncts.addAll(equalityPartition.getScopeEqualities());
            postUnnestConjuncts.addAll(equalityPartition.getScopeComplementEqualities());
            postUnnestConjuncts.addAll(equalityPartition.getScopeStraddlingEqualities());

            PlanNode rewrittenSource = context.rewrite(node.getSource(), combineConjuncts(pushdownConjuncts));

            PlanNode output = node;
            if (rewrittenSource != node.getSource()) {
                output = new UnnestNode(node.getId(), rewrittenSource, node.getReplicateSymbols(), node.getMappings(), node.getOrdinalitySymbol(), node.getJoinType());
            }
            if (!postUnnestConjuncts.isEmpty()) {
                output = new FilterNode(idAllocator.getNextId(), output, combineConjuncts(postUnnestConjuncts));
            }
            return output;
        }

        @Override
        public PlanNode visitSample(SampleNode node, RewriteContext<Expression> context)
        {
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Expression> context)
        {
            Expression predicate = simplifyExpression(context.get());

            if (!TRUE.equals(predicate)) {
                return new FilterNode(idAllocator.getNextId(), node, predicate);
            }

            return node;
        }

        @Override
        public PlanNode visitAssignUniqueId(AssignUniqueId node, RewriteContext<Expression> context)
        {
            Set<Symbol> predicateSymbols = extractUnique(context.get());
            checkState(!predicateSymbols.contains(node.getIdColumn()), "UniqueId in predicate is not yet supported");
            return context.defaultRewrite(node, context.get());
        }
    }
}
