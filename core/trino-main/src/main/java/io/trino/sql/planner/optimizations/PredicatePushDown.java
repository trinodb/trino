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
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.EffectivePredicateExtractor;
import io.trino.sql.planner.EqualityInference;
import io.trino.sql.planner.ExpressionInterpreter;
import io.trino.sql.planner.LiteralEncoder;
import io.trino.sql.planner.NoOpSymbolResolver;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.JoinNode;
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
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.TryExpression;
import io.trino.sql.util.AstUtils;

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
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.ExpressionUtils.filterDeterministicConjuncts;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.ExpressionSymbolInliner.inlineSymbols;
import static io.trino.sql.planner.SymbolsExtractor.extractUnique;
import static io.trino.sql.planner.iterative.rule.CanonicalizeExpressionRewriter.canonicalizeExpression;
import static io.trino.sql.planner.iterative.rule.UnwrapCastInComparison.unwrapCasts;
import static io.trino.sql.planner.plan.JoinNode.Type.FULL;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.JoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.JoinNode.Type.RIGHT;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static java.util.Objects.requireNonNull;

public class PredicatePushDown
        implements PlanOptimizer
{
    private static final Set<ComparisonExpression.Operator> DYNAMIC_FILTERING_SUPPORTED_COMPARISONS = ImmutableSet.of(
            EQUAL,
            GREATER_THAN,
            GREATER_THAN_OR_EQUAL,
            LESS_THAN,
            LESS_THAN_OR_EQUAL);

    private final Metadata metadata;
    private final TypeOperators typeOperators;
    private final LiteralEncoder literalEncoder;
    private final TypeAnalyzer typeAnalyzer;
    private final boolean useTableProperties;
    private final boolean dynamicFiltering;

    public PredicatePushDown(
            Metadata metadata,
            TypeOperators typeOperators,
            TypeAnalyzer typeAnalyzer,
            boolean useTableProperties,
            boolean dynamicFiltering)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.literalEncoder = new LiteralEncoder(metadata);
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        this.useTableProperties = useTableProperties;
        this.dynamicFiltering = dynamicFiltering;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(idAllocator, "idAllocator is null");

        EffectivePredicateExtractor effectivePredicateExtractor = new EffectivePredicateExtractor(
                new DomainTranslator(metadata),
                metadata,
                useTableProperties && isPredicatePushdownUseTableProperties(session));
        return SimplePlanRewriter.rewriteWith(
                new Rewriter(symbolAllocator, idAllocator, metadata, typeOperators, literalEncoder, effectivePredicateExtractor, typeAnalyzer, session, types, dynamicFiltering),
                plan,
                TRUE_LITERAL);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Expression>
    {
        private final SymbolAllocator symbolAllocator;
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final TypeOperators typeOperators;
        private final LiteralEncoder literalEncoder;
        private final EffectivePredicateExtractor effectivePredicateExtractor;
        private final TypeAnalyzer typeAnalyzer;
        private final Session session;
        private final TypeProvider types;
        private final ExpressionEquivalence expressionEquivalence;
        private final boolean dynamicFiltering;

        private Rewriter(
                SymbolAllocator symbolAllocator,
                PlanNodeIdAllocator idAllocator,
                Metadata metadata,
                TypeOperators typeOperators,
                LiteralEncoder literalEncoder,
                EffectivePredicateExtractor effectivePredicateExtractor,
                TypeAnalyzer typeAnalyzer,
                Session session,
                TypeProvider types,
                boolean dynamicFiltering)
        {
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
            this.literalEncoder = requireNonNull(literalEncoder, "literalEncoder is null");
            this.effectivePredicateExtractor = requireNonNull(effectivePredicateExtractor, "effectivePredicateExtractor is null");
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
            this.session = requireNonNull(session, "session is null");
            this.types = requireNonNull(types, "types is null");
            this.expressionEquivalence = new ExpressionEquivalence(metadata, typeAnalyzer);
            this.dynamicFiltering = dynamicFiltering;
        }

        @Override
        public PlanNode visitPlan(PlanNode node, RewriteContext<Expression> context)
        {
            PlanNode rewrittenNode = context.defaultRewrite(node, TRUE_LITERAL);
            if (!context.get().equals(TRUE_LITERAL)) {
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
                Map<Symbol, SymbolReference> outputsToInputs = new HashMap<>();
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
                    isDeterministic(conjunct, metadata) &&
                            extractUnique(conjunct).stream()
                                    .allMatch(partitionSymbols::contains);

            Map<Boolean, List<Expression>> conjuncts = extractConjuncts(context.get()).stream().collect(Collectors.partitioningBy(isSupported));

            PlanNode rewrittenNode = context.defaultRewrite(node, combineConjuncts(metadata, conjuncts.get(true)));

            if (!conjuncts.get(false).isEmpty()) {
                rewrittenNode = new FilterNode(idAllocator.getNextId(), rewrittenNode, combineConjuncts(metadata, conjuncts.get(false)));
            }

            return rewrittenNode;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Expression> context)
        {
            Set<Symbol> deterministicSymbols = node.getAssignments().entrySet().stream()
                    .filter(entry -> isDeterministic(entry.getValue(), metadata))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());

            Predicate<Expression> deterministic = conjunct -> extractUnique(conjunct).stream()
                    .allMatch(deterministicSymbols::contains);

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
                    .map(conjunct -> canonicalizeExpression(conjunct, typeAnalyzer.getTypes(session, types, conjunct), metadata)) // normalize expressions to a form that unwrapCasts understands
                    .map(conjunct -> unwrapCasts(session, metadata, typeOperators, typeAnalyzer, types, conjunct))
                    .collect(Collectors.toList());

            PlanNode rewrittenNode = context.defaultRewrite(node, combineConjuncts(metadata, inlinedDeterministicConjuncts));

            // All deterministic conjuncts that contains non-inlining targets, and non-deterministic conjuncts,
            // if any, will be in the filter node.
            List<Expression> nonInliningConjuncts = inlineConjuncts.get(false);
            nonInliningConjuncts.addAll(conjuncts.get(false));

            if (!nonInliningConjuncts.isEmpty()) {
                rewrittenNode = new FilterNode(idAllocator.getNextId(), rewrittenNode, combineConjuncts(metadata, nonInliningConjuncts));
            }

            return rewrittenNode;
        }

        private boolean isInliningCandidate(Expression expression, ProjectNode node)
        {
            // TryExpressions should not be pushed down. However they are now being handled as lambda
            // passed to a FunctionCall now and should not affect predicate push down. So we want to make
            // sure the conjuncts are not TryExpressions.
            verify(AstUtils.preOrder(expression).noneMatch(TryExpression.class::isInstance));

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
                            || node.getAssignments().get(entry.getKey()) instanceof Literal
                            || node.getAssignments().get(entry.getKey()) instanceof SymbolReference);
        }

        @Override
        public PlanNode visitGroupId(GroupIdNode node, RewriteContext<Expression> context)
        {
            Map<Symbol, SymbolReference> commonGroupingSymbolMapping = node.getGroupingColumns().entrySet().stream()
                    .filter(entry -> node.getCommonGroupingColumns().contains(entry.getKey()))
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().toSymbolReference()));

            Predicate<Expression> pushdownEligiblePredicate = conjunct -> commonGroupingSymbolMapping.keySet().containsAll(extractUnique(conjunct));

            Map<Boolean, List<Expression>> conjuncts = extractConjuncts(context.get()).stream().collect(Collectors.partitioningBy(pushdownEligiblePredicate));

            // Push down conjuncts from the inherited predicate that apply to common grouping symbols
            PlanNode rewrittenNode = context.defaultRewrite(node, inlineSymbols(commonGroupingSymbolMapping, combineConjuncts(metadata, conjuncts.get(true))));

            // All other conjuncts, if any, will be in the filter node.
            if (!conjuncts.get(false).isEmpty()) {
                rewrittenNode = new FilterNode(idAllocator.getNextId(), rewrittenNode, combineConjuncts(metadata, conjuncts.get(false)));
            }

            return rewrittenNode;
        }

        @Override
        public PlanNode visitMarkDistinct(MarkDistinctNode node, RewriteContext<Expression> context)
        {
            Set<Symbol> pushDownableSymbols = ImmutableSet.copyOf(node.getDistinctSymbols());
            Map<Boolean, List<Expression>> conjuncts = extractConjuncts(context.get()).stream()
                    .collect(Collectors.partitioningBy(conjunct -> extractUnique(conjunct).stream().allMatch(pushDownableSymbols::contains)));

            PlanNode rewrittenNode = context.defaultRewrite(node, combineConjuncts(metadata, conjuncts.get(true)));

            if (!conjuncts.get(false).isEmpty()) {
                rewrittenNode = new FilterNode(idAllocator.getNextId(), rewrittenNode, combineConjuncts(metadata, conjuncts.get(false)));
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
            PlanNode rewrittenPlan = context.rewrite(node.getSource(), combineConjuncts(metadata, node.getPredicate(), context.get()));
            if (!(rewrittenPlan instanceof FilterNode)) {
                return rewrittenPlan;
            }

            FilterNode rewrittenFilterNode = (FilterNode) rewrittenPlan;
            if (!areExpressionsEquivalent(rewrittenFilterNode.getPredicate(), node.getPredicate())
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

            Expression leftEffectivePredicate = effectivePredicateExtractor.extract(session, node.getLeft(), types, typeAnalyzer);
            Expression rightEffectivePredicate = effectivePredicateExtractor.extract(session, node.getRight(), types, typeAnalyzer);
            Expression joinPredicate = extractJoinPredicate(node);

            Expression leftPredicate;
            Expression rightPredicate;
            Expression postJoinPredicate;
            Expression newJoinPredicate;

            switch (node.getType()) {
                case INNER:
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
                    break;
                case LEFT:
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
                    break;
                case RIGHT:
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
                    break;
                case FULL:
                    leftPredicate = TRUE_LITERAL;
                    rightPredicate = TRUE_LITERAL;
                    postJoinPredicate = inheritedPredicate;
                    newJoinPredicate = joinPredicate;
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
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
                    ComparisonExpression equality = (ComparisonExpression) conjunct;

                    boolean alignedComparison = node.getLeft().getOutputSymbols().containsAll(extractUnique(equality.getLeft()));
                    Expression leftExpression = (alignedComparison) ? equality.getLeft() : equality.getRight();
                    Expression rightExpression = (alignedComparison) ? equality.getRight() : equality.getLeft();

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
            leftPredicate = combineConjuncts(metadata, leftPredicate, combineConjuncts(metadata, dynamicFiltersResult.getPredicates()));

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

            Optional<Expression> newJoinFilter = Optional.of(combineConjuncts(metadata, joinFilter));
            if (newJoinFilter.get() == TRUE_LITERAL) {
                newJoinFilter = Optional.empty();
            }

            if (node.getType() == INNER && newJoinFilter.isPresent() && equiJoinClauses.isEmpty()) {
                // if we do not have any equi conjunct we do not pushdown non-equality condition into
                // inner join, so we plan execution as nested-loops-join followed by filter instead
                // hash join.
                // todo: remove the code when we have support for filter function in nested loop join
                postJoinPredicate = combineConjuncts(metadata, postJoinPredicate, newJoinFilter.get());
                newJoinFilter = Optional.empty();
            }

            boolean filtersEquivalent =
                    newJoinFilter.isPresent() == node.getFilter().isPresent() &&
                            (newJoinFilter.isEmpty() || areExpressionsEquivalent(newJoinFilter.get(), node.getFilter().get()));

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

            if (!postJoinPredicate.equals(TRUE_LITERAL)) {
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
                Session session, PlanNodeIdAllocator idAllocator)
        {
            if ((node.getType() != INNER && node.getType() != RIGHT) || !isEnableDynamicFiltering(session) || !dynamicFiltering) {
                return new DynamicFiltersResult(ImmutableMap.of(), ImmutableList.of());
            }

            List<DynamicFilterExpression> clauses = Streams.concat(
                    equiJoinClauses
                            .stream()
                            .map(clause -> new DynamicFilterExpression(
                                    new ComparisonExpression(EQUAL, clause.getLeft().toSymbolReference(), clause.getRight().toSymbolReference()))),
                    joinFilterClauses.stream()
                            .flatMap(Rewriter::tryConvertBetweenIntoComparisons)
                            .filter(clause -> joinDynamicFilteringExpression(clause, node.getLeft().getOutputSymbols(), node.getRight().getOutputSymbols()))
                            .map(expression -> {
                                if (expression instanceof NotExpression) {
                                    NotExpression notExpression = ((NotExpression) expression);
                                    ComparisonExpression comparison = (ComparisonExpression) notExpression.getValue();
                                    return new DynamicFilterExpression(new ComparisonExpression(EQUAL, comparison.getLeft(), comparison.getRight()), true);
                                }
                                return new DynamicFilterExpression((ComparisonExpression) expression);
                            })
                            .map(expression -> {
                                ComparisonExpression comparison = expression.getComparison();
                                Expression leftExpression = comparison.getLeft();
                                Expression rightExpression = comparison.getRight();
                                boolean alignedComparison = node.getLeft().getOutputSymbols().containsAll(extractUnique(leftExpression));
                                return new DynamicFilterExpression(
                                        new ComparisonExpression(
                                                alignedComparison ? comparison.getOperator() : comparison.getOperator().flip(),
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
                    .map(ComparisonExpression::getRight)
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
                        ComparisonExpression comparison = clause.getComparison();
                        Expression probeExpression = comparison.getLeft();
                        Symbol buildSymbol = Symbol.from(comparison.getRight());
                        // we can take type of buildSymbol instead probeExpression as comparison expression must have the same type on both sides
                        Type type = symbolAllocator.getTypes().get(buildSymbol);
                        DynamicFilterId id = requireNonNull(buildSymbolToDynamicFilter.get(buildSymbol), () -> "missing dynamic filter for symbol " + buildSymbol);
                        return createDynamicFilterExpression(metadata, id, type, probeExpression, comparison.getOperator(), clause.isNullAllowed());
                    })
                    .collect(toImmutableList());
            // Return a mapping from build symbols to corresponding dynamic filter IDs:
            return new DynamicFiltersResult(buildSymbolToDynamicFilter.inverse(), predicates);
        }

        private static Stream<Expression> tryConvertBetweenIntoComparisons(Expression clause)
        {
            if (clause instanceof BetweenPredicate) {
                BetweenPredicate between = (BetweenPredicate) clause;
                return Stream.of(
                        new ComparisonExpression(GREATER_THAN_OR_EQUAL, between.getValue(), between.getMin()),
                        new ComparisonExpression(LESS_THAN_OR_EQUAL, between.getValue(), between.getMax()));
            }
            return Stream.of(clause);
        }

        private static class DynamicFilterExpression
        {
            private final ComparisonExpression comparison;
            private final boolean nullAllowed;

            private DynamicFilterExpression(ComparisonExpression comparison)
            {
                this(comparison, false);
            }

            private DynamicFilterExpression(ComparisonExpression comparison, boolean nullAllowed)
            {
                this.comparison = requireNonNull(comparison, "comparison is null");
                this.nullAllowed = nullAllowed;
            }

            public ComparisonExpression getComparison()
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

            Expression leftEffectivePredicate = effectivePredicateExtractor.extract(session, node.getLeft(), types, typeAnalyzer);
            Expression rightEffectivePredicate = effectivePredicateExtractor.extract(session, node.getRight(), types, typeAnalyzer);
            Expression joinPredicate = node.getFilter();

            Expression leftPredicate;
            Expression rightPredicate;
            Expression postJoinPredicate;
            Expression newJoinPredicate;

            switch (node.getType()) {
                case INNER:
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
                    break;
                case LEFT:
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
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported spatial join type: " + node.getType());
            }

            newJoinPredicate = simplifyExpression(newJoinPredicate);
            verify(!newJoinPredicate.equals(BooleanLiteral.FALSE_LITERAL), "Spatial join predicate is missing");

            PlanNode leftSource = context.rewrite(node.getLeft(), leftPredicate);
            PlanNode rightSource = context.rewrite(node.getRight(), rightPredicate);

            PlanNode output = node;
            if (leftSource != node.getLeft() ||
                    rightSource != node.getRight() ||
                    !areExpressionsEquivalent(newJoinPredicate, joinPredicate)) {
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

            if (!postJoinPredicate.equals(TRUE_LITERAL)) {
                output = new FilterNode(idAllocator.getNextId(), output, postJoinPredicate);
            }

            return output;
        }

        private Symbol symbolForExpression(Expression expression)
        {
            if (expression instanceof SymbolReference) {
                return Symbol.from(expression);
            }

            return symbolAllocator.newSymbol(expression, typeAnalyzer.getType(session, symbolAllocator.getTypes(), expression));
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
                    .filter(expression -> !isDeterministic(expression, metadata))
                    .forEach(postJoinConjuncts::add);
            inheritedPredicate = filterDeterministicConjuncts(metadata, inheritedPredicate);

            outerEffectivePredicate = filterDeterministicConjuncts(metadata, outerEffectivePredicate);
            innerEffectivePredicate = filterDeterministicConjuncts(metadata, innerEffectivePredicate);
            extractConjuncts(joinPredicate).stream()
                    .filter(expression -> !isDeterministic(expression, metadata))
                    .forEach(joinConjuncts::add);
            joinPredicate = filterDeterministicConjuncts(metadata, joinPredicate);

            // Generate equality inferences
            EqualityInference inheritedInference = EqualityInference.newInstance(metadata, inheritedPredicate);
            EqualityInference outerInference = EqualityInference.newInstance(metadata, inheritedPredicate, outerEffectivePredicate);

            Set<Symbol> innerScope = ImmutableSet.copyOf(innerSymbols);
            Set<Symbol> outerScope = ImmutableSet.copyOf(outerSymbols);

            EqualityInference.EqualityPartition equalityPartition = inheritedInference.generateEqualitiesPartitionedBy(outerScope);
            Expression outerOnlyInheritedEqualities = combineConjuncts(metadata, equalityPartition.getScopeEqualities());
            EqualityInference potentialNullSymbolInference = EqualityInference.newInstance(metadata, outerOnlyInheritedEqualities, outerEffectivePredicate, innerEffectivePredicate, joinPredicate);

            // Push outer and join equalities into the inner side. For example:
            // SELECT * FROM nation LEFT OUTER JOIN region ON nation.regionkey = region.regionkey and nation.name = region.name WHERE nation.name = 'blah'

            EqualityInference potentialNullSymbolInferenceWithoutInnerInferred = EqualityInference.newInstance(metadata, outerOnlyInheritedEqualities, outerEffectivePredicate, joinPredicate);
            innerPushdownConjuncts.addAll(potentialNullSymbolInferenceWithoutInnerInferred.generateEqualitiesPartitionedBy(innerScope).getScopeEqualities());

            // TODO: we can further improve simplifying the equalities by considering other relationships from the outer side
            EqualityInference.EqualityPartition joinEqualityPartition = EqualityInference.newInstance(metadata, joinPredicate).generateEqualitiesPartitionedBy(innerScope);
            innerPushdownConjuncts.addAll(joinEqualityPartition.getScopeEqualities());
            joinConjuncts.addAll(joinEqualityPartition.getScopeComplementEqualities())
                    .addAll(joinEqualityPartition.getScopeStraddlingEqualities());

            // Add the equalities from the inferences back in
            outerPushdownConjuncts.addAll(equalityPartition.getScopeEqualities());
            postJoinConjuncts.addAll(equalityPartition.getScopeComplementEqualities());
            postJoinConjuncts.addAll(equalityPartition.getScopeStraddlingEqualities());

            // See if we can push inherited predicates down
            EqualityInference.nonInferrableConjuncts(metadata, inheritedPredicate).forEach(conjunct -> {
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
            EqualityInference.nonInferrableConjuncts(metadata, outerEffectivePredicate)
                    .map(conjunct -> potentialNullSymbolInference.rewrite(conjunct, innerScope))
                    .filter(Objects::nonNull)
                    .forEach(innerPushdownConjuncts::add);

            // See if we can push down join predicates to the inner side
            EqualityInference.nonInferrableConjuncts(metadata, joinPredicate).forEach(conjunct -> {
                Expression innerRewritten = potentialNullSymbolInference.rewrite(conjunct, innerScope);
                if (innerRewritten != null) {
                    innerPushdownConjuncts.add(innerRewritten);
                }
                else {
                    joinConjuncts.add(conjunct);
                }
            });

            return new OuterJoinPushDownResult(combineConjuncts(metadata, outerPushdownConjuncts.build()),
                    combineConjuncts(metadata, innerPushdownConjuncts.build()),
                    combineConjuncts(metadata, joinConjuncts.build()),
                    combineConjuncts(metadata, postJoinConjuncts.build()));
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

            ImmutableList.Builder<Expression> leftPushDownConjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> rightPushDownConjuncts = ImmutableList.builder();
            ImmutableList.Builder<Expression> joinConjuncts = ImmutableList.builder();

            // Strip out non-deterministic conjuncts
            extractConjuncts(inheritedPredicate).stream()
                    .filter(deterministic -> !isDeterministic(deterministic, metadata))
                    .forEach(joinConjuncts::add);
            inheritedPredicate = filterDeterministicConjuncts(metadata, inheritedPredicate);

            extractConjuncts(joinPredicate).stream()
                    .filter(expression -> !isDeterministic(expression, metadata))
                    .forEach(joinConjuncts::add);
            joinPredicate = filterDeterministicConjuncts(metadata, joinPredicate);

            leftEffectivePredicate = filterDeterministicConjuncts(metadata, leftEffectivePredicate);
            rightEffectivePredicate = filterDeterministicConjuncts(metadata, rightEffectivePredicate);

            ImmutableSet<Symbol> leftScope = ImmutableSet.copyOf(leftSymbols);
            ImmutableSet<Symbol> rightScope = ImmutableSet.copyOf(rightSymbols);

            // Attempt to simplify the effective left/right predicates with the predicate we're pushing down
            // This, effectively, inlines any constants derived from such predicate
            EqualityInference predicateInference = EqualityInference.newInstance(metadata, inheritedPredicate);
            Expression simplifiedLeftEffectivePredicate = predicateInference.rewrite(leftEffectivePredicate, leftScope);
            Expression simplifiedRightEffectivePredicate = predicateInference.rewrite(rightEffectivePredicate, rightScope);

            // simplify predicate based on known equalities guaranteed by the left/right side
            EqualityInference assertions = EqualityInference.newInstance(metadata, leftEffectivePredicate, rightEffectivePredicate);
            inheritedPredicate = assertions.rewrite(inheritedPredicate, Sets.union(leftScope, rightScope));

            // Generate equality inferences
            EqualityInference allInference = EqualityInference.newInstance(metadata, inheritedPredicate, leftEffectivePredicate, rightEffectivePredicate, joinPredicate, simplifiedLeftEffectivePredicate, simplifiedRightEffectivePredicate);
            EqualityInference allInferenceWithoutLeftInferred = EqualityInference.newInstance(metadata, inheritedPredicate, rightEffectivePredicate, joinPredicate, simplifiedRightEffectivePredicate);
            EqualityInference allInferenceWithoutRightInferred = EqualityInference.newInstance(metadata, inheritedPredicate, leftEffectivePredicate, joinPredicate, simplifiedLeftEffectivePredicate);

            // Add equalities from the inference back in
            leftPushDownConjuncts.addAll(allInferenceWithoutLeftInferred.generateEqualitiesPartitionedBy(leftScope).getScopeEqualities());
            rightPushDownConjuncts.addAll(allInferenceWithoutRightInferred.generateEqualitiesPartitionedBy(rightScope).getScopeEqualities());
            joinConjuncts.addAll(allInference.generateEqualitiesPartitionedBy(leftScope).getScopeStraddlingEqualities()); // scope straddling equalities get dropped in as part of the join predicate

            // Sort through conjuncts in inheritedPredicate that were not used for inference
            EqualityInference.nonInferrableConjuncts(metadata, inheritedPredicate).forEach(conjunct -> {
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
                    joinConjuncts.add(conjunct);
                }
            });

            // See if we can push the right effective predicate to the left side
            EqualityInference.nonInferrableConjuncts(metadata, simplifiedRightEffectivePredicate)
                    .map(conjunct -> allInference.rewrite(conjunct, leftScope))
                    .filter(Objects::nonNull)
                    .forEach(leftPushDownConjuncts::add);

            // See if we can push the left effective predicate to the right side
            EqualityInference.nonInferrableConjuncts(metadata, simplifiedLeftEffectivePredicate)
                    .map(conjunct -> allInference.rewrite(conjunct, rightScope))
                    .filter(Objects::nonNull)
                    .forEach(rightPushDownConjuncts::add);

            // See if we can push any parts of the join predicates to either side
            EqualityInference.nonInferrableConjuncts(metadata, joinPredicate).forEach(conjunct -> {
                Expression leftRewritten = allInference.rewrite(conjunct, leftScope);
                if (leftRewritten != null) {
                    leftPushDownConjuncts.add(leftRewritten);
                }

                Expression rightRewritten = allInference.rewrite(conjunct, rightScope);
                if (rightRewritten != null) {
                    rightPushDownConjuncts.add(rightRewritten);
                }

                if (leftRewritten == null && rightRewritten == null) {
                    joinConjuncts.add(conjunct);
                }
            });

            return new InnerJoinPushDownResult(
                    combineConjuncts(metadata, leftPushDownConjuncts.build()),
                    combineConjuncts(metadata, rightPushDownConjuncts.build()),
                    combineConjuncts(metadata, joinConjuncts.build()),
                    TRUE_LITERAL);
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
            return combineConjuncts(metadata, builder.build());
        }

        private JoinNode tryNormalizeToOuterToInnerJoin(JoinNode node, Expression inheritedPredicate)
        {
            checkArgument(EnumSet.of(INNER, RIGHT, LEFT, FULL).contains(node.getType()), "Unsupported join type: %s", node.getType());

            if (node.getType() == JoinNode.Type.INNER) {
                return node;
            }

            if (node.getType() == JoinNode.Type.FULL) {
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
                else {
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
            }

            if (node.getType() == JoinNode.Type.LEFT && !canConvertOuterToInner(node.getRight().getOutputSymbols(), inheritedPredicate) ||
                    node.getType() == JoinNode.Type.RIGHT && !canConvertOuterToInner(node.getLeft().getOutputSymbols(), inheritedPredicate)) {
                return node;
            }
            return new JoinNode(
                    node.getId(),
                    JoinNode.Type.INNER,
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
                if (isDeterministic(conjunct, metadata)) {
                    // Ignore a conjunct for this test if we cannot deterministically get responses from it
                    Object response = nullInputEvaluator(innerSymbols, conjunct);
                    if (response == null || response instanceof NullLiteral || Boolean.FALSE.equals(response)) {
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
            Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(session, symbolAllocator.getTypes(), expression);
            ExpressionInterpreter optimizer = new ExpressionInterpreter(expression, metadata, session, expressionTypes);
            return literalEncoder.toExpression(optimizer.optimize(NoOpSymbolResolver.INSTANCE), expressionTypes.get(NodeRef.of(expression)));
        }

        private boolean areExpressionsEquivalent(Expression leftExpression, Expression rightExpression)
        {
            return expressionEquivalence.areExpressionsEquivalent(session, leftExpression, rightExpression, types);
        }

        /**
         * Evaluates an expression's response to binding the specified input symbols to NULL
         */
        private Object nullInputEvaluator(Collection<Symbol> nullSymbols, Expression expression)
        {
            Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(session, symbolAllocator.getTypes(), expression);
            return new ExpressionInterpreter(expression, metadata, session, expressionTypes)
                    .optimize(symbol -> nullSymbols.contains(symbol) ? null : symbol.toSymbolReference());
        }

        private boolean joinEqualityExpression(Expression expression, Collection<Symbol> leftSymbols, Collection<Symbol> rightSymbols)
        {
            return joinComparisonExpression(expression, leftSymbols, rightSymbols, ImmutableSet.of(EQUAL));
        }

        private boolean joinDynamicFilteringExpression(Expression expression, Collection<Symbol> leftSymbols, Collection<Symbol> rightSymbols)
        {
            ComparisonExpression comparison;
            if (expression instanceof NotExpression) {
                NotExpression notExpression = (NotExpression) expression;
                boolean isDistinctFrom = joinComparisonExpression(notExpression.getValue(), leftSymbols, rightSymbols, ImmutableSet.of(IS_DISTINCT_FROM));
                if (!isDistinctFrom) {
                    return false;
                }
                comparison = (ComparisonExpression) notExpression.getValue();
                Set<Type> expressionTypes = ImmutableSet.of(
                        typeAnalyzer.getType(session, types, comparison.getLeft()),
                        typeAnalyzer.getType(session, types, comparison.getRight()));
                // Dynamic filtering is not supported with IS NOT DISTINCT FROM clause on REAL or DOUBLE types to avoid dealing with NaN values
                if (expressionTypes.contains(REAL) || expressionTypes.contains(DOUBLE)) {
                    return false;
                }
            }
            else {
                if (!joinComparisonExpression(expression, leftSymbols, rightSymbols, DYNAMIC_FILTERING_SUPPORTED_COMPARISONS)) {
                    return false;
                }
                comparison = (ComparisonExpression) expression;
            }

            // Build side expression must be a symbol reference, since DynamicFilterSourceOperator can only collect column values (not expressions)
            return (comparison.getRight() instanceof SymbolReference && rightSymbols.contains(Symbol.from(comparison.getRight())))
                    || (comparison.getLeft() instanceof SymbolReference && rightSymbols.contains(Symbol.from(comparison.getLeft())));
        }

        private boolean joinComparisonExpression(Expression expression, Collection<Symbol> leftSymbols, Collection<Symbol> rightSymbols, Set<ComparisonExpression.Operator> operators)
        {
            // At this point in time, our join predicates need to be deterministic
            if (expression instanceof ComparisonExpression && isDeterministic(expression, metadata)) {
                ComparisonExpression comparison = (ComparisonExpression) expression;
                if (operators.contains(comparison.getOperator())) {
                    Set<Symbol> symbols1 = extractUnique(comparison.getLeft());
                    Set<Symbol> symbols2 = extractUnique(comparison.getRight());
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

            PlanNode rewrittenFilteringSource = context.defaultRewrite(node.getFilteringSource(), TRUE_LITERAL);

            // Push inheritedPredicates down to the source if they don't involve the semi join output
            ImmutableSet<Symbol> sourceScope = ImmutableSet.copyOf(node.getSource().getOutputSymbols());
            EqualityInference inheritedInference = EqualityInference.newInstance(metadata, inheritedPredicate);
            EqualityInference.nonInferrableConjuncts(metadata, inheritedPredicate).forEach(conjunct -> {
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

            PlanNode rewrittenSource = context.rewrite(node.getSource(), combineConjuncts(metadata, sourceConjuncts));

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
                output = new FilterNode(idAllocator.getNextId(), output, combineConjuncts(metadata, postJoinConjuncts));
            }
            return output;
        }

        private PlanNode visitFilteringSemiJoin(SemiJoinNode node, RewriteContext<Expression> context)
        {
            Expression inheritedPredicate = context.get();
            Expression deterministicInheritedPredicate = filterDeterministicConjuncts(metadata, inheritedPredicate);
            Expression sourceEffectivePredicate = filterDeterministicConjuncts(metadata, effectivePredicateExtractor.extract(session, node.getSource(), types, typeAnalyzer));
            Expression filteringSourceEffectivePredicate = filterDeterministicConjuncts(metadata, effectivePredicateExtractor.extract(session, node.getFilteringSource(), types, typeAnalyzer));
            Expression joinExpression = new ComparisonExpression(
                    EQUAL,
                    node.getSourceJoinSymbol().toSymbolReference(),
                    node.getFilteringSourceJoinSymbol().toSymbolReference());

            List<Symbol> sourceSymbols = node.getSource().getOutputSymbols();
            List<Symbol> filteringSourceSymbols = node.getFilteringSource().getOutputSymbols();

            List<Expression> sourceConjuncts = new ArrayList<>();
            List<Expression> filteringSourceConjuncts = new ArrayList<>();
            List<Expression> postJoinConjuncts = new ArrayList<>();

            // Generate equality inferences
            EqualityInference allInference = EqualityInference.newInstance(metadata, deterministicInheritedPredicate, sourceEffectivePredicate, filteringSourceEffectivePredicate, joinExpression);
            EqualityInference allInferenceWithoutSourceInferred = EqualityInference.newInstance(metadata, deterministicInheritedPredicate, filteringSourceEffectivePredicate, joinExpression);
            EqualityInference allInferenceWithoutFilteringSourceInferred = EqualityInference.newInstance(metadata, deterministicInheritedPredicate, sourceEffectivePredicate, joinExpression);

            // Push inheritedPredicates down to the source if they don't involve the semi join output
            Set<Symbol> sourceScope = ImmutableSet.copyOf(sourceSymbols);
            EqualityInference.nonInferrableConjuncts(metadata, inheritedPredicate).forEach(conjunct -> {
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
            EqualityInference.nonInferrableConjuncts(metadata, deterministicInheritedPredicate).forEach(conjunct -> {
                Expression rewrittenConjunct = allInference.rewrite(conjunct, filterScope);
                // We cannot push non-deterministic predicates to filtering side. Each filtering side row have to be
                // logically reevaluated for each source row.
                if (rewrittenConjunct != null) {
                    filteringSourceConjuncts.add(rewrittenConjunct);
                }
            });

            // move effective predicate conjuncts source <-> filter
            // See if we can push the filtering source effective predicate to the source side
            EqualityInference.nonInferrableConjuncts(metadata, filteringSourceEffectivePredicate)
                    .map(conjunct -> allInference.rewrite(conjunct, sourceScope))
                    .filter(Objects::nonNull)
                    .forEach(sourceConjuncts::add);

            // See if we can push the source effective predicate to the filtering soruce side
            EqualityInference.nonInferrableConjuncts(metadata, sourceEffectivePredicate)
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
                        symbolAllocator.getTypes().get(sourceSymbol),
                        sourceSymbol.toSymbolReference(),
                        EQUAL));
            }

            PlanNode rewrittenSource = context.rewrite(node.getSource(), combineConjuncts(metadata, sourceConjuncts));
            PlanNode rewrittenFilteringSource = context.rewrite(node.getFilteringSource(), combineConjuncts(metadata, filteringSourceConjuncts));

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
                output = new FilterNode(idAllocator.getNextId(), output, combineConjuncts(metadata, postJoinConjuncts));
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

            EqualityInference equalityInference = EqualityInference.newInstance(metadata, inheritedPredicate);

            List<Expression> pushdownConjuncts = new ArrayList<>();
            List<Expression> postAggregationConjuncts = new ArrayList<>();

            // Strip out non-deterministic conjuncts
            extractConjuncts(inheritedPredicate).stream()
                    .filter(expression -> !isDeterministic(expression, metadata))
                    .forEach(postAggregationConjuncts::add);
            inheritedPredicate = filterDeterministicConjuncts(metadata, inheritedPredicate);

            // Sort non-equality predicates by those that can be pushed down and those that cannot
            Set<Symbol> groupingKeys = ImmutableSet.copyOf(node.getGroupingKeys());
            EqualityInference.nonInferrableConjuncts(metadata, inheritedPredicate).forEach(conjunct -> {
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

            PlanNode rewrittenSource = context.rewrite(node.getSource(), combineConjuncts(metadata, pushdownConjuncts));

            PlanNode output = node;
            if (rewrittenSource != node.getSource()) {
                output = new AggregationNode(node.getId(),
                        rewrittenSource,
                        node.getAggregations(),
                        node.getGroupingSets(),
                        ImmutableList.of(),
                        node.getStep(),
                        node.getHashSymbol(),
                        node.getGroupIdSymbol());
            }
            if (!postAggregationConjuncts.isEmpty()) {
                output = new FilterNode(idAllocator.getNextId(), output, combineConjuncts(metadata, postAggregationConjuncts));
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
            EqualityInference equalityInference = EqualityInference.newInstance(metadata, inheritedPredicate);

            List<Expression> pushdownConjuncts = new ArrayList<>();
            List<Expression> postUnnestConjuncts = new ArrayList<>();

            // Strip out non-deterministic conjuncts
            extractConjuncts(inheritedPredicate).stream()
                    .filter(expression -> !isDeterministic(expression, metadata))
                    .forEach(postUnnestConjuncts::add);
            inheritedPredicate = filterDeterministicConjuncts(metadata, inheritedPredicate);

            // Sort non-equality predicates by those that can be pushed down and those that cannot
            Set<Symbol> replicatedSymbols = ImmutableSet.copyOf(node.getReplicateSymbols());
            EqualityInference.nonInferrableConjuncts(metadata, inheritedPredicate).forEach(conjunct -> {
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

            PlanNode rewrittenSource = context.rewrite(node.getSource(), combineConjuncts(metadata, pushdownConjuncts));

            PlanNode output = node;
            if (rewrittenSource != node.getSource()) {
                output = new UnnestNode(node.getId(), rewrittenSource, node.getReplicateSymbols(), node.getMappings(), node.getOrdinalitySymbol(), node.getJoinType(), node.getFilter());
            }
            if (!postUnnestConjuncts.isEmpty()) {
                output = new FilterNode(idAllocator.getNextId(), output, combineConjuncts(metadata, postUnnestConjuncts));
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

            if (!TRUE_LITERAL.equals(predicate)) {
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
