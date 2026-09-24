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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.metadata.Metadata;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Booleans;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions.Comparison;
import io.trino.sql.ir.IrUtils;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.IrExpressionOptimizer;
import io.trino.sql.planner.EffectivePredicateProvider;
import io.trino.sql.planner.EqualityInference;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.type.CharVarcharCoercion;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.SystemSessionProperties.isEnableDynamicFiltering;
import static io.trino.spi.type.TypeUtils.typeHasNaN;
import static io.trino.sql.DynamicFilters.createDynamicFilterExpression;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.IDENTICAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.IrExpressions.matchComparison;
import static io.trino.sql.ir.IrExpressions.mayFail;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.ir.IrUtils.filterDeterministicConjuncts;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.DomainTranslator.isStructural;
import static io.trino.sql.planner.EqualityInference.isInferenceCandidate;
import static io.trino.sql.planner.SymbolsExtractor.extractUnique;
import static io.trino.sql.planner.iterative.rule.CanonicalizeExpressionRewriter.canonicalizeExpression;
import static io.trino.sql.planner.plan.JoinType.FULL;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.JoinType.RIGHT;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.partitioningBy;

/// Computes one local rewrite. Child traversal and repeated application belong to the iterative optimizer.
class Pushdown
{
    private static final Set<ComparisonOperator> DYNAMIC_FILTERING_SUPPORTED_COMPARISONS = Sets.immutableEnumSet(
            EQUAL,
            GREATER_THAN,
            GREATER_THAN_OR_EQUAL,
            LESS_THAN,
            LESS_THAN_OR_EQUAL);

    final SymbolAllocator symbolAllocator;
    final PlanNodeIdAllocator idAllocator;
    final PlannerContext plannerContext;
    private final IrExpressionOptimizer optimizer;
    final Metadata metadata;
    final Session session;
    final boolean dynamicFilteringEnabled;
    final EffectivePredicateProvider effectivePredicateProvider;
    private final boolean allowUnsafePushdown;
    private final CharVarcharCoercion charVarcharCoercion;
    private final PredicateEnforcement predicateEnforcement;
    // The value equalities stated by the inputs of a node, for the inherited and the join conjuncts alike
    private final Map<PlanNode, Predicate<Expression>> inputValueEqualities = new IdentityHashMap<>();

    Pushdown(Rule.Context context, PlannerContext plannerContext, boolean dynamicFilteringEnabled)
    {
        this.symbolAllocator = context.getSymbolAllocator();
        this.idAllocator = context.getIdAllocator();
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.metadata = plannerContext.getMetadata();
        this.session = context.getSession();
        this.dynamicFilteringEnabled = dynamicFilteringEnabled;
        this.predicateEnforcement = new PredicateEnforcement(plannerContext, session, symbolAllocator, context.getLookup());

        this.effectivePredicateProvider = context.getEffectivePredicateProvider();
        optimizer = plannerContext.getExpressionOptimizer();
        this.allowUnsafePushdown = SystemSessionProperties.isUnsafePushdownAllowed(session);
        this.charVarcharCoercion = getCharVarcharCoercion(session);
    }

    private PlanNode project(PlanNode source, Assignments assignments)
    {
        if (assignments.isIdentity() && ImmutableList.copyOf(assignments.outputs()).equals(source.getOutputSymbols())) {
            return source;
        }
        return new ProjectNode(idAllocator.getNextId(), source, assignments);
    }

    private static Assignments identityWith(PlanNode source, Map<Symbol, Expression> additions)
    {
        return Assignments.builder()
                .putIdentities(source.getOutputSymbols())
                .putAll(additions)
                .build();
    }

    PlanNode filter(PlanNode source, Expression predicate)
    {
        predicate = canonicalizeExpression(predicate, plannerContext, charVarcharCoercion);
        if (predicate.equals(TRUE)) {
            return source;
        }
        return new FilterNode(idAllocator.getNextId(), source, predicate);
    }

    PlanNode filterWithoutRedundantConjuncts(PlanNode source, Expression predicate, Expression effectivePredicate)
    {
        if (predicate.equals(TRUE)) {
            return source;
        }
        PredicateEnforcement.Implication inputFacts = predicateEnforcement.implication(effectivePredicate);
        return filter(source, combineConjuncts(extractConjuncts(simplifyExpression(predicate)).stream()
                .filter(conjunct -> !isDeterministic(conjunct) ||
                        (!inputFacts.implies(conjunct) && !predicateEnforcement.isEnforcedBySimplified(source, conjunct)))
                .toList()));
    }

    /// Pushes `inheritedPredicate` through `node` with `pushDown`, keeping above `node` the
    /// conjuncts that equalities in the effective predicates of its inputs already state. Such an
    /// equality describes value substitution rather than a row constraint: the projection
    /// `x := f(y)` contributes `x = f(y)`, which is NULL rather than TRUE when `f(y)` is NULL (see
    /// `EffectivePredicateExtractor`). Below `node`, the conjunct would be recognized as present and
    /// dropped, admitting those rows. Conjuncts inferred from input facts can still be dropped this
    /// way, because the facts they were inferred from remain in place.
    PlanNode pushDownRetainingValueEqualities(PlanNode node, Expression inheritedPredicate, Function<Expression, PlanNode> pushDown)
    {
        Map<Boolean, ImmutableList<Expression>> conjuncts = partitionByInputValueEqualities(node, inheritedPredicate);
        if (conjuncts.get(true).isEmpty()) {
            return pushDown.apply(inheritedPredicate);
        }
        Expression retained = combineConjuncts(conjuncts.get(true));
        PlanNode pushed = pushDown.apply(combineConjuncts(conjuncts.get(false)));
        // A filter left above the node takes the retained conjuncts: MergeFilters would combine two
        // adjacent filters, and the rule would split them again on every application
        if (pushed instanceof FilterNode filter) {
            return filter(filter.getSource(), combineConjuncts(filter.getPredicate(), retained));
        }
        return filter(pushed, retained);
    }

    /// Splits the conjuncts of `predicate` into those the value equalities in the effective predicates
    /// of the sources of `node` state (`true`) and the others. The input equalities are only extracted
    /// when a conjunct could match them, once per node for each rule application.
    private Map<Boolean, ImmutableList<Expression>> partitionByInputValueEqualities(PlanNode node, Expression predicate)
    {
        List<Expression> conjuncts = extractConjuncts(predicate);
        if (conjuncts.stream().noneMatch(conjunct -> isDeterministic(conjunct) && isValueEquality(simplifyExpression(conjunct)))) {
            return ImmutableMap.of(true, ImmutableList.of(), false, ImmutableList.copyOf(conjuncts));
        }
        Predicate<Expression> statedByInputs = inputValueEqualities.computeIfAbsent(node, this::statedByInputValueEqualities);
        return conjuncts.stream().collect(partitioningBy(statedByInputs, toImmutableList()));
    }

    /// Whether a deterministic equality conjunct is stated by the value equalities in the effective
    /// predicates of the sources of `node`, either directly or through their equivalence classes.
    private Predicate<Expression> statedByInputValueEqualities(PlanNode node)
    {
        List<Expression> inputEqualities = node.getSources().stream()
                .map(effectivePredicateProvider::getEffectivePredicate)
                .flatMap(predicate -> extractConjuncts(simplifyExpression(predicate)).stream())
                .filter(Pushdown::isValueEquality)
                .toList();
        if (inputEqualities.isEmpty()) {
            return _ -> false;
        }
        // Equality inference rejects some equalities, such as x = nullif(y, 0), so they are also matched directly
        Set<Set<Expression>> equalities = inputEqualities.stream()
                .map(equality -> (Comparison.Equal) matchComparison(equality))
                .map(equality -> ImmutableSet.of(equality.left(), equality.right()))
                .collect(toImmutableSet());
        Collection<Collection<Expression>> equalitySets = new EqualityInference(plannerContext, charVarcharCoercion, inputEqualities).getEqualitySets();
        return conjunct -> isDeterministic(conjunct) &&
                matchComparison(simplifyExpression(conjunct)) instanceof Comparison.Equal(Expression left, Expression right) &&
                (equalities.contains(ImmutableSet.of(left, right)) ||
                        equalitySets.stream().anyMatch(equalitySet -> equalitySet.contains(left) && equalitySet.contains(right)));
    }

    /// Whether `conjunct` is an equality of two non-constant expressions. An equality with a
    /// non-null constant constrains every row; others may only state value substitution.
    private static boolean isValueEquality(Expression conjunct)
    {
        return matchComparison(conjunct) instanceof Comparison.Equal(Expression left, Expression right) &&
                !(left instanceof Constant) && !(right instanceof Constant);
    }

    /// Pushes the conjuncts of `predicate` that `eligible` accepts into the sources of `node`,
    /// each rewritten by `rewrite`, and keeps the others above `node` in their original order.
    PlanNode pushEligibleConjuncts(PlanNode node, Expression predicate, Predicate<Expression> eligible, UnaryOperator<Expression> rewrite)
    {
        Map<Boolean, ImmutableList<Expression>> conjuncts = extractConjuncts(predicate).stream()
                .collect(partitioningBy(eligible, toImmutableList()));
        PlanNode rewritten = pushFilterIntoSources(node, combineConjuncts(conjuncts.get(true).stream()
                .map(rewrite)
                .toList()));
        return filter(rewritten, combineConjuncts(conjuncts.get(false)));
    }

    PlanNode pushEligibleConjuncts(PlanNode node, Expression predicate, Predicate<Expression> eligible)
    {
        return pushEligibleConjuncts(node, predicate, eligible, UnaryOperator.identity());
    }

    PlanNode pushFilterIntoSources(PlanNode node, Expression predicate)
    {
        List<PlanNode> sources = node.getSources();
        List<PlanNode> rewritten = sources.stream().map(source -> filter(source, predicate)).toList();
        if (sources.equals(rewritten)) {
            return node;
        }
        return node.replaceChildren(rewritten);
    }

    public PlanNode pushThroughJoin(JoinNode node, Expression inheritedPredicate)
    {
        // See if we can rewrite outer joins in terms of a plain inner join
        node = normalizeOuterJoin(node, inheritedPredicate);

        Expression leftEffectivePredicate = effectivePredicateProvider.getEffectivePredicate(node.getLeft());
        Expression rightEffectivePredicate = effectivePredicateProvider.getEffectivePredicate(node.getRight());
        Expression joinPredicate = extractJoinPredicate(node);

        // Join conjuncts that the inputs' value equalities state stay out of the inputs, as inherited ones do
        // in pushDownRetainingValueEqualities. Above an inner join, because join reordering drops a
        // one-sided equality from the join filter
        Map<Boolean, ImmutableList<Expression>> joinConjuncts = partitionByInputValueEqualities(node, joinPredicate);
        Expression pushableJoinPredicate = combineConjuncts(joinConjuncts.get(false));
        Expression retainedJoinPredicate = combineConjuncts(joinConjuncts.get(true));

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
                        pushableJoinPredicate,
                        node.getLeft().getOutputSymbols(),
                        node.getRight().getOutputSymbols());
                leftPredicate = innerJoinPushDownResult.leftPredicate();
                rightPredicate = innerJoinPushDownResult.rightPredicate();
                postJoinPredicate = innerJoinPushDownResult.postJoinPredicate();
                newJoinPredicate = innerJoinPushDownResult.joinPredicate();
            }
            case LEFT -> {
                OuterJoinPushDownResult leftOuterJoinPushDownResult = processLimitedOuterJoin(
                        inheritedPredicate,
                        leftEffectivePredicate,
                        rightEffectivePredicate,
                        pushableJoinPredicate,
                        node.getLeft().getOutputSymbols(),
                        node.getRight().getOutputSymbols());
                leftPredicate = leftOuterJoinPushDownResult.outerJoinPredicate();
                rightPredicate = leftOuterJoinPushDownResult.innerJoinPredicate();
                postJoinPredicate = leftOuterJoinPushDownResult.postJoinPredicate();
                newJoinPredicate = leftOuterJoinPushDownResult.joinPredicate();
            }
            case RIGHT -> {
                OuterJoinPushDownResult rightOuterJoinPushDownResult = processLimitedOuterJoin(
                        inheritedPredicate,
                        rightEffectivePredicate,
                        leftEffectivePredicate,
                        pushableJoinPredicate,
                        node.getRight().getOutputSymbols(),
                        node.getLeft().getOutputSymbols());
                leftPredicate = rightOuterJoinPushDownResult.innerJoinPredicate();
                rightPredicate = rightOuterJoinPushDownResult.outerJoinPredicate();
                postJoinPredicate = rightOuterJoinPushDownResult.postJoinPredicate();
                newJoinPredicate = rightOuterJoinPushDownResult.joinPredicate();
            }
            case FULL -> {
                leftPredicate = TRUE;
                rightPredicate = TRUE;
                postJoinPredicate = inheritedPredicate;
                newJoinPredicate = pushableJoinPredicate;
            }
            default -> throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
        }

        if (node.getType() == INNER) {
            postJoinPredicate = combineConjuncts(postJoinPredicate, retainedJoinPredicate);
        }
        else {
            newJoinPredicate = combineConjuncts(newJoinPredicate, retainedJoinPredicate);
        }

        newJoinPredicate = simplifyExpression(newJoinPredicate);

        // Projections for the new join clauses, added to identity projections of the existing symbols
        Map<Symbol, Expression> leftProjections = new LinkedHashMap<>();
        Map<Symbol, Expression> rightProjections = new LinkedHashMap<>();

        List<JoinNode.EquiJoinClause> equiJoinClauses = new ArrayList<>();
        ImmutableList.Builder<Expression> joinFilterBuilder = ImmutableList.builder();
        for (Expression conjunct : extractConjuncts(newJoinPredicate)) {
            if (joinEqualityExpression(conjunct, node.getLeft().getOutputSymbols(), node.getRight().getOutputSymbols())) {
                Comparison equality = matchComparison(conjunct);

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
        DynamicFiltersResult dynamicFiltersResult = createDynamicFilters(node, equiJoinClauses, joinFilter);
        Map<DynamicFilterId, Symbol> dynamicFilters = dynamicFiltersResult.dynamicFilters();
        leftPredicate = combineConjuncts(leftPredicate, combineConjuncts(dynamicFiltersResult.predicates()));

        PlanNode leftSource;
        PlanNode rightSource;
        boolean equiJoinClausesUnmodified = ImmutableSet.copyOf(equiJoinClauses).equals(ImmutableSet.copyOf(node.getCriteria()));
        if (!equiJoinClausesUnmodified) {
            leftSource = filter(project(node.getLeft(), identityWith(node.getLeft(), leftProjections)), leftPredicate);
            rightSource = filter(project(node.getRight(), identityWith(node.getRight(), rightProjections)), rightPredicate);
        }
        else {
            leftSource = filterWithoutRedundantConjuncts(node.getLeft(), leftPredicate, leftEffectivePredicate);
            rightSource = filterWithoutRedundantConjuncts(node.getRight(), rightPredicate, rightEffectivePredicate);
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
                    node.getDistributionType(),
                    node.isSpillable(),
                    dynamicFilters,
                    node.getReorderJoinStatsAndCost());
        }

        if (!postJoinPredicate.equals(TRUE)) {
            output = filter(output, postJoinPredicate);
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
            List<Expression> joinFilterClauses)
    {
        if ((node.getType() != INNER && node.getType() != RIGHT) || !isEnableDynamicFiltering(session) || !dynamicFilteringEnabled) {
            return new DynamicFiltersResult(node.getDynamicFilters(), ImmutableList.of());
        }

        List<DynamicFilterExpression> clauses = dynamicFilterClauses(node, equiJoinClauses, joinFilterClauses);
        if (!node.getDynamicFilters().isEmpty()) {
            // A previous application created consumers for the clauses the join already has, and
            // they may have moved below it. Create consumers only for clauses added since then.
            Set<DynamicFilterExpression> existingClauses = ImmutableSet.copyOf(dynamicFilterClauses(
                    node,
                    node.getCriteria(),
                    node.getFilter().map(IrUtils::extractConjuncts).orElse(ImmutableList.of())));
            clauses = clauses.stream()
                    .filter(clause -> !existingClauses.contains(clause))
                    .collect(toImmutableList());
        }

        // Collect build symbols:
        Set<Symbol> buildSymbols = clauses.stream()
                .map(DynamicFilterExpression::right)
                .map(Symbol::from)
                .collect(toImmutableSet());

        // Allocate IDs once per producer, so new consumers of an existing build symbol share its filter
        BiMap<Symbol, DynamicFilterId> buildSymbolToDynamicFilter = HashBiMap.create(node.getDynamicFilters()).inverse();
        for (Symbol buildSymbol : buildSymbols) {
            buildSymbolToDynamicFilter.computeIfAbsent(
                    buildSymbol,
                    _ -> newDynamicFilterId());
        }

        // Multiple probe symbols may depend on a single build symbol / dynamic filter ID:
        List<Expression> predicates = clauses
                .stream()
                .map(clause -> {
                    Expression probeExpression = clause.left();
                    Symbol buildSymbol = Symbol.from(clause.right());
                    // we can take type of buildSymbol instead probeExpression as comparison expression must have the same type on both sides
                    Type type = buildSymbol.type();
                    DynamicFilterId id = requireNonNull(buildSymbolToDynamicFilter.get(buildSymbol), () -> "missing dynamic filter for symbol " + buildSymbol);
                    return createDynamicFilterExpression(metadata, charVarcharCoercion, id, type, probeExpression, clause.operator(), clause.nullAllowed());
                })
                .collect(toImmutableList());
        // Return a mapping from build symbols to corresponding dynamic filter IDs:
        return new DynamicFiltersResult(buildSymbolToDynamicFilter.inverse(), predicates);
    }

    private List<DynamicFilterExpression> dynamicFilterClauses(JoinNode node, List<JoinNode.EquiJoinClause> equiJoinClauses, List<Expression> joinFilterClauses)
    {
        return Streams.concat(
                        equiJoinClauses
                                .stream()
                                .map(clause -> new DynamicFilterExpression(
                                        EQUAL, clause.getLeft().toSymbolReference(), clause.getRight().toSymbolReference())),
                        joinFilterClauses.stream()
                                .filter(clause -> joinDynamicFilteringExpression(clause, node.getLeft().getOutputSymbols(), node.getRight().getOutputSymbols()))
                                .map(expression -> switch (matchComparison(expression)) {
                                    case Comparison.Identical(Expression left, Expression right) -> new DynamicFilterExpression(EQUAL, left, right, true);
                                    case Comparison comparison -> new DynamicFilterExpression(comparison.operator(), comparison.left(), comparison.right());
                                    case null -> throw new IllegalStateException("Expected a comparison: " + expression);
                                })
                                .map(dynamicFilter -> {
                                    Expression leftExpression = dynamicFilter.left();
                                    Expression rightExpression = dynamicFilter.right();
                                    boolean alignedComparison = node.getLeft().getOutputSymbols().containsAll(extractUnique(leftExpression));
                                    return new DynamicFilterExpression(
                                            alignedComparison ? dynamicFilter.operator() : dynamicFilter.operator().flip(),
                                            alignedComparison ? leftExpression : rightExpression,
                                            alignedComparison ? rightExpression : leftExpression,
                                            dynamicFilter.nullAllowed());
                                }))
                .collect(toImmutableList());
    }

    private record DynamicFilterExpression(ComparisonOperator operator, Expression left, Expression right, boolean nullAllowed)
    {
        private DynamicFilterExpression(ComparisonOperator operator, Expression left, Expression right)
        {
            this(operator, left, right, false);
        }
    }

    private record DynamicFiltersResult(Map<DynamicFilterId, Symbol> dynamicFilters, List<Expression> predicates)
    {
        private DynamicFiltersResult
        {
            dynamicFilters = ImmutableMap.copyOf(dynamicFilters);
            predicates = ImmutableList.copyOf(predicates);
        }
    }

    public PlanNode pushThroughSpatialJoin(SpatialJoinNode node, Expression inheritedPredicate)
    {
        node = normalizeOuterJoin(node, inheritedPredicate);

        Expression leftEffectivePredicate = effectivePredicateProvider.getEffectivePredicate(node.getLeft());
        Expression rightEffectivePredicate = effectivePredicateProvider.getEffectivePredicate(node.getRight());
        Expression joinPredicate = node.getFilter();

        // Join conjuncts that the inputs' value equalities state stay out of the inputs, as inherited ones do
        // in pushDownRetainingValueEqualities. Above an inner join, because join reordering drops a
        // one-sided equality from the join filter
        Map<Boolean, ImmutableList<Expression>> joinConjuncts = partitionByInputValueEqualities(node, joinPredicate);
        Expression pushableJoinPredicate = combineConjuncts(joinConjuncts.get(false));
        Expression retainedJoinPredicate = combineConjuncts(joinConjuncts.get(true));

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
                        pushableJoinPredicate,
                        node.getLeft().getOutputSymbols(),
                        node.getRight().getOutputSymbols());
                leftPredicate = innerJoinPushDownResult.leftPredicate();
                rightPredicate = innerJoinPushDownResult.rightPredicate();
                postJoinPredicate = innerJoinPushDownResult.postJoinPredicate();
                newJoinPredicate = innerJoinPushDownResult.joinPredicate();
            }
            case LEFT -> {
                OuterJoinPushDownResult leftOuterJoinPushDownResult = processLimitedOuterJoin(
                        inheritedPredicate,
                        leftEffectivePredicate,
                        rightEffectivePredicate,
                        pushableJoinPredicate,
                        node.getLeft().getOutputSymbols(),
                        node.getRight().getOutputSymbols());
                leftPredicate = leftOuterJoinPushDownResult.outerJoinPredicate();
                rightPredicate = leftOuterJoinPushDownResult.innerJoinPredicate();
                postJoinPredicate = leftOuterJoinPushDownResult.postJoinPredicate();
                newJoinPredicate = leftOuterJoinPushDownResult.joinPredicate();
            }
            default -> throw new IllegalArgumentException("Unsupported spatial join type: " + node.getType());
        }

        if (node.getType() == SpatialJoinNode.Type.INNER) {
            postJoinPredicate = combineConjuncts(postJoinPredicate, retainedJoinPredicate);
        }
        else {
            newJoinPredicate = combineConjuncts(newJoinPredicate, retainedJoinPredicate);
        }

        newJoinPredicate = simplifyExpression(newJoinPredicate);
        verify(!newJoinPredicate.equals(Booleans.FALSE), "Spatial join predicate is missing");

        PlanNode leftSource = filterWithoutRedundantConjuncts(node.getLeft(), leftPredicate, leftEffectivePredicate);
        PlanNode rightSource = filterWithoutRedundantConjuncts(node.getRight(), rightPredicate, rightEffectivePredicate);

        PlanNode output = node;
        if (leftSource != node.getLeft() ||
                rightSource != node.getRight() ||
                !newJoinPredicate.equals(joinPredicate)) {
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
            output = filter(output, postJoinPredicate);
        }

        return output;
    }

    private Symbol symbolForExpression(Expression expression)
    {
        if (expression instanceof Reference reference) {
            return Symbol.from(reference);
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
        EqualityInference inheritedInference = new EqualityInference(plannerContext, charVarcharCoercion, inheritedPredicate);
        EqualityInference outerInference = new EqualityInference(plannerContext, charVarcharCoercion, inheritedPredicate, outerEffectivePredicate);

        Set<Symbol> innerScope = ImmutableSet.copyOf(innerSymbols);
        Set<Symbol> outerScope = ImmutableSet.copyOf(outerSymbols);

        EqualityInference.EqualityPartition equalityPartition = inheritedInference.generateEqualitiesPartitionedBy(outerScope);
        Expression outerOnlyInheritedEqualities = combineConjuncts(equalityPartition.scopeEqualities());
        EqualityInference potentialNullSymbolInference = new EqualityInference(plannerContext, charVarcharCoercion, outerOnlyInheritedEqualities, outerEffectivePredicate, innerEffectivePredicate, joinPredicate);

        // Push outer and join equalities into the inner side. For example:
        // SELECT * FROM nation LEFT OUTER JOIN region ON nation.regionkey = region.regionkey and nation.name = region.name WHERE nation.name = 'blah'

        EqualityInference potentialNullSymbolInferenceWithoutInnerInferred = new EqualityInference(plannerContext, charVarcharCoercion, outerOnlyInheritedEqualities, outerEffectivePredicate, joinPredicate);
        innerPushdownConjuncts.addAll(potentialNullSymbolInferenceWithoutInnerInferred.generateEqualitiesPartitionedBy(innerScope).scopeEqualities());

        // TODO: we can further improve simplifying the equalities by considering other relationships from the outer side
        EqualityInference.EqualityPartition joinEqualityPartition = new EqualityInference(plannerContext, charVarcharCoercion, joinPredicate).generateEqualitiesPartitionedBy(innerScope);
        innerPushdownConjuncts.addAll(joinEqualityPartition.scopeEqualities());
        joinConjuncts.addAll(joinEqualityPartition.scopeComplementEqualities())
                .addAll(joinEqualityPartition.scopeStraddlingEqualities());

        // Add the equalities from the inferences back in
        outerPushdownConjuncts.addAll(equalityPartition.scopeEqualities());
        postJoinConjuncts.addAll(equalityPartition.scopeComplementEqualities());
        postJoinConjuncts.addAll(equalityPartition.scopeStraddlingEqualities());

        // See if we can push inherited predicates down
        EqualityInference.nonInferrableConjuncts(plannerContext, charVarcharCoercion, inheritedPredicate).forEach(conjunct -> {
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
        EqualityInference.nonInferrableConjuncts(plannerContext, charVarcharCoercion, outerEffectivePredicate)
                .map(conjunct -> potentialNullSymbolInference.rewrite(conjunct, innerScope))
                .filter(Objects::nonNull)
                .forEach(innerPushdownConjuncts::add);

        // See if we can push down join predicates to the inner side
        EqualityInference.nonInferrableConjuncts(plannerContext, charVarcharCoercion, joinPredicate).forEach(conjunct -> {
            Expression innerRewritten = potentialNullSymbolInference.rewrite(conjunct, innerScope);
            if (innerRewritten != null) {
                innerPushdownConjuncts.add(innerRewritten);
            }
            else {
                joinConjuncts.add(conjunct);
            }
        });

        return new OuterJoinPushDownResult(
                combineConjuncts(outerPushdownConjuncts.build()),
                combineConjuncts(innerPushdownConjuncts.build()),
                combineConjuncts(joinConjuncts.build()),
                combineConjuncts(postJoinConjuncts.build()));
    }

    private record OuterJoinPushDownResult(Expression outerJoinPredicate, Expression innerJoinPredicate, Expression joinPredicate, Expression postJoinPredicate)
    {
        private OuterJoinPushDownResult
        {
            requireNonNull(outerJoinPredicate, "outerJoinPredicate is null");
            requireNonNull(innerJoinPredicate, "innerJoinPredicate is null");
            requireNonNull(joinPredicate, "joinPredicate is null");
            requireNonNull(postJoinPredicate, "postJoinPredicate is null");
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

        Set<Symbol> leftScope = ImmutableSet.copyOf(leftSymbols);
        Set<Symbol> rightScope = ImmutableSet.copyOf(rightSymbols);

        List<Expression> nonDeterministic = new ArrayList<>();
        List<Expression> candidates = new ArrayList<>();
        List<Expression> residuals = new ArrayList<>();
        List<Expression> mayFail = new ArrayList<>();
        for (List<Expression> conjuncts : List.of(extractConjuncts(joinPredicate), extractConjuncts(inheritedPredicate))) {
            for (Expression conjunct : conjuncts) {
                if (!isDeterministic(conjunct)) {
                    nonDeterministic.add(conjunct);
                }
                // Unsafe pushdown keeps may-fail conjuncts in their original position
                else if (!allowUnsafePushdown && mayFail(plannerContext, charVarcharCoercion, conjunct)) {
                    mayFail.add(conjunct);
                }
                else if (isInferenceCandidate(plannerContext, charVarcharCoercion, conjunct)) {
                    candidates.add(conjunct);
                }
                else {
                    residuals.add(conjunct);
                }
            }
        }

        List<Expression> leftCandidates = new ArrayList<>();
        List<Expression> leftResiduals = new ArrayList<>();
        partitionEffectiveConjuncts(leftEffectivePredicate, leftCandidates, leftResiduals);

        List<Expression> rightCandidates = new ArrayList<>();
        List<Expression> rightResiduals = new ArrayList<>();
        partitionEffectiveConjuncts(rightEffectivePredicate, rightCandidates, rightResiduals);

        EqualityInference allInference = new EqualityInference(plannerContext, charVarcharCoercion,
                ImmutableList.<Expression>builder()
                        .addAll(candidates)
                        .addAll(leftCandidates)
                        .addAll(rightCandidates)
                        .build());
        EqualityInference inferenceWithoutLeft = new EqualityInference(plannerContext, charVarcharCoercion,
                ImmutableList.<Expression>builder()
                        .addAll(candidates)
                        .addAll(rightCandidates)
                        .build());
        EqualityInference inferenceWithoutRight = new EqualityInference(plannerContext, charVarcharCoercion,
                ImmutableList.<Expression>builder()
                        .addAll(candidates)
                        .addAll(leftCandidates)
                        .build());

        ImmutableList.Builder<Expression> leftPushDownConjuncts = ImmutableList.<Expression>builder()
                .addAll(inferenceWithoutLeft.generateEqualitiesPartitionedBy(leftScope).scopeEqualities())
                .addAll(rightResiduals.stream()
                        .map(conjunct -> allInference.rewrite(conjunct, leftScope))
                        .filter(Objects::nonNull)
                        .toList());

        ImmutableList.Builder<Expression> rightPushDownConjuncts = ImmutableList.<Expression>builder()
                .addAll(inferenceWithoutRight.generateEqualitiesPartitionedBy(rightScope).scopeEqualities())
                .addAll(leftResiduals.stream()
                        .map(conjunct -> allInference.rewrite(conjunct, rightScope))
                        .filter(Objects::nonNull)
                        .toList());

        ImmutableList.Builder<Expression> joinConjuncts = ImmutableList.<Expression>builder()
                .addAll(allInference.generateEqualitiesPartitionedBy(leftScope).scopeStraddlingEqualities())
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

    /// Splits the deterministic conjuncts of an input's effective predicate that cannot fail into
    /// equality-inference candidates and the remaining residuals.
    private void partitionEffectiveConjuncts(Expression effectivePredicate, List<Expression> candidates, List<Expression> residuals)
    {
        for (Expression conjunct : extractConjuncts(effectivePredicate)) {
            if (mayFail(plannerContext, charVarcharCoercion, conjunct) || !isDeterministic(conjunct)) {
                continue;
            }
            if (isInferenceCandidate(plannerContext, charVarcharCoercion, conjunct)) {
                candidates.add(conjunct);
            }
            else {
                residuals.add(conjunct);
            }
        }
    }

    private record InnerJoinPushDownResult(Expression leftPredicate, Expression rightPredicate, Expression joinPredicate, Expression postJoinPredicate)
    {
        private InnerJoinPushDownResult
        {
            requireNonNull(leftPredicate, "leftPredicate is null");
            requireNonNull(rightPredicate, "rightPredicate is null");
            requireNonNull(joinPredicate, "joinPredicate is null");
            requireNonNull(postJoinPredicate, "postJoinPredicate is null");
        }
    }

    private Expression extractJoinPredicate(JoinNode joinNode)
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        for (JoinNode.EquiJoinClause equiJoinClause : joinNode.getCriteria()) {
            builder.add(equiJoinClause.toExpression(plannerContext.getMetadata(), charVarcharCoercion));
        }
        joinNode.getFilter().ifPresent(builder::add);
        return combineConjuncts(builder.build());
    }

    /// Rewrites a left spatial join as an inner join when `inheritedPredicate` rejects its null-extended rows.
    SpatialJoinNode normalizeOuterJoin(SpatialJoinNode node, Expression inheritedPredicate)
    {
        if (node.getType() == SpatialJoinNode.Type.LEFT && canConvertOuterToInner(node.getRight().getOutputSymbols(), inheritedPredicate)) {
            return new SpatialJoinNode(node.getId(), SpatialJoinNode.Type.INNER, node.getLeft(), node.getRight(), node.getOutputSymbols(), node.getFilter(), node.getLeftPartitionSymbol(), node.getRightPartitionSymbol(), node.getKdbTree());
        }
        return node;
    }

    /// Rewrites an outer join as a less outer one when `inheritedPredicate` rejects the rows it null-extends.
    JoinNode normalizeOuterJoin(JoinNode node, Expression inheritedPredicate)
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
                    node.getDistributionType(),
                    node.isSpillable(),
                    node.getDynamicFilters(),
                    node.getReorderJoinStatsAndCost());
        }

        if ((node.getType() == JoinType.LEFT && !canConvertOuterToInner(node.getRight().getOutputSymbols(), inheritedPredicate)) ||
                (node.getType() == JoinType.RIGHT && !canConvertOuterToInner(node.getLeft().getOutputSymbols(), inheritedPredicate))) {
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

    DynamicFilterId newDynamicFilterId()
    {
        return new DynamicFilterId("df_" + idAllocator.getNextId());
    }

    // Simplifies inferred and effective predicates before comparing them, in the form expression cleanup produces
    Expression simplifyExpression(Expression expression)
    {
        return predicateEnforcement.simplifyExpression(expression);
    }

    /// Evaluates an expression's response to binding the specified input symbols to NULL.
    private Expression nullInputEvaluator(Collection<Symbol> nullSymbols, Expression expression)
    {
        Map<Symbol, Expression> inputs = nullSymbols.stream()
                .collect(toImmutableMap(identity(), symbol -> new Constant(symbol.type(), null)));

        return optimizer.process(expression, session, symbolAllocator, inputs).orElse(expression);
    }

    private boolean joinEqualityExpression(Expression expression, Collection<Symbol> leftSymbols, Collection<Symbol> rightSymbols)
    {
        // At this point in time, our join predicates need to be deterministic
        if (matchComparison(expression) instanceof Comparison comparison && comparison.operator() == EQUAL && isDeterministic(expression)) {
            Set<Symbol> symbols1 = extractUnique(comparison.left());
            Set<Symbol> symbols2 = extractUnique(comparison.right());
            if (symbols1.isEmpty() || symbols2.isEmpty()) {
                return false;
            }
            return (leftSymbols.containsAll(symbols1) && rightSymbols.containsAll(symbols2)) ||
                    (rightSymbols.containsAll(symbols1) && leftSymbols.containsAll(symbols2));
        }
        return false;
    }

    private boolean joinDynamicFilteringExpression(Expression expression, Collection<Symbol> leftSymbols, Collection<Symbol> rightSymbols)
    {
        if (!(matchComparison(expression) instanceof Comparison decoded) || !isDeterministic(expression)) {
            return false;
        }

        ComparisonOperator operator = decoded.operator();
        Expression left = decoded.left();
        Expression right = decoded.right();

        Set<Symbol> symbols1 = extractUnique(left);
        Set<Symbol> symbols2 = extractUnique(right);

        if (symbols1.isEmpty() || symbols2.isEmpty()) {
            return false;
        }

        if (!(leftSymbols.containsAll(symbols1) && rightSymbols.containsAll(symbols2)) &&
                !(rightSymbols.containsAll(symbols1) && leftSymbols.containsAll(symbols2))) {
            return false;
        }

        if (operator == IDENTICAL) {
            // Dynamic filters follow = semantics: NaN is not collected, and structural values containing nulls or NaN
            // do not match themselves, while IS NOT DISTINCT FROM matches them
            if (mayBeUnequalToItself(left.type()) || mayBeUnequalToItself(right.type())) {
                return false;
            }
        }
        else if (!DYNAMIC_FILTERING_SUPPORTED_COMPARISONS.contains(operator)) {
            return false;
        }

        // Build side expression must be a symbol reference, since DynamicFilterSourceOperator can only collect column values (not expressions)
        return (right instanceof Reference && rightSymbols.contains(Symbol.from(right)))
                || (left instanceof Reference && rightSymbols.contains(Symbol.from(left)));
    }

    private static boolean mayBeUnequalToItself(Type type)
    {
        return typeHasNaN(type) || isStructural(type);
    }
}
