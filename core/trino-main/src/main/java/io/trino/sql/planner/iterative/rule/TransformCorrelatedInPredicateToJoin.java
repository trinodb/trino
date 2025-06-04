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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrUtils;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.matching.Pattern.nonEmpty;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.ir.IrUtils.or;
import static io.trino.sql.planner.plan.AggregationNode.singleAggregation;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.Patterns.Apply.correlation;
import static io.trino.sql.planner.plan.Patterns.applyNode;
import static java.util.Objects.requireNonNull;

/**
 * Replaces correlated ApplyNode with InPredicate expression with SemiJoin
 * <p>
 * Transforms:
 * <pre>
 * - Apply (output: a in B.b)
 *    - input: some plan A producing symbol a
 *    - subquery: some plan B producing symbol b, using symbols from A
 * </pre>
 * Into:
 * <pre>
 * - Project (output: CASE WHEN (countmatches > 0) THEN true WHEN (countnullmatches > 0) THEN null ELSE false END)
 *   - Aggregate (countmatches=count(*) where a, b not null; countnullmatches where (a is null or b is null) but buildSideKnownNonNull is not null)
 *     grouping by (A'.*)
 *     - LeftJoin on (a = B.b, A and B correlation condition)
 *       - AssignUniqueId (A')
 *         - A
 * </pre>
 *
 * @see TransformCorrelatedGlobalAggregationWithProjection
 */
public class TransformCorrelatedInPredicateToJoin
        implements Rule<ApplyNode>
{
    private static final Pattern<ApplyNode> PATTERN = applyNode()
            .with(nonEmpty(correlation()));

    private final Metadata metadata;

    public TransformCorrelatedInPredicateToJoin(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<ApplyNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ApplyNode apply, Captures captures, Context context)
    {
        Map<Symbol, ApplyNode.SetExpression> subqueryAssignments = apply.getSubqueryAssignments();
        if (subqueryAssignments.size() != 1) {
            return Result.empty();
        }
        ApplyNode.SetExpression assignmentExpression = getOnlyElement(subqueryAssignments.values());
        if (!(assignmentExpression instanceof ApplyNode.In inPredicate)) {
            return Result.empty();
        }

        Symbol inPredicateOutputSymbol = getOnlyElement(subqueryAssignments.keySet());

        return apply(apply, inPredicate, inPredicateOutputSymbol, context.getLookup(), context.getIdAllocator(), context.getSymbolAllocator());
    }

    private Result apply(
            ApplyNode apply,
            ApplyNode.In inPredicate,
            Symbol inPredicateOutputSymbol,
            Lookup lookup,
            PlanNodeIdAllocator idAllocator,
            SymbolAllocator symbolAllocator)
    {
        Optional<Decorrelated> decorrelated = new DecorrelatingVisitor(lookup, apply.getCorrelation())
                .decorrelate(apply.getSubquery());

        if (decorrelated.isEmpty()) {
            return Result.empty();
        }

        PlanNode projection = buildInPredicateEquivalent(
                apply,
                inPredicate,
                inPredicateOutputSymbol,
                decorrelated.get(),
                idAllocator,
                symbolAllocator);

        return Result.ofPlanNode(projection);
    }

    private PlanNode buildInPredicateEquivalent(
            ApplyNode apply,
            ApplyNode.In inPredicate,
            Symbol inPredicateOutputSymbol,
            Decorrelated decorrelated,
            PlanNodeIdAllocator idAllocator,
            SymbolAllocator symbolAllocator)
    {
        Expression correlationCondition = and(decorrelated.getCorrelatedPredicates());
        PlanNode decorrelatedBuildSource = decorrelated.getDecorrelatedNode();

        AssignUniqueId probeSide = new AssignUniqueId(
                idAllocator.getNextId(),
                apply.getInput(),
                symbolAllocator.newSymbol("unique", BIGINT));

        Symbol buildSideKnownNonNull = symbolAllocator.newSymbol("buildSideKnownNonNull", BIGINT);
        ProjectNode buildSide = new ProjectNode(
                idAllocator.getNextId(),
                decorrelatedBuildSource,
                Assignments.builder()
                        .putIdentities(decorrelatedBuildSource.getOutputSymbols())
                        .put(buildSideKnownNonNull, bigint(0))
                        .build());

        Symbol probeSideSymbol = inPredicate.value();
        Symbol buildSideSymbol = inPredicate.reference();

        Expression joinExpression = and(
                or(
                        new IsNull(probeSideSymbol.toSymbolReference()),
                        new Comparison(Comparison.Operator.EQUAL, probeSideSymbol.toSymbolReference(), buildSideSymbol.toSymbolReference()),
                        new IsNull(buildSideSymbol.toSymbolReference())),
                correlationCondition);

        JoinNode leftOuterJoin = leftOuterJoin(idAllocator, probeSide, buildSide, joinExpression);

        Symbol matchConditionSymbol = symbolAllocator.newSymbol("matchConditionSymbol", BOOLEAN);
        Expression matchCondition = and(
                isNotNull(probeSideSymbol),
                isNotNull(buildSideSymbol));

        Symbol nullMatchConditionSymbol = symbolAllocator.newSymbol("nullMatchConditionSymbol", BOOLEAN);
        Expression nullMatchCondition = and(
                isNotNull(buildSideKnownNonNull),
                not(metadata, matchCondition));

        ProjectNode preProjection = new ProjectNode(
                idAllocator.getNextId(),
                leftOuterJoin,
                Assignments.builder()
                        .putIdentities(leftOuterJoin.getOutputSymbols())
                        .put(matchConditionSymbol, matchCondition)
                        .put(nullMatchConditionSymbol, nullMatchCondition)
                        .build());

        Symbol countMatchesSymbol = symbolAllocator.newSymbol("countMatches", BIGINT);
        Symbol countNullMatchesSymbol = symbolAllocator.newSymbol("countNullMatches", BIGINT);

        AggregationNode aggregation = singleAggregation(
                idAllocator.getNextId(),
                preProjection,
                ImmutableMap.<Symbol, AggregationNode.Aggregation>builder()
                        .put(countMatchesSymbol, countWithFilter(matchConditionSymbol))
                        .put(countNullMatchesSymbol, countWithFilter(nullMatchConditionSymbol))
                        .buildOrThrow(),
                singleGroupingSet(probeSide.getOutputSymbols()));

        // TODO since we care only about "some count > 0", we could have specialized node instead of leftOuterJoin that does the job without materializing join results
        Case inPredicateEquivalent = new Case(
                ImmutableList.of(
                        new WhenClause(isGreaterThan(countMatchesSymbol, 0), booleanConstant(true)),
                        new WhenClause(isGreaterThan(countNullMatchesSymbol, 0), booleanConstant(null))),
                FALSE);
        return new ProjectNode(
                idAllocator.getNextId(),
                aggregation,
                Assignments.builder()
                        .putIdentities(apply.getInput().getOutputSymbols())
                        .put(inPredicateOutputSymbol, inPredicateEquivalent)
                        .build());
    }

    private static JoinNode leftOuterJoin(PlanNodeIdAllocator idAllocator, AssignUniqueId probeSide, ProjectNode buildSide, Expression joinExpression)
    {
        return new JoinNode(
                idAllocator.getNextId(),
                JoinType.LEFT,
                probeSide,
                buildSide,
                ImmutableList.of(),
                probeSide.getOutputSymbols(),
                buildSide.getOutputSymbols(),
                false,
                Optional.of(joinExpression),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());
    }

    private AggregationNode.Aggregation countWithFilter(Symbol filter)
    {
        return new AggregationNode.Aggregation(
                metadata.resolveBuiltinFunction("count", ImmutableList.of()),
                ImmutableList.of(),
                false,
                Optional.of(filter),
                Optional.empty(),
                Optional.empty()); /* mask */
    }

    private static Expression isGreaterThan(Symbol symbol, long value)
    {
        return new Comparison(
                Comparison.Operator.GREATER_THAN,
                symbol.toSymbolReference(),
                bigint(value));
    }

    private Expression isNotNull(Symbol symbol)
    {
        return not(metadata, new IsNull(symbol.toSymbolReference()));
    }

    private static Expression bigint(long value)
    {
        return new Constant(BIGINT, value);
    }

    private static Expression booleanConstant(@Nullable Boolean value)
    {
        if (value == null) {
            return new Constant(BOOLEAN, null);
        }
        return new Constant(BOOLEAN, value);
    }

    private static class DecorrelatingVisitor
            extends PlanVisitor<Optional<Decorrelated>, PlanNode>
    {
        private final Lookup lookup;
        private final Set<Symbol> correlation;

        public DecorrelatingVisitor(Lookup lookup, Iterable<Symbol> correlation)
        {
            this.lookup = requireNonNull(lookup, "lookup is null");
            this.correlation = ImmutableSet.copyOf(requireNonNull(correlation, "correlation is null"));
        }

        public Optional<Decorrelated> decorrelate(PlanNode reference)
        {
            return lookup.resolve(reference).accept(this, reference);
        }

        @Override
        public Optional<Decorrelated> visitProject(ProjectNode node, PlanNode reference)
        {
            if (isCorrelatedShallowly(node)) {
                // TODO: handle correlated projection
                return Optional.empty();
            }

            Optional<Decorrelated> result = decorrelate(node.getSource());
            return result.map(decorrelated -> {
                Assignments.Builder assignments = Assignments.builder()
                        .putAll(node.getAssignments());

                // Pull up all symbols used by a filter (except correlation)
                decorrelated.getCorrelatedPredicates().stream()
                        .flatMap(IrUtils::preOrder)
                        .filter(Reference.class::isInstance)
                        .map(Reference.class::cast)
                        .filter(symbolReference -> !correlation.contains(Symbol.from(symbolReference)))
                        .forEach(symbolReference -> assignments.putIdentity(Symbol.from(symbolReference)));

                return new Decorrelated(
                        decorrelated.getCorrelatedPredicates(),
                        new ProjectNode(
                                node.getId(),
                                decorrelated.getDecorrelatedNode(),
                                assignments.build()));
            });
        }

        @Override
        public Optional<Decorrelated> visitFilter(FilterNode node, PlanNode reference)
        {
            Optional<Decorrelated> result = decorrelate(node.getSource());
            return result.map(decorrelated ->
                    new Decorrelated(
                            ImmutableList.<Expression>builder()
                                    .addAll(decorrelated.getCorrelatedPredicates())
                                    // No need to retain uncorrelated conditions, predicate push down will push them back
                                    .add(node.getPredicate())
                                    .build(),
                            decorrelated.getDecorrelatedNode()));
        }

        @Override
        protected Optional<Decorrelated> visitPlan(PlanNode node, PlanNode reference)
        {
            if (isCorrelatedRecursively(node)) {
                return Optional.empty();
            }
            return Optional.of(new Decorrelated(ImmutableList.of(), reference));
        }

        private boolean isCorrelatedRecursively(PlanNode node)
        {
            if (isCorrelatedShallowly(node)) {
                return true;
            }
            return node.getSources().stream()
                    .map(lookup::resolve)
                    .anyMatch(this::isCorrelatedRecursively);
        }

        private boolean isCorrelatedShallowly(PlanNode node)
        {
            return SymbolsExtractor.extractUniqueNonRecursive(node).stream().anyMatch(correlation::contains);
        }
    }

    private static class Decorrelated
    {
        private final List<Expression> correlatedPredicates;
        private final PlanNode decorrelatedNode;

        public Decorrelated(List<Expression> correlatedPredicates, PlanNode decorrelatedNode)
        {
            this.correlatedPredicates = ImmutableList.copyOf(requireNonNull(correlatedPredicates, "correlatedPredicates is null"));
            this.decorrelatedNode = requireNonNull(decorrelatedNode, "decorrelatedNode is null");
        }

        public List<Expression> getCorrelatedPredicates()
        {
            return correlatedPredicates;
        }

        public PlanNode getDecorrelatedNode()
        {
            return decorrelatedNode;
        }
    }
}
