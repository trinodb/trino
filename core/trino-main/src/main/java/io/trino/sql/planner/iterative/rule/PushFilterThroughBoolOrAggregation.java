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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.DomainTranslator.ExtractionResult;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.Rule.Context;
import io.trino.sql.planner.iterative.Rule.Result;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.ValuesNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.matching.Capture.newCapture;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.planner.DomainTranslator.getExtractionResult;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.partitioningBy;

/**
 * Push down aggregation's bool_or based on filter predicate.
 * <p>
 * This rule transforms plans with a FilterNode above an AggregationNode.
 * The AggregationNode must be grouped and contain a single aggregation
 * assignment with `bool_or()` function.
 * <p>
 * If the filter predicate is `false` for the aggregation's result value `false` or `null`,
 * then the aggregation can removed from the aggregation node, and
 * applied as a filter below the AggregationNode. After such transformation,
 * any group such that no rows of that group pass the filter, is removed
 * by the pushed down FilterNode, and so it is not processed by the
 * AggregationNode. Before the transformation, the group would be processed
 * by the AggregationNode, and return `false` or `null`, which would then be filtered out
 * by the root FilterNode.
 * <p>
 * After the symbol pushdown, it is checked whether the root FilterNode is
 * still needed, based on the fact that the aggregation never returns `false` or `null`.
 * <p>
 * Transforms:
 * <pre> {@code
 * - filter (aggr_bool AND predicate)
 *   - aggregation
 *     group by a
 *     aggr_bool <- bool_or(s)
 *       - source (a, s)
 *       }
 * </pre>
 * into:
 * <pre> {@code
 * - filter (predicate)
 *   - project (aggr_bool=true)
 *     - aggregation
 *       group by a
 *         - filter (s)
 *           - source (a, s)
 *       }
 * </pre>
 */
public class PushFilterThroughBoolOrAggregation
{
    private static final CatalogSchemaFunctionName BOOL_OR = builtinFunctionName("bool_or");

    private final PlannerContext plannerContext;

    public PushFilterThroughBoolOrAggregation(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                new PushFilterThroughBoolOrAggregationWithoutProject(plannerContext),
                new PushFilterThroughBoolOrAggregationWithProject(plannerContext));
    }

    @VisibleForTesting
    public static final class PushFilterThroughBoolOrAggregationWithoutProject
            implements Rule<FilterNode>
    {
        private static final Capture<AggregationNode> AGGREGATION = newCapture();

        private final PlannerContext plannerContext;
        private final Pattern<FilterNode> pattern;

        public PushFilterThroughBoolOrAggregationWithoutProject(PlannerContext plannerContext)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.pattern = filter()
                    .with(source().matching(aggregation()
                            .matching(PushFilterThroughBoolOrAggregation::isGroupedBoolOr)
                            .capturedAs(AGGREGATION)));
        }

        @Override
        public Pattern<FilterNode> getPattern()
        {
            return pattern;
        }

        @Override
        public Result apply(FilterNode node, Captures captures, Context context)
        {
            return pushFilter(node, captures.get(AGGREGATION), Optional.empty(), plannerContext, context);
        }
    }

    @VisibleForTesting
    public static final class PushFilterThroughBoolOrAggregationWithProject
            implements Rule<FilterNode>
    {
        private static final Capture<ProjectNode> PROJECT = newCapture();
        private static final Capture<AggregationNode> AGGREGATION = newCapture();

        private final PlannerContext plannerContext;
        private final Pattern<FilterNode> pattern;

        public PushFilterThroughBoolOrAggregationWithProject(PlannerContext plannerContext)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.pattern = filter()
                    .with(source().matching(project()
                            .matching(ProjectNode::isIdentity)
                            .capturedAs(PROJECT)
                            .with(source().matching(aggregation()
                                    .matching(PushFilterThroughBoolOrAggregation::isGroupedBoolOr)
                                    .capturedAs(AGGREGATION)))));
        }

        @Override
        public Pattern<FilterNode> getPattern()
        {
            return pattern;
        }

        @Override
        public Result apply(FilterNode node, Captures captures, Context context)
        {
            return pushFilter(node, captures.get(AGGREGATION), Optional.of(captures.get(PROJECT)), plannerContext, context);
        }
    }

    private static Result pushFilter(FilterNode filterNode, AggregationNode aggregationNode, Optional<ProjectNode> projectNode, PlannerContext plannerContext, Context context)
    {
        Symbol boolOrSymbol = getOnlyElement(aggregationNode.getAggregations().keySet());
        Aggregation aggregation = getOnlyElement(aggregationNode.getAggregations().values());

        ExtractionResult extractionResult = getExtractionResult(plannerContext, context.getSession(), filterNode.getPredicate());
        TupleDomain<Symbol> tupleDomain = extractionResult.getTupleDomain();
        Expression remainingExpression = extractionResult.getRemainingExpression();

        if (tupleDomain.isNone()) {
            // Filter predicate is never satisfied. Replace filter with empty values.
            return Result.ofPlanNode(new ValuesNode(filterNode.getId(), filterNode.getOutputSymbols()));
        }

        // boolOrSymbol (in remaining expressions) should only be used in Coalesce(boolOrSymbol, FALSE) expression
        List<Expression> conjuncts = extractConjuncts(remainingExpression);
        Map<Boolean, List<Expression>> expressions = conjuncts.stream()
                .filter(expression -> SymbolsExtractor.extractUnique(expression).contains(boolOrSymbol))
                .collect(partitioningBy(expression -> isSupportedCoalesce(expression, boolOrSymbol)));

        // unsupported expressions present
        if (!expressions.get(Boolean.FALSE).isEmpty()) {
            return Result.empty();
        }
        Optional<Expression> boolOrCoalesce = Optional.ofNullable(expressions.get(Boolean.TRUE)).filter(expr -> !expr.isEmpty()).map(List::getFirst);
        Optional<Domain> boolOrDomain = Optional.ofNullable(tupleDomain.getDomains().get().get(boolOrSymbol));
        if (boolOrDomain.isPresent() && !boolOrDomain.get().equals(singleValue(BOOLEAN, true))) {
            return Result.empty();
        }

        if (boolOrCoalesce.isEmpty() && boolOrDomain.isEmpty()) {
            return Result.empty();
        }

        // Push down the aggregation's symbol.
        FilterNode source = new FilterNode(
                context.getIdAllocator().getNextId(),
                aggregationNode.getSource(),
                aggregation.getArguments().getFirst());

        // Remove boolOr symbol from the aggregation.
        AggregationNode newAggregationNode = AggregationNode.builderFrom(aggregationNode)
                .setSource(source)
                .setAggregations(ImmutableMap.of())
                .build();

        // Add boolOr projection.
        ProjectNode newProjectNode = new ProjectNode(
                context.getIdAllocator().getNextId(),
                newAggregationNode,
                Assignments.builder()
                        .putIdentities(newAggregationNode.getOutputSymbols())
                        .put(boolOrSymbol, TRUE)
                        .build());

        // Restore identity projection if it is present in the original plan.
        PlanNode filterSource = projectNode.map(project -> project.replaceChildren(ImmutableList.of(newProjectNode))).orElse(newProjectNode);

        if (boolOrCoalesce.isPresent()) {
            remainingExpression = combineConjuncts(conjuncts.stream().filter(expression -> !expression.equals(boolOrCoalesce.get())).toList());
        }

        TupleDomain<Symbol> newTupleDomain = tupleDomain.filter((symbol, domain) -> !symbol.equals(boolOrSymbol));
        Expression newPredicate = combineConjuncts(
                new DomainTranslator(plannerContext.getMetadata()).toPredicate(newTupleDomain),
                remainingExpression);
        if (!newPredicate.equals(TRUE)) {
            return Result.ofPlanNode(new FilterNode(filterNode.getId(), filterSource, newPredicate));
        }

        return Result.ofPlanNode(filterSource);
    }

    private static boolean isSupportedCoalesce(Expression expression, Symbol boolOrSymbol)
    {
        if (!(expression instanceof Coalesce coalesce) || coalesce.operands().size() != 2) {
            return false;
        }
        Expression firstOperand = coalesce.operands().getFirst();
        Expression secondOperand = coalesce.operands().getLast();

        return firstOperand.equals(boolOrSymbol.toSymbolReference()) && secondOperand.equals(FALSE);
    }

    public static boolean isGroupedBoolOr(AggregationNode node)
    {
        if (!isGroupedAggregation(node)) {
            return false;
        }

        if (node.getAggregations().size() != 1) {
            return false;
        }

        Aggregation aggregation = getOnlyElement(node.getAggregations().values());
        if (aggregation.getFilter().isPresent() || aggregation.getMask().isPresent()) {
            return false;
        }

        return aggregation.getResolvedFunction().name().equals(BOOL_OR) && aggregation.getArguments().getFirst() instanceof Reference;
    }

    private static boolean isGroupedAggregation(AggregationNode node)
    {
        return node.hasNonEmptyGroupingSet() &&
               node.getGroupingSetCount() == 1 &&
               node.getStep() == SINGLE;
    }
}
