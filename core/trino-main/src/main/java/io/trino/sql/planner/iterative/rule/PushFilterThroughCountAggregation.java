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
import io.trino.spi.function.BoundSignature;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.iterative.Rule.Context;
import io.trino.sql.planner.iterative.Rule.Result;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.Expression;

import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.planner.DomainTranslator.getExtractionResult;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.Objects.requireNonNull;

/**
 * Push down aggregation's mask based on filter predicate.
 * <p>
 * This rule transforms plans with a FilterNode above an AggregationNode.
 * The AggregationNode must be grouped and contain a single aggregation
 * assignment with `count()` function and a mask.
 * <p>
 * If the filter predicate is `false` for the aggregation's result value `0`,
 * then the aggregation's mask can removed from the aggregation, and
 * applied as a filter below the AggregationNode. After such transformation,
 * any group such that no rows of that group pass the filter, is removed
 * by the pushed down FilterNode, and so it is not processed by the
 * AggregationNode. Before the transformation, the group would be processed
 * by the AggregationNode, and return `0`, which would then be filtered out
 * by the root FilterNode.
 * <p>
 * After the mask pushdown, it is checked whether the root FilterNode is
 * still needed, based on the fact that the aggregation never returns `0`.
 * <p>
 * Transforms:
 * <pre> {@code
 * - filter (count > 0 AND predicate)
 *     - aggregation
 *       group by a
 *       count <- count() mask: m
 *         - source (a, m)
 *       }
 * </pre>
 * into:
 * <pre> {@code
 * - filter (predicate)
 *     - aggregation
 *       group by a
 *       count <- count()
 *         - filter (m)
 *             - source (a, m)
 *       }
 * </pre>
 */
public class PushFilterThroughCountAggregation
{
    private final PlannerContext plannerContext;

    public PushFilterThroughCountAggregation(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                new PushFilterThroughCountAggregationWithoutProject(plannerContext),
                new PushFilterThroughCountAggregationWithProject(plannerContext));
    }

    @VisibleForTesting
    public static final class PushFilterThroughCountAggregationWithoutProject
            implements Rule<FilterNode>
    {
        private static final Capture<AggregationNode> AGGREGATION = newCapture();

        private final PlannerContext plannerContext;
        private final Pattern<FilterNode> pattern;

        public PushFilterThroughCountAggregationWithoutProject(PlannerContext plannerContext)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.pattern = filter()
                    .with(source().matching(aggregation()
                            .matching(PushFilterThroughCountAggregation::isGroupedCountWithMask)
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
    public static final class PushFilterThroughCountAggregationWithProject
            implements Rule<FilterNode>
    {
        private static final Capture<ProjectNode> PROJECT = newCapture();
        private static final Capture<AggregationNode> AGGREGATION = newCapture();

        private final PlannerContext plannerContext;
        private final Pattern<FilterNode> pattern;

        public PushFilterThroughCountAggregationWithProject(PlannerContext plannerContext)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.pattern = filter()
                    .with(source().matching(project()
                            .matching(ProjectNode::isIdentity)
                            .capturedAs(PROJECT)
                            .with(source().matching(aggregation()
                                    .matching(PushFilterThroughCountAggregation::isGroupedCountWithMask)
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
        Symbol countSymbol = getOnlyElement(aggregationNode.getAggregations().keySet());
        Aggregation aggregation = getOnlyElement(aggregationNode.getAggregations().values());

        DomainTranslator.ExtractionResult extractionResult = getExtractionResult(plannerContext, context.getSession(), filterNode.getPredicate(), context.getSymbolAllocator().getTypes());
        TupleDomain<Symbol> tupleDomain = extractionResult.getTupleDomain();

        if (tupleDomain.isNone()) {
            // Filter predicate is never satisfied. Replace filter with empty values.
            return Result.ofPlanNode(new ValuesNode(filterNode.getId(), filterNode.getOutputSymbols(), ImmutableList.of()));
        }
        Domain countDomain = tupleDomain.getDomains().get().get(countSymbol);
        if (countDomain == null) {
            // Filter predicate domain contains all countSymbol values. Cannot filter out `0`.
            return Result.empty();
        }
        if (countDomain.contains(Domain.singleValue(countDomain.getType(), 0L))) {
            // Filter predicate domain contains `0`. Cannot filter out `0`.
            return Result.empty();
        }

        // Push down the aggregation's mask.
        FilterNode source = new FilterNode(
                context.getIdAllocator().getNextId(),
                aggregationNode.getSource(),
                aggregation.getMask().get().toSymbolReference());

        // Remove mask from the aggregation.
        Aggregation newAggregation = new Aggregation(
                aggregation.getResolvedFunction(),
                aggregation.getArguments(),
                aggregation.isDistinct(),
                aggregation.getFilter(),
                aggregation.getOrderingScheme(),
                Optional.empty());

        AggregationNode newAggregationNode = AggregationNode.builderFrom(aggregationNode)
                .setSource(source)
                .setAggregations(ImmutableMap.of(countSymbol, newAggregation))
                .build();

        // Restore identity projection if it is present in the original plan.
        PlanNode filterSource = projectNode.map(project -> project.replaceChildren(ImmutableList.of(newAggregationNode))).orElse(newAggregationNode);

        // Try to simplify filter above the aggregation.
        if (countDomain.getValues().contains(ValueSet.ofRanges(Range.greaterThanOrEqual(countDomain.getType(), 1L)))) {
            // After filtering out `0` values, filter predicate's domain contains all remaining countSymbol values. Remove the countSymbol domain.
            TupleDomain<Symbol> newTupleDomain = tupleDomain.filter((symbol, domain) -> !symbol.equals(countSymbol));
            Expression newPredicate = combineConjuncts(
                    plannerContext.getMetadata(),
                    new DomainTranslator(plannerContext).toPredicate(context.getSession(), newTupleDomain),
                    extractionResult.getRemainingExpression());
            if (newPredicate.equals(TRUE_LITERAL)) {
                return Result.ofPlanNode(filterSource);
            }
            return Result.ofPlanNode(new FilterNode(filterNode.getId(), filterSource, newPredicate));
        }

        // Filter predicate cannot be simplified.
        return Result.ofPlanNode(filterNode.replaceChildren(ImmutableList.of(filterSource)));
    }

    private static boolean isGroupedCountWithMask(AggregationNode aggregationNode)
    {
        if (!isGroupedAggregation(aggregationNode)) {
            return false;
        }
        if (aggregationNode.getAggregations().size() != 1) {
            return false;
        }
        Aggregation aggregation = getOnlyElement(aggregationNode.getAggregations().values());

        if (aggregation.getMask().isEmpty() || aggregation.getFilter().isPresent()) {
            return false;
        }

        BoundSignature signature = aggregation.getResolvedFunction().getSignature();
        return signature.getArgumentTypes().isEmpty() && signature.getName().equals("count");
    }

    private static boolean isGroupedAggregation(AggregationNode aggregationNode)
    {
        return aggregationNode.hasNonEmptyGroupingSet() &&
                aggregationNode.getGroupingSetCount() == 1 &&
                aggregationNode.getStep() == SINGLE;
    }
}
