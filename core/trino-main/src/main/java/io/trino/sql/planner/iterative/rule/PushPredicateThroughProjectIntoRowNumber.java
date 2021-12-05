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
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.sql.ExpressionUtils;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.DomainTranslator.ExtractionResult;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.Expression;

import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.matching.Capture.newCapture;
import static io.trino.spi.predicate.Range.range;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.rowNumber;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * This rule pushes filter predicate concerning row number symbol into RowNumberNode
 * by modifying maxRowCountPerPartition. It skips an identity projection
 * separating FilterNode from RowNumberNode in the plan tree.
 * TODO This rule should be removed as soon as RowNumberNode becomes capable of absorbing pruning projections (i.e. capable of pruning outputs).
 * <p>
 * Transforms:
 * <pre>
 * - Filter (rowNumber <= 5 && a > 1)
 *     - Project (a, rowNumber)
 *         - RowNumber (maxRowCountPerPartition = 10)
 *             - source (a, b)
 * </pre>
 * into:
 * <pre>
 * - Filter (a > 1)
 *     - Project (a, rowNumber)
 *         - RowNumber (maxRowCountPerPartition = 5)
 *             - source (a, b)
 * </pre>
 */
public class PushPredicateThroughProjectIntoRowNumber
        implements Rule<FilterNode>
{
    private static final Capture<ProjectNode> PROJECT = newCapture();
    private static final Capture<RowNumberNode> ROW_NUMBER = newCapture();

    private static final Pattern<FilterNode> PATTERN = filter()
            .with(source().matching(project()
                    .matching(ProjectNode::isIdentity)
                    .capturedAs(PROJECT)
                    .with(source().matching(rowNumber()
                            .capturedAs(ROW_NUMBER)))));

    private final PlannerContext plannerContext;

    public PushPredicateThroughProjectIntoRowNumber(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(FilterNode filter, Captures captures, Context context)
    {
        ProjectNode project = captures.get(PROJECT);
        RowNumberNode rowNumber = captures.get(ROW_NUMBER);

        Symbol rowNumberSymbol = rowNumber.getRowNumberSymbol();
        if (!project.getAssignments().getSymbols().contains(rowNumberSymbol)) {
            return Result.empty();
        }

        ExtractionResult extractionResult = DomainTranslator.getExtractionResult(
                plannerContext,
                context.getSession(),
                filter.getPredicate(),
                context.getSymbolAllocator().getTypes());
        TupleDomain<Symbol> tupleDomain = extractionResult.getTupleDomain();
        OptionalInt upperBound = extractUpperBound(tupleDomain, rowNumberSymbol);
        if (upperBound.isEmpty()) {
            return Result.empty();
        }
        if (upperBound.getAsInt() <= 0) {
            return Result.ofPlanNode(new ValuesNode(filter.getId(), filter.getOutputSymbols(), ImmutableList.of()));
        }
        boolean updatedMaxRowCountPerPartition = false;
        if (rowNumber.getMaxRowCountPerPartition().isEmpty() || rowNumber.getMaxRowCountPerPartition().get() > upperBound.getAsInt()) {
            rowNumber = new RowNumberNode(
                    rowNumber.getId(),
                    rowNumber.getSource(),
                    rowNumber.getPartitionBy(),
                    rowNumber.isOrderSensitive(),
                    rowNumber.getRowNumberSymbol(),
                    Optional.of(upperBound.getAsInt()),
                    rowNumber.getHashSymbol());
            project = (ProjectNode) project.replaceChildren(ImmutableList.of(rowNumber));
            updatedMaxRowCountPerPartition = true;
        }
        if (!allRowNumberValuesInDomain(tupleDomain, rowNumberSymbol, rowNumber.getMaxRowCountPerPartition().get())) {
            if (updatedMaxRowCountPerPartition) {
                return Result.ofPlanNode(filter.replaceChildren(ImmutableList.of(project)));
            }
            return Result.empty();
        }
        // Remove the row number domain because it is absorbed into the node
        TupleDomain<Symbol> newTupleDomain = tupleDomain.filter((symbol, domain) -> !symbol.equals(rowNumberSymbol));
        Expression newPredicate = ExpressionUtils.combineConjuncts(
                plannerContext.getMetadata(),
                extractionResult.getRemainingExpression(),
                new DomainTranslator(plannerContext).toPredicate(context.getSession(), newTupleDomain));
        if (newPredicate.equals(TRUE_LITERAL)) {
            return Result.ofPlanNode(project);
        }
        return Result.ofPlanNode(new FilterNode(filter.getId(), project, newPredicate));
    }

    private static OptionalInt extractUpperBound(TupleDomain<Symbol> tupleDomain, Symbol symbol)
    {
        if (tupleDomain.isNone()) {
            return OptionalInt.empty();
        }

        Domain rowNumberDomain = tupleDomain.getDomains().get().get(symbol);
        if (rowNumberDomain == null) {
            return OptionalInt.empty();
        }
        ValueSet values = rowNumberDomain.getValues();
        if (values.isAll() || values.isNone() || values.getRanges().getRangeCount() <= 0) {
            return OptionalInt.empty();
        }

        Range span = values.getRanges().getSpan();

        if (span.isHighUnbounded()) {
            return OptionalInt.empty();
        }

        long upperBound = (Long) span.getHighBoundedValue();
        if (!span.isHighInclusive()) {
            upperBound--;
        }

        if (upperBound >= Integer.MIN_VALUE && upperBound <= Integer.MAX_VALUE) {
            return OptionalInt.of(toIntExact(upperBound));
        }
        return OptionalInt.empty();
    }

    private static boolean allRowNumberValuesInDomain(TupleDomain<Symbol> tupleDomain, Symbol symbol, long upperBound)
    {
        if (tupleDomain.isNone()) {
            return false;
        }
        Domain domain = tupleDomain.getDomains().get().get(symbol);
        if (domain == null) {
            return true;
        }
        return domain.getValues().contains(ValueSet.ofRanges(range(domain.getType(), 0L, true, upperBound, true)));
    }
}
