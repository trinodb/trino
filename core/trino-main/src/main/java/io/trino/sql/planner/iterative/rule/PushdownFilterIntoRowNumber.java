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
import io.trino.Session;
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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Expression;

import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Verify.verify;
import static io.trino.matching.Capture.newCapture;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.rowNumber;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PushdownFilterIntoRowNumber
        implements Rule<FilterNode>
{
    private static final Capture<RowNumberNode> CHILD = newCapture();
    private static final Pattern<FilterNode> PATTERN = filter().with(source().matching(rowNumber().capturedAs(CHILD)));

    private final PlannerContext plannerContext;

    public PushdownFilterIntoRowNumber(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(FilterNode node, Captures captures, Context context)
    {
        Session session = context.getSession();
        TypeProvider types = context.getSymbolAllocator().getTypes();

        DomainTranslator.ExtractionResult extractionResult = DomainTranslator.getExtractionResult(plannerContext, session, node.getPredicate(), types);
        TupleDomain<Symbol> tupleDomain = extractionResult.getTupleDomain();

        RowNumberNode source = captures.get(CHILD);
        Symbol rowNumberSymbol = source.getRowNumberSymbol();
        OptionalInt upperBound = extractUpperBound(tupleDomain, rowNumberSymbol);

        if (upperBound.isEmpty()) {
            return Result.empty();
        }

        if (upperBound.getAsInt() <= 0) {
            return Result.ofPlanNode(new ValuesNode(node.getId(), node.getOutputSymbols(), ImmutableList.of()));
        }

        RowNumberNode merged = mergeLimit(source, upperBound.getAsInt());
        boolean needRewriteSource = !merged.getMaxRowCountPerPartition().equals(source.getMaxRowCountPerPartition());
        if (needRewriteSource) {
            source = merged;
        }

        if (!allRowNumberValuesInDomain(tupleDomain, rowNumberSymbol, source.getMaxRowCountPerPartition().get())) {
            if (needRewriteSource) {
                return Result.ofPlanNode(new FilterNode(node.getId(), source, node.getPredicate()));
            }
            return Result.empty();
        }

        TupleDomain<Symbol> newTupleDomain = tupleDomain.filter((symbol, domain) -> !symbol.equals(rowNumberSymbol));
        Expression newPredicate = ExpressionUtils.combineConjuncts(
                plannerContext.getMetadata(),
                extractionResult.getRemainingExpression(),
                new DomainTranslator(plannerContext).toPredicate(session, newTupleDomain));

        if (newPredicate.equals(BooleanLiteral.TRUE_LITERAL)) {
            return Result.ofPlanNode(source);
        }

        if (!newPredicate.equals(node.getPredicate())) {
            return Result.ofPlanNode(new FilterNode(node.getId(), source, newPredicate));
        }

        return Result.empty();
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
        return domain.getValues().contains(ValueSet.ofRanges(range(domain.getType(), 1L, true, upperBound, true)));
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

        verify(rowNumberDomain.getType().equals(BIGINT));
        long upperBound = (Long) span.getHighBoundedValue();
        if (!span.isHighInclusive()) {
            upperBound--;
        }

        if (upperBound >= Integer.MIN_VALUE && upperBound <= Integer.MAX_VALUE) {
            return OptionalInt.of(toIntExact(upperBound));
        }
        return OptionalInt.empty();
    }

    private static RowNumberNode mergeLimit(RowNumberNode node, int newRowCountPerPartition)
    {
        if (node.getMaxRowCountPerPartition().isPresent()) {
            newRowCountPerPartition = Math.min(node.getMaxRowCountPerPartition().get(), newRowCountPerPartition);
        }
        return new RowNumberNode(
                node.getId(),
                node.getSource(),
                node.getPartitionBy(),
                node.isOrderSensitive(),
                node.getRowNumberSymbol(),
                Optional.of(newRowCountPerPartition),
                node.getHashSymbol());
    }
}
