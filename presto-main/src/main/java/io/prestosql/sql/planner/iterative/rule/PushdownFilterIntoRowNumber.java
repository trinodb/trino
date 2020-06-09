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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.type.TypeOperators;
import io.prestosql.sql.ExpressionUtils;
import io.prestosql.sql.planner.DomainTranslator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.RowNumberNode;
import io.prestosql.sql.planner.plan.ValuesNode;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Expression;

import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Verify.verify;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.spi.predicate.Marker.Bound.BELOW;
import static io.prestosql.spi.predicate.Range.range;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.planner.DomainTranslator.fromPredicate;
import static io.prestosql.sql.planner.plan.Patterns.filter;
import static io.prestosql.sql.planner.plan.Patterns.rowNumber;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static java.lang.Math.toIntExact;

public class PushdownFilterIntoRowNumber
        implements Rule<FilterNode>
{
    private static final Capture<RowNumberNode> CHILD = newCapture();
    private static final Pattern<FilterNode> PATTERN = filter().with(source().matching(rowNumber().capturedAs(CHILD)));
    private final Metadata metadata;
    private final DomainTranslator domainTranslator;
    private final TypeOperators typeOperators;

    public PushdownFilterIntoRowNumber(Metadata metadata, TypeOperators typeOperators)
    {
        this.metadata = metadata;
        this.domainTranslator = new DomainTranslator(metadata);
        this.typeOperators = typeOperators;
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

        DomainTranslator.ExtractionResult extractionResult = fromPredicate(metadata, typeOperators, session, node.getPredicate(), types);
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

        boolean needRewriteSource = true;
        if (source.getMaxRowCountPerPartition().isPresent()) {
            int newRowCountPerPartition = Math.min(source.getMaxRowCountPerPartition().get(), upperBound.getAsInt());
            // No need to rewrite only source node if rowCountPerPartition results in identical.
            needRewriteSource = source.getMaxRowCountPerPartition().get() != newRowCountPerPartition;
            if (needRewriteSource) {
                source = new RowNumberNode(
                        source.getId(),
                        source.getSource(),
                        source.getPartitionBy(),
                        source.isOrderSensitive(),
                        source.getRowNumberSymbol(),
                        Optional.of(newRowCountPerPartition),
                        source.getHashSymbol());
            }
        }
        else {
            // Pushdown the upper bound of the filter
            source = new RowNumberNode(
                    source.getId(),
                    source.getSource(),
                    source.getPartitionBy(),
                    source.isOrderSensitive(),
                    source.getRowNumberSymbol(),
                    Optional.of(upperBound.getAsInt()),
                    source.getHashSymbol());
        }

        if (!allRowNumberValuesInDomain(tupleDomain, rowNumberSymbol, source.getMaxRowCountPerPartition().get())) {
            if (needRewriteSource) {
                return Result.ofPlanNode(new FilterNode(node.getId(), source, node.getPredicate()));
            }
            else {
                return Result.empty();
            }
        }

        TupleDomain<Symbol> newTupleDomain = tupleDomain.filter((symbol, domain) -> !symbol.equals(rowNumberSymbol));
        Expression newPredicate = ExpressionUtils.combineConjuncts(
                metadata,
                extractionResult.getRemainingExpression(),
                domainTranslator.toPredicate(newTupleDomain));

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

        if (span.getHigh().isUpperUnbounded()) {
            return OptionalInt.empty();
        }

        verify(rowNumberDomain.getType().equals(BIGINT));
        long upperBound = (Long) span.getHigh().getValue();
        if (span.getHigh().getBound() == BELOW) {
            upperBound--;
        }

        if (upperBound >= Integer.MIN_VALUE && upperBound <= Integer.MAX_VALUE) {
            return OptionalInt.of(toIntExact(upperBound));
        }
        return OptionalInt.empty();
    }
}
