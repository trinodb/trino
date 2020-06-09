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
import io.prestosql.metadata.FunctionId;
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
import io.prestosql.sql.planner.plan.TopNRowNumberNode;
import io.prestosql.sql.planner.plan.ValuesNode;
import io.prestosql.sql.planner.plan.WindowNode;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.QualifiedName;

import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.SystemSessionProperties.isOptimizeTopNRowNumber;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.spi.predicate.Marker.Bound.BELOW;
import static io.prestosql.spi.predicate.Range.range;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.planner.DomainTranslator.fromPredicate;
import static io.prestosql.sql.planner.plan.Patterns.filter;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.window;
import static java.lang.Math.toIntExact;

public class PushdownFilterIntoWindow
        implements Rule<FilterNode>
{
    private final Capture<WindowNode> childCapture;
    private final Pattern<FilterNode> pattern;

    private final Metadata metadata;
    private final DomainTranslator domainTranslator;
    private final FunctionId rowNumberFunctionId;
    private final TypeOperators typeOperators;

    public PushdownFilterIntoWindow(Metadata metadata, TypeOperators typeOperators)
    {
        this.metadata = metadata;
        this.domainTranslator = new DomainTranslator(metadata);
        this.rowNumberFunctionId = metadata.resolveFunction(QualifiedName.of("row_number"), ImmutableList.of()).getFunctionId();
        this.childCapture = newCapture();
        this.pattern = filter()
                .with(source().matching(window()
                        .matching(window -> window.getWindowFunctions().size() == 1 && getOnlyElement(window.getWindowFunctions().values()).getResolvedFunction().getFunctionId().equals(rowNumberFunctionId))
                        .capturedAs(childCapture)));
        this.typeOperators = typeOperators;
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return pattern;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isOptimizeTopNRowNumber(session);
    }

    @Override
    public Result apply(FilterNode node, Captures captures, Context context)
    {
        Session session = context.getSession();
        TypeProvider types = context.getSymbolAllocator().getTypes();

        WindowNode windowNode = captures.get(childCapture);

        DomainTranslator.ExtractionResult extractionResult = fromPredicate(metadata, typeOperators, session, node.getPredicate(), types);
        TupleDomain<Symbol> tupleDomain = extractionResult.getTupleDomain();

        Symbol rowNumberSymbol = getOnlyElement(windowNode.getWindowFunctions().entrySet()).getKey();
        OptionalInt upperBound = extractUpperBound(tupleDomain, rowNumberSymbol);

        if (upperBound.isPresent()) {
            if (upperBound.getAsInt() <= 0) {
                return Result.ofPlanNode(new ValuesNode(node.getId(), node.getOutputSymbols(), ImmutableList.of()));
            }
            if (windowNode.getOrderingScheme().isPresent()) {
                TopNRowNumberNode newSource = new TopNRowNumberNode(
                        windowNode.getId(),
                        windowNode.getSource(),
                        windowNode.getSpecification(),
                        getOnlyElement(windowNode.getWindowFunctions().keySet()),
                        upperBound.getAsInt(),
                        false,
                        Optional.empty());

                if (!allRowNumberValuesInDomain(tupleDomain, rowNumberSymbol, upperBound.getAsInt())) {
                    return Result.ofPlanNode(new FilterNode(node.getId(), newSource, node.getPredicate()));
                }

                // Remove the row number domain because it is absorbed into the node
                TupleDomain<Symbol> newTupleDomain = tupleDomain.filter((symbol, domain) -> !symbol.equals(rowNumberSymbol));
                Expression newPredicate = ExpressionUtils.combineConjuncts(
                        metadata,
                        extractionResult.getRemainingExpression(),
                        domainTranslator.toPredicate(newTupleDomain));

                if (newPredicate.equals(BooleanLiteral.TRUE_LITERAL)) {
                    return Result.ofPlanNode(newSource);
                }
                return Result.ofPlanNode(new FilterNode(node.getId(), newSource, newPredicate));
            }
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
