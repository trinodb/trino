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
import io.prestosql.sql.ExpressionUtils;
import io.prestosql.sql.planner.DomainTranslator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.TopNRowNumberNode;
import io.prestosql.sql.planner.plan.ValuesNode;
import io.prestosql.sql.planner.plan.WindowNode;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.QualifiedName;

import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.SystemSessionProperties.isOptimizeTopNRowNumber;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.spi.predicate.Marker.Bound.BELOW;
import static io.prestosql.spi.predicate.Range.range;
import static io.prestosql.sql.planner.DomainTranslator.fromPredicate;
import static io.prestosql.sql.planner.plan.Patterns.filter;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.window;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * This rule pushes filter predicate concerning row number symbol into WindowNode
 * by converting it into TopNRowNumberNode. It skips an identity projection
 * separating FilterNode from WindowNode in the plan tree.
 * TODO This rule should be removed as soon as WindowNode becomes capable of absorbing pruning projections (i.e. capable of pruning outputs).
 * <p>
 * Transforms:
 * <pre>
 * - Filter (rowNumber <= 5 && a > 1)
 *     - Project (a, rowNumber)
 *         - Window (row_number() OVER (ORDER BY a))
 *             - source (a, b)
 * </pre>
 * into:
 * <pre>
 * - Filter (a > 1)
 *     - Project (a, rowNumber)
 *         - TopNRowNumber (maxRowCountPerPartition = 5, order by a)
 *             - source (a, b)
 * </pre>
 */
public class PushPredicateThroughProjectIntoWindow
        implements Rule<FilterNode>
{
    private static final Capture<ProjectNode> PROJECT = newCapture();
    private static final Capture<WindowNode> WINDOW = newCapture();

    private final Pattern<FilterNode> pattern;
    private final Metadata metadata;

    public PushPredicateThroughProjectIntoWindow(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.pattern = filter()
                .with(source().matching(project()
                        .matching(ProjectNode::isIdentity)
                        .capturedAs(PROJECT)
                        .with(source().matching(window()
                                .matching(window -> {
                                    if (window.getOrderingScheme().isEmpty()) {
                                        return false;
                                    }
                                    if (window.getWindowFunctions().size() != 1) {
                                        return false;
                                    }
                                    FunctionId functionId = getOnlyElement(window.getWindowFunctions().values()).getResolvedFunction().getFunctionId();
                                    return functionId.equals(metadata.resolveFunction(QualifiedName.of("row_number"), ImmutableList.of()).getFunctionId());
                                })
                                .capturedAs(WINDOW)))));
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
    public Result apply(FilterNode filter, Captures captures, Context context)
    {
        ProjectNode project = captures.get(PROJECT);
        WindowNode window = captures.get(WINDOW);

        Symbol rowNumberSymbol = getOnlyElement(window.getWindowFunctions().keySet());
        if (!project.getAssignments().getSymbols().contains(rowNumberSymbol)) {
            return Result.empty();
        }

        DomainTranslator.ExtractionResult extractionResult = fromPredicate(metadata, context.getSession(), filter.getPredicate(), context.getSymbolAllocator().getTypes());
        TupleDomain<Symbol> tupleDomain = extractionResult.getTupleDomain();
        OptionalInt upperBound = extractUpperBound(tupleDomain, rowNumberSymbol);
        if (upperBound.isEmpty()) {
            return Result.empty();
        }
        if (upperBound.getAsInt() <= 0) {
            return Result.ofPlanNode(new ValuesNode(filter.getId(), filter.getOutputSymbols(), ImmutableList.of()));
        }
        project = (ProjectNode) project.replaceChildren(ImmutableList.of(new TopNRowNumberNode(
                window.getId(),
                window.getSource(),
                window.getSpecification(),
                rowNumberSymbol,
                upperBound.getAsInt(),
                false,
                Optional.empty())));
        if (!allRowNumberValuesInDomain(tupleDomain, rowNumberSymbol, upperBound.getAsInt())) {
            return Result.ofPlanNode(filter.replaceChildren(ImmutableList.of(project)));
        }
        // Remove the row number domain because it is absorbed into the node
        TupleDomain<Symbol> newTupleDomain = tupleDomain.filter((symbol, domain) -> !symbol.equals(rowNumberSymbol));
        Expression newPredicate = ExpressionUtils.combineConjuncts(
                metadata,
                extractionResult.getRemainingExpression(),
                new DomainTranslator(metadata).toPredicate(newTupleDomain));
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

        if (span.getHigh().isUpperUnbounded()) {
            return OptionalInt.empty();
        }

        long upperBound = (Long) span.getHigh().getValue();
        if (span.getHigh().getBound() == BELOW) {
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
