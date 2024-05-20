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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import io.trino.operator.join.SortedPositionLinks;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrUtils;
import io.trino.sql.ir.IrVisitor;
import io.trino.sql.ir.Reference;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static java.util.Collections.singletonList;
import static java.util.Comparator.comparing;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

/**
 * Extracts sort expression to be used for creating {@link SortedPositionLinks} from join filter expression.
 * Currently this class can extract sort and search expressions from filter function conjuncts of shape:
 * <p>
 * {@code A.a < f(B.x, B.y, B.z)} or {@code f(B.x, B.y, B.z) < A.a}
 * <p>
 * where {@code a} is the build side symbol reference and {@code x,y,z} are probe
 * side symbol references. Any of inequality operators ({@code <,<=,>,>=}) can be used.
 * Same build side symbol need to be used in all conjuncts.
 */
public final class SortExpressionExtractor
{
    /* TODO:
       This class could be extended to handle any expressions like:
       A.a * sin(A.b) / log(B.x) < cos(B.z)
       by transforming it to:
       f(A.a, A.b) < g(B.x, B.z)
       Where f(...) and g(...) would be some functions/expressions. That
       would allow us to perform binary search on arbitrary complex expressions
       by sorting position links according to the result of f(...) function.
     */
    private SortExpressionExtractor() {}

    public static Optional<SortExpressionContext> extractSortExpression(Set<Symbol> buildSymbols, Expression filter)
    {
        List<Expression> filterConjuncts = IrUtils.extractConjuncts(filter);
        SortExpressionVisitor visitor = new SortExpressionVisitor(buildSymbols);

        List<SortExpressionContext> sortExpressionCandidates = ImmutableList.copyOf(filterConjuncts.stream()
                .filter(DeterminismEvaluator::isDeterministic)
                .map(visitor::process)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toMap(SortExpressionContext::getSortExpression, identity(), SortExpressionExtractor::merge))
                .values());

        // For now heuristically pick sort expression which has most search expressions assigned to it.
        // TODO: make it cost based decision based on symbol statistics
        return sortExpressionCandidates.stream()
                .sorted(comparing(context -> -1 * context.getSearchExpressions().size()))
                .findFirst();
    }

    private static SortExpressionContext merge(SortExpressionContext left, SortExpressionContext right)
    {
        checkArgument(left.getSortExpression().equals(right.getSortExpression()));
        ImmutableList.Builder<Expression> searchExpressions = ImmutableList.builder();
        searchExpressions.addAll(left.getSearchExpressions());
        searchExpressions.addAll(right.getSearchExpressions());
        return new SortExpressionContext(left.getSortExpression(), searchExpressions.build());
    }

    private static class SortExpressionVisitor
            extends IrVisitor<Optional<SortExpressionContext>, Void>
    {
        private final Set<Symbol> buildSymbols;

        public SortExpressionVisitor(Set<Symbol> buildSymbols)
        {
            this.buildSymbols = buildSymbols;
        }

        @Override
        protected Optional<SortExpressionContext> visitExpression(Expression expression, Void context)
        {
            return Optional.empty();
        }

        @Override
        protected Optional<SortExpressionContext> visitComparison(Comparison comparison, Void context)
        {
            return switch (comparison.operator()) {
                case GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL -> {
                    Optional<Reference> sortChannel = asBuildSymbolReference(buildSymbols, comparison.right());
                    boolean hasBuildReferencesOnOtherSide = hasBuildSymbolReference(buildSymbols, comparison.left());
                    if (sortChannel.isEmpty()) {
                        sortChannel = asBuildSymbolReference(buildSymbols, comparison.left());
                        hasBuildReferencesOnOtherSide = hasBuildSymbolReference(buildSymbols, comparison.right());
                    }
                    if (sortChannel.isPresent() && !hasBuildReferencesOnOtherSide) {
                        yield sortChannel.map(symbolReference -> new SortExpressionContext(symbolReference, singletonList(comparison)));
                    }
                    yield Optional.empty();
                }
                default -> Optional.empty();
            };
        }

        @Override
        protected Optional<SortExpressionContext> visitBetween(Between node, Void context)
        {
            Optional<SortExpressionContext> result = visitComparison(new Comparison(GREATER_THAN_OR_EQUAL, node.value(), node.min()), context);
            if (result.isPresent()) {
                return result;
            }
            return visitComparison(new Comparison(LESS_THAN_OR_EQUAL, node.value(), node.max()), context);
        }
    }

    private static Optional<Reference> asBuildSymbolReference(Set<Symbol> buildLayout, Expression expression)
    {
        // Currently we only support symbol as sort expression on build side
        if (expression instanceof Reference reference) {
            if (buildLayout.contains(new Symbol(reference.type(), reference.name()))) {
                return Optional.of(reference);
            }
        }
        return Optional.empty();
    }

    private static boolean hasBuildSymbolReference(Set<Symbol> buildSymbols, Expression expression)
    {
        return new BuildSymbolReferenceFinder(buildSymbols).process(expression);
    }

    private static class BuildSymbolReferenceFinder
            extends IrVisitor<Boolean, Void>
    {
        private final Set<String> buildSymbols;

        public BuildSymbolReferenceFinder(Set<Symbol> buildSymbols)
        {
            this.buildSymbols = buildSymbols.stream()
                    .map(Symbol::name)
                    .collect(toImmutableSet());
        }

        @Override
        protected Boolean visitExpression(Expression node, Void context)
        {
            for (Expression child : node.children()) {
                if (process(child, context)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        protected Boolean visitReference(Reference reference, Void context)
        {
            return buildSymbols.contains(reference.name());
        }
    }
}
