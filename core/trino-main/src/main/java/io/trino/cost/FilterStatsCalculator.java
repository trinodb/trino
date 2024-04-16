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
package io.trino.cost;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Booleans;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IrVisitor;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Not;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.IrExpressionInterpreter;
import io.trino.sql.planner.Symbol;
import io.trino.util.DisjointSet;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.getFilterConjunctionIndependenceFactor;
import static io.trino.cost.ComparisonStatsCalculator.estimateExpressionToExpressionComparison;
import static io.trino.cost.ComparisonStatsCalculator.estimateExpressionToLiteralComparison;
import static io.trino.cost.PlanNodeStatsEstimateMath.addStatsAndSumDistinctValues;
import static io.trino.cost.PlanNodeStatsEstimateMath.capStats;
import static io.trino.cost.PlanNodeStatsEstimateMath.estimateCorrelatedConjunctionRowCount;
import static io.trino.cost.PlanNodeStatsEstimateMath.intersectCorrelatedStats;
import static io.trino.cost.PlanNodeStatsEstimateMath.subtractSubsetStats;
import static io.trino.spi.statistics.StatsUtil.toStatsRepresentation;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.DynamicFilters.isDynamicFilter;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.IrUtils.and;
import static io.trino.sql.planner.SymbolsExtractor.extractUnique;
import static java.lang.Double.NaN;
import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;
import static java.lang.Double.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FilterStatsCalculator
{
    static final double UNKNOWN_FILTER_COEFFICIENT = 0.9;

    private final PlannerContext plannerContext;
    private final ScalarStatsCalculator scalarStatsCalculator;
    private final StatsNormalizer normalizer;

    @Inject
    public FilterStatsCalculator(PlannerContext plannerContext, ScalarStatsCalculator scalarStatsCalculator, StatsNormalizer normalizer)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.scalarStatsCalculator = requireNonNull(scalarStatsCalculator, "scalarStatsCalculator is null");
        this.normalizer = requireNonNull(normalizer, "normalizer is null");
    }

    public PlanNodeStatsEstimate filterStats(
            PlanNodeStatsEstimate statsEstimate,
            Expression predicate,
            Session session)
    {
        Expression simplifiedExpression = simplifyExpression(session, predicate);
        return new FilterExpressionStatsCalculatingVisitor(statsEstimate, session)
                .process(simplifiedExpression);
    }

    private Expression simplifyExpression(Session session, Expression predicate)
    {
        // TODO reuse io.trino.sql.planner.iterative.rule.SimplifyExpressions.rewrite
        Expression value = new IrExpressionInterpreter(predicate, plannerContext, session).optimize();

        if (value instanceof Constant constant && constant.value() == null) {
            // Expression evaluates to SQL null, which in Filter is equivalent to false. This assumes the expression is a top-level expression (eg. not in NOT).
            value = Booleans.FALSE;
        }

        return value;
    }

    private class FilterExpressionStatsCalculatingVisitor
            extends IrVisitor<PlanNodeStatsEstimate, Void>
    {
        private final PlanNodeStatsEstimate input;
        private final Session session;

        FilterExpressionStatsCalculatingVisitor(PlanNodeStatsEstimate input, Session session)
        {
            this.input = input;
            this.session = session;
        }

        @Override
        public PlanNodeStatsEstimate process(Expression node, @Nullable Void context)
        {
            PlanNodeStatsEstimate output;
            if (input.getOutputRowCount() == 0 || input.isOutputRowCountUnknown()) {
                output = input;
            }
            else {
                output = super.process(node, context);
            }
            return normalizer.normalize(output);
        }

        @Override
        protected PlanNodeStatsEstimate visitExpression(Expression node, Void context)
        {
            return PlanNodeStatsEstimate.unknown();
        }

        @Override
        protected PlanNodeStatsEstimate visitNot(Not node, Void context)
        {
            if (node.value() instanceof IsNull inner) {
                if (inner.value() instanceof Reference) {
                    Symbol symbol = Symbol.from(inner.value());
                    SymbolStatsEstimate symbolStats = input.getSymbolStatistics(symbol);
                    PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(input);
                    result.setOutputRowCount(input.getOutputRowCount() * (1 - symbolStats.getNullsFraction()));
                    result.addSymbolStatistics(symbol, symbolStats.mapNullsFraction(x -> 0.0));
                    return result.build();
                }
                return PlanNodeStatsEstimate.unknown();
            }
            return subtractSubsetStats(input, process(node.value()));
        }

        @Override
        protected PlanNodeStatsEstimate visitLogical(Logical node, Void context)
        {
            return switch (node.operator()) {
                case AND -> estimateLogicalAnd(node.terms());
                case OR -> estimateLogicalOr(node.terms());
            };
        }

        private PlanNodeStatsEstimate estimateLogicalAnd(List<Expression> terms)
        {
            double filterConjunctionIndependenceFactor = getFilterConjunctionIndependenceFactor(session);
            List<PlanNodeStatsEstimate> estimates = estimateCorrelatedExpressions(terms, filterConjunctionIndependenceFactor);
            double outputRowCount = estimateCorrelatedConjunctionRowCount(
                    input,
                    estimates,
                    filterConjunctionIndependenceFactor);
            if (isNaN(outputRowCount)) {
                return PlanNodeStatsEstimate.unknown();
            }
            return normalizer.normalize(new PlanNodeStatsEstimate(outputRowCount, intersectCorrelatedStats(estimates)));
        }

        /**
         * There can be multiple predicate expressions for the same symbol, e.g. x > 0 AND x <= 1, x BETWEEN 1 AND 10.
         * We attempt to detect such cases in extractCorrelatedGroups and calculate a combined estimate for each
         * such group of expressions. This is done so that we don't apply the above scaling factors when combining estimates
         * from conjunction of multiple predicates on the same symbol and underestimate the output.
         **/
        private List<PlanNodeStatsEstimate> estimateCorrelatedExpressions(List<Expression> terms, double filterConjunctionIndependenceFactor)
        {
            List<List<Expression>> extractedCorrelatedGroups = extractCorrelatedGroups(terms, filterConjunctionIndependenceFactor);
            ImmutableList.Builder<PlanNodeStatsEstimate> estimatesBuilder = ImmutableList.builderWithExpectedSize(extractedCorrelatedGroups.size());
            boolean hasUnestimatedTerm = false;
            for (List<Expression> correlatedExpressions : extractedCorrelatedGroups) {
                PlanNodeStatsEstimate combinedEstimate = PlanNodeStatsEstimate.unknown();
                for (Expression expression : correlatedExpressions) {
                    PlanNodeStatsEstimate estimate;
                    // combinedEstimate is unknown until the 1st known estimated term
                    if (combinedEstimate.isOutputRowCountUnknown()) {
                        estimate = process(expression);
                    }
                    else {
                        estimate = new FilterExpressionStatsCalculatingVisitor(combinedEstimate, session)
                                .process(expression);
                    }

                    if (estimate.isOutputRowCountUnknown()) {
                        hasUnestimatedTerm = true;
                    }
                    else {
                        // update combinedEstimate only when the term estimate is known so that all the known estimates
                        // can be applied progressively through FilterExpressionStatsCalculatingVisitor calls.
                        combinedEstimate = estimate;
                    }
                }
                estimatesBuilder.add(combinedEstimate);
            }
            if (hasUnestimatedTerm) {
                estimatesBuilder.add(PlanNodeStatsEstimate.unknown());
            }
            return estimatesBuilder.build();
        }

        private PlanNodeStatsEstimate estimateLogicalOr(List<Expression> terms)
        {
            PlanNodeStatsEstimate previous = process(terms.get(0));
            if (previous.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            for (int i = 1; i < terms.size(); i++) {
                PlanNodeStatsEstimate current = process(terms.get(i));
                if (current.isOutputRowCountUnknown()) {
                    return PlanNodeStatsEstimate.unknown();
                }

                PlanNodeStatsEstimate andEstimate = new FilterExpressionStatsCalculatingVisitor(previous, session).process(terms.get(i));
                if (andEstimate.isOutputRowCountUnknown()) {
                    return PlanNodeStatsEstimate.unknown();
                }

                previous = capStats(
                        subtractSubsetStats(
                                addStatsAndSumDistinctValues(previous, current),
                                andEstimate),
                        input);
            }

            return previous;
        }

        @Override
        protected PlanNodeStatsEstimate visitConstant(Constant node, Void context)
        {
            if (node.type().equals(BOOLEAN) && node.value() != null) {
                if ((boolean) node.value()) {
                    return input;
                }

                PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
                result.setOutputRowCount(0.0);
                input.getSymbolsWithKnownStatistics().forEach(symbol -> result.addSymbolStatistics(symbol, SymbolStatsEstimate.zero()));
                return result.build();
            }

            return super.visitConstant(node, context);
        }

        @Override
        protected PlanNodeStatsEstimate visitIsNull(IsNull node, Void context)
        {
            if (node.value() instanceof Reference) {
                Symbol symbol = Symbol.from(node.value());
                SymbolStatsEstimate symbolStats = input.getSymbolStatistics(symbol);
                PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(input);
                result.setOutputRowCount(input.getOutputRowCount() * symbolStats.getNullsFraction());
                result.addSymbolStatistics(symbol, SymbolStatsEstimate.builder()
                        .setNullsFraction(1.0)
                        .setLowValue(NaN)
                        .setHighValue(NaN)
                        .setDistinctValuesCount(0.0)
                        .build());
                return result.build();
            }
            return PlanNodeStatsEstimate.unknown();
        }

        @Override
        protected PlanNodeStatsEstimate visitBetween(Between node, Void context)
        {
            SymbolStatsEstimate valueStats = getExpressionStats(node.value());
            if (valueStats.isUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }
            if (!getExpressionStats(node.min()).isSingleValue()) {
                return PlanNodeStatsEstimate.unknown();
            }
            if (!getExpressionStats(node.max()).isSingleValue()) {
                return PlanNodeStatsEstimate.unknown();
            }

            Expression lowerBound = new Comparison(GREATER_THAN_OR_EQUAL, node.value(), node.min());
            Expression upperBound = new Comparison(LESS_THAN_OR_EQUAL, node.value(), node.max());

            Expression transformed;
            if (isInfinite(valueStats.getLowValue())) {
                // We want to do heuristic cut (infinite range to finite range) ASAP and then do filtering on finite range.
                // We rely on 'and()' being processed left to right
                transformed = and(lowerBound, upperBound);
            }
            else {
                transformed = and(upperBound, lowerBound);
            }
            return process(transformed);
        }

        @Override
        protected PlanNodeStatsEstimate visitIn(In node, Void context)
        {
            ImmutableList<PlanNodeStatsEstimate> equalityEstimates = node.valueList().stream()
                    .map(inValue -> process(new Comparison(EQUAL, node.value(), inValue)))
                    .collect(toImmutableList());

            if (equalityEstimates.stream().anyMatch(PlanNodeStatsEstimate::isOutputRowCountUnknown)) {
                return PlanNodeStatsEstimate.unknown();
            }

            PlanNodeStatsEstimate inEstimate = equalityEstimates.stream()
                    .reduce(PlanNodeStatsEstimateMath::addStatsAndSumDistinctValues)
                    .orElse(PlanNodeStatsEstimate.unknown());

            if (inEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            SymbolStatsEstimate valueStats = getExpressionStats(node.value());
            if (valueStats.isUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            double notNullValuesBeforeIn = input.getOutputRowCount() * (1 - valueStats.getNullsFraction());

            PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(input);
            result.setOutputRowCount(min(inEstimate.getOutputRowCount(), notNullValuesBeforeIn));

            if (node.value() instanceof Reference) {
                Symbol valueSymbol = Symbol.from(node.value());
                SymbolStatsEstimate newSymbolStats = inEstimate.getSymbolStatistics(valueSymbol)
                        .mapDistinctValuesCount(newDistinctValuesCount -> min(newDistinctValuesCount, valueStats.getDistinctValuesCount()));
                result.addSymbolStatistics(valueSymbol, newSymbolStats);
            }
            return result.build();
        }

        @SuppressWarnings("ArgumentSelectionDefectChecker")
        @Override
        protected PlanNodeStatsEstimate visitComparison(Comparison node, Void context)
        {
            Comparison.Operator operator = node.operator();
            Expression left = node.left();
            Expression right = node.right();

            checkArgument(!(left instanceof Constant && right instanceof Constant), "Literal-to-literal not supported here, should be eliminated earlier");

            if (!(left instanceof Reference) && right instanceof Reference) {
                // normalize so that symbol is on the left
                return process(new Comparison(operator.flip(), right, left));
            }

            if (left instanceof Constant) {
                // normalize so that literal is on the right
                return process(new Comparison(operator.flip(), right, left));
            }

            if (left instanceof Reference && left.equals(right)) {
                return process(new Not(new IsNull(left)));
            }

            SymbolStatsEstimate leftStats = getExpressionStats(left);
            Optional<Symbol> leftSymbol = left instanceof Reference ? Optional.of(Symbol.from(left)) : Optional.empty();
            if (right instanceof Constant constant) {
                Type type = right.type();
                Object literalValue = constant.value();
                if (literalValue == null) {
                    // Possible when we process `x IN (..., NULL)` case.
                    return input.mapOutputRowCount(rowCountEstimate -> 0.);
                }
                OptionalDouble literal = toStatsRepresentation(type, literalValue);
                return estimateExpressionToLiteralComparison(input, leftStats, leftSymbol, literal, operator);
            }

            SymbolStatsEstimate rightStats = getExpressionStats(right);
            if (rightStats.isSingleValue()) {
                OptionalDouble value = isNaN(rightStats.getLowValue()) ? OptionalDouble.empty() : OptionalDouble.of(rightStats.getLowValue());
                return estimateExpressionToLiteralComparison(input, leftStats, leftSymbol, value, operator);
            }

            Optional<Symbol> rightSymbol = right instanceof Reference ? Optional.of(Symbol.from(right)) : Optional.empty();
            return estimateExpressionToExpressionComparison(input, leftStats, leftSymbol, rightStats, rightSymbol, operator);
        }

        @Override
        protected PlanNodeStatsEstimate visitCall(Call node, Void context)
        {
            if (isDynamicFilter(node)) {
                return process(Booleans.TRUE, context);
            }
            return PlanNodeStatsEstimate.unknown();
        }

        private SymbolStatsEstimate getExpressionStats(Expression expression)
        {
            if (expression instanceof Reference) {
                Symbol symbol = Symbol.from(expression);
                return requireNonNull(input.getSymbolStatistics(symbol), () -> format("No statistics for symbol %s", symbol));
            }
            return scalarStatsCalculator.calculate(expression, input, session);
        }
    }

    private static List<List<Expression>> extractCorrelatedGroups(List<Expression> terms, double filterConjunctionIndependenceFactor)
    {
        if (filterConjunctionIndependenceFactor == 1) {
            // Allows the filters to be estimated as if there is no correlation between any of the terms
            return ImmutableList.of(terms);
        }

        ListMultimap<Expression, Symbol> expressionUniqueSymbols = ArrayListMultimap.create();
        terms.forEach(expression -> expressionUniqueSymbols.putAll(expression, extractUnique(expression)));
        // Partition symbols into disjoint sets such that the symbols belonging to different disjoint sets
        // do not appear together in any expression.
        DisjointSet<Symbol> symbolsPartitioner = new DisjointSet<>();
        for (Expression term : terms) {
            List<Symbol> expressionSymbols = expressionUniqueSymbols.get(term);
            if (expressionSymbols.isEmpty()) {
                continue;
            }
            // Ensure that symbol is added to DisjointSet when there is only one symbol in the list
            symbolsPartitioner.find(expressionSymbols.get(0));
            for (int i = 1; i < expressionSymbols.size(); i++) {
                symbolsPartitioner.findAndUnion(expressionSymbols.get(0), expressionSymbols.get(i));
            }
        }

        // Use disjoint sets of symbols to partition the given list of expressions
        List<Set<Symbol>> symbolPartitions = ImmutableList.copyOf(symbolsPartitioner.getEquivalentClasses());
        checkState(symbolPartitions.size() <= terms.size(), "symbolPartitions size exceeds number of expressions");
        ListMultimap<Integer, Expression> expressionPartitions = ArrayListMultimap.create();
        for (Expression term : terms) {
            List<Symbol> expressionSymbols = expressionUniqueSymbols.get(term);
            int expressionPartitionId;
            if (expressionSymbols.isEmpty()) {
                expressionPartitionId = symbolPartitions.size(); // For expressions with no symbols
            }
            else {
                Symbol symbol = expressionSymbols.get(0); // Lookup any symbol to find the partition id
                expressionPartitionId = IntStream.range(0, symbolPartitions.size())
                        .filter(partition -> symbolPartitions.get(partition).contains(symbol))
                        .findFirst()
                        .orElseThrow();
            }
            expressionPartitions.put(expressionPartitionId, term);
        }

        return expressionPartitions.keySet().stream()
                .map(expressionPartitions::get)
                .collect(toImmutableList());
    }
}
