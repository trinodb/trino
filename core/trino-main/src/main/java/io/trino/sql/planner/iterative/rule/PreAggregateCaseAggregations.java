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
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.ImmutableSetMultimap.toImmutableSetMultimap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SystemSessionProperties.isPreAggregateCaseAggregationsEnabled;
import static io.trino.matching.Capture.newCapture;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.IrExpressions.ifExpression;
import static io.trino.sql.ir.IrExpressions.mayFail;
import static io.trino.sql.ir.IrUtils.or;
import static io.trino.sql.ir.optimizer.IrExpressionOptimizer.newOptimizer;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.function.Predicate.not;

/**
 * Rule that transforms selected aggregations from:
 * <pre> {@code
 * - Aggregation[key]: sum(aggr1), sum(aggr2), ..., sum(aggrN)
 *   - Project:
 *       aggr1 = CASE WHEN col=1 THEN expr ELSE 0
 *       aggr2 = CASE WHEN col=2 THEN expr ELSE null
 *       ...
 *       aggrN = CASE WHEN col=N THEN expr
 *     - source
 * }
 * </pre>
 * into:
 * <pre> {@code
 * - Aggregation[key]: sum(aggr1), sum(aggr2), ..., sum(aggrN)
 *   - Project:
 *       aggr1 = CASE WHEN col=1 THEN pre_aggregate
 *       aggr2 = CASE WHEN col=2 THEN pre_aggregate
 *       ..
 *       aggrN = CASE WHEN col=N THEN pre_aggregate
 *     - Aggregation[key, col]: pre_aggregate = sum(expr)
 *       - source
 * }
 * </pre>
 */
public class PreAggregateCaseAggregations
        implements Rule<AggregationNode>
{
    private static final int MIN_AGGREGATION_COUNT = 4;

    // BE EXTREMELY CAREFUL WHEN ADDING NEW FUNCTIONS TO THIS SET
    // This code appears to be generic, but is not. It only works because the allowed functions have very specific behavior.
    private static final CatalogSchemaFunctionName MAX = builtinFunctionName("max");
    private static final CatalogSchemaFunctionName MIN = builtinFunctionName("min");
    private static final CatalogSchemaFunctionName SUM = builtinFunctionName("sum");
    private static final Set<CatalogSchemaFunctionName> ALLOWED_FUNCTIONS = ImmutableSet.of(MAX, MIN, SUM);

    private static final Capture<ProjectNode> PROJECT_CAPTURE = newCapture();
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(aggregation -> aggregation.getStep() == SINGLE && aggregation.getGroupingSetCount() == 1)
            .with(source().matching(project().capturedAs(PROJECT_CAPTURE)
                    // prevent rule from looping by ensuring that projection source is not aggregation
                    .with(source().matching(not(AggregationNode.class::isInstance)))));

    private final PlannerContext plannerContext;

    public PreAggregateCaseAggregations(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isPreAggregateCaseAggregationsEnabled(session);
    }

    @Override
    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
    {
        ProjectNode projectNode = captures.get(PROJECT_CAPTURE);
        Optional<List<CaseAggregation>> aggregationsOptional = extractCaseAggregations(aggregationNode, projectNode, context);
        if (aggregationsOptional.isEmpty()) {
            return Result.empty();
        }

        List<CaseAggregation> aggregations = aggregationsOptional.get();
        if (aggregations.size() < MIN_AGGREGATION_COUNT) {
            return Result.empty();
        }

        Set<Symbol> extraGroupingKeys = aggregations.stream()
                .flatMap(expression -> SymbolsExtractor.extractUnique(expression.getOperand()).stream())
                .collect(toImmutableSet());
        if (extraGroupingKeys.size() != 1) {
            // pre-aggregation can have single extra symbol
            return Result.empty();
        }

        Map<PreAggregationKey, PreAggregation> preAggregations = getPreAggregations(aggregations, context);
        if (preAggregations.size() == aggregations.size()) {
            // Prevent rule execution if number of pre-aggregations is equal to number of case aggregations.
            // In such case there is no gain in performance, and it could lead to infinite rule execution loop.
            return Result.empty();
        }

        Assignments.Builder preGroupingExpressionsBuilder = Assignments.builder();
        preGroupingExpressionsBuilder.putIdentities(extraGroupingKeys);
        aggregationNode.getGroupingKeys().forEach(symbol -> preGroupingExpressionsBuilder.put(symbol, projectNode.getAssignments().get(symbol)));
        Assignments preGroupingExpressions = preGroupingExpressionsBuilder.build();

        ProjectNode preProjection = createPreProjection(
                projectNode.getSource(),
                preGroupingExpressions,
                preAggregations,
                context);
        AggregationNode preAggregation = createPreAggregation(
                preProjection,
                preGroupingExpressions.getOutputs(),
                preAggregations,
                context);
        Map<CaseAggregation, Symbol> newProjectionSymbols = getNewProjectionSymbols(aggregations, context);
        ProjectNode newProjection = createNewProjection(
                preAggregation,
                aggregationNode,
                projectNode,
                newProjectionSymbols,
                preAggregations);
        return Result.ofPlanNode(createNewAggregation(newProjection, aggregationNode, newProjectionSymbols));
    }

    private AggregationNode createNewAggregation(
            PlanNode source,
            AggregationNode aggregationNode,
            Map<CaseAggregation, Symbol> newProjectionSymbols)
    {
        return new AggregationNode(
                aggregationNode.getId(),
                source,
                newProjectionSymbols.entrySet().stream()
                        .collect(toImmutableMap(
                                entry -> entry.getKey().getAggregationSymbol(),
                                entry -> new Aggregation(
                                        entry.getKey().getCumulativeFunction(),
                                        ImmutableList.of(entry.getValue().toSymbolReference()),
                                        false,
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()))),
                aggregationNode.getGroupingSets(),
                aggregationNode.getPreGroupedSymbols(),
                aggregationNode.getStep(),
                aggregationNode.getHashSymbol(),
                aggregationNode.getGroupIdSymbol());
    }

    private ProjectNode createNewProjection(
            PlanNode source,
            AggregationNode aggregationNode,
            ProjectNode projectNode,
            Map<CaseAggregation, Symbol> newProjectionSymbols,
            Map<PreAggregationKey, PreAggregation> preAggregations)
    {
        Assignments.Builder assignments = Assignments.builder();
        // grouping key expressions are already evaluated in pre-projection
        assignments.putIdentities(aggregationNode.getGroupingKeys());
        newProjectionSymbols.forEach((aggregation, symbol) -> assignments.put(
                symbol,
                new Case(ImmutableList.of(
                        new WhenClause(
                                aggregation.getOperand(),
                                preAggregations.get(new PreAggregationKey(aggregation)).getAggregationSymbol().toSymbolReference())),
                        aggregation.getCumulativeAggregationDefaultValue())));
        return new ProjectNode(projectNode.getId(), source, assignments.build());
    }

    private Map<CaseAggregation, Symbol> getNewProjectionSymbols(List<CaseAggregation> aggregations, Context context)
    {
        return aggregations.stream()
                .collect(toImmutableMap(
                        identity(),
                        // new projection has the same type as original aggregation
                        aggregation -> context.getSymbolAllocator().newSymbol(aggregation.getAggregationSymbol())));
    }

    private AggregationNode createPreAggregation(
            PlanNode source,
            List<Symbol> groupingKeys,
            Map<PreAggregationKey, PreAggregation> preAggregations,
            Context context)
    {
        return new AggregationNode(
                context.getIdAllocator().getNextId(),
                source,
                preAggregations.entrySet().stream()
                        .collect(toImmutableMap(
                                entry -> entry.getValue().getAggregationSymbol(),
                                entry -> new Aggregation(
                                        entry.getKey().getFunction(),
                                        ImmutableList.of(entry.getValue().getProjectionSymbol().toSymbolReference()),
                                        false,
                                        Optional.empty(),
                                        Optional.empty(),
                                        Optional.empty()))),
                singleGroupingSet(groupingKeys),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty());
    }

    private ProjectNode createPreProjection(
            PlanNode source,
            Assignments groupingExpressions,
            Map<PreAggregationKey, PreAggregation> preAggregations,
            Context context)
    {
        Assignments.Builder assignments = Assignments.builder();
        assignments.putAll(groupingExpressions);
        preAggregations.values().forEach(aggregation -> assignments.put(aggregation.getProjectionSymbol(), aggregation.getProjection()));
        return new ProjectNode(context.getIdAllocator().getNextId(), source, assignments.build());
    }

    private Map<PreAggregationKey, PreAggregation> getPreAggregations(List<CaseAggregation> aggregations, Context context)
    {
        return aggregations.stream()
                .collect(toImmutableSetMultimap(PreAggregationKey::new, identity()))
                .asMap().entrySet().stream().collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> {
                            PreAggregationKey key = entry.getKey();
                            Set<CaseAggregation> caseAggregations = (Set<CaseAggregation>) entry.getValue();
                            Expression preProjection = key.projection;

                            // Cast pre-projection if needed to match aggregation input type.
                            // This is because entire "CASE WHEN" expression could be wrapped in CAST.
                            Type preProjectionType = getType(preProjection);
                            Type aggregationInputType = getOnlyElement(key.getFunction().signature().getArgumentTypes());
                            if (!preProjectionType.equals(aggregationInputType)) {
                                preProjection = new Cast(preProjection, aggregationInputType);
                            }

                            // Wrap the preProjection with IF to retain the conditional nature on the CASE aggregation(s) during pre-aggregation
                            if (mayFail(plannerContext, preProjection)) {
                                Expression unionConditions = or(caseAggregations.stream()
                                        .map(CaseAggregation::getOperand)
                                        .collect(toImmutableSet()));
                                preProjection = ifExpression(unionConditions, preProjection);
                            }

                            Symbol preProjectionSymbol = context.getSymbolAllocator().newSymbol(preProjection);
                            Symbol preAggregationSymbol = context.getSymbolAllocator().newSymbol(caseAggregations.iterator().next().getAggregationSymbol());
                            return new PreAggregation(preAggregationSymbol, preProjection, preProjectionSymbol);
                        }));
    }

    private Optional<List<CaseAggregation>> extractCaseAggregations(AggregationNode aggregationNode, ProjectNode projectNode, Context context)
    {
        ImmutableList.Builder<CaseAggregation> caseAggregations = ImmutableList.builder();
        for (Map.Entry<Symbol, Aggregation> aggregation : aggregationNode.getAggregations().entrySet()) {
            Optional<CaseAggregation> caseAggregation = extractCaseAggregation(
                    aggregation.getKey(),
                    aggregation.getValue(),
                    projectNode,
                    context);
            if (caseAggregation.isEmpty()) {
                return Optional.empty();
            }
            caseAggregations.add(caseAggregation.get());
        }
        return Optional.of(caseAggregations.build());
    }

    private Optional<CaseAggregation> extractCaseAggregation(Symbol aggregationSymbol, Aggregation aggregation, ProjectNode projectNode, Context context)
    {
        if (aggregation.getArguments().size() != 1
                || !(aggregation.getArguments().get(0) instanceof Reference)
                || aggregation.isDistinct()
                || aggregation.getFilter().isPresent()
                || aggregation.getMask().isPresent()
                || aggregation.getOrderingScheme().isPresent()) {
            // aggregation must be a basic aggregation
            return Optional.empty();
        }

        ResolvedFunction resolvedFunction = aggregation.getResolvedFunction();
        CatalogSchemaFunctionName name = resolvedFunction.signature().getName();
        if (!ALLOWED_FUNCTIONS.contains(name)) {
            // only cumulative aggregations (e.g. that can be split into aggregation of aggregations) are supported
            return Optional.empty();
        }

        Symbol projectionSymbol = Symbol.from(aggregation.getArguments().get(0));
        Expression projection = projectNode.getAssignments().get(projectionSymbol);
        Expression unwrappedProjection;
        // unwrap top-level cast
        if (projection instanceof Cast cast) {
            unwrappedProjection = cast.expression();
        }
        else {
            unwrappedProjection = projection;
        }

        if (!(unwrappedProjection instanceof Case caseExpression)) {
            return Optional.empty();
        }

        if (caseExpression.whenClauses().size() != 1) {
            return Optional.empty();
        }

        Type aggregationType = resolvedFunction.signature().getReturnType();
        ResolvedFunction cumulativeFunction;
        try {
            cumulativeFunction = plannerContext.getMetadata().resolveBuiltinFunction(name.getFunctionName(), fromTypes(aggregationType));
        }
        catch (TrinoException e) {
            // there is no cumulative aggregation
            return Optional.empty();
        }

        if (!cumulativeFunction.signature().getReturnType().equals(aggregationType)) {
            // aggregation type after rewrite must not change
            return Optional.empty();
        }

        Expression defaultValue = optimizeExpression(caseExpression.defaultValue(), context);
        if (defaultValue instanceof Constant(Type type, Object value)) {
            if (value != null) {
                if (!name.equals(SUM)) {
                    return Optional.empty();
                }

                // sum aggregation is only supported if default value is null or 0, otherwise it wouldn't be cumulative
                if (!value.equals(0L) && !value.equals(0.0d) && !value.equals(Int128.ZERO)) {
                    return Optional.empty();
                }

                if (!(type instanceof BigintType)
                        && type != INTEGER
                        && type != SMALLINT
                        && type != TINYINT
                        && type != DOUBLE
                        && type != REAL
                        && !(type instanceof DecimalType)) {
                    return Optional.empty();
                }
            }

            return Optional.of(new CaseAggregation(
                    aggregationSymbol,
                    resolvedFunction,
                    cumulativeFunction,
                    name,
                    caseExpression.whenClauses().get(0).getOperand(),
                    caseExpression.whenClauses().get(0).getResult(),
                    new Cast(caseExpression.defaultValue(), aggregationType)));
        }

        return Optional.empty();
    }

    private Type getType(Expression expression)
    {
        return expression.type();
    }

    private Expression optimizeExpression(Expression expression, Context context)
    {
        return newOptimizer(plannerContext).process(expression, context.getSession(), ImmutableMap.of()).orElse(expression);
    }

    private static class CaseAggregation
    {
        // original aggregation symbol
        private final Symbol aggregationSymbol;
        // original aggregation function
        private final ResolvedFunction function;
        // cumulative aggregation function (e.g. aggregation of aggregations)
        private final ResolvedFunction cumulativeFunction;
        // aggregation function name
        private final CatalogSchemaFunctionName name;
        // CASE expression only operand expression
        private final Expression operand;
        // CASE expression only result expression
        private final Expression result;
        // default value of cumulative aggregation
        private final Expression cumulativeAggregationDefaultValue;

        public CaseAggregation(
                Symbol aggregationSymbol,
                ResolvedFunction function,
                ResolvedFunction cumulativeFunction,
                CatalogSchemaFunctionName name,
                Expression operand,
                Expression result,
                Expression cumulativeAggregationDefaultValue)
        {
            this.aggregationSymbol = requireNonNull(aggregationSymbol, "aggregationSymbol is null");
            this.function = requireNonNull(function, "function is null");
            this.cumulativeFunction = requireNonNull(cumulativeFunction, "cumulativeFunction is null");
            this.name = requireNonNull(name, "name is null");
            this.operand = requireNonNull(operand, "operand is null");
            this.result = requireNonNull(result, "result is null");
            this.cumulativeAggregationDefaultValue = requireNonNull(cumulativeAggregationDefaultValue, "cumulativeAggregationDefaultValue is null");
        }

        public Symbol getAggregationSymbol()
        {
            return aggregationSymbol;
        }

        public ResolvedFunction getFunction()
        {
            return function;
        }

        public ResolvedFunction getCumulativeFunction()
        {
            return cumulativeFunction;
        }

        public CatalogSchemaFunctionName getName()
        {
            return name;
        }

        public Expression getOperand()
        {
            return operand;
        }

        public Expression getResult()
        {
            return result;
        }

        public Expression getCumulativeAggregationDefaultValue()
        {
            return cumulativeAggregationDefaultValue;
        }
    }

    private static class PreAggregationKey
    {
        // original aggregation function
        private final ResolvedFunction function;
        // projected input to aggregation (CASE expression only result expression)
        private final Expression projection;

        private PreAggregationKey(CaseAggregation aggregation)
        {
            this.function = aggregation.getFunction();
            this.projection = aggregation.getResult();
        }

        public ResolvedFunction getFunction()
        {
            return function;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PreAggregationKey that = (PreAggregationKey) o;
            return Objects.equals(function, that.function) &&
                    Objects.equals(projection, that.projection);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(function, projection);
        }
    }

    private static class PreAggregation
    {
        private final Symbol aggregationSymbol;
        private final Expression projection;
        private final Symbol projectionSymbol;

        public PreAggregation(Symbol aggregationSymbol, Expression projection, Symbol projectionSymbol)
        {
            this.aggregationSymbol = requireNonNull(aggregationSymbol, "aggregationSymbol is null");
            this.projection = requireNonNull(projection, "projection is null");
            this.projectionSymbol = requireNonNull(projectionSymbol, "projectionSymbol is null");
        }

        public Symbol getAggregationSymbol()
        {
            return aggregationSymbol;
        }

        public Expression getProjection()
        {
            return projection;
        }

        public Symbol getProjectionSymbol()
        {
            return projectionSymbol;
        }
    }
}
