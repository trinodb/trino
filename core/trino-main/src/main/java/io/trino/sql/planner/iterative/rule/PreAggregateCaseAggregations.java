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
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.ExpressionInterpreter;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.WhenClause;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SystemSessionProperties.isPreAggregateCaseAggregationsEnabled;
import static io.trino.matching.Capture.newCapture;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
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
    private static final Set<String> ALLOWED_FUNCTIONS = ImmutableSet.of("max", "min", "sum");
    private static final Capture<ProjectNode> PROJECT_CAPTURE = newCapture();
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(aggregation -> aggregation.getStep() == SINGLE && aggregation.getGroupingSetCount() == 1)
            .with(source().matching(project().capturedAs(PROJECT_CAPTURE)
                    // prevent rule from looping by ensuring that projection source is not aggregation
                    .with(source().matching(not(AggregationNode.class::isInstance)))));

    private final PlannerContext plannerContext;
    private final TypeAnalyzer typeAnalyzer;

    public PreAggregateCaseAggregations(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
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
                new SearchedCaseExpression(ImmutableList.of(
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
        Set<PreAggregationKey> keys = new HashSet<>();
        ImmutableMap.Builder<PreAggregationKey, PreAggregation> preAggregations = ImmutableMap.builder();
        for (CaseAggregation aggregation : aggregations) {
            PreAggregationKey preAggregationKey = new PreAggregationKey(aggregation);
            if (keys.contains(preAggregationKey)) {
                continue;
            }

            // Cast pre-projection if needed to match aggregation input type.
            // This is because entire "CASE WHEN" expression could be wrapped in CAST.
            Expression preProjection = aggregation.getResult();
            Type preProjectionType = getType(context, preProjection);
            Type aggregationInputType = getOnlyElement(aggregation.getFunction().getSignature().getArgumentTypes());
            if (!preProjectionType.equals(aggregationInputType)) {
                preProjection = new Cast(preProjection, toSqlType(aggregationInputType));
                preProjectionType = aggregationInputType;
            }

            Symbol preProjectionSymbol = context.getSymbolAllocator().newSymbol(preProjection, preProjectionType);
            Symbol preAggregationSymbol = context.getSymbolAllocator().newSymbol(aggregation.getAggregationSymbol());
            preAggregations.put(preAggregationKey, new PreAggregation(preAggregationSymbol, preProjection, preProjectionSymbol));
            keys.add(preAggregationKey);
        }
        return ImmutableMap.copyOf(preAggregations.buildOrThrow());
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
                || !(aggregation.getArguments().get(0) instanceof SymbolReference)
                || aggregation.isDistinct()
                || aggregation.getFilter().isPresent()
                || aggregation.getMask().isPresent()
                || aggregation.getOrderingScheme().isPresent()) {
            // aggregation must be a basic aggregation
            return Optional.empty();
        }

        String name = aggregation.getResolvedFunction().getSignature().getName();
        if (!ALLOWED_FUNCTIONS.contains(name)) {
            // only cumulative aggregations (e.g. that can be split into aggregation of aggregations) are supported
            return Optional.empty();
        }

        Symbol projectionSymbol = Symbol.from(aggregation.getArguments().get(0));
        Expression projection = projectNode.getAssignments().get(projectionSymbol);
        Expression unwrappedProjection;
        // unwrap top-level cast
        if (projection instanceof Cast) {
            unwrappedProjection = ((Cast) projection).getExpression();
        }
        else {
            unwrappedProjection = projection;
        }

        if (!(unwrappedProjection instanceof SearchedCaseExpression caseExpression)) {
            return Optional.empty();
        }

        if (caseExpression.getWhenClauses().size() != 1) {
            return Optional.empty();
        }

        Type aggregationType = aggregation.getResolvedFunction().getSignature().getReturnType();
        ResolvedFunction cumulativeFunction;
        try {
            cumulativeFunction = plannerContext.getMetadata().resolveFunction(context.getSession(), QualifiedName.of(name), fromTypes(aggregationType));
        }
        catch (TrinoException e) {
            // there is no cumulative aggregation
            return Optional.empty();
        }

        if (!cumulativeFunction.getSignature().getReturnType().equals(aggregationType)) {
            // aggregation type after rewrite must not change
            return Optional.empty();
        }

        Optional<Expression> cumulativeAggregationDefaultValue = Optional.empty();
        if (caseExpression.getDefaultValue().isPresent()) {
            Type defaultType = getType(context, caseExpression.getDefaultValue().get());
            Object defaultValue = optimizeExpression(caseExpression.getDefaultValue().get(), context);
            if (defaultValue != null) {
                if (!name.equals("sum")) {
                    return Optional.empty();
                }

                // sum aggregation is only supported if default value is null or 0, otherwise it wouldn't be cumulative
                if (defaultType instanceof BigintType
                        || defaultType == INTEGER
                        || defaultType == SMALLINT
                        || defaultType == TINYINT
                        || defaultType == DOUBLE
                        || defaultType == REAL
                        || defaultType instanceof DecimalType) {
                    if (!defaultValue.equals(0L) && !defaultValue.equals(0.0d) && !defaultValue.equals(Int128.ZERO)) {
                        return Optional.empty();
                    }
                }
                else {
                    return Optional.empty();
                }
            }

            // cumulative aggregation default value need to be CAST to cumulative aggregation input type
            cumulativeAggregationDefaultValue = Optional.of(new Cast(
                    caseExpression.getDefaultValue().get(),
                    toSqlType(aggregationType)));
        }

        return Optional.of(new CaseAggregation(
                aggregationSymbol,
                aggregation.getResolvedFunction(),
                cumulativeFunction,
                name,
                caseExpression.getWhenClauses().get(0).getOperand(),
                caseExpression.getWhenClauses().get(0).getResult(),
                cumulativeAggregationDefaultValue));
    }

    private Type getType(Context context, Expression expression)
    {
        return typeAnalyzer.getType(context.getSession(), context.getSymbolAllocator().getTypes(), expression);
    }

    private Object optimizeExpression(Expression expression, Context context)
    {
        Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(context.getSession(), context.getSymbolAllocator().getTypes(), expression);
        ExpressionInterpreter expressionInterpreter = new ExpressionInterpreter(expression, plannerContext, context.getSession(), expressionTypes);
        return expressionInterpreter.optimize(Symbol::toSymbolReference);
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
        private final String name;
        // CASE expression only operand expression
        private final Expression operand;
        // CASE expression only result expression
        private final Expression result;
        // default value of cumulative aggregation
        private final Optional<Expression> cumulativeAggregationDefaultValue;

        public CaseAggregation(
                Symbol aggregationSymbol,
                ResolvedFunction function,
                ResolvedFunction cumulativeFunction,
                String name,
                Expression operand,
                Expression result,
                Optional<Expression> cumulativeAggregationDefaultValue)
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

        public String getName()
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

        public Optional<Expression> getCumulativeAggregationDefaultValue()
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
