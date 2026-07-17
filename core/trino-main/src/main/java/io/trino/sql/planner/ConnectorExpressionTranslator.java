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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.scalar.JsonPath;
import io.trino.operator.scalar.TryCastFunction;
import io.trino.plugin.base.expression.ConnectorExpressions;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Bind;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IrExpressions;
import io.trino.sql.ir.IrExpressions.Comparison;
import io.trino.sql.ir.IrVisitor;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.tree.QualifiedName;
import io.trino.type.JoniRegexp;
import io.trino.type.JsonPathType;
import io.trino.type.LikePattern;
import io.trino.type.Re2JRegexp;
import io.trino.type.Re2JRegexpType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.trino.SystemSessionProperties.isComplexExpressionPushdown;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.metadata.GlobalFunctionCatalog.isBuiltinFunctionName;
import static io.trino.metadata.LanguageFunctionManager.isInlineFunction;
import static io.trino.operator.scalar.JsonStringToArrayCast.JSON_STRING_TO_ARRAY_NAME;
import static io.trino.operator.scalar.JsonStringToMapCast.JSON_STRING_TO_MAP_NAME;
import static io.trino.operator.scalar.JsonStringToRowCast.JSON_STRING_TO_ROW_NAME;
import static io.trino.spi.expression.StandardFunctions.ADD_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.BETWEEN_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.COALESCE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.DIVIDE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IDENTICAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_NULL_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.MODULO_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.MULTIPLY_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NEGATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NULLIF_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.SUBTRACT_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.TRY_CAST_FUNCTION_NAME;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.function.OperatorType.MODULO;
import static io.trino.spi.function.OperatorType.MULTIPLY;
import static io.trino.spi.function.OperatorType.NEGATION;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.DynamicFilters.isDynamicFilterFunction;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.ir.IrExpressions.cast;
import static io.trino.sql.ir.IrExpressions.comparison;
import static io.trino.sql.ir.IrExpressions.matchBetween;
import static io.trino.sql.ir.IrExpressions.matchComparison;
import static io.trino.sql.ir.IrExpressions.matchNullIf;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.planner.EngineExpressions.ENGINE_EXPRESSION_FUNCTION_NAME;
import static io.trino.type.JoniRegexpType.JONI_REGEXP;
import static io.trino.type.LikeFunctions.LIKE_FUNCTION_NAME;
import static io.trino.type.LikeFunctions.LIKE_PATTERN_FUNCTION_NAME;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static java.util.Objects.requireNonNull;

public final class ConnectorExpressionTranslator
{
    private ConnectorExpressionTranslator() {}

    /**
     * Translates a connector expression to its IR equivalent. {@code $engine_expression} calls are
     * translated by deserializing their payload back into the IR expression they wrap; the deserialized
     * expression references the symbols captured by {@link EngineExpressions#buildEngineExpression},
     * bypassing {@code variableMappings}, so callers must ensure those symbols are in scope.
     */
    public static Expression translate(Session session, ConnectorExpression expression, PlannerContext plannerContext, Map<String, Symbol> variableMappings, SymbolAllocator symbolAllocator)
    {
        return new ConnectorToSqlExpressionTranslator(session, plannerContext, variableMappings, symbolAllocator)
                .translate(expression)
                .orElseThrow(() -> new UnsupportedOperationException("Expression is not supported: " + expression.toString()));
    }

    public static Optional<ConnectorExpression> translate(Session session, Expression expression)
    {
        return new SqlToConnectorExpressionTranslator(session)
                .process(expression);
    }

    /**
     * Translates each conjunct of {@code expression} to a {@link ConnectorExpression} where
     * possible. Conjuncts that are structurally untranslatable, or that reference a symbol absent
     * from {@code columnNames} (e.g. a correlation variable from an outer scope that has no
     * {@link io.trino.spi.connector.ColumnHandle} in the scan), are moved to
     * {@code remainingExpression} so the engine handles them. The column-name check is performed
     * on the IR expression before translation so that lambda-argument variables, which are not
     * column references, are not mistakenly treated as unmapped.
     */
    public static ConnectorExpressionTranslation translateConjuncts(
            Session session,
            Expression expression,
            Set<String> columnNames)
    {
        SqlToConnectorExpressionTranslator translator = new SqlToConnectorExpressionTranslator(session);

        List<Expression> conjuncts = extractConjuncts(expression);
        List<Expression> remaining = new ArrayList<>();
        List<ConnectorExpression> converted = new ArrayList<>(conjuncts.size());
        for (Expression conjunct : conjuncts) {
            if (SymbolsExtractor.extractUnique(conjunct).stream()
                    .anyMatch(symbol -> !columnNames.contains(symbol.name()))) {
                remaining.add(conjunct);
                continue;
            }
            Optional<ConnectorExpression> connectorExpression = translator.process(conjunct);
            if (connectorExpression.isPresent()) {
                converted.add(connectorExpression.get());
            }
            else {
                remaining.add(conjunct);
            }
        }
        return new ConnectorExpressionTranslation(
                ConnectorExpressions.and(converted),
                combineConjuncts(remaining));
    }

    @VisibleForTesting
    static FunctionName functionNameForComparisonOperator(ComparisonOperator operator)
    {
        return switch (operator) {
            case EQUAL -> EQUAL_OPERATOR_FUNCTION_NAME;
            case NOT_EQUAL -> NOT_EQUAL_OPERATOR_FUNCTION_NAME;
            case LESS_THAN -> LESS_THAN_OPERATOR_FUNCTION_NAME;
            case LESS_THAN_OR_EQUAL -> LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
            case GREATER_THAN -> GREATER_THAN_OPERATOR_FUNCTION_NAME;
            case GREATER_THAN_OR_EQUAL -> GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
            case IDENTICAL -> IDENTICAL_OPERATOR_FUNCTION_NAME;
        };
    }

    public record ConnectorExpressionTranslation(ConnectorExpression connectorExpression, Expression remainingExpression)
    {
        public ConnectorExpressionTranslation
        {
            requireNonNull(connectorExpression, "connectorExpression is null");
            requireNonNull(remainingExpression, "remainingExpression is null");
        }
    }

    private static class ConnectorToSqlExpressionTranslator
    {
        private final Session session;
        private final PlannerContext plannerContext;
        private final Map<String, Symbol> variableMappings;
        private final SymbolAllocator symbolAllocator;

        public ConnectorToSqlExpressionTranslator(Session session, PlannerContext plannerContext, Map<String, Symbol> variableMappings, SymbolAllocator symbolAllocator)
        {
            this.session = requireNonNull(session, "session is null");
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.variableMappings = requireNonNull(variableMappings, "variableMappings is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        }

        public Optional<Expression> translate(ConnectorExpression expression)
        {
            return translate(expression, Map.of());
        }

        private Optional<Expression> translate(ConnectorExpression expression, Map<String, Symbol> lambdaArguments)
        {
            if (expression instanceof Variable variable) {
                String name = variable.getName();
                Symbol lambdaArgument = lambdaArguments.get(name);
                if (lambdaArgument != null) {
                    return Optional.of(lambdaArgument.toSymbolReference());
                }
                return Optional.of(variableMappings.get(name).toSymbolReference());
            }

            if (expression instanceof io.trino.spi.expression.Constant constant) {
                return Optional.of(new Constant(constant.getType(), constant.getValue()));
            }

            if (expression instanceof FieldDereference dereference) {
                return translate(dereference.getTarget(), lambdaArguments)
                        .map(base -> new FieldReference(base, dereference.getField()));
            }

            if (expression instanceof io.trino.spi.expression.Lambda lambda) {
                return translateLambda(lambda, lambdaArguments);
            }

            if (expression instanceof io.trino.spi.expression.Call call) {
                return translateCall(call, lambdaArguments);
            }

            return Optional.empty();
        }

        private Optional<Expression> translateLambda(io.trino.spi.expression.Lambda lambda, Map<String, Symbol> lambdaArguments)
        {
            ImmutableList.Builder<Symbol> arguments = ImmutableList.builderWithExpectedSize(lambda.getArguments().size());
            Map<String, Symbol> nestedLambdaArguments = new HashMap<>(lambdaArguments);
            for (Variable argument : lambda.getArguments()) {
                Symbol symbol = new Symbol(argument.getType(), argument.getName());
                arguments.add(symbol);
                nestedLambdaArguments.put(argument.getName(), symbol);
            }

            return translate(lambda.getBody(), nestedLambdaArguments)
                    .map(body -> new Lambda(arguments.build(), body));
        }

        protected Optional<Expression> translateCall(io.trino.spi.expression.Call call, Map<String, Symbol> lambdaArguments)
        {
            if (call.getFunctionName().equals(ENGINE_EXPRESSION_FUNCTION_NAME)) {
                Slice payload = (Slice) requireNonNull(
                        ((io.trino.spi.expression.Constant) call.getArguments().getFirst()).getValue(),
                        "engine expression payload is null");
                return Optional.of(plannerContext.getExpressionCodec().fromJson(payload.toStringUtf8()));
            }

            if (call.getFunctionName().getCatalogSchema().isPresent()) {
                CatalogSchemaName catalogSchemaName = call.getFunctionName().getCatalogSchema().get();
                checkArgument(!catalogSchemaName.getCatalogName().equals(GlobalSystemConnector.NAME), "System functions must not be fully qualified");
                // this uses allow all access control because connector expressions are not allowed access any function
                ResolvedFunction resolved = plannerContext.getFunctionResolver().resolveFunction(
                        session,
                        QualifiedName.of(catalogSchemaName.getCatalogName(), catalogSchemaName.getSchemaName(), call.getFunctionName().getName()),
                        fromTypes(call.getArguments().stream().map(ConnectorExpression::getType).collect(toImmutableList())),
                        new AllowAllAccessControl());

                return translateCall(call.getFunctionName().getName(), resolved, call.getArguments(), lambdaArguments);
            }

            if (AND_FUNCTION_NAME.equals(call.getFunctionName())) {
                return translateLogicalExpression(Logical.Operator.AND, call.getArguments(), lambdaArguments);
            }
            if (OR_FUNCTION_NAME.equals(call.getFunctionName())) {
                return translateLogicalExpression(Logical.Operator.OR, call.getArguments(), lambdaArguments);
            }
            if (BETWEEN_FUNCTION_NAME.equals(call.getFunctionName()) && call.getArguments().size() == 3) {
                return translate(call.getArguments().get(0), lambdaArguments).flatMap(value ->
                        translate(call.getArguments().get(1), lambdaArguments).flatMap(min ->
                                translate(call.getArguments().get(2), lambdaArguments).map(max ->
                                        IrExpressions.between(plannerContext.getMetadata(), symbolAllocator, value, min, max))));
            }
            if (NOT_FUNCTION_NAME.equals(call.getFunctionName()) && call.getArguments().size() == 1) {
                ConnectorExpression expression = getOnlyElement(call.getArguments());

                if (expression instanceof io.trino.spi.expression.Call innerCall) {
                    if (innerCall.getFunctionName().equals(IS_NULL_FUNCTION_NAME) && innerCall.getArguments().size() == 1) {
                        return translateIsNotNull(innerCall.getArguments().get(0), lambdaArguments);
                    }
                }

                return translateNot(expression, lambdaArguments);
            }
            if (IS_NULL_FUNCTION_NAME.equals(call.getFunctionName()) && call.getArguments().size() == 1) {
                return translateIsNull(call.getArguments().get(0), lambdaArguments);
            }

            if (NULLIF_FUNCTION_NAME.equals(call.getFunctionName()) && call.getArguments().size() == 2) {
                return translateNullIf(call.getArguments().get(0), call.getArguments().get(1), lambdaArguments);
            }
            if (COALESCE_FUNCTION_NAME.equals(call.getFunctionName()) && call.getArguments().size() >= 2) {
                return translateCoalesce(call.getArguments(), lambdaArguments);
            }
            if (CAST_FUNCTION_NAME.equals(call.getFunctionName()) && call.getArguments().size() == 1) {
                return translateCast(call.getType(), call.getArguments().get(0), lambdaArguments);
            }

            if (TRY_CAST_FUNCTION_NAME.equals(call.getFunctionName()) && call.getArguments().size() == 1) {
                return translateTryCast(call.getType(), call.getArguments().get(0), lambdaArguments);
            }

            // comparisons
            if (call.getArguments().size() == 2) {
                Optional<ComparisonOperator> operator = comparisonOperatorForFunctionName(call.getFunctionName());
                if (operator.isPresent()) {
                    return translateComparison(operator.get(), call.getArguments().get(0), call.getArguments().get(1), lambdaArguments);
                }
            }

            // arithmetic binary
            if (call.getArguments().size() == 2) {
                Optional<OperatorType> operator = arithmeticBinaryOperatorForFunctionName(call.getFunctionName());
                if (operator.isPresent()) {
                    return translateArithmeticBinary(operator.get(), call.getArguments().get(0), call.getArguments().get(1), lambdaArguments);
                }
            }

            // arithmetic unary
            if (NEGATE_FUNCTION_NAME.equals(call.getFunctionName()) && call.getArguments().size() == 1) {
                ConnectorExpression argument = getOnlyElement(call.getArguments());
                ResolvedFunction function = plannerContext.getMetadata().resolveOperator(NEGATION, ImmutableList.of(argument.getType()));
                return translate(argument, lambdaArguments).map(value -> new Call(function, ImmutableList.of(value)));
            }

            if (StandardFunctions.LIKE_FUNCTION_NAME.equals(call.getFunctionName())) {
                return switch (call.getArguments().size()) {
                    case 2 -> translateLike(call.getArguments().get(0), call.getArguments().get(1), Optional.empty(), lambdaArguments);
                    case 3 -> translateLike(call.getArguments().get(0), call.getArguments().get(1), Optional.of(call.getArguments().get(2)), lambdaArguments);
                    default -> Optional.empty();
                };
            }

            if (IN_PREDICATE_FUNCTION_NAME.equals(call.getFunctionName()) && call.getArguments().size() == 2) {
                return translateInPredicate(call.getArguments().get(0), call.getArguments().get(1), lambdaArguments);
            }

            ResolvedFunction resolved;
            if (JSON_STRING_TO_MAP_NAME.equals(call.getFunctionName().getName()) ||
                    JSON_STRING_TO_ARRAY_NAME.equals(call.getFunctionName().getName()) ||
                    JSON_STRING_TO_ROW_NAME.equals(call.getFunctionName().getName())) {
                // These are special functions that currently need to be resolved via getCoercion() -- TODO: fix this
                resolved = plannerContext.getMetadata().getCoercion(builtinFunctionName(call.getFunctionName().getName()), call.getArguments().get(0).getType(), call.getType());
            }
            else {
                resolved = plannerContext.getMetadata().resolveBuiltinFunction(
                        call.getFunctionName().getName(),
                        fromTypes(call.getArguments().stream().map(ConnectorExpression::getType).collect(toImmutableList())));
            }

            return translateCall(call.getFunctionName().getName(), resolved, call.getArguments(), lambdaArguments);
        }

        private Optional<Expression> translateTryCast(Type type, ConnectorExpression argument, Map<String, Symbol> lambdaArguments)
        {
            Optional<Expression> translatedArgument = translate(argument, lambdaArguments);
            if (translatedArgument.isEmpty()) {
                return Optional.empty();
            }

            return Optional.of(new Call(
                    plannerContext.getMetadata().getCoercion(
                            builtinFunctionName(TryCastFunction.TRY_CAST_FUNCTION_NAME),
                            argument.getType(),
                            type),
                    ImmutableList.of(translatedArgument.get())));
        }

        private Optional<Expression> translateCall(String functionName, ResolvedFunction resolved, List<ConnectorExpression> arguments, Map<String, Symbol> lambdaArguments)
        {
            ResolvedFunctionCallBuilder builder = ResolvedFunctionCallBuilder.builder(resolved);
            for (int i = 0; i < arguments.size(); i++) {
                ConnectorExpression argument = arguments.get(i);
                Type formalType = resolved.signature().getArgumentTypes().get(i);
                Type argumentType = argument.getType();
                Optional<Expression> translated = translate(argument, lambdaArguments);
                if (translated.isEmpty()) {
                    return Optional.empty();
                }
                Expression expression = translated.get();
                if ((formalType == JONI_REGEXP || formalType instanceof Re2JRegexpType || formalType instanceof JsonPathType)
                        && argumentType instanceof VarcharType) {
                    // These types are not used in connector expressions, so require special handling when translating back to expressions.
                    expression = cast(plannerContext.getTypeManager(), expression, formalType);
                }
                else if (!argumentType.equals(formalType)) {
                    // There are no implicit coercions in connector expressions except for engine types that are not exposed in connector expressions.
                    throw new IllegalArgumentException("Unexpected type %s for argument %s of type %s of %s".formatted(argumentType, formalType, i, functionName));
                }
                builder.addArgument(expression);
            }
            return Optional.of(builder.build());
        }

        private Optional<Expression> translateIsNotNull(ConnectorExpression argument, Map<String, Symbol> lambdaArguments)
        {
            Optional<Expression> translatedArgument = translate(argument, lambdaArguments);
            if (translatedArgument.isPresent()) {
                return Optional.of(not(plannerContext.getMetadata(), new IsNull(translatedArgument.get())));
            }

            return Optional.empty();
        }

        private Optional<Expression> translateIsNull(ConnectorExpression argument, Map<String, Symbol> lambdaArguments)
        {
            Optional<Expression> translatedArgument = translate(argument, lambdaArguments);
            if (translatedArgument.isPresent()) {
                return Optional.of(new IsNull(translatedArgument.get()));
            }

            return Optional.empty();
        }

        private Optional<Expression> translateNot(ConnectorExpression argument, Map<String, Symbol> lambdaArguments)
        {
            Optional<Expression> translatedArgument = translate(argument, lambdaArguments);
            if (argument.getType().equals(BOOLEAN) && translatedArgument.isPresent()) {
                return Optional.of(not(plannerContext.getMetadata(), translatedArgument.get()));
            }
            return Optional.empty();
        }

        private Optional<Expression> translateCast(Type type, ConnectorExpression expression, Map<String, Symbol> lambdaArguments)
        {
            Optional<Expression> translatedExpression = translate(expression, lambdaArguments);

            if (translatedExpression.isPresent()) {
                return Optional.of(cast(plannerContext.getTypeManager(), translatedExpression.get(), type));
            }

            return Optional.empty();
        }

        private Optional<Expression> translateLogicalExpression(Logical.Operator operator, List<ConnectorExpression> arguments, Map<String, Symbol> lambdaArguments)
        {
            Optional<List<Expression>> translatedArguments = translateExpressions(arguments, lambdaArguments);
            return translatedArguments.map(expressions -> new Logical(operator, expressions));
        }

        private Optional<Expression> translateComparison(ComparisonOperator operator, ConnectorExpression left, ConnectorExpression right, Map<String, Symbol> lambdaArguments)
        {
            return translate(left, lambdaArguments).flatMap(leftTranslated ->
                    translate(right, lambdaArguments).map(rightTranslated ->
                            comparison(plannerContext.getMetadata(), operator, leftTranslated, rightTranslated)));
        }

        private Optional<Expression> translateNullIf(ConnectorExpression first, ConnectorExpression second, Map<String, Symbol> lambdaArguments)
        {
            Optional<Expression> firstExpression = translate(first, lambdaArguments);
            Optional<Expression> secondExpression = translate(second, lambdaArguments);
            if (firstExpression.isPresent() && secondExpression.isPresent()) {
                return Optional.of(IrExpressions.nullIf(plannerContext.getMetadata(), plannerContext.getTypeManager(), symbolAllocator, firstExpression.get(), secondExpression.get()));
            }

            return Optional.empty();
        }

        private Optional<Expression> translateCoalesce(List<ConnectorExpression> arguments, Map<String, Symbol> lambdaArguments)
        {
            return translateExpressions(arguments, lambdaArguments)
                    .map(Coalesce::new);
        }

        private Optional<ComparisonOperator> comparisonOperatorForFunctionName(FunctionName functionName)
        {
            if (EQUAL_OPERATOR_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(ComparisonOperator.EQUAL);
            }
            if (NOT_EQUAL_OPERATOR_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(ComparisonOperator.NOT_EQUAL);
            }
            if (LESS_THAN_OPERATOR_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(ComparisonOperator.LESS_THAN);
            }
            if (LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(ComparisonOperator.LESS_THAN_OR_EQUAL);
            }
            if (GREATER_THAN_OPERATOR_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(ComparisonOperator.GREATER_THAN);
            }
            if (GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(ComparisonOperator.GREATER_THAN_OR_EQUAL);
            }
            if (IDENTICAL_OPERATOR_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(ComparisonOperator.IDENTICAL);
            }
            return Optional.empty();
        }

        private Optional<Expression> translateArithmeticBinary(OperatorType operator, ConnectorExpression left, ConnectorExpression right, Map<String, Symbol> lambdaArguments)
        {
            ResolvedFunction function = plannerContext.getMetadata().resolveOperator(operator, ImmutableList.of(left.getType(), right.getType()));

            return translate(left, lambdaArguments).flatMap(leftTranslated ->
                    translate(right, lambdaArguments).map(rightTranslated ->
                            new Call(function, ImmutableList.of(leftTranslated, rightTranslated))));
        }

        private Optional<OperatorType> arithmeticBinaryOperatorForFunctionName(FunctionName functionName)
        {
            if (ADD_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(ADD);
            }
            if (SUBTRACT_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(SUBTRACT);
            }
            if (MULTIPLY_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(MULTIPLY);
            }
            if (DIVIDE_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(DIVIDE);
            }
            if (MODULO_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(MODULO);
            }
            return Optional.empty();
        }

        protected Optional<Expression> translateLike(ConnectorExpression value, ConnectorExpression pattern, Optional<ConnectorExpression> escape, Map<String, Symbol> lambdaArguments)
        {
            Optional<Expression> translatedValue = translate(value, lambdaArguments);
            Optional<Expression> translatedPattern = translate(pattern, lambdaArguments);

            if (translatedValue.isPresent() && translatedPattern.isPresent()) {
                Call patternCall;
                if (escape.isPresent()) {
                    Optional<Expression> translatedEscape = translate(escape.get(), lambdaArguments);
                    if (translatedEscape.isEmpty()) {
                        return Optional.empty();
                    }

                    patternCall = BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                            .setName(LIKE_PATTERN_FUNCTION_NAME)
                            .addArgument(VARCHAR, castIfNecessary(translatedPattern.get(), VARCHAR))
                            .addArgument(VARCHAR, castIfNecessary(translatedEscape.get(), VARCHAR))
                            .build();
                }
                else {
                    patternCall = BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                            .setName(LIKE_PATTERN_FUNCTION_NAME)
                            .addArgument(VARCHAR, castIfNecessary(translatedPattern.get(), VARCHAR))
                            .build();
                }

                Call call = BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                        .setName(LIKE_FUNCTION_NAME)
                        .addArgument(value.getType(), translatedValue.get())
                        .addArgument(LIKE_PATTERN, patternCall)
                        .build();

                return Optional.of(call);
            }

            return Optional.empty();
        }

        protected Optional<Expression> translateInPredicate(ConnectorExpression value, ConnectorExpression values, Map<String, Symbol> lambdaArguments)
        {
            Optional<Expression> translatedValue = translate(value, lambdaArguments);
            Optional<List<Expression>> translatedValues = extractExpressionsFromArrayCall(values, lambdaArguments);

            if (translatedValue.isPresent() && translatedValues.isPresent()) {
                return Optional.of(new In(translatedValue.get(), translatedValues.get()));
            }

            return Optional.empty();
        }

        protected Optional<List<Expression>> extractExpressionsFromArrayCall(ConnectorExpression expression, Map<String, Symbol> lambdaArguments)
        {
            if (!(expression instanceof io.trino.spi.expression.Call call)) {
                return Optional.empty();
            }

            if (!call.getFunctionName().equals(ARRAY_CONSTRUCTOR_FUNCTION_NAME)) {
                return Optional.empty();
            }

            return translateExpressions(call.getArguments(), lambdaArguments);
        }

        protected Optional<List<Expression>> translateExpressions(List<ConnectorExpression> expressions, Map<String, Symbol> lambdaArguments)
        {
            ImmutableList.Builder<Expression> translatedExpressions = ImmutableList.builderWithExpectedSize(expressions.size());
            for (ConnectorExpression expression : expressions) {
                Optional<Expression> translated = translate(expression, lambdaArguments);
                if (translated.isEmpty()) {
                    return Optional.empty();
                }
                translatedExpressions.add(translated.get());
            }

            return Optional.of(translatedExpressions.build());
        }

        private Expression castIfNecessary(Expression expression, Type type)
        {
            if (expression.type().equals(type)) {
                return expression;
            }

            return cast(plannerContext.getTypeManager(), expression, type);
        }
    }

    public static class SqlToConnectorExpressionTranslator
            extends IrVisitor<Optional<ConnectorExpression>, SqlToConnectorExpressionTranslator.Context>
    {
        private final Session session;

        public SqlToConnectorExpressionTranslator(Session session)
        {
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        public Optional<ConnectorExpression> process(Expression node)
        {
            return process(node, Context.empty());
        }

        @Override
        protected Optional<ConnectorExpression> visitReference(Reference node, Context context)
        {
            ConnectorExpression binding = context.bindings().get(Symbol.from(node));
            if (binding != null) {
                return Optional.of(binding);
            }
            return Optional.of(new Variable(node.name(), ((Expression) node).type()));
        }

        @Override
        protected Optional<ConnectorExpression> visitConstant(Constant node, Context context)
        {
            return Optional.of(constantFor(node.type(), node.value()));
        }

        @Override
        protected Optional<ConnectorExpression> visitLogical(Logical node, Context context)
        {
            if (!isComplexExpressionPushdown(session)) {
                return Optional.empty();
            }

            io.trino.spi.expression.Constant uselessArgument = switch (node.operator()) {
                case AND -> io.trino.spi.expression.Constant.TRUE;
                case OR -> io.trino.spi.expression.Constant.FALSE;
            };

            ImmutableList.Builder<ConnectorExpression> arguments = ImmutableList.builderWithExpectedSize(node.terms().size());
            for (Expression argument : node.terms()) {
                Optional<ConnectorExpression> translated = process(argument, context);
                if (translated.isEmpty()) {
                    return Optional.empty();
                }
                // Skip useless components.
                if (translated.get().equals(uselessArgument)) {
                    continue;
                }
                arguments.add(translated.get());
            }
            return switch (node.operator()) {
                case AND -> Optional.of(new io.trino.spi.expression.Call(BOOLEAN, AND_FUNCTION_NAME, arguments.build()));
                case OR -> Optional.of(new io.trino.spi.expression.Call(BOOLEAN, OR_FUNCTION_NAME, arguments.build()));
            };
        }

        @Override
        protected Optional<ConnectorExpression> visitLet(Let node, Context context)
        {
            if (!isComplexExpressionPushdown(session)) {
                return Optional.empty();
            }
            // Only the Let-wrapped forms are translated to their function-call equivalents.
            // The trivial forms (AND of comparisons / a Case expression on a trivial value)
            // already route through `visitLogical` / `visitCase` and are pushed as their plain
            // shape, since duplicating a Reference or Constant on the connector side is harmless.
            IrExpressions.Between between = matchBetween(node);
            if (between != null) {
                Optional<ConnectorExpression> translated = process(between.value(), context).flatMap(value ->
                        process(between.min(), context).flatMap(min ->
                                process(between.max(), context).map(max ->
                                        new io.trino.spi.expression.Call(BOOLEAN, BETWEEN_FUNCTION_NAME, ImmutableList.of(value, min, max)))));
                if (translated.isPresent()) {
                    return translated;
                }
            }
            IrExpressions.NullIf nullIf = matchNullIf(node);
            if (nullIf == null) {
                return Optional.empty();
            }
            return translateNullIfPattern(nullIf, node.type(), context);
        }

        private Optional<ConnectorExpression> translateComparison(Type type, Comparison comparison, Context context)
        {
            return process(comparison.left(), context).flatMap(left -> process(comparison.right(), context).map(right ->
                    new io.trino.spi.expression.Call(type, functionNameForComparisonOperator(comparison.operator()), ImmutableList.of(left, right))));
        }

        protected Optional<ConnectorExpression> translateNegation(Call node, Context context)
        {
            return process(node.arguments().getFirst(), context)
                    .map(value -> new io.trino.spi.expression.Call(node.type(), NEGATE_FUNCTION_NAME, ImmutableList.of(value)));
        }

        @Override
        protected Optional<ConnectorExpression> visitCast(Cast node, Context context)
        {
            if (isSpecialType(((Expression) node).type())) {
                // We don't want to expose some internal types to connectors.
                // These should be constant-folded (if appropriate) separately and
                // handled by the regular visitConstant path
                return Optional.empty();
            }

            if (!isComplexExpressionPushdown(session)) {
                return Optional.empty();
            }

            Optional<ConnectorExpression> translatedExpression = process(node.expression(), context);
            if (translatedExpression.isPresent()) {
                return Optional.of(new io.trino.spi.expression.Call(node.type(), CAST_FUNCTION_NAME, List.of(translatedExpression.get())));
            }

            return Optional.empty();
        }

        @Override
        protected Optional<ConnectorExpression> visitCall(Call node, Context context)
        {
            if (!isComplexExpressionPushdown(session)) {
                return Optional.empty();
            }

            if (matchComparison(node) instanceof Comparison comparison) {
                return translateComparison(node.type(), comparison, context);
            }

            CatalogSchemaFunctionName functionName = node.function().name();
            checkArgument(!isDynamicFilterFunction(functionName), "Dynamic filter has no meaning for a connector, it should not be translated into ConnectorExpression");

            if (functionName.equals(builtinFunctionName(LIKE_FUNCTION_NAME))) {
                return translateLike(node, context);
            }
            else if (functionName.equals(builtinFunctionName(NEGATION))) {
                return translateNegation(node, context);
            }
            else if (functionName.equals(builtinFunctionName(ADD))) {
                return process(node.arguments().get(0), context).flatMap(left -> process(node.arguments().get(1), context).map(right ->
                        new io.trino.spi.expression.Call(node.type(), ADD_FUNCTION_NAME, ImmutableList.of(left, right))));
            }
            else if (functionName.equals(builtinFunctionName(SUBTRACT))) {
                return process(node.arguments().get(0), context).flatMap(left -> process(node.arguments().get(1), context).map(right ->
                        new io.trino.spi.expression.Call(node.type(), SUBTRACT_FUNCTION_NAME, ImmutableList.of(left, right))));
            }
            else if (functionName.equals(builtinFunctionName(MULTIPLY))) {
                return process(node.arguments().get(0), context).flatMap(left -> process(node.arguments().get(1), context).map(right ->
                        new io.trino.spi.expression.Call(node.type(), MULTIPLY_FUNCTION_NAME, ImmutableList.of(left, right))));
            }
            else if (functionName.equals(builtinFunctionName(DIVIDE))) {
                return process(node.arguments().get(0), context).flatMap(left -> process(node.arguments().get(1), context).map(right ->
                        new io.trino.spi.expression.Call(node.type(), DIVIDE_FUNCTION_NAME, ImmutableList.of(left, right))));
            }
            else if (functionName.equals(builtinFunctionName(MODULO))) {
                return process(node.arguments().get(0), context).flatMap(left -> process(node.arguments().get(1), context).map(right ->
                        new io.trino.spi.expression.Call(node.type(), MODULO_FUNCTION_NAME, ImmutableList.of(left, right))));
            }

            ImmutableList.Builder<ConnectorExpression> arguments = ImmutableList.builder();
            for (Expression argumentExpression : node.arguments()) {
                Optional<ConnectorExpression> argument = process(argumentExpression, context);
                if (argument.isEmpty()) {
                    return Optional.empty();
                }
                arguments.add(argument.get());
            }

            FunctionName name;
            if (isInlineFunction(functionName)) {
                return Optional.empty();
            }
            if (isBuiltinFunctionName(functionName)) {
                name = new FunctionName(functionName.functionName());
            }
            else {
                name = new FunctionName(Optional.of(new CatalogSchemaName(functionName.catalogName(), functionName.schemaName())), functionName.functionName());
            }
            return Optional.of(new io.trino.spi.expression.Call(((Expression) node).type(), name, arguments.build()));
        }

        @Override
        protected Optional<ConnectorExpression> visitLambda(Lambda node, Context context)
        {
            ImmutableList.Builder<Variable> arguments = ImmutableList.builderWithExpectedSize(node.arguments().size());
            for (Symbol argument : node.arguments()) {
                arguments.add(new Variable(argument.name(), argument.type()));
            }

            Optional<ConnectorExpression> body = process(node.body(), context.withoutBindings(node.arguments()));
            return body.map(expression -> new io.trino.spi.expression.Lambda(node.type(), arguments.build(), expression));
        }

        @Override
        protected Optional<ConnectorExpression> visitBind(Bind node, Context context)
        {
            ImmutableMap.Builder<Symbol, ConnectorExpression> bindings = ImmutableMap.builder();
            for (int i = 0; i < node.values().size(); i++) {
                Optional<ConnectorExpression> value = process(node.values().get(i), context);
                if (value.isEmpty()) {
                    return Optional.empty();
                }
                bindings.put(node.function().arguments().get(i), value.get());
            }

            List<Symbol> arguments = node.function().arguments().subList(node.values().size(), node.function().arguments().size());
            List<Variable> connectorArguments = new ArrayList<>();
            for (Symbol argument : arguments) {
                connectorArguments.add(new Variable(argument.name(), argument.type()));
            }

            Context lambdaContext = context.withBindings(bindings.buildOrThrow()).withoutBindings(arguments);
            Optional<ConnectorExpression> body = process(node.function().body(), lambdaContext);
            return body.map(expression ->
                    new io.trino.spi.expression.Lambda(node.type(), connectorArguments, expression));
        }

        private Optional<ConnectorExpression> translateLike(Call node, Context context)
        {
            // we need special handling for LIKE because within the engine IR a LIKE expression
            // is modeled as $like(value, $like_pattern(pattern, escape)) and we want
            // to expose it to connectors as if if were $like(value, pattern, escape)
            ImmutableList.Builder<ConnectorExpression> arguments = ImmutableList.builder();

            Optional<ConnectorExpression> value = process(node.arguments().get(0), context);
            if (value.isEmpty()) {
                return Optional.empty();
            }
            arguments.add(value.get());

            Expression patternArgument = node.arguments().get(1);
            if (patternArgument instanceof Constant constant) {
                LikePattern matcher = (LikePattern) constant.value();

                arguments.add(new io.trino.spi.expression.Constant(Slices.utf8Slice(matcher.getPattern()), createVarcharType(matcher.getPattern().length())));
                if (matcher.getEscape().isPresent()) {
                    arguments.add(new io.trino.spi.expression.Constant(Slices.utf8Slice(matcher.getEscape().get().toString()), createVarcharType(1)));
                }
            }
            else if (patternArgument instanceof Call call && call.function().name().equals(builtinFunctionName(LIKE_PATTERN_FUNCTION_NAME))) {
                Optional<ConnectorExpression> translatedPattern = process(call.arguments().get(0), context);
                if (translatedPattern.isEmpty()) {
                    return Optional.empty();
                }
                arguments.add(translatedPattern.get());

                if (call.arguments().size() == 2) {
                    Optional<ConnectorExpression> translatedEscape = process(call.arguments().get(1), context);
                    if (translatedEscape.isEmpty()) {
                        return Optional.empty();
                    }
                    arguments.add(translatedEscape.get());
                }
            }
            else {
                return Optional.empty();
            }

            return Optional.of(new io.trino.spi.expression.Call(node.type(), StandardFunctions.LIKE_FUNCTION_NAME, arguments.build()));
        }

        @Override
        protected Optional<ConnectorExpression> visitIsNull(IsNull node, Context context)
        {
            Optional<ConnectorExpression> translatedValue = process(node.value(), context);
            if (translatedValue.isPresent()) {
                return Optional.of(new io.trino.spi.expression.Call(BOOLEAN, IS_NULL_FUNCTION_NAME, ImmutableList.of(translatedValue.get())));
            }
            return Optional.empty();
        }

        private boolean isSpecialType(Type type)
        {
            return type.equals(JONI_REGEXP) ||
                    type instanceof Re2JRegexpType ||
                    type instanceof JsonPathType;
        }

        private ConnectorExpression constantFor(Type type, Object value)
        {
            if (type == JONI_REGEXP) {
                Slice pattern = ((JoniRegexp) value).pattern();
                return new io.trino.spi.expression.Constant(pattern, createVarcharType(countCodePoints(pattern)));
            }
            if (type instanceof Re2JRegexpType) {
                Slice pattern = Slices.utf8Slice(((Re2JRegexp) value).pattern());
                return new io.trino.spi.expression.Constant(pattern, createVarcharType(countCodePoints(pattern)));
            }
            if (type instanceof JsonPathType) {
                Slice pattern = Slices.utf8Slice(((JsonPath) value).pattern());
                return new io.trino.spi.expression.Constant(pattern, createVarcharType(countCodePoints(pattern)));
            }
            return new io.trino.spi.expression.Constant(value, type);
        }

        @Override
        protected Optional<ConnectorExpression> visitCase(Case node, Context context)
        {
            if (!isComplexExpressionPushdown(session)) {
                return Optional.empty();
            }
            // Generic Case isn't translated; only the trivial NULLIF shape
            // (`if(first = second) then null else first`) is recognized so it can be pushed as
            // `$nullif`. The Let-wrapped NULLIF is handled by `visitLet`.
            IrExpressions.NullIf nullIf = matchNullIf(node);
            if (nullIf == null) {
                return Optional.empty();
            }
            return translateNullIfPattern(nullIf, ((Expression) node).type(), context);
        }

        private Optional<ConnectorExpression> translateNullIfPattern(IrExpressions.NullIf pattern, Type type, Context context)
        {
            return process(pattern.first(), context).flatMap(first ->
                    process(pattern.second(), context).map(second ->
                            new io.trino.spi.expression.Call(type, NULLIF_FUNCTION_NAME, ImmutableList.of(first, second))));
        }

        @Override
        protected Optional<ConnectorExpression> visitCoalesce(Coalesce node, Context context)
        {
            if (!isComplexExpressionPushdown(session)) {
                return Optional.empty();
            }

            ImmutableList.Builder<ConnectorExpression> arguments = ImmutableList.builderWithExpectedSize(node.operands().size());
            for (Expression operand : node.operands()) {
                Optional<ConnectorExpression> translated = process(operand, context);
                if (translated.isEmpty()) {
                    return Optional.empty();
                }
                arguments.add(translated.get());
            }
            return Optional.of(new io.trino.spi.expression.Call(node.type(), COALESCE_FUNCTION_NAME, arguments.build()));
        }

        @Override
        protected Optional<ConnectorExpression> visitFieldReference(FieldReference node, Context context)
        {
            if (!(node.base().type() instanceof RowType)) {
                return Optional.empty();
            }

            Optional<ConnectorExpression> translatedBase = process(node.base(), context);
            if (translatedBase.isEmpty()) {
                return Optional.empty();
            }

            return Optional.of(new FieldDereference(((Expression) node).type(), translatedBase.get(), node.field()));
        }

        @Override
        protected Optional<ConnectorExpression> visitIn(In node, Context context)
        {
            Optional<ConnectorExpression> valueExpression = process(node.value(), context);

            if (valueExpression.isEmpty()) {
                return Optional.empty();
            }

            ImmutableList.Builder<ConnectorExpression> values = ImmutableList.builderWithExpectedSize(node.valueList().size());
            for (Expression value : node.valueList()) {
                // TODO: NULL should be eliminated on the engine side (within a rule)
                if (value == null) {
                    return Optional.empty();
                }

                Optional<ConnectorExpression> processedValue = process(value, context);

                if (processedValue.isEmpty()) {
                    return Optional.empty();
                }

                values.add(processedValue.get());
            }

            ConnectorExpression arrayExpression = new io.trino.spi.expression.Call(new ArrayType(node.value().type()), ARRAY_CONSTRUCTOR_FUNCTION_NAME, values.build());
            return Optional.of(new io.trino.spi.expression.Call(((Expression) node).type(), IN_PREDICATE_FUNCTION_NAME, List.of(valueExpression.get(), arrayExpression)));
        }

        @Override
        protected Optional<ConnectorExpression> visitExpression(Expression node, Context context)
        {
            return Optional.empty();
        }

        protected record Context(Map<Symbol, ConnectorExpression> bindings)
        {
            public Context
            {
                bindings = ImmutableMap.copyOf(requireNonNull(bindings, "bindings is null"));
            }

            public static Context empty()
            {
                return new Context(Map.of());
            }

            public Context withBindings(Map<Symbol, ConnectorExpression> bindings)
            {
                return new Context(ImmutableMap.<Symbol, ConnectorExpression>builder()
                        .putAll(this.bindings)
                        .putAll(bindings)
                        .buildKeepingLast());
            }

            public Context withoutBindings(List<Symbol> arguments)
            {
                Map<Symbol, ConnectorExpression> newBindings = new HashMap<>(bindings);
                arguments.forEach(newBindings::remove);
                return new Context(newBindings);
            }
        }
    }
}
