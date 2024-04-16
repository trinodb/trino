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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.scalar.JsonPath;
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
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IrVisitor;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Not;
import io.trino.sql.ir.NullIf;
import io.trino.sql.ir.Reference;
import io.trino.sql.tree.QualifiedName;
import io.trino.type.JoniRegexp;
import io.trino.type.JsonPathType;
import io.trino.type.LikePattern;
import io.trino.type.Re2JRegexp;
import io.trino.type.Re2JRegexpType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.trino.SystemSessionProperties.isComplexExpressionPushdown;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.metadata.GlobalFunctionCatalog.isBuiltinFunctionName;
import static io.trino.metadata.LanguageFunctionManager.isInlineFunction;
import static io.trino.spi.expression.StandardFunctions.ADD_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.DIVIDE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_NULL_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.MODULUS_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.MULTIPLY_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NEGATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NULLIF_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.SUBTRACT_FUNCTION_NAME;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.function.OperatorType.MODULUS;
import static io.trino.spi.function.OperatorType.MULTIPLY;
import static io.trino.spi.function.OperatorType.NEGATION;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.DynamicFilters.isDynamicFilterFunction;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.type.JoniRegexpType.JONI_REGEXP;
import static io.trino.type.LikeFunctions.LIKE_FUNCTION_NAME;
import static io.trino.type.LikeFunctions.LIKE_PATTERN_FUNCTION_NAME;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static java.util.Objects.requireNonNull;

public final class ConnectorExpressionTranslator
{
    private ConnectorExpressionTranslator() {}

    public static Expression translate(Session session, ConnectorExpression expression, PlannerContext plannerContext, Map<String, Symbol> variableMappings)
    {
        return new ConnectorToSqlExpressionTranslator(session, plannerContext, variableMappings)
                .translate(expression)
                .orElseThrow(() -> new UnsupportedOperationException("Expression is not supported: " + expression.toString()));
    }

    public static Optional<ConnectorExpression> translate(Session session, Expression expression)
    {
        return new SqlToConnectorExpressionTranslator(session)
                .process(expression);
    }

    public static ConnectorExpressionTranslation translateConjuncts(
            Session session,
            Expression expression)
    {
        SqlToConnectorExpressionTranslator translator = new SqlToConnectorExpressionTranslator(session);

        List<Expression> conjuncts = extractConjuncts(expression);
        List<Expression> remaining = new ArrayList<>();
        List<ConnectorExpression> converted = new ArrayList<>(conjuncts.size());
        for (Expression conjunct : conjuncts) {
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
    static FunctionName functionNameForComparisonOperator(Comparison.Operator operator)
    {
        return switch (operator) {
            case EQUAL -> EQUAL_OPERATOR_FUNCTION_NAME;
            case NOT_EQUAL -> NOT_EQUAL_OPERATOR_FUNCTION_NAME;
            case LESS_THAN -> LESS_THAN_OPERATOR_FUNCTION_NAME;
            case LESS_THAN_OR_EQUAL -> LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
            case GREATER_THAN -> GREATER_THAN_OPERATOR_FUNCTION_NAME;
            case GREATER_THAN_OR_EQUAL -> GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
            case IS_DISTINCT_FROM -> IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME;
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

        public ConnectorToSqlExpressionTranslator(Session session, PlannerContext plannerContext, Map<String, Symbol> variableMappings)
        {
            this.session = requireNonNull(session, "session is null");
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.variableMappings = requireNonNull(variableMappings, "variableMappings is null");
        }

        public Optional<Expression> translate(ConnectorExpression expression)
        {
            if (expression instanceof Variable variable) {
                String name = variable.getName();
                return Optional.of(variableMappings.get(name).toSymbolReference());
            }

            if (expression instanceof io.trino.spi.expression.Constant constant) {
                return Optional.of(new Constant(constant.getType(), constant.getValue()));
            }

            if (expression instanceof FieldDereference dereference) {
                return translate(dereference.getTarget())
                        .map(base -> new FieldReference(base, dereference.getField()));
            }

            if (expression instanceof io.trino.spi.expression.Call call) {
                return translateCall(call);
            }

            return Optional.empty();
        }

        protected Optional<Expression> translateCall(io.trino.spi.expression.Call call)
        {
            if (call.getFunctionName().getCatalogSchema().isPresent()) {
                CatalogSchemaName catalogSchemaName = call.getFunctionName().getCatalogSchema().get();
                checkArgument(!catalogSchemaName.getCatalogName().equals(GlobalSystemConnector.NAME), "System functions must not be fully qualified");
                // this uses allow allow all access control because connector expressions are not allowed access any function
                ResolvedFunction resolved = plannerContext.getFunctionResolver().resolveFunction(
                        session,
                        QualifiedName.of(catalogSchemaName.getCatalogName(), catalogSchemaName.getSchemaName(), call.getFunctionName().getName()),
                        fromTypes(call.getArguments().stream().map(ConnectorExpression::getType).collect(toImmutableList())),
                        new AllowAllAccessControl());

                return translateCall(call.getFunctionName().getName(), resolved, call.getArguments());
            }

            if (AND_FUNCTION_NAME.equals(call.getFunctionName())) {
                return translateLogicalExpression(Logical.Operator.AND, call.getArguments());
            }
            if (OR_FUNCTION_NAME.equals(call.getFunctionName())) {
                return translateLogicalExpression(Logical.Operator.OR, call.getArguments());
            }
            if (NOT_FUNCTION_NAME.equals(call.getFunctionName()) && call.getArguments().size() == 1) {
                ConnectorExpression expression = getOnlyElement(call.getArguments());

                if (expression instanceof io.trino.spi.expression.Call innerCall) {
                    if (innerCall.getFunctionName().equals(IS_NULL_FUNCTION_NAME) && innerCall.getArguments().size() == 1) {
                        return translateIsNotNull(innerCall.getArguments().get(0));
                    }
                }

                return translateNot(expression);
            }
            if (IS_NULL_FUNCTION_NAME.equals(call.getFunctionName()) && call.getArguments().size() == 1) {
                return translateIsNull(call.getArguments().get(0));
            }

            if (NULLIF_FUNCTION_NAME.equals(call.getFunctionName()) && call.getArguments().size() == 2) {
                return translateNullIf(call.getArguments().get(0), call.getArguments().get(1));
            }
            if (CAST_FUNCTION_NAME.equals(call.getFunctionName()) && call.getArguments().size() == 1) {
                return translateCast(call.getType(), call.getArguments().get(0));
            }

            // comparisons
            if (call.getArguments().size() == 2) {
                Optional<Comparison.Operator> operator = comparisonOperatorForFunctionName(call.getFunctionName());
                if (operator.isPresent()) {
                    return translateComparison(operator.get(), call.getArguments().get(0), call.getArguments().get(1));
                }
            }

            // arithmetic binary
            if (call.getArguments().size() == 2) {
                Optional<OperatorType> operator = arithmeticBinaryOperatorForFunctionName(call.getFunctionName());
                if (operator.isPresent()) {
                    return translateArithmeticBinary(operator.get(), call.getArguments().get(0), call.getArguments().get(1));
                }
            }

            // arithmetic unary
            if (NEGATE_FUNCTION_NAME.equals(call.getFunctionName()) && call.getArguments().size() == 1) {
                ConnectorExpression argument = getOnlyElement(call.getArguments());
                ResolvedFunction function = plannerContext.getMetadata().resolveOperator(NEGATION, ImmutableList.of(argument.getType()));
                return translate(argument).map(value -> new Call(function, ImmutableList.of(value)));
            }

            if (StandardFunctions.LIKE_FUNCTION_NAME.equals(call.getFunctionName())) {
                return switch (call.getArguments().size()) {
                    case 2 -> translateLike(call.getArguments().get(0), call.getArguments().get(1), Optional.empty());
                    case 3 -> translateLike(call.getArguments().get(0), call.getArguments().get(1), Optional.of(call.getArguments().get(2)));
                    default -> Optional.empty();
                };
            }

            if (IN_PREDICATE_FUNCTION_NAME.equals(call.getFunctionName()) && call.getArguments().size() == 2) {
                return translateInPredicate(call.getArguments().get(0), call.getArguments().get(1));
            }

            ResolvedFunction resolved = plannerContext.getMetadata().resolveBuiltinFunction(
                    call.getFunctionName().getName(),
                    fromTypes(call.getArguments().stream().map(ConnectorExpression::getType).collect(toImmutableList())));

            return translateCall(call.getFunctionName().getName(), resolved, call.getArguments());
        }

        private Optional<Expression> translateCall(String functionName, ResolvedFunction resolved, List<ConnectorExpression> arguments)
        {
            ResolvedFunctionCallBuilder builder = ResolvedFunctionCallBuilder.builder(resolved);
            for (int i = 0; i < arguments.size(); i++) {
                ConnectorExpression argument = arguments.get(i);
                Type formalType = resolved.signature().getArgumentTypes().get(i);
                Type argumentType = argument.getType();
                Optional<Expression> translated = translate(argument);
                if (translated.isEmpty()) {
                    return Optional.empty();
                }
                Expression expression = translated.get();
                if ((formalType == JONI_REGEXP || formalType instanceof Re2JRegexpType || formalType instanceof JsonPathType)
                        && argumentType instanceof VarcharType) {
                    // These types are not used in connector expressions, so require special handling when translating back to expressions.
                    expression = new Cast(expression, formalType);
                }
                else if (!argumentType.equals(formalType)) {
                    // There are no implicit coercions in connector expressions except for engine types that are not exposed in connector expressions.
                    throw new IllegalArgumentException("Unexpected type %s for argument %s of type %s of %s".formatted(argumentType, formalType, i, functionName));
                }
                builder.addArgument(expression);
            }
            return Optional.of(builder.build());
        }

        private Optional<Expression> translateIsNotNull(ConnectorExpression argument)
        {
            Optional<Expression> translatedArgument = translate(argument);
            if (translatedArgument.isPresent()) {
                return Optional.of(new Not(new IsNull(translatedArgument.get())));
            }

            return Optional.empty();
        }

        private Optional<Expression> translateIsNull(ConnectorExpression argument)
        {
            Optional<Expression> translatedArgument = translate(argument);
            if (translatedArgument.isPresent()) {
                return Optional.of(new IsNull(translatedArgument.get()));
            }

            return Optional.empty();
        }

        private Optional<Expression> translateNot(ConnectorExpression argument)
        {
            Optional<Expression> translatedArgument = translate(argument);
            if (argument.getType().equals(BOOLEAN) && translatedArgument.isPresent()) {
                return Optional.of(new Not(translatedArgument.get()));
            }
            return Optional.empty();
        }

        private Optional<Expression> translateCast(Type type, ConnectorExpression expression)
        {
            Optional<Expression> translatedExpression = translate(expression);

            if (translatedExpression.isPresent()) {
                return Optional.of(new Cast(translatedExpression.get(), type));
            }

            return Optional.empty();
        }

        private Optional<Expression> translateLogicalExpression(Logical.Operator operator, List<ConnectorExpression> arguments)
        {
            Optional<List<Expression>> translatedArguments = translateExpressions(arguments);
            return translatedArguments.map(expressions -> new Logical(operator, expressions));
        }

        private Optional<Expression> translateComparison(Comparison.Operator operator, ConnectorExpression left, ConnectorExpression right)
        {
            return translate(left).flatMap(leftTranslated ->
                    translate(right).map(rightTranslated ->
                            new Comparison(operator, leftTranslated, rightTranslated)));
        }

        private Optional<Expression> translateNullIf(ConnectorExpression first, ConnectorExpression second)
        {
            Optional<Expression> firstExpression = translate(first);
            Optional<Expression> secondExpression = translate(second);
            if (firstExpression.isPresent() && secondExpression.isPresent()) {
                return Optional.of(new NullIf(firstExpression.get(), secondExpression.get()));
            }

            return Optional.empty();
        }

        private Optional<Comparison.Operator> comparisonOperatorForFunctionName(FunctionName functionName)
        {
            if (EQUAL_OPERATOR_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(Comparison.Operator.EQUAL);
            }
            if (NOT_EQUAL_OPERATOR_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(Comparison.Operator.NOT_EQUAL);
            }
            if (LESS_THAN_OPERATOR_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(Comparison.Operator.LESS_THAN);
            }
            if (LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(Comparison.Operator.LESS_THAN_OR_EQUAL);
            }
            if (GREATER_THAN_OPERATOR_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(Comparison.Operator.GREATER_THAN);
            }
            if (GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(Comparison.Operator.GREATER_THAN_OR_EQUAL);
            }
            if (IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(Comparison.Operator.IS_DISTINCT_FROM);
            }
            return Optional.empty();
        }

        private Optional<Expression> translateArithmeticBinary(OperatorType operator, ConnectorExpression left, ConnectorExpression right)
        {
            ResolvedFunction function = plannerContext.getMetadata().resolveOperator(operator, ImmutableList.of(left.getType(), right.getType()));

            return translate(left).flatMap(leftTranslated ->
                    translate(right).map(rightTranslated ->
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
            if (MODULUS_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(MODULUS);
            }
            return Optional.empty();
        }

        protected Optional<Expression> translateLike(ConnectorExpression value, ConnectorExpression pattern, Optional<ConnectorExpression> escape)
        {
            Optional<Expression> translatedValue = translate(value);
            Optional<Expression> translatedPattern = translate(pattern);

            if (translatedValue.isPresent() && translatedPattern.isPresent()) {
                Call patternCall;
                if (escape.isPresent()) {
                    Optional<Expression> translatedEscape = translate(escape.get());
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

        protected Optional<Expression> translateInPredicate(ConnectorExpression value, ConnectorExpression values)
        {
            Optional<Expression> translatedValue = translate(value);
            Optional<List<Expression>> translatedValues = extractExpressionsFromArrayCall(values);

            if (translatedValue.isPresent() && translatedValues.isPresent()) {
                return Optional.of(new In(translatedValue.get(), translatedValues.get()));
            }

            return Optional.empty();
        }

        protected Optional<List<Expression>> extractExpressionsFromArrayCall(ConnectorExpression expression)
        {
            if (!(expression instanceof io.trino.spi.expression.Call call)) {
                return Optional.empty();
            }

            if (!call.getFunctionName().equals(ARRAY_CONSTRUCTOR_FUNCTION_NAME)) {
                return Optional.empty();
            }

            return translateExpressions(call.getArguments());
        }

        protected Optional<List<Expression>> translateExpressions(List<ConnectorExpression> expressions)
        {
            ImmutableList.Builder<Expression> translatedExpressions = ImmutableList.builderWithExpectedSize(expressions.size());
            for (ConnectorExpression expression : expressions) {
                Optional<Expression> translated = translate(expression);
                if (translated.isEmpty()) {
                    return Optional.empty();
                }
                translatedExpressions.add(translated.get());
            }

            return Optional.of(translatedExpressions.build());
        }

        private static Expression castIfNecessary(Expression expression, Type type)
        {
            if (expression.type().equals(type)) {
                return expression;
            }

            return new Cast(expression, type);
        }
    }

    public static class SqlToConnectorExpressionTranslator
            extends IrVisitor<Optional<ConnectorExpression>, Void>
    {
        private final Session session;

        public SqlToConnectorExpressionTranslator(Session session)
        {
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        protected Optional<ConnectorExpression> visitReference(Reference node, Void context)
        {
            return Optional.of(new Variable(node.name(), ((Expression) node).type()));
        }

        @Override
        protected Optional<ConnectorExpression> visitConstant(Constant node, Void context)
        {
            return Optional.of(constantFor(node.type(), node.value()));
        }

        @Override
        protected Optional<ConnectorExpression> visitLogical(Logical node, Void context)
        {
            if (!isComplexExpressionPushdown(session)) {
                return Optional.empty();
            }

            ImmutableList.Builder<ConnectorExpression> arguments = ImmutableList.builderWithExpectedSize(node.terms().size());
            for (Expression argument : node.terms()) {
                Optional<ConnectorExpression> translated = process(argument);
                if (translated.isEmpty()) {
                    return Optional.empty();
                }
                arguments.add(translated.get());
            }
            return switch (node.operator()) {
                case AND -> Optional.of(new io.trino.spi.expression.Call(BOOLEAN, AND_FUNCTION_NAME, arguments.build()));
                case OR -> Optional.of(new io.trino.spi.expression.Call(BOOLEAN, OR_FUNCTION_NAME, arguments.build()));
            };
        }

        @Override
        protected Optional<ConnectorExpression> visitComparison(Comparison node, Void context)
        {
            if (!isComplexExpressionPushdown(session)) {
                return Optional.empty();
            }

            return process(node.left()).flatMap(left -> process(node.right()).map(right ->
                    new io.trino.spi.expression.Call(((Expression) node).type(), functionNameForComparisonOperator(node.operator()), ImmutableList.of(left, right))));
        }

        @Override
        protected Optional<ConnectorExpression> visitBetween(Between node, Void context)
        {
            if (!isComplexExpressionPushdown(session)) {
                return Optional.empty();
            }
            return process(node.value()).flatMap(value ->
                    process(node.min()).flatMap(min ->
                            process(node.max()).map(max ->
                                    new io.trino.spi.expression.Call(
                                            BOOLEAN,
                                            AND_FUNCTION_NAME,
                                            ImmutableList.of(
                                                    new io.trino.spi.expression.Call(BOOLEAN, GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(value, min)),
                                                    new io.trino.spi.expression.Call(BOOLEAN, LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ImmutableList.of(value, max)))))));
        }

        protected Optional<ConnectorExpression> translateNegation(Call node)
        {
            return process(node.arguments().getFirst())
                    .map(value -> new io.trino.spi.expression.Call(node.type(), NEGATE_FUNCTION_NAME, ImmutableList.of(value)));
        }

        @Override
        protected Optional<ConnectorExpression> visitCast(Cast node, Void context)
        {
            if (isSpecialType(((Expression) node).type())) {
                // We don't want to expose some internal types to connectors.
                // These should be constant-folded (if appropriate) separately and
                // handled by the regular visitConstant path
                return Optional.empty();
            }

            if (node.safe()) {
                // try_cast would need to be modeled separately
                return Optional.empty();
            }

            if (!isComplexExpressionPushdown(session)) {
                return Optional.empty();
            }

            Optional<ConnectorExpression> translatedExpression = process(node.expression());
            if (translatedExpression.isPresent()) {
                return Optional.of(new io.trino.spi.expression.Call(node.type(), CAST_FUNCTION_NAME, List.of(translatedExpression.get())));
            }

            return Optional.empty();
        }

        @Override
        protected Optional<ConnectorExpression> visitCall(Call node, Void context)
        {
            if (!isComplexExpressionPushdown(session)) {
                return Optional.empty();
            }

            CatalogSchemaFunctionName functionName = node.function().name();
            checkArgument(!isDynamicFilterFunction(functionName), "Dynamic filter has no meaning for a connector, it should not be translated into ConnectorExpression");

            if (functionName.equals(builtinFunctionName(LIKE_FUNCTION_NAME))) {
                return translateLike(node);
            }
            else if (functionName.equals(builtinFunctionName(NEGATION))) {
                return translateNegation(node);
            }
            else if (functionName.equals(builtinFunctionName(ADD))) {
                return process(node.arguments().get(0)).flatMap(left -> process(node.arguments().get(1)).map(right ->
                        new io.trino.spi.expression.Call(node.type(), ADD_FUNCTION_NAME, ImmutableList.of(left, right))));
            }
            else if (functionName.equals(builtinFunctionName(SUBTRACT))) {
                return process(node.arguments().get(0)).flatMap(left -> process(node.arguments().get(1)).map(right ->
                        new io.trino.spi.expression.Call(node.type(), SUBTRACT_FUNCTION_NAME, ImmutableList.of(left, right))));
            }
            else if (functionName.equals(builtinFunctionName(MULTIPLY))) {
                return process(node.arguments().get(0)).flatMap(left -> process(node.arguments().get(1)).map(right ->
                        new io.trino.spi.expression.Call(node.type(), MULTIPLY_FUNCTION_NAME, ImmutableList.of(left, right))));
            }
            else if (functionName.equals(builtinFunctionName(DIVIDE))) {
                return process(node.arguments().get(0)).flatMap(left -> process(node.arguments().get(1)).map(right ->
                        new io.trino.spi.expression.Call(node.type(), DIVIDE_FUNCTION_NAME, ImmutableList.of(left, right))));
            }
            else if (functionName.equals(builtinFunctionName(MODULUS))) {
                return process(node.arguments().get(0)).flatMap(left -> process(node.arguments().get(1)).map(right ->
                        new io.trino.spi.expression.Call(node.type(), MODULUS_FUNCTION_NAME, ImmutableList.of(left, right))));
            }

            ImmutableList.Builder<ConnectorExpression> arguments = ImmutableList.builder();
            for (Expression argumentExpression : node.arguments()) {
                Optional<ConnectorExpression> argument = process(argumentExpression);
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
                name = new FunctionName(functionName.getFunctionName());
            }
            else {
                name = new FunctionName(Optional.of(new CatalogSchemaName(functionName.getCatalogName(), functionName.getSchemaName())), functionName.getFunctionName());
            }
            return Optional.of(new io.trino.spi.expression.Call(((Expression) node).type(), name, arguments.build()));
        }

        private Optional<ConnectorExpression> translateLike(Call node)
        {
            // we need special handling for LIKE because within the engine IR a LIKE expression
            // is modeled as $like(value, $like_pattern(pattern, escape)) and we want
            // to expose it to connectors as if if were $like(value, pattern, escape)
            ImmutableList.Builder<ConnectorExpression> arguments = ImmutableList.builder();

            Optional<ConnectorExpression> value = process(node.arguments().get(0));
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
                Optional<ConnectorExpression> translatedPattern = process(call.arguments().get(0));
                if (translatedPattern.isEmpty()) {
                    return Optional.empty();
                }
                arguments.add(translatedPattern.get());

                if (call.arguments().size() == 2) {
                    Optional<ConnectorExpression> translatedEscape = process(call.arguments().get(1));
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
        protected Optional<ConnectorExpression> visitIsNull(IsNull node, Void context)
        {
            Optional<ConnectorExpression> translatedValue = process(node.value());
            if (translatedValue.isPresent()) {
                return Optional.of(new io.trino.spi.expression.Call(BOOLEAN, IS_NULL_FUNCTION_NAME, ImmutableList.of(translatedValue.get())));
            }
            return Optional.empty();
        }

        @Override
        protected Optional<ConnectorExpression> visitNot(Not node, Void context)
        {
            Optional<ConnectorExpression> translatedValue = process(node.value());
            if (translatedValue.isPresent()) {
                return Optional.of(new io.trino.spi.expression.Call(BOOLEAN, NOT_FUNCTION_NAME, List.of(translatedValue.get())));
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
        protected Optional<ConnectorExpression> visitNullIf(NullIf node, Void context)
        {
            Optional<ConnectorExpression> firstValue = process(node.first());
            Optional<ConnectorExpression> secondValue = process(node.second());
            if (firstValue.isPresent() && secondValue.isPresent()) {
                return Optional.of(new io.trino.spi.expression.Call(((Expression) node).type(), NULLIF_FUNCTION_NAME, ImmutableList.of(firstValue.get(), secondValue.get())));
            }
            return Optional.empty();
        }

        @Override
        protected Optional<ConnectorExpression> visitFieldReference(FieldReference node, Void context)
        {
            if (!(node.base().type() instanceof RowType)) {
                return Optional.empty();
            }

            Optional<ConnectorExpression> translatedBase = process(node.base());
            if (translatedBase.isEmpty()) {
                return Optional.empty();
            }

            return Optional.of(new FieldDereference(((Expression) node).type(), translatedBase.get(), node.field()));
        }

        @Override
        protected Optional<ConnectorExpression> visitIn(In node, Void context)
        {
            Optional<ConnectorExpression> valueExpression = process(node.value());

            if (valueExpression.isEmpty()) {
                return Optional.empty();
            }

            ImmutableList.Builder<ConnectorExpression> values = ImmutableList.builderWithExpectedSize(node.valueList().size());
            for (Expression value : node.valueList()) {
                // TODO: NULL should be eliminated on the engine side (within a rule)
                if (value == null) {
                    return Optional.empty();
                }

                Optional<ConnectorExpression> processedValue = process(value);

                if (processedValue.isEmpty()) {
                    return Optional.empty();
                }

                values.add(processedValue.get());
            }

            ConnectorExpression arrayExpression = new io.trino.spi.expression.Call(new ArrayType(node.value().type()), ARRAY_CONSTRUCTOR_FUNCTION_NAME, values.build());
            return Optional.of(new io.trino.spi.expression.Call(((Expression) node).type(), IN_PREDICATE_FUNCTION_NAME, List.of(valueExpression.get(), arrayExpression)));
        }

        @Override
        protected Optional<ConnectorExpression> visitExpression(Expression node, Void context)
        {
            return Optional.empty();
        }
    }
}
