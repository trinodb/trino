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
import io.trino.metadata.LiteralFunction;
import io.trino.metadata.ResolvedFunction;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;
import io.trino.sql.DynamicFilters;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CharLiteral;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.type.JoniRegexp;
import io.trino.type.Re2JRegexp;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.trino.SystemSessionProperties.isComplexExpressionPushdown;
import static io.trino.spi.expression.StandardFunctions.ADD_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.DIVIDE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LIKE_PATTERN_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.MODULUS_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.MULTIPLY_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NEGATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.SUBTRACT_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ExpressionUtils.isEffectivelyLiteral;
import static io.trino.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class ConnectorExpressionTranslator
{
    private ConnectorExpressionTranslator() {}

    public static Expression translate(Session session, ConnectorExpression expression, PlannerContext plannerContext, Map<String, Symbol> variableMappings, LiteralEncoder literalEncoder)
    {
        return new ConnectorToSqlExpressionTranslator(session, plannerContext, literalEncoder, variableMappings)
                .translate(expression)
                .orElseThrow(() -> new UnsupportedOperationException("Expression is not supported: " + expression.toString()));
    }

    public static Optional<ConnectorExpression> translate(Session session, Expression expression, TypeAnalyzer types, TypeProvider inputTypes, PlannerContext plannerContext)
    {
        return new SqlToConnectorExpressionTranslator(session, types.getTypes(session, inputTypes, expression), plannerContext)
                .process(expression);
    }

    @VisibleForTesting
    static FunctionName functionNameForComparisonOperator(ComparisonExpression.Operator operator)
    {
        switch (operator) {
            case EQUAL:
                return EQUAL_OPERATOR_FUNCTION_NAME;
            case NOT_EQUAL:
                return NOT_EQUAL_OPERATOR_FUNCTION_NAME;
            case LESS_THAN:
                return LESS_THAN_OPERATOR_FUNCTION_NAME;
            case LESS_THAN_OR_EQUAL:
                return LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
            case GREATER_THAN:
                return GREATER_THAN_OPERATOR_FUNCTION_NAME;
            case GREATER_THAN_OR_EQUAL:
                return GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
            case IS_DISTINCT_FROM:
                return IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME;
        }
        throw new UnsupportedOperationException("Unsupported operator: " + operator);
    }

    @VisibleForTesting
    static FunctionName functionNameForArithmeticBinaryOperator(ArithmeticBinaryExpression.Operator operator)
    {
        switch (operator) {
            case ADD:
                return ADD_FUNCTION_NAME;
            case SUBTRACT:
                return SUBTRACT_FUNCTION_NAME;
            case MULTIPLY:
                return MULTIPLY_FUNCTION_NAME;
            case DIVIDE:
                return DIVIDE_FUNCTION_NAME;
            case MODULUS:
                return MODULUS_FUNCTION_NAME;
        }
        throw new UnsupportedOperationException("Unsupported operator: " + operator);
    }

    private static class ConnectorToSqlExpressionTranslator
    {
        private final Session session;
        private final PlannerContext plannerContext;
        private final LiteralEncoder literalEncoder;
        private final Map<String, Symbol> variableMappings;

        public ConnectorToSqlExpressionTranslator(Session session, PlannerContext plannerContext, LiteralEncoder literalEncoder, Map<String, Symbol> variableMappings)
        {
            this.session = requireNonNull(session, "session is null");
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.literalEncoder = requireNonNull(literalEncoder, "literalEncoder is null");
            this.variableMappings = requireNonNull(variableMappings, "variableMappings is null");
        }

        public Optional<Expression> translate(ConnectorExpression expression)
        {
            if (expression instanceof Variable) {
                String name = ((Variable) expression).getName();
                return Optional.of(variableMappings.get(name).toSymbolReference());
            }

            if (expression instanceof Constant) {
                return Optional.of(literalEncoder.toExpression(session, ((Constant) expression).getValue(), expression.getType()));
            }

            if (expression instanceof FieldDereference) {
                FieldDereference dereference = (FieldDereference) expression;
                return translate(dereference.getTarget())
                        .map(base -> new SubscriptExpression(base, new LongLiteral(Long.toString(dereference.getField() + 1))));
            }

            if (expression instanceof Call) {
                return translateCall((Call) expression);
            }

            return Optional.empty();
        }

        protected Optional<Expression> translateCall(Call call)
        {
            if (call.getFunctionName().getCatalogSchema().isPresent()) {
                return Optional.empty();
            }

            if (AND_FUNCTION_NAME.equals(call.getFunctionName())) {
                return translateLogicalExpression(LogicalExpression.Operator.AND, call.getArguments());
            }
            if (OR_FUNCTION_NAME.equals(call.getFunctionName())) {
                return translateLogicalExpression(LogicalExpression.Operator.OR, call.getArguments());
            }

            // comparisons
            if (call.getArguments().size() == 2) {
                Optional<ComparisonExpression.Operator> operator = comparisonOperatorForFunctionName(call.getFunctionName());
                if (operator.isPresent()) {
                    return translateComparison(operator.get(), call.getArguments().get(0), call.getArguments().get(1));
                }
            }

            // arithmetic binary
            if (call.getArguments().size() == 2) {
                Optional<ArithmeticBinaryExpression.Operator> operator = arithmeticBinaryOperatorForFunctionName(call.getFunctionName());
                if (operator.isPresent()) {
                    return translateArithmeticBinary(operator.get(), call.getArguments().get(0), call.getArguments().get(1));
                }
            }

            // arithmetic unary
            if (NEGATE_FUNCTION_NAME.equals(call.getFunctionName()) && call.getArguments().size() == 1) {
                return translate(getOnlyElement(call.getArguments())).map(argument -> new ArithmeticUnaryExpression(ArithmeticUnaryExpression.Sign.MINUS, argument));
            }

            if (LIKE_PATTERN_FUNCTION_NAME.equals(call.getFunctionName())) {
                switch (call.getArguments().size()) {
                    case 2:
                        return translateLike(call.getArguments().get(0), call.getArguments().get(1), Optional.empty());
                    case 3:
                        return translateLike(call.getArguments().get(0), call.getArguments().get(1), Optional.of(call.getArguments().get(2)));
                    default:
                        return Optional.empty();
                }
            }

            QualifiedName name = QualifiedName.of(call.getFunctionName().getName());
            List<TypeSignature> argumentTypes = call.getArguments().stream()
                    .map(argument -> argument.getType().getTypeSignature())
                    .collect(toImmutableList());
            ResolvedFunction resolved = plannerContext.getMetadata().resolveFunction(session, name, TypeSignatureProvider.fromTypeSignatures(argumentTypes));

            FunctionCallBuilder builder = FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                    .setName(name);
            for (int i = 0; i < call.getArguments().size(); i++) {
                Type type = resolved.getSignature().getArgumentTypes().get(i);
                Expression expression = ConnectorExpressionTranslator.translate(session, call.getArguments().get(i), plannerContext, variableMappings, literalEncoder);
                builder.addArgument(type, expression);
            }
            return Optional.of(builder.build());
        }

        private Optional<Expression> translateLogicalExpression(LogicalExpression.Operator operator, List<ConnectorExpression> arguments)
        {
            ImmutableList.Builder<Expression> translatedArguments = ImmutableList.builderWithExpectedSize(arguments.size());
            for (ConnectorExpression argument : arguments) {
                Optional<Expression> translated = translate(argument);
                if (translated.isEmpty()) {
                    return Optional.empty();
                }
                translatedArguments.add(translated.get());
            }
            return Optional.of(new LogicalExpression(operator, translatedArguments.build()));
        }

        private Optional<Expression> translateComparison(ComparisonExpression.Operator operator, ConnectorExpression left, ConnectorExpression right)
        {
            return translate(left).flatMap(leftTranslated ->
                    translate(right).map(rightTranslated ->
                            new ComparisonExpression(operator, leftTranslated, rightTranslated)));
        }

        private Optional<ComparisonExpression.Operator> comparisonOperatorForFunctionName(FunctionName functionName)
        {
            if (EQUAL_OPERATOR_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(ComparisonExpression.Operator.EQUAL);
            }
            if (NOT_EQUAL_OPERATOR_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(ComparisonExpression.Operator.NOT_EQUAL);
            }
            if (LESS_THAN_OPERATOR_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(ComparisonExpression.Operator.LESS_THAN);
            }
            if (LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(ComparisonExpression.Operator.LESS_THAN_OR_EQUAL);
            }
            if (GREATER_THAN_OPERATOR_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(ComparisonExpression.Operator.GREATER_THAN);
            }
            if (GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL);
            }
            if (IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(ComparisonExpression.Operator.IS_DISTINCT_FROM);
            }
            return Optional.empty();
        }

        private Optional<Expression> translateArithmeticBinary(ArithmeticBinaryExpression.Operator operator, ConnectorExpression left, ConnectorExpression right)
        {
            return translate(left).flatMap(leftTranslated ->
                    translate(right).map(rightTranslated ->
                            new ArithmeticBinaryExpression(operator, leftTranslated, rightTranslated)));
        }

        private Optional<ArithmeticBinaryExpression.Operator> arithmeticBinaryOperatorForFunctionName(FunctionName functionName)
        {
            if (ADD_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(ArithmeticBinaryExpression.Operator.ADD);
            }
            if (SUBTRACT_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(ArithmeticBinaryExpression.Operator.SUBTRACT);
            }
            if (MULTIPLY_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(ArithmeticBinaryExpression.Operator.MULTIPLY);
            }
            if (DIVIDE_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(ArithmeticBinaryExpression.Operator.DIVIDE);
            }
            if (MODULUS_FUNCTION_NAME.equals(functionName)) {
                return Optional.of(ArithmeticBinaryExpression.Operator.MODULUS);
            }
            return Optional.empty();
        }

        protected Optional<Expression> translateLike(ConnectorExpression value, ConnectorExpression pattern, Optional<ConnectorExpression> escape)
        {
            Optional<Expression> translatedValue = translate(value);
            Optional<Expression> translatedPattern = translate(pattern);

            if (translatedValue.isPresent() && translatedPattern.isPresent()) {
                if (escape.isPresent()) {
                    Optional<Expression> translatedEscape = translate(escape.get());
                    if (translatedEscape.isEmpty()) {
                        return Optional.empty();
                    }

                    return Optional.of(new LikePredicate(translatedValue.get(), translatedPattern.get(), translatedEscape));
                }

                return Optional.of(new LikePredicate(translatedValue.get(), translatedPattern.get(), Optional.empty()));
            }

            return Optional.empty();
        }
    }

    public static class SqlToConnectorExpressionTranslator
            extends AstVisitor<Optional<ConnectorExpression>, Void>
    {
        private final Session session;
        private final Map<NodeRef<Expression>, Type> types;
        private final PlannerContext plannerContext;

        public SqlToConnectorExpressionTranslator(Session session, Map<NodeRef<Expression>, Type> types, PlannerContext plannerContext)
        {
            this.session = requireNonNull(session, "session is null");
            this.types = requireNonNull(types, "types is null");
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        }

        @Override
        protected Optional<ConnectorExpression> visitSymbolReference(SymbolReference node, Void context)
        {
            return Optional.of(new Variable(node.getName(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            return Optional.of(new Constant(node.getValue(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitStringLiteral(StringLiteral node, Void context)
        {
            return Optional.of(new Constant(node.getSlice(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            return Optional.of(new Constant(node.getValue(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitDecimalLiteral(DecimalLiteral node, Void context)
        {
            return Optional.of(new Constant(Decimals.parse(node.getValue()).getObject(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitCharLiteral(CharLiteral node, Void context)
        {
            return Optional.of(new Constant(node.getSlice(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitBinaryLiteral(BinaryLiteral node, Void context)
        {
            return Optional.of(new Constant(node.getValue(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitLongLiteral(LongLiteral node, Void context)
        {
            return Optional.of(new Constant(node.getValue(), typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitNullLiteral(NullLiteral node, Void context)
        {
            return Optional.of(new Constant(null, typeOf(node)));
        }

        @Override
        protected Optional<ConnectorExpression> visitGenericLiteral(GenericLiteral node, Void context)
        {
            if (!isComplexExpressionPushdown(session)) {
                return Optional.empty();
            }
            return Optional.of(constantFor(node));
        }

        @Override
        protected Optional<ConnectorExpression> visitLogicalExpression(LogicalExpression node, Void context)
        {
            if (!isComplexExpressionPushdown(session)) {
                return Optional.empty();
            }

            ImmutableList.Builder<ConnectorExpression> arguments = ImmutableList.builderWithExpectedSize(node.getTerms().size());
            for (Node argument : node.getChildren()) {
                Optional<ConnectorExpression> translated = process(argument);
                if (translated.isEmpty()) {
                    return Optional.empty();
                }
                arguments.add(translated.get());
            }
            switch (node.getOperator()) {
                case AND:
                    return Optional.of(new Call(BOOLEAN, AND_FUNCTION_NAME, arguments.build()));
                case OR:
                    return Optional.of(new Call(BOOLEAN, OR_FUNCTION_NAME, arguments.build()));
            }
            throw new UnsupportedOperationException("Unsupported operator: " + node.getOperator());
        }

        @Override
        protected Optional<ConnectorExpression> visitComparisonExpression(ComparisonExpression node, Void context)
        {
            if (!isComplexExpressionPushdown(session)) {
                return Optional.empty();
            }

            return process(node.getLeft()).flatMap(left -> process(node.getRight()).map(right ->
                    new Call(typeOf(node), functionNameForComparisonOperator(node.getOperator()), ImmutableList.of(left, right))));
        }

        @Override
        protected Optional<ConnectorExpression> visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {
            if (!isComplexExpressionPushdown(session)) {
                return Optional.empty();
            }
            return process(node.getLeft()).flatMap(left -> process(node.getRight()).map(right ->
                    new Call(typeOf(node), functionNameForArithmeticBinaryOperator(node.getOperator()), ImmutableList.of(left, right))));
        }

        @Override
        protected Optional<ConnectorExpression> visitArithmeticUnary(ArithmeticUnaryExpression node, Void context)
        {
            if (!isComplexExpressionPushdown(session)) {
                return Optional.empty();
            }
            switch (node.getSign()) {
                case PLUS:
                    return process(node.getValue());
                case MINUS:
                    return process(node.getValue()).map(value -> new Call(typeOf(node), NEGATE_FUNCTION_NAME, ImmutableList.of(value)));
            }
            throw new UnsupportedOperationException("Unsupported sign: " + node.getSign());
        }

        @Override
        protected Optional<ConnectorExpression> visitCast(Cast node, Void context)
        {
            if (isEffectivelyLiteral(plannerContext, session, node)) {
                return Optional.of(constantFor(node));
            }
            return Optional.empty();
        }

        @Override
        protected Optional<ConnectorExpression> visitFunctionCall(FunctionCall node, Void context)
        {
            if (!isComplexExpressionPushdown(session)) {
                return Optional.empty();
            }

            if (isEffectivelyLiteral(plannerContext, session, node)) {
                return Optional.of(constantFor(node));
            }

            if (node.getFilter().isPresent() || node.getOrderBy().isPresent() || node.getWindow().isPresent() || node.getNullTreatment().isPresent() || node.isDistinct()) {
                return Optional.empty();
            }

            String functionName = ResolvedFunction.extractFunctionName(node.getName());
            checkArgument(!DynamicFilters.Function.NAME.equals(functionName), "Dynamic filter has no meaning for a connector, it should not be translated into ConnectorExpression");

            if (LiteralFunction.LITERAL_FUNCTION_NAME.equalsIgnoreCase(functionName)) {
                Object value = evaluateConstant(node);
                if (value instanceof JoniRegexp) {
                    Slice pattern = ((JoniRegexp) value).pattern();
                    return Optional.of(new Constant(pattern, VarcharType.createVarcharType(countCodePoints(pattern))));
                }
                if (value instanceof Re2JRegexp) {
                    Slice pattern = Slices.utf8Slice(((Re2JRegexp) value).pattern());
                    return Optional.of(new Constant(pattern, VarcharType.createVarcharType(countCodePoints(pattern))));
                }
                return Optional.of(new Constant(value, types.get(NodeRef.of(node))));
            }

            ImmutableList.Builder<ConnectorExpression> arguments = ImmutableList.builder();
            for (Expression argumentExpression : node.getArguments()) {
                Optional<ConnectorExpression> argument = process(argumentExpression);
                if (argument.isEmpty()) {
                    return Optional.empty();
                }
                arguments.add(argument.get());
            }

            // Currently, plugin-provided and runtime-added functions doesn't have a catalog/schema qualifier.
            // TODO Translate catalog/schema qualifier when available.
            FunctionName name = new FunctionName(functionName);
            return Optional.of(new Call(typeOf(node), name, arguments.build()));
        }

        private ConnectorExpression constantFor(Expression node)
        {
            Type type = typeOf(node);
            Object value = evaluateConstantExpression(
                    node,
                    type,
                    plannerContext,
                    session,
                    new AllowAllAccessControl(),
                    ImmutableMap.of());
            return new Constant(value, type);
        }

        @Override
        protected Optional<ConnectorExpression> visitLikePredicate(LikePredicate node, Void context)
        {
            Optional<ConnectorExpression> value = process(node.getValue());
            Optional<ConnectorExpression> pattern = process(node.getPattern());
            if (value.isPresent() && pattern.isPresent()) {
                if (node.getEscape().isEmpty()) {
                    return Optional.of(new Call(typeOf(node), LIKE_PATTERN_FUNCTION_NAME, List.of(value.get(), pattern.get())));
                }

                Optional<ConnectorExpression> escape = process(node.getEscape().get());
                if (escape.isPresent()) {
                    return Optional.of(new Call(typeOf(node), LIKE_PATTERN_FUNCTION_NAME, List.of(value.get(), pattern.get(), escape.get())));
                }
            }
            return Optional.empty();
        }

        @Override
        protected Optional<ConnectorExpression> visitSubscriptExpression(SubscriptExpression node, Void context)
        {
            if (!(typeOf(node.getBase()) instanceof RowType)) {
                return Optional.empty();
            }

            Optional<ConnectorExpression> translatedBase = process(node.getBase());
            if (translatedBase.isEmpty()) {
                return Optional.empty();
            }

            return Optional.of(new FieldDereference(typeOf(node), translatedBase.get(), toIntExact(((LongLiteral) node.getIndex()).getValue() - 1)));
        }

        @Override
        protected Optional<ConnectorExpression> visitExpression(Expression node, Void context)
        {
            return Optional.empty();
        }

        private Type typeOf(Expression node)
        {
            return types.get(NodeRef.of(node));
        }

        private Object evaluateConstant(Expression node)
        {
            Type type = typeOf(node);
            Object value = evaluateConstantExpression(
                    node,
                    type,
                    plannerContext,
                    session,
                    new AllowAllAccessControl(),
                    ImmutableMap.of());
            verify(!(value instanceof Expression), "Expression %s did not evaluate to constant: %s", node, value);
            return value;
        }
    }
}
