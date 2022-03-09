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
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;
import io.trino.sql.DynamicFilters;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.CharLiteral;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.LongLiteral;
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
import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.trino.SystemSessionProperties.isComplexExpressionPushdown;
import static io.trino.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static java.util.Objects.requireNonNull;

public final class ConnectorExpressionTranslator
{
    private ConnectorExpressionTranslator() {}

    public static Expression translate(Session session, ConnectorExpression expression, PlannerContext plannerContext, Map<String, Symbol> variableMappings, LiteralEncoder literalEncoder)
    {
        return new ConnectorToSqlExpressionTranslator(session, plannerContext, literalEncoder, variableMappings)
                .translate(session, expression)
                .orElseThrow(() -> new UnsupportedOperationException("Expression is not supported: " + expression.toString()));
    }

    public static Optional<ConnectorExpression> translate(Session session, Expression expression, TypeAnalyzer types, TypeProvider inputTypes, PlannerContext plannerContext)
    {
        return new SqlToConnectorExpressionTranslator(session, types.getTypes(session, inputTypes, expression), plannerContext)
                .process(expression);
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

        public Optional<Expression> translate(Session session, ConnectorExpression expression)
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
                return translate(session, dereference.getTarget())
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

            // TODO Support ESCAPE character
            if (StandardFunctions.LIKE_PATTERN_FUNCTION_NAME.equals(call.getFunctionName()) && call.getArguments().size() == 2) {
                return translateLike(call.getArguments().get(0), call.getArguments().get(1));
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

        protected Optional<Expression> translateLike(ConnectorExpression value, ConnectorExpression pattern)
        {
            Optional<Expression> translatedValue = translate(session, value);
            Optional<Expression> translatedPattern = translate(session, pattern);
            if (translatedValue.isPresent() && translatedPattern.isPresent()) {
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
        protected Optional<ConnectorExpression> visitFunctionCall(FunctionCall node, Void context)
        {
            if (!isComplexExpressionPushdown(session)) {
                return Optional.empty();
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

        @Override
        protected Optional<ConnectorExpression> visitLikePredicate(LikePredicate node, Void context)
        {
            // TODO Support ESCAPE character
            if (node.getEscape().isEmpty()) {
                Optional<ConnectorExpression> value = process(node.getValue());
                Optional<ConnectorExpression> pattern = process(node.getPattern());
                if (value.isPresent() && pattern.isPresent()) {
                    return Optional.of(new Call(typeOf(node), StandardFunctions.LIKE_PATTERN_FUNCTION_NAME, List.of(value.get(), pattern.get())));
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

            return Optional.of(new FieldDereference(typeOf(node), translatedBase.get(), (int) (((LongLiteral) node.getIndex()).getValue() - 1)));
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
