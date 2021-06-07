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

import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.metadata.LiteralFunction;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.scalar.JoniRegexpCasts;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.ConnectorExpressionVisitor;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;
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
import io.trino.type.JoniRegexpType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public final class ConnectorExpressionTranslator
{
    public static final String LIKE_FUNCTION_NAME = "like_function";
    public static final String REGEXP_LIKE_FUNCTION_NAME = "regexp_like";

    private ConnectorExpressionTranslator() {}

    public static Expression translate(ConnectorExpression expression, Metadata metadata, LiteralEncoder literalEncoder, Optional<Map<String, Symbol>> variableMappings)
    {
        return new ConnectorToSqlExpressionTranslator(metadata, literalEncoder, variableMappings)
                .process(expression)
                .orElseThrow(() -> new UnsupportedOperationException("Expression type not supported: " + expression.getClass().getName()));
    }

    public static Optional<ConnectorExpression> translate(Session session, Expression expression, TypeAnalyzer types, TypeProvider inputTypes, Metadata metadata)
    {
        return new SqlToConnectorExpressionTranslator(session, types.getTypes(session, inputTypes, expression), metadata)
                .process(expression);
    }

    private static class ConnectorToSqlExpressionTranslator
            extends ConnectorExpressionVisitor<Optional<Expression>, Void>
    {
        private final Metadata metadata;
        private final LiteralEncoder literalEncoder;
        private final Optional<Map<String, Symbol>> variableMappings;

        public ConnectorToSqlExpressionTranslator(Metadata metadata, LiteralEncoder literalEncoder, Optional<Map<String, Symbol>> variableMappings)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.literalEncoder = requireNonNull(literalEncoder, "literalEncoder is null");
            this.variableMappings = requireNonNull(variableMappings, "variableMappings is null");
        }

        @Override
        public Optional<Expression> process(ConnectorExpression node, @Nullable Void context)
        {
            return super.process(node, context);
        }

        @Override
        protected Optional<Expression> visitConstant(Constant node, Void context)
        {
            return Optional.of(literalEncoder.toExpression(node.getValue(), node.getType()));
        }

        @Override
        protected Optional<Expression> visitFieldDereference(FieldDereference node, Void context)
        {
            Optional<Expression> base = process(node.getTarget());
            return base.map(expression -> new SubscriptExpression(expression, new LongLiteral(Long.toString(node.getField() + 1))));
        }

        @Override
        protected Optional<Expression> visitCall(Call node, Void context)
        {
            if (LIKE_FUNCTION_NAME.equals(node.getName())) {
                return visitLike(node.getArguments().get(0), node.getArguments().get(1), context);
            }

            QualifiedName qualifiedName = getQualifiedName(node);

            List<Expression> arguments = node.getArguments().stream()
                                             .map(argument -> ConnectorExpressionPredicateTranslator.toPredicate(argument, literalEncoder, metadata))
                                             .collect(Collectors.toList());

            if (REGEXP_LIKE_FUNCTION_NAME.equalsIgnoreCase(node.getName()) && arguments.size() == 2) {
                Expression pattern = arguments.get(1);
                if (pattern instanceof StringLiteral) {
                    Slice slice = ((StringLiteral) pattern).getSlice();
                    JoniRegexp joniRegexp = JoniRegexpCasts.castVarcharToJoniRegexp(slice);
                    arguments.set(1, literalEncoder.toExpression(joniRegexp, JoniRegexpType.JONI_REGEXP));
                }
            }

            return Optional.of(new FunctionCall(Optional.empty(),
                                                qualifiedName,
                                                Optional.empty(),
                                                Optional.empty(),
                                                Optional.empty(),
                                                false,
                                                Optional.empty(),
                                                Optional.empty(),
                                                arguments));
        }

        protected Optional<Expression> visitLike(ConnectorExpression value, ConnectorExpression pattern, Void context)
        {
            Optional<Expression> valueExpression = process(value);
            Optional<Expression> patternExpression = process(pattern);
            if (valueExpression.isPresent() && patternExpression.isPresent()) {
                return Optional.of(new LikePredicate(valueExpression.get(), patternExpression.get(), Optional.empty()));
            }
            return Optional.empty();
        }

        @Override
        protected Optional<Expression> visitVariable(Variable node, Void context)
        {
            Expression expression = variableMappings.map(map -> map.get(node.getName()).toSymbolReference())
                                                    .orElse(new SymbolReference(node.getName()));
            return Optional.of(expression);
        }

        private QualifiedName getQualifiedName(Call call)
        {
            List<TypeSignature> typeSignatures = call.getArguments().stream().map(argument -> argument.getType().getTypeSignature()).collect(Collectors.toList());
            return metadata.resolveFunction(QualifiedName.of(call.getName()),
                                            TypeSignatureProvider.fromTypeSignatures(typeSignatures))
                           .toQualifiedName();
        }
    }

    static class SqlToConnectorExpressionTranslator
            extends AstVisitor<Optional<ConnectorExpression>, Void>
    {
        private final Session session;
        private final Map<NodeRef<Expression>, Type> types;
        private final Metadata metadata;

        public SqlToConnectorExpressionTranslator(Session session, Map<NodeRef<Expression>, Type> types, Metadata metadata)
        {
            this.session = requireNonNull(session, "types is null");
            this.types = requireNonNull(types, "types is null");
            this.metadata = requireNonNull(metadata, "types is null");
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
            if (node.getFilter().isPresent() || node.getOrderBy().isPresent() || node.getWindow().isPresent() || node.getNullTreatment().isPresent() || node.isDistinct()) {
                return Optional.empty();
            }

            String functionName = ResolvedFunction.extractFunctionName(node.getName());

            if (LiteralFunction.LITERAL_FUNCTION_NAME.equalsIgnoreCase(functionName)) {
                ExpressionInterpreter interpreter = new ExpressionInterpreter(node, metadata, session, types);
                Object value = interpreter.evaluate();
                if (value instanceof JoniRegexp) {
                    Slice pattern = ((JoniRegexp) value).pattern();
                    return Optional.of(new Constant(pattern, VarcharType.createVarcharType(pattern.length())));
                }
                return Optional.empty();
            }

            List<ConnectorExpression> arguments = new ArrayList<>();
            for (Expression argumentExpression : node.getArguments()) {
                Optional<ConnectorExpression> argument = process(argumentExpression);
                if (argument.isEmpty()) {
                    return Optional.empty();
                }
                arguments.add(argument.get());
            }

            return Optional.of(new Call(typeOf(node), functionName, arguments));
        }

        @Override
        protected Optional<ConnectorExpression> visitLikePredicate(LikePredicate node, Void context)
        {
            if (node.getValue() instanceof SymbolReference &&
                    node.getPattern() instanceof StringLiteral &&
                    node.getEscape().isEmpty()) {
                Optional<ConnectorExpression> value = process(node.getValue());
                Optional<ConnectorExpression> pattern = process(node.getPattern());
                if (value.isPresent() && pattern.isPresent()) {
                    Type columnType = typeOf(node.getValue());
                    return Optional.of(new Call(columnType, LIKE_FUNCTION_NAME, List.of(value.get(), pattern.get())));
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
    }
}
