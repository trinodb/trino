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

import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.ConnectorExpressionVisitor;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.Function;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public final class ConnectorExpressionTranslator
{
    private ConnectorExpressionTranslator() {}

    public static Expression translate(ConnectorExpression expression, Metadata metadata, LiteralEncoder literalEncoder, Optional<Map<String, Symbol>> variableMappings)
    {
        return new ConnectorToSqlExpressionTranslator(metadata, literalEncoder, variableMappings)
                .process(expression)
                .orElseThrow(() -> new UnsupportedOperationException("Expression type not supported: " + expression.getClass().getName()));
    }

    public static Optional<ConnectorExpression> translate(Session session, Expression expression, TypeAnalyzer types, TypeProvider inputTypes)
    {
        return new SqlToConnectorExpressionTranslator(types.getTypes(session, inputTypes, expression))
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
        protected Optional<Expression> visitFunction(Function node, Void context)
        {
            if (Function.LIKE_FUNCTION_NAME.equals(node.getName())) {
                return visitLikeFunction(node.getArguments().get(0), node.getArguments().get(1), context);
            }

            QualifiedName qualifiedName = getQualifiedName(node);

            List<Expression> arguments = node.getArguments().stream()
                                             .map(argument -> ConnectorExpressionPredicateTranslator.toPredicate(argument, literalEncoder, metadata))
                                             .collect(Collectors.toList());

            Optional<FunctionCall.NullTreatment> nullTreatment;
            if (node.getNullTreatment().isEmpty()) {
                nullTreatment = Optional.empty();
            }
            else if (node.getNullTreatment().get() == Function.NullTreatment.IGNORE) {
                nullTreatment = Optional.of(FunctionCall.NullTreatment.IGNORE);
            }
            else if (node.getNullTreatment().get() == Function.NullTreatment.RESPECT) {
                nullTreatment = Optional.of(FunctionCall.NullTreatment.RESPECT);
            }
            else {
                throw new RuntimeException("Unknown NullTreatment value " + node.getNullTreatment().get().name());
            }

            return Optional.of(new FunctionCall(Optional.empty(),
                                                qualifiedName,
                                                Optional.empty(),
                                                Optional.empty(),
                                                Optional.empty(),
                                                false,
                                                nullTreatment,
                                                Optional.empty(),
                                                arguments));
        }

        protected Optional<Expression> visitLikeFunction(ConnectorExpression value, ConnectorExpression pattern, Void context)
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

        private QualifiedName getQualifiedName(Function function)
        {
            List<TypeSignature> typeSignatures = function.getArguments().stream().map(argument -> argument.getType().getTypeSignature()).collect(Collectors.toList());
            return metadata.resolveFunction(QualifiedName.of(function.getName()),
                                            TypeSignatureProvider.fromTypeSignatures(typeSignatures))
                           .toQualifiedName();
        }
    }

    static class SqlToConnectorExpressionTranslator
            extends AstVisitor<Optional<ConnectorExpression>, Void>
    {
        private final Map<NodeRef<Expression>, Type> types;

        public SqlToConnectorExpressionTranslator(Map<NodeRef<Expression>, Type> types)
        {
            this.types = requireNonNull(types, "types is null");
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
            if (!node.getFilter().isEmpty() || !node.getOrderBy().isEmpty() || !node.getWindow().isEmpty() || node.isDistinct()) {
                return Optional.empty();
            }

            String functionName = ResolvedFunction.extractFunctionName(node.getName());

            List<ConnectorExpression> arguments = new ArrayList<>();
            for (Expression argumentExpression : node.getArguments()) {
                if (argumentExpression instanceof SymbolReference || argumentExpression instanceof FunctionCall || argumentExpression instanceof StringLiteral) {
                    Optional<ConnectorExpression> argument = process(argumentExpression);
                    if (argument.isEmpty()) {
                        return Optional.empty();
                    }
                    arguments.add(argument.get());
                }
            }

            // Not sure if it can be relevant (maybe always Ignore \ Respect in applyFilter functions?)
            Optional<Function.NullTreatment> nullTreatment;
            if (node.getNullTreatment().isEmpty()) {
                nullTreatment = Optional.empty();
            }
            else if (node.getNullTreatment().get() == FunctionCall.NullTreatment.IGNORE) {
                nullTreatment = Optional.of(Function.NullTreatment.IGNORE);
            }
            else if (node.getNullTreatment().get() == FunctionCall.NullTreatment.RESPECT) {
                nullTreatment = Optional.of(Function.NullTreatment.RESPECT);
            }
            else {
                throw new RuntimeException("Unknown NullTreatment value " + node.getNullTreatment().get().name());
            }

            // TODO: replace after resolving the issue with FunctionPushdown
            // return Optional.of(new Function(typeOf(node), functionName, arguments, nullTreatment));
            return Optional.empty();
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
                    return Optional.of(Function.ofLikeFunction(columnType, value.get(), pattern.get()));
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
