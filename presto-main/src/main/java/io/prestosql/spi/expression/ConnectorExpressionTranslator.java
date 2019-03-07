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
package io.prestosql.spi.expression;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.LiteralEncoder;
import io.prestosql.sql.planner.LiteralInterpreter;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.SymbolReference;

import java.util.Map;
import java.util.stream.Collectors;

public class ConnectorExpressionTranslator
{
    private ConnectorExpressionTranslator()
    {
    }

    public static Expression translate(ConnectorExpression expression, Map<ColumnHandle, Symbol> mappings, Metadata metadata)
    {
        return new ConnectorToSqlExpressionTranslator(mappings, metadata).translate(expression);
    }

    public static ConnectorExpression translate(Session session, Expression expression, Map<Symbol, ColumnHandle> assignments, TypeAnalyzer types, TypeProvider inputTypes, Metadata metadata)
    {
        return new SqlToConnectorExpressionTranslator(session, metadata, assignments, types.getTypes(session, inputTypes, expression))
                .process(expression);
    }

    private static class ConnectorToSqlExpressionTranslator
    {
        private final Map<ColumnHandle, Symbol> mappings;
        private final LiteralEncoder literalEncoder;

        public ConnectorToSqlExpressionTranslator(Map<ColumnHandle, Symbol> mappings, Metadata metadata)
        {
            this.mappings = mappings;
            this.literalEncoder = new LiteralEncoder(metadata.getBlockEncodingSerde());
        }

        private String nameOf(FunctionId function)
        {
            return function.getName(); // TODO
        }

        public Expression translate(ConnectorExpression expression)
        {
            if (expression instanceof Constant) {
                return literalEncoder.toExpression(((Constant) expression).getValue(), expression.getType());
            }

            if (expression instanceof ColumnReference) {
                return mappings.get(((ColumnReference) expression).getColumn()).toSymbolReference();
            }

            if (expression instanceof Apply) {
                Apply apply = (Apply) expression;

                return new FunctionCall(
                        QualifiedName.of(nameOf(apply.getFunction())),
                        apply.getArguments().stream()
                                .map(this::translate)
                                .collect(Collectors.toList()));
            }

            throw new UnsupportedOperationException("Expression type not supported: " + expression.getClass().getName());

        }
    }

    private static class SqlToConnectorExpressionTranslator
            extends AstVisitor<ConnectorExpression, Void>
    {
        private final Session session;
        private final Metadata metadata;
        private final Map<Symbol, ColumnHandle> assignments;
        private final Map<NodeRef<Expression>, Type> types;

        private SqlToConnectorExpressionTranslator(Session session, Metadata metadata, Map<Symbol, ColumnHandle> assignments, Map<NodeRef<Expression>, Type> types)
        {
            this.session = session;
            this.metadata = metadata;
            this.assignments = assignments;
            this.types = types;
        }

        private Type typeOf(Expression node)
        {
            return types.get(NodeRef.of(node));
        }

        // TODO: need to return a FunctionHandle for the operator 
        private FunctionId signatureOf(ComparisonExpression.Operator operator, Type left, Type right)
        {
            return new FunctionId("$operator_" + operator.name());
        }

        @Override
        protected ConnectorExpression visitComparisonExpression(ComparisonExpression node, Void context)
        {
            ConnectorExpression left = process(node.getLeft());
            ConnectorExpression right = process(node.getRight());

            return new Apply(
                    typeOf(node), signatureOf(node.getOperator(), left.getType(), right.getType()),
                    ImmutableList.of(left, right));
        }

        @Override
        protected ConnectorExpression visitSymbolReference(SymbolReference node, Void context)
        {
            return new ColumnReference(assignments.get(Symbol.from(node)), typeOf(node));
        }

        @Override
        protected ConnectorExpression visitGenericLiteral(GenericLiteral node, Void context)
        {
            return new Constant(LiteralInterpreter.evaluate(metadata, session.toConnectorSession(), node), typeOf(node));
        }

        @Override
        protected ConnectorExpression visitStringLiteral(StringLiteral node, Void context)
        {
            return new Constant(node.getSlice(), typeOf(node));
        }

        @Override
        protected ConnectorExpression visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: expression translator for " + node.getClass().getName());
        }
    }
}
