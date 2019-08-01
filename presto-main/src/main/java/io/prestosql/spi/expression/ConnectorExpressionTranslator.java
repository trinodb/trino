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

import io.prestosql.Session;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.LiteralEncoder;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.BinaryLiteral;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.CharLiteral;
import io.prestosql.sql.tree.DecimalLiteral;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.DoubleLiteral;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class ConnectorExpressionTranslator
{
    private ConnectorExpressionTranslator() {}

    public static Expression translate(ConnectorExpression expression, Map<String, Symbol> variableMappings, LiteralEncoder literalEncoder)
    {
        return new ConnectorToSqlExpressionTranslator(variableMappings, literalEncoder).translate(expression);
    }

    public static ConnectorExpression translate(Session session, Expression expression, TypeAnalyzer types, TypeProvider inputTypes)
    {
        return new SqlToConnectorExpressionTranslator(types.getTypes(session, inputTypes, expression))
                .process(expression);
    }

    private static class ConnectorToSqlExpressionTranslator
    {
        private final Map<String, Symbol> variableMappings;
        private final LiteralEncoder literalEncoder;

        public ConnectorToSqlExpressionTranslator(Map<String, Symbol> variableMappings, LiteralEncoder literalEncoder)
        {
            this.variableMappings = requireNonNull(variableMappings, "variableMappings is null");
            this.literalEncoder = requireNonNull(literalEncoder, "literalEncoder is null");
        }

        public Expression translate(ConnectorExpression expression)
        {
            if (expression instanceof Variable) {
                return variableMappings.get(((Variable) expression).getName()).toSymbolReference();
            }

            if (expression instanceof Constant) {
                return literalEncoder.toExpression(((Constant) expression).getValue(), expression.getType());
            }

            if (expression instanceof FieldDereference) {
                FieldDereference dereference = (FieldDereference) expression;

                RowType type = (RowType) expression.getType();
                String name = type.getFields().get(dereference.getField()).getName().get();
                return new DereferenceExpression(translate(dereference.getTarget()), new Identifier(name));
            }

            throw new UnsupportedOperationException("Expression type not supported: " + expression.getClass().getName());
        }
    }

    private static class SqlToConnectorExpressionTranslator
            extends AstVisitor<ConnectorExpression, Void>
    {
        private final Map<NodeRef<Expression>, Type> types;

        private SqlToConnectorExpressionTranslator(Map<NodeRef<Expression>, Type> types)
        {
            this.types = requireNonNull(types, "types is null");
        }

        @Override
        protected ConnectorExpression visitSymbolReference(SymbolReference node, Void context)
        {
            return new Variable(node.getName(), typeOf(node));
        }

        @Override
        protected ConnectorExpression visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            return new Constant(node.getValue(), typeOf(node));
        }

        @Override
        protected ConnectorExpression visitStringLiteral(StringLiteral node, Void context)
        {
            return new Constant(node.getSlice(), typeOf(node));
        }

        @Override
        protected ConnectorExpression visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            return new Constant(node.getValue(), typeOf(node));
        }

        @Override
        protected ConnectorExpression visitDecimalLiteral(DecimalLiteral node, Void context)
        {
            return new Constant(Decimals.parse(node.getValue()).getObject(), typeOf(node));
        }

        @Override
        protected ConnectorExpression visitCharLiteral(CharLiteral node, Void context)
        {
            return new Constant(node.getSlice(), typeOf(node));
        }

        @Override
        protected ConnectorExpression visitBinaryLiteral(BinaryLiteral node, Void context)
        {
            return new Constant(node.getValue(), typeOf(node));
        }

        @Override
        protected ConnectorExpression visitLongLiteral(LongLiteral node, Void context)
        {
            return new Constant(node.getValue(), typeOf(node));
        }

        @Override
        protected ConnectorExpression visitNullLiteral(NullLiteral node, Void context)
        {
            return new Constant(null, typeOf(node));
        }

        @Override
        protected ConnectorExpression visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            RowType rowType = (RowType) typeOf(node.getBase());
            String fieldName = node.getField().getValue();
            List<RowType.Field> fields = rowType.getFields();
            int index = -1;
            for (int i = 0; i < fields.size(); i++) {
                RowType.Field field = fields.get(i);
                if (field.getName().isPresent() && field.getName().get().equalsIgnoreCase(fieldName)) {
                    checkArgument(index < 0, "Ambiguous field %s in type %s", field, rowType.getDisplayName());
                    index = i;
                }
            }

            checkState(index >= 0, "could not find field name: %s", node.getField());

            return new FieldDereference(typeOf(node), process(node.getBase()), index);
        }

        @Override
        protected ConnectorExpression visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: expression translator for " + node.getClass().getName());
        }

        private Type typeOf(Expression node)
        {
            return types.get(NodeRef.of(node));
        }
    }
}
