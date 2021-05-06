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
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.type.Type;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;

import java.util.Map;
import java.util.Optional;

import static io.trino.spi.expression.Constant.TRUE_CONSTANT;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.Objects.requireNonNull;

public class ConnectorExpressionPredicateTranslator
{
    private ConnectorExpressionPredicateTranslator()
    {
    }

    public static Expression toPredicate(ConnectorExpression connectorExpression, LiteralEncoder literalEncoder, Metadata metadata)
    {
        return ConnectorExpressionTranslator.translate(connectorExpression, metadata, literalEncoder, Optional.empty());
    }

    public static ExtractionResult translate(Session session, Expression expression, TypeAnalyzer types, TypeProvider inputTypes)
    {
        return new ToConnectorExpressionVisitor(types.getTypes(session, inputTypes, expression)).process(expression);
    }

    private static class ToConnectorExpressionVisitor
            extends AstVisitor<ExtractionResult, Void>
    {
        private final ConnectorExpressionTranslator.SqlToConnectorExpressionTranslator translator;

        ToConnectorExpressionVisitor(Map<NodeRef<Expression>, Type> types)
        {
            this.translator = new ConnectorExpressionTranslator.SqlToConnectorExpressionTranslator(types);
        }

        // TODO: Support partial translation
        @Override
        public ExtractionResult visitExpression(Expression node, Void context)
        {
            Optional<ConnectorExpression> result = translator.process(node, context);
            return result.map(connectorExpression -> new ExtractionResult(connectorExpression, TRUE_LITERAL))
                         .orElse(new ExtractionResult(node));
        }
    }

    public static class ExtractionResult
    {
        private final ConnectorExpression connectorExpression;
        private final Expression remainingExpression;

        public ExtractionResult(Expression node)
        {
            this(TRUE_CONSTANT, node);
        }

        public ExtractionResult(ConnectorExpression connectorExpression, Expression remainingExpression)
        {
            this.connectorExpression = requireNonNull(connectorExpression, "connectorExpression is null");
            this.remainingExpression = requireNonNull(remainingExpression, "remainingExpression is null");
        }

        public ConnectorExpression getConnectorExpression()
        {
            return connectorExpression;
        }

        public Expression getRemainingExpression()
        {
            return remainingExpression;
        }
    }
}
