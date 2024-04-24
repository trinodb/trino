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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrVisitor;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.NodeRef;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class PartialTranslator
{
    private PartialTranslator() {}

    /**
     * Produces {@link ConnectorExpression} translations for disjoint components in the input expression in a
     * top-down manner. i.e. if an expression node is translatable, we do not consider its children.
     */
    public static Map<NodeRef<Expression>, ConnectorExpression> extractPartialTranslations(
            Expression inputExpression,
            Session session)
    {
        requireNonNull(inputExpression, "inputExpression is null");
        requireNonNull(session, "session is null");

        Map<NodeRef<Expression>, ConnectorExpression> partialTranslations = new HashMap<>();
        new Visitor(session, partialTranslations).process(inputExpression);
        return ImmutableMap.copyOf(partialTranslations);
    }

    private static class Visitor
            extends IrVisitor<Void, Void>
    {
        private final Map<NodeRef<Expression>, ConnectorExpression> translatedSubExpressions;
        private final ConnectorExpressionTranslator.SqlToConnectorExpressionTranslator translator;

        Visitor(Session session, Map<NodeRef<Expression>, ConnectorExpression> translatedSubExpressions)
        {
            this.translatedSubExpressions = requireNonNull(translatedSubExpressions, "translatedSubExpressions is null");
            this.translator = new ConnectorExpressionTranslator.SqlToConnectorExpressionTranslator(session);
        }

        @Override
        public Void visitExpression(Expression node, Void context)
        {
            Optional<ConnectorExpression> result = translator.process(node);

            if (result.isPresent()) {
                translatedSubExpressions.put(NodeRef.of(node), result.get());
            }
            else {
                node.children().forEach(this::process);
            }

            return null;
        }

        // TODO support lambda expressions for partial projection
        @Override
        public Void visitLambda(Lambda functionCall, Void context)
        {
            return null;
        }
    }
}
