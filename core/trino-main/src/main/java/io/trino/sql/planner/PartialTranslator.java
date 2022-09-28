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
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.NodeRef;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class PartialTranslator
{
    private PartialTranslator() {}

    /**
     * Produces {@link ConnectorExpression} translations for disjoint components in the {@param inputExpression} in a
     * top-down manner. i.e. if an expression node is translatable, we do not consider its children.
     */
    public static Map<NodeRef<Expression>, ConnectorExpression> extractPartialTranslations(
            Expression inputExpression,
            Session session,
            TypeAnalyzer typeAnalyzer,
            TypeProvider typeProvider,
            PlannerContext plannerContext)
    {
        requireNonNull(inputExpression, "inputExpression is null");
        requireNonNull(session, "session is null");
        requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        requireNonNull(typeProvider, "typeProvider is null");

        Map<NodeRef<Expression>, ConnectorExpression> partialTranslations = new HashMap<>();
        new Visitor(session, typeAnalyzer.getTypes(session, typeProvider, inputExpression), partialTranslations, plannerContext).process(inputExpression);
        return ImmutableMap.copyOf(partialTranslations);
    }

    private static class Visitor
            extends AstVisitor<Void, Void>
    {
        private final Map<NodeRef<Expression>, ConnectorExpression> translatedSubExpressions;
        private final ConnectorExpressionTranslator.SqlToConnectorExpressionTranslator translator;

        Visitor(Session session, Map<NodeRef<Expression>, Type> types, Map<NodeRef<Expression>, ConnectorExpression> translatedSubExpressions, PlannerContext plannerContext)
        {
            requireNonNull(types, "types is null");
            this.translatedSubExpressions = requireNonNull(translatedSubExpressions, "translatedSubExpressions is null");
            this.translator = new ConnectorExpressionTranslator.SqlToConnectorExpressionTranslator(session, types, plannerContext);
        }

        @Override
        public Void visitExpression(Expression node, Void context)
        {
            Optional<ConnectorExpression> result = translator.process(node);

            if (result.isPresent()) {
                translatedSubExpressions.put(NodeRef.of(node), result.get());
            }
            else {
                node.getChildren().forEach(this::process);
            }

            return null;
        }

        // TODO support lambda expressions for partial projection
        @Override
        public Void visitLambdaExpression(LambdaExpression functionCall, Void context)
        {
            return null;
        }
    }
}
