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
package io.trino.sql.analyzer;

import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FieldReference;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeRef;

import java.util.Set;

import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_CONSTANT;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;

public final class ConstantExpressionVerifier
{
    private ConstantExpressionVerifier() {}

    public static void verifyExpressionIsConstant(Set<NodeRef<Expression>> columnReferences, Expression expression)
    {
        new ConstantExpressionVerifierVisitor(columnReferences, expression).process(expression, null);
    }

    private static class ConstantExpressionVerifierVisitor
            extends DefaultTraversalVisitor<Void>
    {
        private final Set<NodeRef<Expression>> columnReferences;
        private final Expression expression;

        public ConstantExpressionVerifierVisitor(Set<NodeRef<Expression>> columnReferences, Expression expression)
        {
            this.columnReferences = columnReferences;
            this.expression = expression;
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            if (columnReferences.contains(NodeRef.<Expression>of(node))) {
                throw semanticException(EXPRESSION_NOT_CONSTANT, expression, "Constant expression cannot contain column references");
            }

            process(node.getBase(), context);
            return null;
        }

        @Override
        protected Void visitIdentifier(Identifier node, Void context)
        {
            throw semanticException(EXPRESSION_NOT_CONSTANT, expression, "Constant expression cannot contain column references");
        }

        @Override
        protected Void visitFieldReference(FieldReference node, Void context)
        {
            throw semanticException(EXPRESSION_NOT_CONSTANT, expression, "Constant expression cannot contain column references");
        }
    }
}
