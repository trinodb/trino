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

import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FieldReference;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;

import java.util.Set;

import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_CONSTANT;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.tree.AstTraversal.forEachChild;

public final class ConstantExpressionVerifier
{
    private ConstantExpressionVerifier() {}

    public static void verifyExpressionIsConstant(Set<NodeRef<Expression>> columnReferences, Expression expression)
    {
        verify(columnReferences, expression, expression);
    }

    private static void verify(Set<NodeRef<Expression>> columnReferences, Expression expression, Node node)
    {
        switch (node) {
            case DereferenceExpression dereference -> {
                if (columnReferences.contains(NodeRef.<Expression>of(dereference))) {
                    throw semanticException(EXPRESSION_NOT_CONSTANT, expression, "Constant expression cannot contain column references");
                }
                verify(columnReferences, expression, dereference.getBase());
            }
            case Identifier _, FieldReference _ -> throw semanticException(EXPRESSION_NOT_CONSTANT, expression, "Constant expression cannot contain column references");
            default -> forEachChild(node, child -> verify(columnReferences, expression, child));
        }
    }
}
