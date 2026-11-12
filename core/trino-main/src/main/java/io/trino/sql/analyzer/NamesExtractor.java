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

import com.google.common.collect.ImmutableSet;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SubqueryExpression;

import java.util.Set;

import static io.trino.sql.tree.AstTraversal.forEachChild;

public final class NamesExtractor
{
    private NamesExtractor() {}

    // to extract qualified name with prefix
    public static Set<QualifiedName> extractNames(Expression expression, Set<NodeRef<Expression>> columnReferences)
    {
        ImmutableSet.Builder<QualifiedName> builder = ImmutableSet.builder();
        collectNames(expression, columnReferences, true, builder);
        return builder.build();
    }

    public static Set<QualifiedName> extractNamesNoSubqueries(Expression expression, Set<NodeRef<Expression>> columnReferences)
    {
        ImmutableSet.Builder<QualifiedName> builder = ImmutableSet.builder();
        collectNames(expression, columnReferences, false, builder);
        return builder.build();
    }

    private static void collectNames(
            Node node,
            Set<NodeRef<Expression>> columnReferences,
            boolean recurseIntoSubqueries,
            ImmutableSet.Builder<QualifiedName> builder)
    {
        switch (node) {
            case DereferenceExpression dereference -> {
                if (columnReferences.contains(NodeRef.<Expression>of(dereference))) {
                    builder.add(DereferenceExpression.getQualifiedName(dereference));
                }
                else {
                    collectNames(dereference.getBase(), columnReferences, recurseIntoSubqueries, builder);
                }
            }
            case Identifier identifier -> builder.add(QualifiedName.of(identifier.getValue()));
            case SubqueryExpression _ when !recurseIntoSubqueries -> {}
            default -> forEachChild(node, child -> collectNames(child, columnReferences, recurseIntoSubqueries, builder));
        }
    }
}
