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
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SubqueryExpression;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public final class NamesExtractor
{
    private NamesExtractor() {}

    // to extract qualified name with prefix
    public static Set<QualifiedName> extractNames(Expression expression, Set<NodeRef<Expression>> columnReferences)
    {
        ImmutableSet.Builder<QualifiedName> builder = ImmutableSet.builder();
        new QualifiedNameBuilderVisitor(columnReferences, true).process(expression, builder);
        return builder.build();
    }

    public static Set<QualifiedName> extractNamesNoSubqueries(Expression expression, Set<NodeRef<Expression>> columnReferences)
    {
        ImmutableSet.Builder<QualifiedName> builder = ImmutableSet.builder();
        new QualifiedNameBuilderVisitor(columnReferences, false).process(expression, builder);
        return builder.build();
    }

    private static class QualifiedNameBuilderVisitor
            extends DefaultTraversalVisitor<ImmutableSet.Builder<QualifiedName>>
    {
        private final Set<NodeRef<Expression>> columnReferences;
        private final boolean recurseIntoSubqueries;

        private QualifiedNameBuilderVisitor(Set<NodeRef<Expression>> columnReferences, boolean recurseIntoSubqueries)
        {
            this.columnReferences = requireNonNull(columnReferences, "columnReferences is null");
            this.recurseIntoSubqueries = recurseIntoSubqueries;
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, ImmutableSet.Builder<QualifiedName> builder)
        {
            if (columnReferences.contains(NodeRef.<Expression>of(node))) {
                builder.add(DereferenceExpression.getQualifiedName(node));
            }
            else {
                process(node.getBase(), builder);
            }
            return null;
        }

        @Override
        protected Void visitIdentifier(Identifier node, ImmutableSet.Builder<QualifiedName> builder)
        {
            builder.add(QualifiedName.of(node.getValue()));
            return null;
        }

        @Override
        protected Void visitSubqueryExpression(SubqueryExpression node, ImmutableSet.Builder<QualifiedName> context)
        {
            if (!recurseIntoSubqueries) {
                return null;
            }

            return super.visitSubqueryExpression(node, context);
        }
    }
}
