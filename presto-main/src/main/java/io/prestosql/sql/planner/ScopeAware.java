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
package io.prestosql.sql.planner;

import io.prestosql.sql.analyzer.Analysis;
import io.prestosql.sql.analyzer.ResolvedField;
import io.prestosql.sql.analyzer.Scope;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Node;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ScopeAware<T extends Node>
{
    private final Analysis analysis;
    private final Scope scope;
    private final T node;

    private ScopeAware(Analysis analysis, Scope scope, T node)
    {
        this.analysis = requireNonNull(analysis, "analysis is null");
        this.scope = requireNonNull(scope, "scope is null");
        this.node = requireNonNull(node, "node is null");
    }

    public static <T extends Node> ScopeAware<T> scopeAwareKey(T node, Analysis analysis, Scope scope)
    {
        return new ScopeAware<T>(analysis, scope, node);
    }

    public T getNode()
    {
        return node;
    }

    @Override
    public int hashCode()
    {
        return hash(node);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ScopeAware<T> other = (ScopeAware<T>) o;
        checkArgument(scope.isLocalScope(other.scope) || other.scope.isLocalScope(scope), "Expressions must be in the same local scope");

        return equal(node, other.node);
    }

    @Override
    public String toString()
    {
        return "ScopeAware(" + node + ")";
    }

    private boolean equal(Node left, Node right)
    {
        if (left instanceof Expression && right instanceof Expression) {
            Expression leftExpression = (Expression) left;
            Expression rightExpression = (Expression) right;
            if (analysis.isColumnReference(leftExpression) && analysis.isColumnReference(rightExpression)) {
                ResolvedField leftField = analysis.getResolvedField(leftExpression);
                ResolvedField rightField = analysis.getResolvedField(rightExpression);

                Scope leftScope = leftField.getScope();
                Scope rightScope = rightField.getScope();

                if (isLocalScope(leftScope) || isLocalScope(rightScope)) {
                    // At least one of the fields comes from the same scope as the original expression, so compare by field id
                    return leftField.getFieldId().equals(rightField.getFieldId());
                }

                // Otherwise, they are either:
                // 1. Local to a subquery. In this case, the specific fields they reference don't matter as long as everything else is syntactically equal.
                // 2. From a scope that is a parent to the base scope. In this case, the fields must necessarily be the same if they are syntactically equal.
                return leftExpression.equals(rightExpression);
            }
        }

        if (!left.shallowEquals(right)) {
            return false;
        }

        List<? extends Node> leftChildren = left.getChildren();
        List<? extends Node> rightChildren = right.getChildren();

        if (leftChildren.size() != rightChildren.size()) {
            return false;
        }

        for (int i = 0; i < leftChildren.size(); i++) {
            if (!equal(leftChildren.get(i), rightChildren.get(i))) {
                return false;
            }
        }

        return true;
    }

    private int hash(Node node)
    {
        if (node instanceof Expression) {
            Expression expression = (Expression) node;
            if (analysis.isColumnReference(expression)) {
                ResolvedField field = analysis.getResolvedField(expression);

                Scope resolvedScope = field.getScope();
                if (isLocalScope(resolvedScope)) {
                    return field.getFieldId().hashCode();
                }

                return expression.hashCode();
            }
        }

        List<? extends Node> children = node.getChildren();

        if (children.isEmpty()) {
            return node.hashCode();
        }

        int result = 1;
        for (Node element : children) {
            result = 31 * result + hash(element);
        }

        return result;
    }

    private boolean isLocalScope(Scope scope)
    {
        return this.scope.isLocalScope(scope) || scope.isLocalScope(this.scope);
    }
}
