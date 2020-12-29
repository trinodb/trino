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
import io.prestosql.sql.analyzer.CanonicalizationAware;
import io.prestosql.sql.analyzer.ResolvedField;
import io.prestosql.sql.analyzer.Scope;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.Node;

import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.sql.util.AstUtils.treeEqual;
import static io.prestosql.sql.util.AstUtils.treeHash;
import static java.util.Objects.requireNonNull;

/**
 * A wrapper for Expressions that can be used as a key in maps and sets.
 * <p>
 * Expressions are considered equal if they are structurally equal and column references refer to the same logical fields.
 * <p>
 * For example, given
 *
 * <pre>SELECT t.a, a FROM (VALUES 1) t(a)</pre>
 * <p>
 * "t.a" and "a" are considered equal because they reference the same field of "t"
 * <p>
 * Limitation: the expressions in the following query are currently not considered equal to each other, even though they refer to the same field from the same table or
 * named query "t". This is because we currently don't assign identity to table references. (TODO: implement this)
 *
 * <pre>SELECT (SELECT t.a FROM t), (SELECT a FROM t)</pre>
 */
public class ScopeAware<T extends Node>
{
    private final Analysis analysis;
    private final Scope queryScope;
    private final T node;
    private final int hash;

    private ScopeAware(Analysis analysis, Scope scope, T node)
    {
        requireNonNull(scope, "scope is null");

        this.queryScope = scope.getQueryBoundaryScope();
        this.analysis = requireNonNull(analysis, "analysis is null");
        this.node = requireNonNull(node, "node is null");
        this.hash = treeHash(node, this::scopeAwareHash);
    }

    public static <T extends Node> ScopeAware<T> scopeAwareKey(T node, Analysis analysis, Scope scope)
    {
        return new ScopeAware<>(analysis, scope, node);
    }

    public T getNode()
    {
        return node;
    }

    @Override
    public int hashCode()
    {
        return hash;
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
        checkArgument(this.queryScope == other.queryScope, "Expressions must be in the same local scope");

        return treeEqual(node, other.node, this::scopeAwareComparison);
    }

    @Override
    public String toString()
    {
        return "ScopeAware(" + node + ")";
    }

    private Boolean scopeAwareComparison(Node left, Node right)
    {
        if (left instanceof Expression && right instanceof Expression) {
            Expression leftExpression = (Expression) left;
            Expression rightExpression = (Expression) right;
            if (analysis.isColumnReference(leftExpression) && analysis.isColumnReference(rightExpression)) {
                ResolvedField leftField = analysis.getResolvedField(leftExpression);
                ResolvedField rightField = analysis.getResolvedField(rightExpression);

                Scope leftScope = leftField.getScope();
                Scope rightScope = rightField.getScope();

                // For subqueries of the query associated with the current expression, compare by syntax
                // Note: it'd appear that hash() and equal() are inconsistent with each other in the case that:
                //    * left.hasOuterParent(...) == true and right.hasOuterParent(...) == false
                //    * leftField.getFieldId().equals(rightField.getFieldId()) == true
                // Both fields would seem to have different hashes but be equal to each other.
                // However, this cannot happen because we *require* that both expressions being compared by
                // rooted in the same "query scope" (i.e., sub-scopes that are local to each other) -- see ScopeAwareKey.equals().
                // If both fields have the same field id, by definition they will produce the same result for hasOuterParent().
                checkState(leftScope.hasOuterParent(queryScope) == rightScope.hasOuterParent(queryScope));
                if (leftScope.hasOuterParent(queryScope) && rightScope.hasOuterParent(queryScope)) {
                    return treeEqual(leftExpression, rightExpression, CanonicalizationAware::canonicalizationAwareComparison);
                }

                // Otherwise, for references that come from the current query scope or an outer scope of the current
                // expression, compare by resolved field
                return leftField.getFieldId().equals(rightField.getFieldId());
            }
            else if (leftExpression instanceof Identifier && rightExpression instanceof Identifier) {
                return treeEqual(leftExpression, rightExpression, CanonicalizationAware::canonicalizationAwareComparison);
            }
        }

        if (!left.shallowEquals(right)) {
            return false;
        }

        return null;
    }

    private OptionalInt scopeAwareHash(Node node)
    {
        if (node instanceof Expression) {
            Expression expression = (Expression) node;
            if (analysis.isColumnReference(expression)) {
                ResolvedField field = analysis.getResolvedField(expression);

                Scope resolvedScope = field.getScope();
                if (resolvedScope.hasOuterParent(queryScope)) {
                    return OptionalInt.of(treeHash(expression, CanonicalizationAware::canonicalizationAwareHash));
                }

                return OptionalInt.of(field.getFieldId().hashCode());
            }
            else if (expression instanceof Identifier) {
                return OptionalInt.of(treeHash(expression, CanonicalizationAware::canonicalizationAwareHash));
            }
            else if (node.getChildren().isEmpty()) {
                // Calculate shallow hash since node doesn't have any children
                return OptionalInt.of(expression.hashCode());
            }
        }

        return OptionalInt.empty();
    }
}
