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
package io.trino.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class CaseStatementWhenClause
        extends Node
{
    private final Expression expression;
    private final List<ControlStatement> statements;

    public CaseStatementWhenClause(NodeLocation location, Expression expression, List<ControlStatement> statements)
    {
        super(Optional.of(location));
        this.expression = requireNonNull(expression, "expression is null");
        this.statements = requireNonNull(statements, "statements is null");
    }

    public Expression getExpression()
    {
        return expression;
    }

    public List<ControlStatement> getStatements()
    {
        return statements;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCaseStatementWhenClause(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .add(expression)
                .addAll(statements)
                .build();
    }

    @Override
    public boolean equals(Object obj)
    {
        return (obj instanceof CaseStatementWhenClause other) &&
                Objects.equals(expression, other.expression) &&
                Objects.equals(statements, other.statements);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, statements);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("expression", expression)
                .add("statements", statements)
                .toString();
    }
}
