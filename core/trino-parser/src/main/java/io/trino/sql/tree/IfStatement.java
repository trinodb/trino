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

public final class IfStatement
        extends ControlStatement
{
    private final Expression expression;
    private final List<ControlStatement> statements;
    private final List<ElseIfClause> elseIfClauses;
    private final Optional<ElseClause> elseClause;

    public IfStatement(
            NodeLocation location,
            Expression expression,
            List<ControlStatement> statements,
            List<ElseIfClause> elseIfClauses,
            Optional<ElseClause> elseClause)
    {
        super(location);
        this.expression = requireNonNull(expression, "expression is null");
        this.statements = requireNonNull(statements, "statements is null");
        this.elseIfClauses = requireNonNull(elseIfClauses, "elseIfClauses is null");
        this.elseClause = requireNonNull(elseClause, "elseClause is null");
    }

    public Expression getExpression()
    {
        return expression;
    }

    public List<ControlStatement> getStatements()
    {
        return statements;
    }

    public List<ElseIfClause> getElseIfClauses()
    {
        return elseIfClauses;
    }

    public Optional<ElseClause> getElseClause()
    {
        return elseClause;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        ImmutableList.Builder<Node> children = ImmutableList.<Node>builder()
                .add(expression)
                .addAll(statements)
                .addAll(elseIfClauses);
        elseClause.ifPresent(children::add);
        return children.build();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitIfStatement(this, context);
    }

    @Override
    public boolean equals(Object obj)
    {
        return (obj instanceof IfStatement other) &&
                Objects.equals(expression, other.expression) &&
                Objects.equals(statements, other.statements) &&
                Objects.equals(elseIfClauses, other.elseIfClauses) &&
                Objects.equals(elseClause, other.elseClause);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, statements, elseIfClauses, elseClause);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("expression", expression)
                .add("statements", statements)
                .add("elseIfClauses", elseIfClauses)
                .add("elseClause", elseClause)
                .toString();
    }
}
