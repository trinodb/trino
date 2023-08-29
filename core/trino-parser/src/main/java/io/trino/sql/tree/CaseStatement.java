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

public final class CaseStatement
        extends ControlStatement
{
    private final Optional<Expression> expression;
    private final List<CaseStatementWhenClause> whenClauses;
    private final Optional<ElseClause> elseClause;

    public CaseStatement(
            NodeLocation location,
            Optional<Expression> expression,
            List<CaseStatementWhenClause> whenClauses,
            Optional<ElseClause> elseClause)
    {
        super(location);
        this.expression = requireNonNull(expression, "expression is null");
        this.whenClauses = requireNonNull(whenClauses, "whenClauses is null");
        this.elseClause = requireNonNull(elseClause, "elseClause is null");
    }

    public Optional<Expression> getExpression()
    {
        return expression;
    }

    public List<CaseStatementWhenClause> getWhenClauses()
    {
        return whenClauses;
    }

    public Optional<ElseClause> getElseClause()
    {
        return elseClause;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCaseStatement(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        ImmutableList.Builder<Node> children = ImmutableList.builder();
        expression.ifPresent(children::add);
        children.addAll(whenClauses);
        elseClause.ifPresent(children::add);
        return children.build();
    }

    @Override
    public boolean equals(Object obj)
    {
        return (obj instanceof CaseStatement other) &&
                Objects.equals(expression, other.expression) &&
                Objects.equals(whenClauses, other.whenClauses) &&
                Objects.equals(elseClause, other.elseClause);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, whenClauses, elseClause);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("expression", expression)
                .add("whenClauses", whenClauses)
                .add("elseClause", elseClause)
                .toString();
    }
}
