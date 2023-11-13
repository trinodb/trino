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

public final class RepeatStatement
        extends ControlStatement
{
    private final Optional<Identifier> label;
    private final List<ControlStatement> statements;
    private final Expression condition;

    public RepeatStatement(NodeLocation location, Optional<Identifier> label, List<ControlStatement> statements, Expression condition)
    {
        super(location);
        this.label = requireNonNull(label, "label is null");
        this.statements = requireNonNull(statements, "statements is null");
        this.condition = requireNonNull(condition, "condition is null");
    }

    public Optional<Identifier> getLabel()
    {
        return label;
    }

    public List<ControlStatement> getStatements()
    {
        return statements;
    }

    public Expression getCondition()
    {
        return condition;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRepeatStatement(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .addAll(statements)
                .add(condition)
                .build();
    }

    @Override
    public boolean equals(Object obj)
    {
        return (obj instanceof RepeatStatement other) &&
                Objects.equals(label, other.label) &&
                Objects.equals(statements, other.statements) &&
                Objects.equals(condition, other.condition);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(label, statements, condition);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("label", label)
                .add("statements", statements)
                .add("condition", condition)
                .toString();
    }
}
