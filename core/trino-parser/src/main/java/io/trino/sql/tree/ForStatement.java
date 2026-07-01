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

public final class ForStatement
        extends ControlStatement
{
    private final Optional<Identifier> label;
    private final Identifier variable;
    private final Expression lowerBound;
    private final Expression upperBound;
    private final Optional<Expression> step;
    private final List<ControlStatement> statements;

    public ForStatement(
            NodeLocation location,
            Optional<Identifier> label,
            Identifier variable,
            Expression lowerBound,
            Expression upperBound,
            Optional<Expression> step,
            List<ControlStatement> statements)
    {
        super(location);
        this.label = requireNonNull(label, "label is null");
        this.variable = requireNonNull(variable, "variable is null");
        this.lowerBound = requireNonNull(lowerBound, "lowerBound is null");
        this.upperBound = requireNonNull(upperBound, "upperBound is null");
        this.step = requireNonNull(step, "step is null");
        this.statements = requireNonNull(statements, "statements is null");
    }

    public Optional<Identifier> getLabel()
    {
        return label;
    }

    public Identifier getVariable()
    {
        return variable;
    }

    public Expression getLowerBound()
    {
        return lowerBound;
    }

    public Expression getUpperBound()
    {
        return upperBound;
    }

    public Optional<Expression> getStep()
    {
        return step;
    }

    public List<ControlStatement> getStatements()
    {
        return statements;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitForStatement(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        ImmutableList.Builder<Node> children = ImmutableList.<Node>builder()
                .add(variable)
                .add(lowerBound)
                .add(upperBound);
        step.ifPresent(children::add);
        return children.addAll(statements).build();
    }

    @Override
    public boolean equals(Object obj)
    {
        return (obj instanceof ForStatement other) &&
                Objects.equals(label, other.label) &&
                Objects.equals(variable, other.variable) &&
                Objects.equals(lowerBound, other.lowerBound) &&
                Objects.equals(upperBound, other.upperBound) &&
                Objects.equals(step, other.step) &&
                Objects.equals(statements, other.statements);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(label, variable, lowerBound, upperBound, step, statements);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("label", label)
                .add("variable", variable)
                .add("lowerBound", lowerBound)
                .add("upperBound", upperBound)
                .add("step", step)
                .add("statements", statements)
                .toString();
    }
}
