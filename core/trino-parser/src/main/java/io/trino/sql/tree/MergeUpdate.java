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
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class MergeUpdate
        extends MergeCase
{
    private final List<Assignment> assignments;

    @Deprecated
    public MergeUpdate(Optional<Expression> expression, List<Assignment> assignments)
    {
        super(Optional.empty(), expression);
        this.assignments = ImmutableList.copyOf(requireNonNull(assignments, "assignments is null"));
    }

    public MergeUpdate(NodeLocation location, Optional<Expression> expression, List<Assignment> assignments)
    {
        super(location, expression);
        this.assignments = ImmutableList.copyOf(requireNonNull(assignments, "assignments is null"));
    }

    public List<Assignment> getAssignments()
    {
        return assignments;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitMergeUpdate(this, context);
    }

    @Override
    public List<Identifier> getSetColumns()
    {
        return assignments.stream()
                .map(Assignment::getTarget)
                .collect(toImmutableList());
    }

    @Override
    public List<Expression> getSetExpressions()
    {
        return assignments.stream()
                .map(Assignment::getValue)
                .collect(toImmutableList());
    }

    @Override
    public List<? extends Node> getChildren()
    {
        ImmutableList.Builder<Node> builder = ImmutableList.builder();
        expression.ifPresent(builder::add);
        assignments.forEach(assignment -> {
            builder.add(assignment.getTarget());
            builder.add(assignment.getValue());
        });
        return builder.build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, assignments);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MergeUpdate o = (MergeUpdate) obj;
        return Objects.equals(expression, o.expression) &&
                Objects.equals(assignments, o.assignments);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("expression", expression.orElse(null))
                .add("assignments", assignments)
                .omitNullValues()
                .toString();
    }

    public static class Assignment
    {
        private final Identifier target;
        private final Expression value;

        public Assignment(Identifier target, Expression value)
        {
            this.target = requireNonNull(target, "target is null");
            this.value = requireNonNull(value, "value is null");
        }

        public Identifier getTarget()
        {
            return target;
        }

        public Expression getValue()
        {
            return value;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(target, value);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Assignment o = (Assignment) obj;
            return Objects.equals(target, o.target) &&
                    Objects.equals(value, o.value);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("target", target)
                    .add("value", value)
                    .toString();
        }
    }
}
