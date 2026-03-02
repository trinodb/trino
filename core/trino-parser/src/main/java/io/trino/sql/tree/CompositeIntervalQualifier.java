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
import java.util.OptionalInt;

import static java.util.Objects.requireNonNull;

public final class CompositeIntervalQualifier
        extends IntervalQualifier
{
    private final OptionalInt precision;
    private final IntervalField from;
    private final IntervalField to;

    @Deprecated
    public CompositeIntervalQualifier(OptionalInt precision, IntervalField from, IntervalField to)
    {
        super();

        this.precision = requireNonNull(precision, "precision is null");
        this.from = from;
        this.to = requireNonNull(to, "to is null");
    }

    public CompositeIntervalQualifier(NodeLocation location, OptionalInt precision, IntervalField from, IntervalField to)
    {
        super(location);

        this.precision = requireNonNull(precision, "precision is null");
        this.from = from;
        this.to = requireNonNull(to, "to is null");
    }

    public OptionalInt getPrecision()
    {
        return precision;
    }

    public IntervalField getFrom()
    {
        return from;
    }

    public IntervalField getTo()
    {
        return to;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCompositeIntervalQualifier(this, context);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof CompositeIntervalQualifier that)) {
            return false;
        }
        return Objects.equals(precision, that.precision) && Objects.equals(from, that.from) && Objects.equals(to, that.to);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(precision, from, to);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(from.name());
        if (precision.isPresent()) {
            builder.append('(')
                    .append(precision.getAsInt())
                    .append(')');
        }
        builder.append(" TO ")
                .append(to.name());

        if (to instanceof IntervalField.Second(OptionalInt fractionalPrecision) && fractionalPrecision.isPresent()) {
            builder.append("(")
                    .append(fractionalPrecision.getAsInt())
                    .append(")");
        }

        return builder.toString();
    }
}
