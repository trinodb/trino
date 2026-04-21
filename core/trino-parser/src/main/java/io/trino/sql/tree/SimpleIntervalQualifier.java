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

public final class SimpleIntervalQualifier
        extends IntervalQualifier
{
    private final OptionalInt precision;
    private final IntervalField field;

    public SimpleIntervalQualifier(NodeLocation location, OptionalInt precision, IntervalField field)
    {
        super(location);
        this.precision = precision;
        this.field = field;
    }

    public OptionalInt getPrecision()
    {
        return precision;
    }

    public IntervalField getField()
    {
        return field;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSimpleIntervalQualifier(this, context);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof SimpleIntervalQualifier that)) {
            return false;
        }
        return Objects.equals(precision, that.precision) && Objects.equals(field, that.field);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(precision, field);
    }

    @Override
    public String toString()
    {
        if (field instanceof IntervalField.Second(OptionalInt fractionalPrecision) && fractionalPrecision.isPresent()) {
            return field.name() +
                    "(" +
                    (precision.isPresent() ? precision.getAsInt() : "_") +
                    ", " +
                    fractionalPrecision.getAsInt() +
                    ")";
        }

        return field.name() + (precision.isPresent() ? "(" + precision.getAsInt() + ")" : "");
    }
}
