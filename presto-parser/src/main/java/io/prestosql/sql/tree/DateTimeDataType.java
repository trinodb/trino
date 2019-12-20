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
package io.prestosql.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DateTimeDataType
        extends DataType
{
    public enum Type
    {
        TIMESTAMP, TIME
    }

    private final Type type;
    private final boolean withTimeZone;
    private final Optional<String> precision;

    public DateTimeDataType(NodeLocation location, Type type, boolean withTimeZone, Optional<String> precision)
    {
        super(Optional.of(location));
        this.type = requireNonNull(type, "type is null");
        this.withTimeZone = withTimeZone;
        this.precision = requireNonNull(precision, "precision is null");
    }

    public Type getType()
    {
        return type;
    }

    public boolean isWithTimeZone()
    {
        return withTimeZone;
    }

    public Optional<String> getPrecision()
    {
        return precision;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDateTimeType(this, context);
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
        DateTimeDataType that = (DateTimeDataType) o;
        return withTimeZone == that.withTimeZone &&
                type == that.type &&
                precision.equals(that.precision);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, withTimeZone, precision);
    }
}
