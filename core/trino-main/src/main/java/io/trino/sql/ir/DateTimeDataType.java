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
package io.trino.sql.ir;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.DateTimeDataType.Type;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
public class DateTimeDataType
        extends DataType
{
    private final Type type;
    private final boolean withTimeZone;
    private final Optional<DataTypeParameter> precision;

    @JsonCreator
    public DateTimeDataType(
            @JsonProperty("type") Type type,
            @JsonProperty("withTimeZone") boolean withTimeZone,
            @JsonProperty("precision") Optional<DataTypeParameter> precision)
    {
        this.type = requireNonNull(type, "type is null");
        this.withTimeZone = withTimeZone;
        this.precision = requireNonNull(precision, "precision is null");
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public boolean isWithTimeZone()
    {
        return withTimeZone;
    }

    @JsonProperty
    public Optional<DataTypeParameter> getPrecision()
    {
        return precision;
    }

    @Override
    public List<? extends Expression> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    protected <R, C> R accept(IrVisitor<R, C> visitor, C context)
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

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        DateTimeDataType otherType = (DateTimeDataType) other;
        return type.equals(otherType.type) &&
                withTimeZone == otherType.withTimeZone &&
                precision.equals(otherType.precision);
    }
}
