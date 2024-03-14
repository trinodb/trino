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
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class GenericLiteral
        extends Literal
{
    private final Type type;
    private final String value;

    @JsonCreator
    public GenericLiteral(Type type, String value)
    {
        requireNonNull(type, "type is null");
        requireNonNull(value, "value is null");
        this.type = type;
        this.value = value;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public String getValue()
    {
        return value;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitGenericLiteral(this, context);
    }

    @Override
    public List<? extends Expression> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, type);
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

        GenericLiteral other = (GenericLiteral) obj;
        return Objects.equals(this.value, other.value) &&
                Objects.equals(this.type, other.type);
    }

    @Override
    public String toString()
    {
        return "Literal[%s, %s]".formatted(type, value);
    }
}
