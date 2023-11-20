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
package io.trino.sql.relational;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.DoNotCall;
import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Objects;

import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class ConstantExpression
        extends RowExpression
{
    @JsonCreator
    @DoNotCall // For JSON deserialization only
    public static ConstantExpression fromJson(
            @JsonProperty Block value,
            @JsonProperty Type type)
    {
        return new ConstantExpression(readNativeValue(type, value, 0), type);
    }

    private final Object value;
    private final Type type;

    public ConstantExpression(Object value, Type type)
    {
        requireNonNull(type, "type is null");

        this.value = value;
        this.type = type;
    }

    public Object getValue()
    {
        return value;
    }

    @JsonProperty("value")
    public Block getBlockValue()
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        writeNativeValue(type, blockBuilder, value);
        return blockBuilder.build();
    }

    @JsonProperty
    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        if (value instanceof Slice slice) {
            if (type instanceof VarcharType || type instanceof CharType) {
                return slice.toStringUtf8();
            }
            return format("Slice(length=%s)", slice.length());
        }

        return String.valueOf(value);
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
        ConstantExpression other = (ConstantExpression) obj;
        return Objects.equals(this.value, other.value) && Objects.equals(this.type, other.type);
    }

    @Override
    public <R, C> R accept(RowExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitConstant(this, context);
    }
}
