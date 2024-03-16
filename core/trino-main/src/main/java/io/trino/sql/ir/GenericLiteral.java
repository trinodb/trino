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
import com.google.common.primitives.Primitives;
import com.google.errorprone.annotations.DoNotCall;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;

public final class GenericLiteral
        extends Literal
{
    private final Type type;
    private final Object rawValue;

    public static GenericLiteral constant(Type type, Object rawValue)
    {
        return new GenericLiteral(type, rawValue);
    }

    @JsonCreator
    @DoNotCall // For JSON deserialization only
    public static GenericLiteral fromJson(
            @JsonProperty Type type,
            @JsonProperty Block rawValueAsBlock)
    {
        return new GenericLiteral(type, readNativeValue(type, rawValueAsBlock, 0));
    }

    public GenericLiteral(Type type, Object rawValue)
    {
        checkArgument(
                Primitives.wrap(type.getJavaType()).isAssignableFrom(rawValue.getClass()),
                "Improper Java type (%s) for type '%s'",
                rawValue.getClass().getName(),
                type.toString());
        this.type = type;
        this.rawValue = rawValue;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public Block getRawValueAsBlock()
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        writeNativeValue(type, blockBuilder, rawValue);
        return blockBuilder.build();
    }

    public Object getRawValue()
    {
        return rawValue;
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
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GenericLiteral that = (GenericLiteral) o;
        return Objects.equals(type, that.type) && Objects.equals(rawValue, that.rawValue);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, rawValue);
    }

    @Override
    public String toString()
    {
        return "Literal[%s, %s]".formatted(type, rawValue);
    }
}
