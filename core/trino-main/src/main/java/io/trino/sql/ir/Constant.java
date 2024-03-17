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

import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;

public final class Constant
        extends Expression
{
    private final Type type;
    private final Object value;

    @JsonCreator
    @DoNotCall // For JSON deserialization only
    public static Constant fromJson(
            @JsonProperty Type type,
            @JsonProperty Block valueAsBlock)
    {
        return new Constant(type, readNativeValue(type, valueAsBlock, 0));
    }

    public Constant(Type type, Object value)
    {
        if (value != null && !Primitives.wrap(type.getJavaType()).isAssignableFrom(value.getClass())) {
            throw new IllegalArgumentException("Improper Java type (%s) for type '%s'".formatted(value.getClass().getName(), type));
        }
        this.type = type;
        this.value = value;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public Block getValueAsBlock()
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        writeNativeValue(type, blockBuilder, value);
        return blockBuilder.build();
    }

    public Object getValue()
    {
        return value;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitConstant(this, context);
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
        Constant that = (Constant) o;
        return Objects.equals(type, that.type) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, value);
    }

    @Override
    public String toString()
    {
        return "Constant[%s, %s]".formatted(
                type,
                value == null ? "<null>" : type.getObjectValue(null, getValueAsBlock(), 0));
    }
}
