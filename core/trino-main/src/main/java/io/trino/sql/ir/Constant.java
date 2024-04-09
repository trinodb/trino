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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import com.google.errorprone.annotations.DoNotCall;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;

import java.util.List;

import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;

public record Constant(Type type, @JsonIgnore Object value)
        implements Expression
{
    @JsonCreator
    @DoNotCall // For JSON deserialization only
    public static Constant fromJson(
            @JsonProperty Type type,
            @JsonProperty Block valueAsBlock)
    {
        return new Constant(type, readNativeValue(type, valueAsBlock, 0));
    }

    public Constant
    {
        if (value != null && !Primitives.wrap(type.getJavaType()).isAssignableFrom(value.getClass())) {
            throw new IllegalArgumentException("Improper Java type (%s) for type '%s'".formatted(value.getClass().getName(), type));
        }
    }

    @JsonProperty
    public Block getValueAsBlock()
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        writeNativeValue(type, blockBuilder, value);
        return blockBuilder.build();
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitConstant(this, context);
    }

    @Override
    public List<? extends Expression> children()
    {
        return ImmutableList.of();
    }

    @Override
    public String toString()
    {
        return "[%s]::%s".formatted(
                value == null ? "<null>" : type.getObjectValue(null, getValueAsBlock(), 0),
                type);
    }
}
