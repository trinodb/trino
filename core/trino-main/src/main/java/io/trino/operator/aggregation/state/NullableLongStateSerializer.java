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
package io.trino.operator.aggregation.state;

import io.trino.annotation.UsedByGeneratedCode;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class NullableLongStateSerializer
        implements AccumulatorStateSerializer<NullableLongState>
{
    private final Type type;

    @UsedByGeneratedCode
    public NullableLongStateSerializer()
    {
        this(BIGINT);
    }

    public NullableLongStateSerializer(Type type)
    {
        this.type = requireNonNull(type, "type is null");
        checkArgument(type.getJavaType() == long.class, "Type must use long stack type: " + type);
    }

    @Override
    public Type getSerializedType()
    {
        return type;
    }

    @Override
    public void serialize(NullableLongState state, BlockBuilder out)
    {
        if (state.isNull()) {
            out.appendNull();
        }
        else {
            type.writeLong(out, state.getValue());
        }
    }

    @Override
    public void deserialize(Block block, int index, NullableLongState state)
    {
        if (block.isNull(index)) {
            state.setNull(true);
        }
        else {
            state.setNull(false);
            state.setValue(type.getLong(block, index));
        }
    }
}
