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

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

public class GenericLongStateSerializer
        implements AccumulatorStateSerializer<GenericLongState>
{
    private final Type serializedType;

    public GenericLongStateSerializer(Type serializedType)
    {
        this.serializedType = requireNonNull(serializedType, "serializedType is null");
    }

    @Override
    public Type getSerializedType()
    {
        return serializedType;
    }

    @Override
    public void serialize(GenericLongState state, BlockBuilder out)
    {
        if (state.isNull()) {
            out.appendNull();
        }
        else {
            serializedType.writeLong(out, state.getLong());
        }
    }

    @Override
    public void deserialize(Block block, int index, GenericLongState state)
    {
        state.setNull(false);
        state.setLong(serializedType.getLong(block, index));
    }
}
