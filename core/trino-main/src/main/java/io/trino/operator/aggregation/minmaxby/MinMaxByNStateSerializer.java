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
package io.trino.operator.aggregation.minmaxby;

import io.trino.operator.aggregation.TypedKeyValueHeap;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;

public class MinMaxByNStateSerializer
        implements AccumulatorStateSerializer<MinMaxByNState>
{
    private final MethodHandle keyComparisonMethod;
    private final Type keyType;
    private final Type valueType;
    private final Type serializedType;

    public MinMaxByNStateSerializer(MethodHandle keyComparisonMethod, Type keyType, Type valueType)
    {
        this.keyComparisonMethod = keyComparisonMethod;
        this.keyType = keyType;
        this.valueType = valueType;
        this.serializedType = TypedKeyValueHeap.getSerializedType(keyType, valueType);
    }

    @Override
    public Type getSerializedType()
    {
        return serializedType;
    }

    @Override
    public void serialize(MinMaxByNState state, BlockBuilder out)
    {
        TypedKeyValueHeap heap = state.getTypedKeyValueHeap();
        if (heap == null) {
            out.appendNull();
            return;
        }

        heap.serialize(out);
    }

    @Override
    public void deserialize(Block block, int index, MinMaxByNState state)
    {
        Block currentBlock = (Block) serializedType.getObject(block, index);
        state.setTypedKeyValueHeap(TypedKeyValueHeap.deserialize(currentBlock, keyType, valueType, keyComparisonMethod));
    }
}
