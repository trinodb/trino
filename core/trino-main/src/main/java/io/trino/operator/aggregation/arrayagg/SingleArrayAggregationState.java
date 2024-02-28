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
package io.trino.operator.aggregation.arrayagg;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;

public class SingleArrayAggregationState
        implements ArrayAggregationState
{
    private static final int INSTANCE_SIZE = instanceSize(SingleArrayAggregationState.class);

    private final FlatArrayBuilder arrayBuilder;
    private Block tempDeserializeBlock;

    public SingleArrayAggregationState(Type type, MethodHandle readFlat, MethodHandle writeFlat)
    {
        arrayBuilder = new FlatArrayBuilder(type, readFlat, writeFlat, false);
    }

    private SingleArrayAggregationState(SingleArrayAggregationState state)
    {
        // tempDeserializeBlock should never be set during a copy operation it is only used during deserialization
        checkArgument(state.tempDeserializeBlock == null);

        arrayBuilder = state.arrayBuilder.copy();
        tempDeserializeBlock = null;
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE + arrayBuilder.getEstimatedSize();
    }

    @Override
    public void add(ValueBlock block, int position)
    {
        arrayBuilder.add(block, position);
    }

    @Override
    public void writeAll(BlockBuilder blockBuilder)
    {
        arrayBuilder.writeAll(blockBuilder);
    }

    @Override
    public boolean isEmpty()
    {
        return arrayBuilder.size() == 0;
    }

    @Override
    public ArrayAggregationState copy()
    {
        return new SingleArrayAggregationState(this);
    }

    Block removeTempDeserializeBlock()
    {
        Block block = tempDeserializeBlock;
        checkState(block != null, "tempDeserializeBlock is null");
        tempDeserializeBlock = null;
        return block;
    }

    void setTempDeserializeBlock(Block tempDeserializeBlock)
    {
        this.tempDeserializeBlock = tempDeserializeBlock;
    }
}
