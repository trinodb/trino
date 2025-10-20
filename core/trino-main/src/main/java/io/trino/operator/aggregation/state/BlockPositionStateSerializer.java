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

public class BlockPositionStateSerializer
        implements AccumulatorStateSerializer<BlockPositionState>
{
    private final Type type;

    public BlockPositionStateSerializer(Type type)
    {
        this.type = type;
    }

    @Override
    public Type getSerializedType()
    {
        return type;
    }

    @Override
    public void serialize(BlockPositionState state, BlockBuilder out)
    {
        if (state.getBlock() == null) {
            out.appendNull();
        }
        else {
            Block block = state.getBlock();
            out.append(block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(state.getPosition()));
        }
    }

    @Override
    public void deserialize(Block block, int index, BlockPositionState state)
    {
        // Use the original serialized block as the underlying block for the state to save object creation overhead
        state.setPosition(index);
        state.setBlock(block);
    }
}
