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
package io.trino.operator.output;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import static io.airlift.slice.SizeOf.instanceSize;

class TypedPositionsAppender
        implements PositionsAppender
{
    private static final int INSTANCE_SIZE = instanceSize(TypedPositionsAppender.class);

    private BlockBuilder blockBuilder;

    TypedPositionsAppender(Type type, int expectedPositions)
    {
        this.blockBuilder = type.createBlockBuilder(null, expectedPositions);
    }

    public TypedPositionsAppender(BlockBuilder blockBuilder)
    {
        this.blockBuilder = blockBuilder;
    }

    @Override
    public void append(IntArrayList positions, ValueBlock block)
    {
        blockBuilder.appendPositions(block, positions.elements(), 0, positions.size());
    }

    @Override
    public void appendRle(ValueBlock block, int count)
    {
        blockBuilder.appendRepeated(block, 0, count);
    }

    @Override
    public void append(int position, ValueBlock block)
    {
        blockBuilder.append(block, position);
    }

    @Override
    public Block build()
    {
        Block result = blockBuilder.build();
        reset();
        return result;
    }

    @Override
    public void reset()
    {
        if (blockBuilder.getPositionCount() > 0) {
            blockBuilder = blockBuilder.newBlockBuilderLike(null);
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + blockBuilder.getRetainedSizeInBytes();
    }

    @Override
    public long getSizeInBytes()
    {
        return blockBuilder.getSizeInBytes();
    }
}
