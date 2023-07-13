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
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

class TypedPositionsAppender
        implements PositionsAppender
{
    private static final int INSTANCE_SIZE = instanceSize(TypedPositionsAppender.class);

    private final Type type;
    private BlockBuilder blockBuilder;

    TypedPositionsAppender(Type type, int expectedPositions)
    {
        this(
                type,
                type.createBlockBuilder(null, expectedPositions));
    }

    TypedPositionsAppender(Type type, BlockBuilder blockBuilder)
    {
        this.type = requireNonNull(type, "type is null");
        this.blockBuilder = requireNonNull(blockBuilder, "blockBuilder is null");
    }

    @Override
    public void append(IntArrayList positions, Block source)
    {
        int[] positionArray = positions.elements();
        for (int i = 0; i < positions.size(); i++) {
            type.appendTo(source, positionArray[i], blockBuilder);
        }
    }

    @Override
    public void appendRle(Block block, int rlePositionCount)
    {
        for (int i = 0; i < rlePositionCount; i++) {
            type.appendTo(block, 0, blockBuilder);
        }
    }

    @Override
    public void append(int position, Block source)
    {
        type.appendTo(source, position, blockBuilder);
    }

    @Override
    public Block build()
    {
        Block result = blockBuilder.build();
        blockBuilder = blockBuilder.newBlockBuilderLike(null);
        return result;
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
