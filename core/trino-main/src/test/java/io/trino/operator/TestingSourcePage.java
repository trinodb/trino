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
package io.trino.operator;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;

import java.util.Arrays;
import java.util.function.ObjLongConsumer;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TestingSourcePage
        implements SourcePage
{
    private final int positionCount;
    private final Block[] blocks;
    private final boolean[] loaded;

    public TestingSourcePage(int positionCount, Block... blocks)
    {
        this.positionCount = positionCount;
        this.blocks = requireNonNull(blocks, "blocks is null");
        this.loaded = new boolean[blocks.length];
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        long sizeInBytes = 0;
        for (Block block : blocks) {
            if (block != null) {
                sizeInBytes += block.getSizeInBytes();
            }
        }
        return sizeInBytes;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long retainedSizeInBytes = 0;
        for (Block block : blocks) {
            if (block != null) {
                retainedSizeInBytes += block.getRetainedSizeInBytes();
            }
        }
        return retainedSizeInBytes;
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        for (Block block : blocks) {
            if (block != null) {
                block.retainedBytesForEachPart(consumer);
            }
        }
    }

    @Override
    public int getChannelCount()
    {
        return blocks.length;
    }

    public boolean wasLoaded(int channel)
    {
        return loaded[channel];
    }

    @Override
    public Block getBlock(int channel)
    {
        Block block = blocks[channel];
        checkArgument(block != null, "Block %s should not be accessed", channel);
        loaded[channel] = true;
        return block;
    }

    @Override
    public Page getPage()
    {
        for (Block block : blocks) {
            checkArgument(block != null, "Page cannot be created because block is null");
        }
        Arrays.fill(loaded, true);
        Block[] blocks = this.blocks.clone();
        return new Page(positionCount, blocks);
    }

    @Override
    public void selectPositions(int[] positions, int offset, int size)
    {
        for (int i = 0; i < blocks.length; i++) {
            Block block = blocks[i];
            if (block != null) {
                blocks[i] = block.getPositions(positions, offset, size);
            }
        }
    }
}
