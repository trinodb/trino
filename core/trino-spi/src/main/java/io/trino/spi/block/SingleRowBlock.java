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

package io.trino.spi.block;

import org.openjdk.jol.info.ClassLayout;

import java.util.function.BiConsumer;

import static io.trino.spi.block.BlockUtil.ensureBlocksAreLoaded;
import static java.lang.String.format;

public class SingleRowBlock
        extends AbstractSingleRowBlock
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleRowBlock.class).instanceSize();

    private final Block[] fieldBlocks;
    private final int rowIndex;

    SingleRowBlock(int rowIndex, Block[] fieldBlocks)
    {
        this.rowIndex = rowIndex;
        this.fieldBlocks = fieldBlocks;
    }

    int getNumFields()
    {
        return fieldBlocks.length;
    }

    @Override
    Block[] getRawFieldBlocks()
    {
        return fieldBlocks;
    }

    @Override
    protected Block getRawFieldBlock(int fieldIndex)
    {
        return fieldBlocks[fieldIndex];
    }

    @Override
    public int getPositionCount()
    {
        return fieldBlocks.length;
    }

    @Override
    public long getSizeInBytes()
    {
        long sizeInBytes = 0;
        for (int i = 0; i < fieldBlocks.length; i++) {
            sizeInBytes += getRawFieldBlock(i).getRegionSizeInBytes(getRowIndex(), 1);
        }
        return sizeInBytes;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long retainedSizeInBytes = INSTANCE_SIZE;
        for (int i = 0; i < fieldBlocks.length; i++) {
            retainedSizeInBytes += getRawFieldBlock(i).getRetainedSizeInBytes();
        }
        return retainedSizeInBytes;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        for (Block fieldBlock : fieldBlocks) {
            consumer.accept(fieldBlock, fieldBlock.getRetainedSizeInBytes());
        }
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public String getEncodingName()
    {
        return SingleRowBlockEncoding.NAME;
    }

    @Override
    public int getRowIndex()
    {
        return rowIndex;
    }

    @Override
    public String toString()
    {
        return format("SingleRowBlock{numFields=%d}", fieldBlocks.length);
    }

    @Override
    public boolean isLoaded()
    {
        for (Block fieldBlock : fieldBlocks) {
            if (!fieldBlock.isLoaded()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Block getLoadedBlock()
    {
        Block[] loadedFieldBlocks = ensureBlocksAreLoaded(fieldBlocks);
        if (loadedFieldBlocks == fieldBlocks) {
            // All blocks are already loaded
            return this;
        }
        return new SingleRowBlock(getRowIndex(), loadedFieldBlocks);
    }
}
