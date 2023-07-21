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

import java.util.List;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SqlRow
{
    private static final int INSTANCE_SIZE = instanceSize(SqlRow.class);

    private final Block[] fieldBlocks;
    private final List<Block> fieldBlocksList;
    private final int rawIndex;

    public SqlRow(int rawIndex, Block[] fieldBlocks)
    {
        this.rawIndex = rawIndex;
        this.fieldBlocks = requireNonNull(fieldBlocks, "fieldBlocks is null");
        fieldBlocksList = List.of(fieldBlocks);
    }

    public int getFieldCount()
    {
        return fieldBlocks.length;
    }

    public int getRawIndex()
    {
        return rawIndex;
    }

    public Block getRawFieldBlock(int fieldIndex)
    {
        return fieldBlocks[fieldIndex];
    }

    public List<Block> getRawFieldBlocks()
    {
        return fieldBlocksList;
    }

    public long getSizeInBytes()
    {
        long sizeInBytes = 0;
        for (Block fieldBlock : fieldBlocks) {
            sizeInBytes += fieldBlock.getRegionSizeInBytes(rawIndex, 1);
        }
        return sizeInBytes;
    }

    public long getRetainedSizeInBytes()
    {
        long retainedSizeInBytes = INSTANCE_SIZE;
        for (Block fieldBlock : fieldBlocks) {
            retainedSizeInBytes += fieldBlock.getRetainedSizeInBytes();
        }
        return retainedSizeInBytes;
    }

    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        for (Block fieldBlock : fieldBlocks) {
            consumer.accept(fieldBlock, fieldBlock.getRetainedSizeInBytes());
        }
        consumer.accept(this, INSTANCE_SIZE);
    }

    public int getUnderlyingFieldPosition(int fieldIndex)
    {
        return fieldBlocks[fieldIndex].getUnderlyingValuePosition(rawIndex);
    }

    public ValueBlock getUnderlyingFieldBlock(int fieldIndex)
    {
        return fieldBlocks[fieldIndex].getUnderlyingValueBlock();
    }

    @Override
    public String toString()
    {
        return format("SingleRowBlock{numFields=%d}", fieldBlocks.length);
    }
}
