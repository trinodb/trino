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
package io.trino.spi;

import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.DictionaryId;
import io.trino.spi.block.RowBlock;
import jakarta.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOfObjectArray;
import static java.util.Objects.requireNonNull;

public final class Page
        extends RowBlock
{
    public static final int INSTANCE_SIZE = instanceSize(Page.class);

    public static long getInstanceSizeInBytes(int blockCount)
    {
        return INSTANCE_SIZE + sizeOfObjectArray(blockCount);
    }

    /**
     * Visible to give trusted classes like {@link PageBuilder} access to a constructor that doesn't
     * defensively copy the blocks
     */
    static Page createPageInternal(int positionCount, Block[] blocks)
    {
        return new Page(false, positionCount, blocks, computeFixedSizePerRow(blocks));
    }

    public Page(Block... blocks)
    {
        this(true, determinePositionCount(blocks), blocks, computeFixedSizePerRow(blocks));
    }

    public Page(int positionCount)
    {
        this(false, positionCount, EMPTY_BLOCKS, 0);
    }

    public Page(int positionCount, Block... blocks)
    {
        this(true, positionCount, blocks, computeFixedSizePerRow(blocks));
    }

    private Page(boolean blocksCopyRequired, int positionCount, Block[] blocks, int fixedSizePerRow)
    {
        super(positionCount, null, blocks.length > 0 && blocksCopyRequired ? blocks.clone() : blocks, fixedSizePerRow);
    }

    @Override
    protected Page createDerrivedRowBlock(int positionCount, @Nullable boolean[] rowIsNull, Block[] fieldBlocks)
    {
        if (rowIsNull != null) {
            throw new IllegalArgumentException("rowIsNull is not supported");
        }
        return new Page(false, positionCount, fieldBlocks, fixedSizePerRow);
    }

    public int getChannelCount()
    {
        return fieldBlocks.length;
    }

    /**
     * @Deprecated Use {@link #getFieldBlock(int)} instead
     */
    @Deprecated
    public Block getBlock(int channel)
    {
        return getFieldBlock(channel);
    }

    /**
     * @Deprecated Use {@link #appendField(Block)} instead
     */
    @Deprecated
    public Page appendColumn(Block block)
    {
        return appendField(block);
    }

    @Override
    public Page appendField(Block block)
    {
        return (Page) super.appendField(block);
    }

    public void compact()
    {
        if (getRetainedSizeInBytes() <= getSizeInBytes()) {
            return;
        }

        for (int i = 0; i < fieldBlocks.length; i++) {
            Block block = fieldBlocks[i];
            if (block instanceof DictionaryBlock) {
                continue;
            }
            // Compact the block
            fieldBlocks[i] = block.copyRegion(0, block.getPositionCount());
        }

        Map<DictionaryId, DictionaryBlockIndexes> dictionaryBlocks = getRelatedDictionaryBlocks();
        for (DictionaryBlockIndexes blockIndexes : dictionaryBlocks.values()) {
            List<DictionaryBlock> compactBlocks = DictionaryBlock.compactRelatedBlocks(blockIndexes.getBlocks());
            List<Integer> indexes = blockIndexes.getIndexes();
            for (int i = 0; i < compactBlocks.size(); i++) {
                fieldBlocks[indexes.get(i)] = compactBlocks.get(i);
            }
        }

        updateRetainedSize();
    }

    private Map<DictionaryId, DictionaryBlockIndexes> getRelatedDictionaryBlocks()
    {
        Map<DictionaryId, DictionaryBlockIndexes> relatedDictionaryBlocks = new HashMap<>();

        for (int i = 0; i < fieldBlocks.length; i++) {
            Block block = fieldBlocks[i];
            if (block instanceof DictionaryBlock dictionaryBlock) {
                relatedDictionaryBlocks.computeIfAbsent(dictionaryBlock.getDictionarySourceId(), _ -> new DictionaryBlockIndexes())
                        .addBlock(dictionaryBlock, i);
            }
        }
        return relatedDictionaryBlocks;
    }

    /**
     * @Deprecated Use {@link #getLoadedBlock()} instead
     */
    @Deprecated
    public Page getLoadedPage()
    {
        return (Page) getLoadedBlock();
    }

    public Page getLoadedPage(int... columns)
    {
        loadFields(columns);
        return (Page) getFields(columns);
    }

    public Page getLoadedPage(int[] columns, int[] eagerlyLoadedColumns)
    {
        loadFields(eagerlyLoadedColumns);
        return (Page) getFields(columns);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("Page{");
        builder.append("positions=").append(positionCount);
        builder.append(", channels=").append(getChannelCount());
        builder.append('}');
        builder.append("@").append(Integer.toHexString(System.identityHashCode(this)));
        return builder.toString();
    }

    private static int determinePositionCount(Block... blocks)
    {
        requireNonNull(blocks, "blocks is null");
        if (blocks.length == 0) {
            throw new IllegalArgumentException("blocks is empty");
        }

        return blocks[0].getPositionCount();
    }

    /**
     * @Deprecated Use {@link #getSingleValueBlock(int)} instead
     */
    @Deprecated
    public Page getSingleValuePage(int position)
    {
        return (Page) super.getSingleValueBlock(position);
    }

    @Override
    public Page getSingleValueBlock(int position)
    {
        return (Page) super.getSingleValueBlock(position);
    }

    @Override
    public Page getPositions(int[] retainedPositions, int offset, int length)
    {
        requireNonNull(retainedPositions, "retainedPositions is null");

        // this is different from row block which simply wrappers the entire row block with a dictionary block
        Block[] blocks = new Block[this.fieldBlocks.length];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = this.fieldBlocks[i].getPositions(retainedPositions, offset, length);
        }
        return createDerrivedRowBlock(length, null, blocks);
    }

    @Override
    public Page copyPositions(int[] positions, int offset, int length)
    {
        return (Page) super.copyPositions(positions, offset, length);
    }

    @Override
    public Page getRegion(int positionOffset, int length)
    {
        return (Page) super.getRegion(positionOffset, length);
    }

    /**
     * @Deprecated Use {@link #getFields(int...)} instead
     */
    @Deprecated
    public Page getColumns(int... columns)
    {
        return (Page) getFields(columns);
    }

    /**
     * @Deprecated Use {@link #prependField(Block)} instead
     */
    @Deprecated
    public Page prependColumn(Block column)
    {
        return prependField(column);
    }

    @Override
    public Page prependField(Block column)
    {
        return (Page) super.prependField(column);
    }

    private static class DictionaryBlockIndexes
    {
        private final List<DictionaryBlock> blocks = new ArrayList<>();
        private final List<Integer> indexes = new ArrayList<>();

        public void addBlock(DictionaryBlock block, int index)
        {
            blocks.add(block);
            indexes.add(index);
        }

        public List<DictionaryBlock> getBlocks()
        {
            return blocks;
        }

        public List<Integer> getIndexes()
        {
            return indexes;
        }
    }
}
