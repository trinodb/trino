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

import static java.util.Objects.requireNonNull;

public class ColumnarMap
{
    private final Block nullCheckBlock;
    private final int offsetsOffset;
    private final int[] offsets;
    private final Block keysBlock;
    private final Block valuesBlock;

    public static ColumnarMap toColumnarMap(Block block)
    {
        requireNonNull(block, "block is null");

        if (block instanceof DictionaryBlock dictionaryBlock) {
            return toColumnarMap(dictionaryBlock);
        }
        if (block instanceof RunLengthEncodedBlock runLengthEncodedBlock) {
            return toColumnarMap(runLengthEncodedBlock);
        }

        if (!(block instanceof MapBlock mapBlock)) {
            throw new IllegalArgumentException("Invalid map block: " + block.getClass().getName());
        }

        int offsetBase = mapBlock.getOffsetBase();
        int[] offsets = mapBlock.getOffsets();

        // get the keys and values for visible region
        int firstEntryPosition = offsets[offsetBase];
        int position = block.getPositionCount();
        int totalEntryCount = offsets[position + offsetBase] - firstEntryPosition;
        Block keysBlock = mapBlock.getRawKeyBlock().getRegion(firstEntryPosition, totalEntryCount);
        Block valuesBlock = mapBlock.getRawValueBlock().getRegion(firstEntryPosition, totalEntryCount);

        return new ColumnarMap(block, offsetBase, offsets, keysBlock, valuesBlock);
    }

    private static ColumnarMap toColumnarMap(DictionaryBlock dictionaryBlock)
    {
        ColumnarMap columnarMap = toColumnarMap(dictionaryBlock.getDictionary());

        // build new offsets
        int[] offsets = new int[dictionaryBlock.getPositionCount() + 1];
        for (int position = 0; position < dictionaryBlock.getPositionCount(); position++) {
            int dictionaryId = dictionaryBlock.getId(position);
            offsets[position + 1] = offsets[position] + columnarMap.getEntryCount(dictionaryId);
        }

        // reindex dictionary
        int[] dictionaryIds = new int[offsets[dictionaryBlock.getPositionCount()]];
        int nextDictionaryIndex = 0;
        for (int position = 0; position < dictionaryBlock.getPositionCount(); position++) {
            int dictionaryId = dictionaryBlock.getId(position);
            int entryCount = columnarMap.getEntryCount(dictionaryId);

            int startOffset = columnarMap.getOffset(dictionaryId);
            for (int entryIndex = 0; entryIndex < entryCount; entryIndex++) {
                dictionaryIds[nextDictionaryIndex] = startOffset + entryIndex;
                nextDictionaryIndex++;
            }
        }

        return new ColumnarMap(
                dictionaryBlock,
                0,
                offsets,
                DictionaryBlock.create(dictionaryIds.length, columnarMap.getKeysBlock(), dictionaryIds),
                DictionaryBlock.create(dictionaryIds.length, columnarMap.getValuesBlock(), dictionaryIds));
    }

    private static ColumnarMap toColumnarMap(RunLengthEncodedBlock rleBlock)
    {
        ColumnarMap columnarMap = toColumnarMap(rleBlock.getValue());

        // build new offsets block
        int[] offsets = new int[rleBlock.getPositionCount() + 1];
        int entryCount = columnarMap.getEntryCount(0);
        for (int i = 0; i < offsets.length; i++) {
            offsets[i] = i * entryCount;
        }

        // create indexes for a dictionary block of the elements
        int[] dictionaryIds = new int[rleBlock.getPositionCount() * entryCount];
        int nextDictionaryIndex = 0;
        for (int position = 0; position < rleBlock.getPositionCount(); position++) {
            for (int entryIndex = 0; entryIndex < entryCount; entryIndex++) {
                dictionaryIds[nextDictionaryIndex] = entryIndex;
                nextDictionaryIndex++;
            }
        }

        return new ColumnarMap(
                rleBlock,
                0,
                offsets,
                DictionaryBlock.create(dictionaryIds.length, columnarMap.getKeysBlock(), dictionaryIds),
                DictionaryBlock.create(dictionaryIds.length, columnarMap.getValuesBlock(), dictionaryIds));
    }

    private ColumnarMap(Block nullCheckBlock, int offsetsOffset, int[] offsets, Block keysBlock, Block valuesBlock)
    {
        this.nullCheckBlock = requireNonNull(nullCheckBlock, "nullCheckBlock is null");
        this.offsetsOffset = offsetsOffset;
        this.offsets = offsets;
        this.keysBlock = keysBlock;
        this.valuesBlock = valuesBlock;
    }

    public int getPositionCount()
    {
        return nullCheckBlock.getPositionCount();
    }

    public boolean mayHaveNull()
    {
        return nullCheckBlock.mayHaveNull();
    }

    public boolean isNull(int position)
    {
        return nullCheckBlock.isNull(position);
    }

    public int getEntryCount(int position)
    {
        return (offsets[position + 1 + offsetsOffset] - offsets[position + offsetsOffset]);
    }

    public int getOffset(int position)
    {
        return (offsets[position + offsetsOffset] - offsets[offsetsOffset]);
    }

    public Block getKeysBlock()
    {
        return keysBlock;
    }

    public Block getValuesBlock()
    {
        return valuesBlock;
    }
}
