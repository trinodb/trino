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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkValidPosition;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static io.trino.spi.block.BlockUtil.compactArray;
import static io.trino.spi.block.DictionaryId.randomDictionaryId;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

/**
 * A dictionary block maps each position to a position in an underlying dictionary.
 * <p>
 * A dictionary block and its underlying dictionary both contain at least two positions.
 * <p>
 * Methods returning {@link Block} may return a simpler equivalent representation:
 * <ul>
 * <li>empty and single-position results are returned directly from the dictionary;</li>
 * <li>multi-position results backed by a single-entry or run-length encoded dictionary are
 * represented as a {@link RunLengthEncodedBlock};</li>
 * <li>compacted identity mappings are returned directly as the underlying value block; and</li>
 * <li>nested dictionaries are flattened to a single dictionary layer.</li>
 * </ul>
 * Callers should not assume the returned block is a {@link DictionaryBlock}.
 */
public final class DictionaryBlock
        implements Block
{
    private static final int INSTANCE_SIZE = instanceSize(DictionaryBlock.class) + instanceSize(DictionaryId.class);
    private static final int NULL_NOT_FOUND = -1;
    private static final byte SEQUENTIAL_IDS_UNCHECKED = 0;
    private static final byte SEQUENTIAL_IDS = 1;
    private static final byte NON_SEQUENTIAL_IDS = 2;

    private final int positionCount;
    private final ValueBlock dictionary;
    private final int idsOffset;
    private final int[] ids;
    private final long retainedSizeInBytes;
    private volatile long sizeInBytes = -1;
    private volatile int uniqueIds = -1;
    // SEQUENTIAL_IDS_UNCHECKED means the ids have not been inspected. The computed states are valid
    // independently from uniqueIds because some construction paths know only one fact. countUniqueIds
    // publishes uniqueIds before the computed sequential state.
    private volatile byte sequentialIdsState = SEQUENTIAL_IDS_UNCHECKED;
    private final DictionaryId dictionarySourceId;
    private final boolean mayHaveNull;

    public static Block create(int positionCount, Block dictionary, int[] ids)
    {
        return createInternal(0, positionCount, dictionary, ids, randomDictionaryId());
    }

    /**
     * This should not only be used when creating a projection of another dictionary block.
     */
    public static Block createProjectedDictionaryBlock(int positionCount, Block dictionary, int[] ids, DictionaryId dictionarySourceId)
    {
        return createInternal(0, positionCount, dictionary, ids, dictionarySourceId);
    }

    static Block createInternal(int idsOffset, int positionCount, Block dictionary, int[] ids, DictionaryId dictionarySourceId)
    {
        if (dictionary instanceof ValueBlock valueBlock) {
            return createInternal(idsOffset, positionCount, valueBlock, ids, false, SEQUENTIAL_IDS_UNCHECKED, dictionarySourceId);
        }

        // if dictionary is an RLE then this can just be a new RLE
        if (dictionary instanceof RunLengthEncodedBlock rle) {
            return RunLengthEncodedBlock.create(rle.getValue(), positionCount);
        }

        // unwrap dictionary in dictionary
        int[] newIds = new int[positionCount];
        for (int position = 0; position < positionCount; position++) {
            newIds[position] = dictionary.getUnderlyingValuePosition(ids[idsOffset + position]);
        }
        return createInternal(0, positionCount, dictionary.getUnderlyingValueBlock(), newIds, false, SEQUENTIAL_IDS_UNCHECKED, randomDictionaryId());
    }

    private static Block createInternal(int idsOffset, int positionCount, ValueBlock dictionary, int[] ids, boolean dictionaryIsCompacted, byte sequentialIdsState, DictionaryId dictionarySourceId)
    {
        if (positionCount == 0) {
            return dictionary.copyRegion(0, 0);
        }
        if (positionCount == 1) {
            return dictionary.getRegion(ids[idsOffset], 1);
        }

        // A dictionary with a single entry contains the same value at every position.
        if (dictionary.getPositionCount() == 1) {
            return RunLengthEncodedBlock.create(dictionary, positionCount);
        }

        return new DictionaryBlock(idsOffset, positionCount, dictionary, ids, dictionaryIsCompacted, sequentialIdsState, dictionarySourceId);
    }

    private DictionaryBlock(int idsOffset, int positionCount, ValueBlock dictionary, int[] ids, boolean dictionaryIsCompacted, byte sequentialIdsState, DictionaryId dictionarySourceId)
    {
        requireNonNull(dictionary, "dictionary is null");
        requireNonNull(ids, "ids is null");

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        if (positionCount < 2) {
            throw new IllegalArgumentException("positionCount must be at least 2");
        }
        if (dictionary.getPositionCount() < 2) {
            throw new IllegalArgumentException("dictionary must have at least 2 positions");
        }

        this.idsOffset = idsOffset;
        if (ids.length - idsOffset < positionCount) {
            throw new IllegalArgumentException("ids length is less than positionCount");
        }

        this.positionCount = positionCount;
        this.dictionary = dictionary;
        this.ids = ids;
        this.dictionarySourceId = requireNonNull(dictionarySourceId, "dictionarySourceId is null");
        this.retainedSizeInBytes = INSTANCE_SIZE + sizeOf(ids);
        this.mayHaveNull = dictionary.mayHaveNull();

        if (dictionaryIsCompacted) {
            this.uniqueIds = dictionary.getPositionCount();
        }

        this.sequentialIdsState = sequentialIdsState;
    }

    public int[] getRawIds()
    {
        return ids;
    }

    public int getRawIdsOffset()
    {
        return idsOffset;
    }

    @Override
    public ValueBlock getSingleValueBlock(int position)
    {
        return dictionary.getSingleValueBlock(getId(position));
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        long sizeInBytes = this.sizeInBytes;
        if (sizeInBytes == -1) {
            // size is estimated based on the average dictionary entry size
            double averageEntrySize = dictionary.getSizeInBytes() / (double) dictionary.getPositionCount();
            sizeInBytes = (long) (averageEntrySize * positionCount) + (Integer.BYTES * (long) positionCount);
            this.sizeInBytes = sizeInBytes;
        }
        return sizeInBytes;
    }

    private void countUniqueIds()
    {
        int uniqueIds = 0;
        boolean[] used = new boolean[dictionary.getPositionCount()];
        boolean sequentialIds = true;
        int previousPosition = -1;
        for (int i = 0; i < positionCount; i++) {
            int position = ids[idsOffset + i];
            // Avoid branching
            uniqueIds += used[position] ? 0 : 1;
            used[position] = true;
            if (sequentialIds) {
                // this branch is predictable and will switch paths at most once while looping
                sequentialIds = previousPosition < position;
                previousPosition = position;
            }
        }

        this.uniqueIds = uniqueIds;
        this.sequentialIdsState = sequentialIds ? SEQUENTIAL_IDS : NON_SEQUENTIAL_IDS;
    }

    @Override
    public long getRegionSizeInBytes(int positionOffset, int length)
    {
        if (positionOffset == 0 && length == getPositionCount()) {
            // Calculation of getRegionSizeInBytes is expensive in this class.
            // On the other hand, getSizeInBytes result is cached.
            return getSizeInBytes();
        }

        if (length == 0) {
            return 0;
        }
        if (length == 1) {
            return dictionary.getRegionSizeInBytes(getId(positionOffset), 1);
        }

        double averageEntrySize = dictionary.getSizeInBytes() / (double) dictionary.getPositionCount();
        return (long) (averageEntrySize * length) + (Integer.BYTES * (long) length);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes + dictionary.getRetainedSizeInBytes();
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return dictionary.getEstimatedDataSizeForStats(getId(position));
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        consumer.accept(dictionary, dictionary.getRetainedSizeInBytes());
        consumer.accept(ids, sizeOf(ids));
        consumer.accept(this, INSTANCE_SIZE);
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        if (length <= 1 || uniqueIds == positionCount) {
            // each block position is unique or the dictionary is a nested dictionary block,
            // therefore it makes sense to unwrap this outer dictionary layer directly
            int[] positionsToCopy = new int[length];
            for (int i = 0; i < length; i++) {
                positionsToCopy[i] = getId(positions[offset + i]);
            }
            return dictionary.copyPositions(positionsToCopy, 0, length);
        }

        IntArrayList positionsToCopy = new IntArrayList();
        Int2IntOpenHashMap oldIndexToNewIndex = new Int2IntOpenHashMap(min(length, dictionary.getPositionCount()));
        int[] newIds = new int[length];

        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            int oldIndex = getId(position);
            int newId = oldIndexToNewIndex.putIfAbsent(oldIndex, positionsToCopy.size());
            if (newId == -1) {
                newId = positionsToCopy.size();
                positionsToCopy.add(oldIndex);
            }
            newIds[i] = newId;
        }
        ValueBlock compactDictionary = dictionary.copyPositions(positionsToCopy.elements(), 0, positionsToCopy.size());
        if (positionsToCopy.size() == length) {
            // discovered that all positions are unique, so return the unwrapped underlying dictionary directly
            return compactDictionary;
        }
        return createInternal(
                0,
                length,
                compactDictionary,
                newIds,
                true, // new dictionary is compact
                NON_SEQUENTIAL_IDS,
                randomDictionaryId());
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(positionCount, positionOffset, length);

        if (length == positionCount) {
            return this;
        }

        return createInternal(idsOffset + positionOffset, length, dictionary, ids, false, SEQUENTIAL_IDS_UNCHECKED, dictionarySourceId);
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        checkValidRegion(positionCount, position, length);
        if (length == 0) {
            // explicit support for case when length == 0 which might otherwise fail
            // on getId(position) if position == positionCount
            return dictionary.copyRegion(0, 0);
        }
        // Avoid repeated volatile reads to the uniqueIds field
        int uniqueIds = this.uniqueIds;
        if (length <= 1 || (uniqueIds == dictionary.getPositionCount() && sequentialIdsState == SEQUENTIAL_IDS)) {
            // copy the contiguous range directly via copyRegion
            return dictionary.copyRegion(getId(position), length);
        }
        if (uniqueIds == positionCount) {
            // each block position is unique or the dictionary is a nested dictionary block,
            // therefore it makes sense to unwrap this outer dictionary layer directly
            return dictionary.copyPositions(ids, idsOffset + position, length);
        }
        int[] newIds = compactArray(ids, idsOffset + position, length);
        if (newIds == ids) {
            return this;
        }
        DictionaryBlock result = (DictionaryBlock) createInternal(
                0,
                length,
                dictionary,
                newIds,
                false,
                SEQUENTIAL_IDS_UNCHECKED,
                randomDictionaryId());
        return result.compact();
    }

    @Override
    public boolean mayHaveNull()
    {
        return mayHaveNull;
    }

    @Override
    public boolean hasNull()
    {
        return mayHaveNull && dictionary.hasNull();
    }

    @Override
    public boolean isNull(int position)
    {
        if (!mayHaveNull()) {
            return false;
        }
        checkValidPosition(position, positionCount);
        return dictionary.isNull(getIdUnchecked(position));
    }

    @Override
    public Block getPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        int[] newIds = new int[length];
        boolean isCompact = length >= dictionary.getPositionCount() && isCompact();
        boolean[] usedIds = isCompact ? new boolean[dictionary.getPositionCount()] : null;
        int uniqueIds = 0;
        boolean sequentialIds = true;
        int previousId = -1;
        for (int i = 0; i < length; i++) {
            int id = getId(positions[offset + i]);
            newIds[i] = id;
            if (usedIds != null) {
                uniqueIds += usedIds[id] ? 0 : 1;
                usedIds[id] = true;
                if (sequentialIds) {
                    sequentialIds = previousId < id;
                    previousId = id;
                }
            }
        }
        // All positions must have been referenced in order to be compact
        isCompact &= (usedIds != null && usedIds.length == uniqueIds);
        byte sequentialIdsState = SEQUENTIAL_IDS_UNCHECKED;
        if (isCompact) {
            sequentialIdsState = sequentialIds ? SEQUENTIAL_IDS : NON_SEQUENTIAL_IDS;
        }
        Block result = createInternal(0, newIds.length, dictionary, newIds, isCompact, sequentialIdsState, getDictionarySourceId());
        if (result instanceof DictionaryBlock dictionaryBlock && usedIds != null && !isCompact) {
            // resulting dictionary is not compact, but we know the number of unique ids and which positions are used
            dictionaryBlock.uniqueIds = uniqueIds;
        }
        return result;
    }

    @Override
    public Block copyWithAppendedNull()
    {
        int desiredLength = idsOffset + positionCount + 1;
        int[] newIds = Arrays.copyOf(ids, desiredLength);
        ValueBlock newDictionary = dictionary;

        int nullIndex = NULL_NOT_FOUND;

        if (dictionary.mayHaveNull()) {
            int dictionaryPositionCount = dictionary.getPositionCount();
            for (int i = 0; i < dictionaryPositionCount; i++) {
                if (dictionary.isNull(i)) {
                    nullIndex = i;
                    break;
                }
            }
        }

        if (nullIndex == NULL_NOT_FOUND) {
            newIds[idsOffset + positionCount] = dictionary.getPositionCount();
            newDictionary = dictionary.copyWithAppendedNull();
        }
        else {
            newIds[idsOffset + positionCount] = nullIndex;
        }

        boolean compact = isCompact();
        byte sequentialIdsState = SEQUENTIAL_IDS_UNCHECKED;
        if (compact) {
            sequentialIdsState = nullIndex == NULL_NOT_FOUND ? this.sequentialIdsState : NON_SEQUENTIAL_IDS;
        }
        return new DictionaryBlock(idsOffset, positionCount + 1, newDictionary, newIds, compact, sequentialIdsState, getDictionarySourceId());
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("DictionaryBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }

    @Override
    public ValueBlock getUnderlyingValueBlock()
    {
        return dictionary;
    }

    @Override
    public int getUnderlyingValuePosition(int position)
    {
        return getId(position);
    }

    public ValueBlock getDictionary()
    {
        return dictionary;
    }

    public Block createProjection(Block newDictionary)
    {
        if (newDictionary.getPositionCount() != dictionary.getPositionCount()) {
            throw new IllegalArgumentException("newDictionary must have the same position count");
        }

        if (newDictionary instanceof ValueBlock valueBlock) {
            boolean compact = isCompact();
            return new DictionaryBlock(idsOffset, positionCount, valueBlock, ids, compact, sequentialIdsState, dictionarySourceId);
        }
        if (newDictionary instanceof RunLengthEncodedBlock rle) {
            return RunLengthEncodedBlock.create(rle.getValue(), positionCount);
        }

        // unwrap dictionary in dictionary
        int[] newIds = new int[positionCount];
        for (int position = 0; position < positionCount; position++) {
            newIds[position] = newDictionary.getUnderlyingValuePosition(getIdUnchecked(position));
        }
        return new DictionaryBlock(0, positionCount, newDictionary.getUnderlyingValueBlock(), newIds, false, SEQUENTIAL_IDS_UNCHECKED, randomDictionaryId());
    }

    boolean isSequentialIds()
    {
        byte sequentialIdsState = this.sequentialIdsState;
        if (sequentialIdsState == SEQUENTIAL_IDS_UNCHECKED) {
            countUniqueIds();
            sequentialIdsState = this.sequentialIdsState;
        }

        return sequentialIdsState == SEQUENTIAL_IDS;
    }

    int getUniqueIds()
    {
        if (uniqueIds == -1) {
            countUniqueIds();
        }

        return uniqueIds;
    }

    public int getId(int position)
    {
        checkValidPosition(position, positionCount);
        return getIdUnchecked(position);
    }

    private int getIdUnchecked(int position)
    {
        return ids[position + idsOffset];
    }

    public DictionaryId getDictionarySourceId()
    {
        return dictionarySourceId;
    }

    public boolean isCompact()
    {
        if (uniqueIds == -1) {
            countUniqueIds();
        }
        return uniqueIds == dictionary.getPositionCount();
    }

    public Block compact()
    {
        if (isCompact()) {
            if (isSequentialIds()) {
                return dictionary;
            }
            return this;
        }

        // determine which dictionary entries are referenced and build a reindex for them
        int dictionarySize = dictionary.getPositionCount();
        IntArrayList dictionaryPositionsToCopy = new IntArrayList(min(dictionarySize, positionCount));
        int[] remapIndex = new int[dictionarySize];
        Arrays.fill(remapIndex, -1);

        int newIndex = 0;
        for (int i = 0; i < positionCount; i++) {
            int dictionaryIndex = getId(i);
            if (remapIndex[dictionaryIndex] == -1) {
                dictionaryPositionsToCopy.add(dictionaryIndex);
                remapIndex[dictionaryIndex] = newIndex;
                newIndex++;
            }
        }

        // entire dictionary is referenced
        if (dictionaryPositionsToCopy.size() == dictionarySize) {
            return this;
        }

        try {
            ValueBlock compactDictionary = dictionary.copyPositions(dictionaryPositionsToCopy.elements(), 0, dictionaryPositionsToCopy.size());
            if (dictionaryPositionsToCopy.size() == positionCount) {
                return compactDictionary;
            }

            int[] newIds = getNewIds(positionCount, this, remapIndex);
            return createInternal(
                    0,
                    positionCount,
                    compactDictionary,
                    newIds,
                    true,
                    NON_SEQUENTIAL_IDS,
                    randomDictionaryId());
        }
        catch (UnsupportedOperationException e) {
            // ignore if copy positions is not supported for the dictionary block
            return this;
        }
    }

    /**
     * Compact the dictionary down to only the used positions for a set of
     * blocks that have been projected from the same dictionary.
     */
    public static List<? extends Block> compactRelatedBlocks(List<DictionaryBlock> blocks)
    {
        DictionaryBlock firstDictionaryBlock = blocks.get(0);
        for (DictionaryBlock dictionaryBlock : blocks) {
            if (!firstDictionaryBlock.getDictionarySourceId().equals(dictionaryBlock.getDictionarySourceId())) {
                throw new IllegalArgumentException("dictionarySourceIds must be the same");
            }
        }

        Block dictionary = firstDictionaryBlock.getDictionary();

        int positionCount = firstDictionaryBlock.getPositionCount();
        int dictionarySize = dictionary.getPositionCount();

        // determine which dictionary entries are referenced and build a reindex for them
        int[] dictionaryPositionsToCopy = new int[min(dictionarySize, positionCount)];
        int[] remapIndex = new int[dictionarySize];
        Arrays.fill(remapIndex, -1);

        int numberOfIndexes = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = firstDictionaryBlock.getId(i);
            if (remapIndex[position] == -1) {
                dictionaryPositionsToCopy[numberOfIndexes] = position;
                remapIndex[position] = numberOfIndexes;
                numberOfIndexes++;
            }
        }

        // entire dictionary is referenced
        if (numberOfIndexes == dictionarySize) {
            if (numberOfIndexes == positionCount && firstDictionaryBlock.isSequentialIds()) {
                List<Block> outputBlocks = new ArrayList<>(blocks.size());
                for (DictionaryBlock dictionaryBlock : blocks) {
                    outputBlocks.add(dictionaryBlock.getDictionary());
                }
                return outputBlocks;
            }
            return blocks;
        }

        // compact the dictionaries
        boolean isIdentity = numberOfIndexes == positionCount;
        int[] newIds = isIdentity ? null : getNewIds(positionCount, firstDictionaryBlock, remapIndex);
        List<Block> outputBlocks = new ArrayList<>(blocks.size());
        DictionaryId newDictionaryId = randomDictionaryId();
        for (DictionaryBlock dictionaryBlock : blocks) {
            try {
                ValueBlock compactDictionary = dictionaryBlock.getDictionary().copyPositions(dictionaryPositionsToCopy, 0, numberOfIndexes);
                if (isIdentity) {
                    outputBlocks.add(compactDictionary);
                }
                else {
                    outputBlocks.add(createInternal(
                            0,
                            positionCount,
                            compactDictionary,
                            newIds,
                            true,
                            NON_SEQUENTIAL_IDS,
                            newDictionaryId));
                }
            }
            catch (UnsupportedOperationException e) {
                // ignore if copy positions is not supported for the dictionary
                outputBlocks.add(dictionaryBlock);
            }
        }
        return outputBlocks;
    }

    private static int[] getNewIds(int positionCount, DictionaryBlock dictionaryBlock, int[] remapIndex)
    {
        int[] newIds = new int[positionCount];
        for (int i = 0; i < positionCount; i++) {
            int newId = remapIndex[dictionaryBlock.getId(i)];
            if (newId == -1) {
                throw new IllegalStateException("reference to a non-existent key");
            }
            newIds[i] = newId;
        }
        return newIds;
    }
}
