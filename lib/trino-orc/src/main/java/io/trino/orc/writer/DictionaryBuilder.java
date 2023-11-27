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
package io.trino.orc.writer;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.XxHash64;
import io.trino.array.IntBigArray;
import io.trino.spi.block.VariableWidthBlock;

import java.util.Arrays;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class DictionaryBuilder
{
    private static final int INSTANCE_SIZE = instanceSize(DictionaryBuilder.class);

    // See jdk.internal.util.ArraysSupport.SOFT_MAX_ARRAY_LENGTH for an explanation
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    private static final float FILL_RATIO = 0.75f;
    private static final int EMPTY_SLOT = -1;
    private static final int NULL_POSITION = 0;
    private static final int EXPECTED_BYTES_PER_ENTRY = 32;

    private final IntBigArray blockPositionByHash = new IntBigArray();

    private int entryCount = 1;
    private SliceOutput sliceOutput;
    private int[] offsets;

    private int maxFill;
    private int hashMask;

    public DictionaryBuilder(int expectedSize)
    {
        checkArgument(expectedSize >= 0, "expectedSize must not be negative");

        // todo we can do better
        int expectedEntries = min(expectedSize, DEFAULT_MAX_PAGE_SIZE_IN_BYTES / EXPECTED_BYTES_PER_ENTRY);
        // it is guaranteed expectedEntries * EXPECTED_BYTES_PER_ENTRY will not overflow
        int expectedBytes = expectedEntries * EXPECTED_BYTES_PER_ENTRY;
        sliceOutput = new DynamicSliceOutput(min(expectedBytes, MAX_ARRAY_SIZE));

        int hashSize = arraySize(expectedSize, FILL_RATIO);
        this.maxFill = calculateMaxFill(hashSize);
        this.hashMask = hashSize - 1;

        this.offsets = new int[maxFill + 1];

        blockPositionByHash.ensureCapacity(hashSize);
        blockPositionByHash.fill(EMPTY_SLOT);
    }

    public long getSizeInBytes()
    {
        return sliceOutput.size() + sizeOf(offsets);
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                sliceOutput.getRetainedSize() +
                sizeOf(offsets) +
                blockPositionByHash.sizeOf();
    }

    public VariableWidthBlock getElementBlock()
    {
        boolean[] isNull = new boolean[entryCount];
        isNull[NULL_POSITION] = true;
        return new VariableWidthBlock(entryCount, sliceOutput.slice(), offsets, Optional.of(isNull));
    }

    public void clear()
    {
        blockPositionByHash.fill(EMPTY_SLOT);

        int initialSize = min((int) (sliceOutput.size() * 1.25), MAX_ARRAY_SIZE);
        sliceOutput = new DynamicSliceOutput(initialSize);
        entryCount = 1;
        Arrays.fill(offsets, 0);
    }

    public int putIfAbsent(VariableWidthBlock block, int position)
    {
        requireNonNull(block, "block must not be null");

        if (block.isNull(position)) {
            return NULL_POSITION;
        }

        int blockPosition;
        long hashPosition = getHashPositionOfElement(block, position);
        if (blockPositionByHash.get(hashPosition) != EMPTY_SLOT) {
            blockPosition = blockPositionByHash.get(hashPosition);
        }
        else {
            blockPosition = addNewElement(hashPosition, block, position);
        }
        verify(blockPosition != NULL_POSITION);
        return blockPosition;
    }

    public int getEntryCount()
    {
        return entryCount;
    }

    /**
     * Get slot position of the element at {@code position} of {@code block}
     */
    private long getHashPositionOfElement(VariableWidthBlock block, int position)
    {
        checkArgument(!block.isNull(position), "position is null");
        Slice rawSlice = block.getRawSlice();
        int rawSliceOffset = block.getRawSliceOffset(position);
        int length = block.getSliceLength(position);

        long hashPosition = getMaskedHash(XxHash64.hash(rawSlice, rawSliceOffset, length));
        while (true) {
            int entryPosition = blockPositionByHash.get(hashPosition);
            if (entryPosition == EMPTY_SLOT) {
                // Doesn't have this element
                return hashPosition;
            }
            int entryOffset = offsets[entryPosition];
            int entryLength = offsets[entryPosition + 1] - entryOffset;
            if (rawSlice.equals(rawSliceOffset, length, sliceOutput.getUnderlyingSlice(), entryOffset, entryLength)) {
                // Already has this element
                return hashPosition;
            }

            hashPosition = getMaskedHash(hashPosition + 1);
        }
    }

    private int addNewElement(long hashPosition, VariableWidthBlock block, int position)
    {
        checkArgument(!block.isNull(position), "position is null");

        int newElementPositionInBlock = entryCount;

        sliceOutput.writeBytes(block.getRawSlice(), block.getRawSliceOffset(position), block.getSliceLength(position));
        entryCount++;
        offsets[entryCount] = sliceOutput.size();

        blockPositionByHash.set(hashPosition, newElementPositionInBlock);

        // increase capacity, if necessary
        if (entryCount >= maxFill) {
            rehash(maxFill * 2);
        }

        return newElementPositionInBlock;
    }

    private void rehash(int size)
    {
        int newHashSize = arraySize(size + 1, FILL_RATIO);
        hashMask = newHashSize - 1;
        maxFill = calculateMaxFill(newHashSize);

        // offsets are not changed during rehashing, but we grow them hold the maxFill
        offsets = Arrays.copyOf(offsets, maxFill + 1);

        blockPositionByHash.ensureCapacity(newHashSize);
        blockPositionByHash.fill(EMPTY_SLOT);

        // the first element of elementBlock is always null
        for (int entryPosition = 1; entryPosition < entryCount; entryPosition++) {
            int entryOffset = offsets[entryPosition];
            int entryLength = offsets[entryPosition + 1] - entryOffset;
            long entryHashCode = XxHash64.hash(sliceOutput.getUnderlyingSlice(), entryOffset, entryLength);

            // values are already distinct, so just find the first empty slot
            long hashPosition = getMaskedHash(entryHashCode);
            while (true) {
                int hashEntryIndex = blockPositionByHash.get(hashPosition);
                if (hashEntryIndex == EMPTY_SLOT) {
                    blockPositionByHash.set(hashPosition, entryPosition);
                    break;
                }

                hashPosition = getMaskedHash(hashPosition + 1);
            }
        }
    }

    private static int calculateMaxFill(int hashSize)
    {
        int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
        if (maxFill == hashSize) {
            maxFill--;
        }
        return maxFill;
    }

    private long getMaskedHash(long rawHash)
    {
        return rawHash & hashMask;
    }
}
