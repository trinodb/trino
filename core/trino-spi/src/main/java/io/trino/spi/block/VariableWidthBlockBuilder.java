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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import jakarta.annotation.Nullable;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorSpecies;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static io.trino.spi.block.Bitmap.clear;
import static io.trino.spi.block.Bitmap.clearBits;
import static io.trino.spi.block.Bitmap.copyBits;
import static io.trino.spi.block.Bitmap.hasSetBit;
import static io.trino.spi.block.Bitmap.hasUnsetBit;
import static io.trino.spi.block.Bitmap.set;
import static io.trino.spi.block.Bitmap.setBits;
import static io.trino.spi.block.BlockUtil.MAX_ARRAY_SIZE;
import static io.trino.spi.block.BlockUtil.calculateBlockResetBytes;
import static io.trino.spi.block.BlockUtil.calculateBlockResetSize;
import static io.trino.spi.block.BlockUtil.calculateNewArraySize;
import static io.trino.spi.block.BlockUtil.checkValidArrayGrowth;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.checkIndex;

public class VariableWidthBlockBuilder
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = instanceSize(VariableWidthBlockBuilder.class);
    static final Block NULL_VALUE_BLOCK = new VariableWidthBlock(0, 1, EMPTY_SLICE, new int[] {0, 0}, new long[] {0});
    private static final int SIZE_IN_BYTES_PER_POSITION = Integer.BYTES + Byte.BYTES;

    // Values are copied in chunks of this size, which is a vector register on every supported processor
    private static final VectorSpecies<Byte> CHUNK = ByteVector.SPECIES_128;
    private static final int CHUNK_SIZE = CHUNK.vectorByteSize();
    // the most bytes that are copied in chunks for a value when some values are longer than that
    private static final int MAX_COPIED_BYTES = 4 * CHUNK_SIZE;
    // the most bytes that are copied in chunks for a value when no value is longer than that
    private static final int MAX_COPIED_LONG_BYTES = 16 * CHUNK_SIZE;
    // where the JVM would emulate 128-bit vectors, copying in chunks is slower than copying the bytes of the values
    private static final boolean COPY_VALUES_IN_CHUNKS = ByteVector.SPECIES_PREFERRED.vectorBitSize() >= CHUNK.vectorBitSize();

    private final BlockBuilderStatus blockBuilderStatus;

    private boolean initialized;
    private final int initialEntryCount;
    private final int initialSliceOutputSize;

    private byte[] bytes = new byte[0];

    private boolean hasNullValue;
    private boolean hasNonNullValue;
    @Nullable
    private long[] valueIsValid;
    private int[] offsets = new int[1];

    private int positionCount;

    private long arraysRetainedSizeInBytes;

    public VariableWidthBlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytes)
    {
        this.blockBuilderStatus = blockBuilderStatus;

        initialEntryCount = expectedEntries;
        initialSliceOutputSize = min(expectedBytes, MAX_ARRAY_SIZE);

        updateRetainedSize();
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        return offsets[positionCount] + SIZE_IN_BYTES_PER_POSITION * (long) positionCount;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long size = INSTANCE_SIZE + arraysRetainedSizeInBytes;
        if (blockBuilderStatus != null) {
            size += BlockBuilderStatus.INSTANCE_SIZE;
        }
        return size;
    }

    public VariableWidthBlockBuilder writeEntry(Slice source)
    {
        return writeEntry(source, 0, source.length());
    }

    public VariableWidthBlockBuilder writeEntry(Slice source, int sourceIndex, int length)
    {
        ensureFreeSpace(length);
        source.getBytes(sourceIndex, bytes, offsets[positionCount], length);
        entryAdded(length, false);
        return this;
    }

    public VariableWidthBlockBuilder writeEntry(byte[] source, int sourceIndex, int length)
    {
        ensureFreeSpace(length);
        System.arraycopy(source, sourceIndex, bytes, offsets[positionCount], length);
        entryAdded(length, false);
        return this;
    }

    @Override
    public void append(ValueBlock block, int position)
    {
        ensureCapacity(positionCount + 1);

        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) block;
        int bytesWritten = 0;
        if (variableWidthBlock.isNull(position)) {
            initializeValidityForFirstNull();
            clear(valueIsValid, 0, positionCount);
            hasNullValue = true;
        }
        else {
            int rawArrayBase = variableWidthBlock.getRawArrayBase();
            int[] rawOffsets = variableWidthBlock.getRawOffsets();
            int startValueOffset = rawOffsets[rawArrayBase + position];
            int endValueOffset = rawOffsets[rawArrayBase + position + 1];
            int length = endValueOffset - startValueOffset;
            ensureFreeSpace(length);

            Slice rawSlice = variableWidthBlock.getRawSlice();

            rawSlice.getBytes(startValueOffset, bytes, offsets[positionCount], length);

            bytesWritten = length;
            if (valueIsValid != null) {
                set(valueIsValid, 0, positionCount);
            }
            hasNonNullValue = true;
        }
        offsets[positionCount + 1] = offsets[positionCount] + bytesWritten;
        positionCount++;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(SIZE_IN_BYTES_PER_POSITION + bytesWritten);
        }
    }

    @Override
    public void appendRepeated(ValueBlock block, int position, int count)
    {
        if (count == 0) {
            return;
        }
        if (count == 1) {
            append(block, position);
            return;
        }

        ensureCapacity(positionCount + count);

        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) block;
        int bytesWritten = 0;
        if (variableWidthBlock.isNull(position)) {
            initializeValidityForFirstNull();
            clearBits(valueIsValid, 0, positionCount, count);
            Arrays.fill(offsets, positionCount + 1, positionCount + count + 1, offsets[positionCount]);
            hasNullValue = true;
        }
        else {
            int rawArrayBase = variableWidthBlock.getRawArrayBase();
            int[] rawOffsets = variableWidthBlock.getRawOffsets();
            int startValueOffset = rawOffsets[rawArrayBase + position];
            int endValueOffset = rawOffsets[rawArrayBase + position + 1];
            int length = endValueOffset - startValueOffset;

            if (length > 0) {
                bytesWritten = toIntExact((long) length * count);
                ensureFreeSpace(bytesWritten);

                // copy in the value
                Slice rawSlice = variableWidthBlock.getRawSlice();
                int currentOffset = offsets[positionCount];
                rawSlice.getBytes(startValueOffset, bytes, currentOffset, length);

                // repeatedly duplicate the written vales, doubling the number of values copied each time
                int duplicatedBytes = length;
                while (duplicatedBytes * 2 <= bytesWritten) {
                    System.arraycopy(bytes, currentOffset, bytes, currentOffset + duplicatedBytes, duplicatedBytes);
                    duplicatedBytes = duplicatedBytes * 2;
                }
                // copy the remaining values
                System.arraycopy(bytes, currentOffset, bytes, currentOffset + duplicatedBytes, bytesWritten - duplicatedBytes);

                // set the offsets
                int previousOffset = currentOffset;
                for (int i = 0; i < count; i++) {
                    previousOffset += length;
                    offsets[positionCount + i + 1] = previousOffset;
                }
                if (valueIsValid != null) {
                    setBits(valueIsValid, 0, positionCount, count);
                }
            }
            else {
                // zero length array
                Arrays.fill(offsets, positionCount + 1, positionCount + count + 1, offsets[positionCount]);
                if (valueIsValid != null) {
                    setBits(valueIsValid, 0, positionCount, count);
                }
            }

            hasNonNullValue = true;
        }
        positionCount += count;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(count * SIZE_IN_BYTES_PER_POSITION + bytesWritten);
        }
    }

    @Override
    public void appendRange(ValueBlock block, int offset, int length)
    {
        if (length == 0) {
            return;
        }
        if (length == 1) {
            append(block, offset);
            return;
        }

        ensureCapacity(positionCount + length);

        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) block;
        int rawArrayBase = variableWidthBlock.getRawArrayBase();
        int[] rawOffsets = variableWidthBlock.getRawOffsets();
        int startValueOffset = rawOffsets[rawArrayBase + offset];
        int totalSize = rawOffsets[rawArrayBase + offset + length] - startValueOffset;
        // grow the buffer for the new data
        ensureFreeSpace(totalSize);

        Slice sourceSlice = variableWidthBlock.getRawSlice();
        sourceSlice.getBytes(startValueOffset, bytes, offsets[positionCount], totalSize);

        // update offsets for copied data
        int offsetDelta = offsets[positionCount] - rawOffsets[rawArrayBase + offset];
        for (int i = 0; i < length; i++) {
            offsets[positionCount + i + 1] = rawOffsets[rawArrayBase + offset + i + 1] + offsetDelta;
        }

        // update nulls
        long[] rawValueIsValid = variableWidthBlock.getRawValueIsValid();
        if (rawValueIsValid == null || !hasUnsetBit(rawValueIsValid, rawArrayBase + offset, length)) {
            if (valueIsValid != null) {
                setBits(valueIsValid, 0, positionCount, length);
            }
            hasNonNullValue = true;
        }
        else {
            initializeValidityForFirstNull();
            copyBits(rawValueIsValid, rawArrayBase + offset, valueIsValid, positionCount, length);
            hasNullValue = true;
            hasNonNullValue |= hasSetBit(rawValueIsValid, rawArrayBase + offset, length);
        }
        positionCount += length;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(length * SIZE_IN_BYTES_PER_POSITION + totalSize);
        }
    }

    @Override
    public void appendPositions(ValueBlock block, int[] positions, int offset, int length)
    {
        if (length == 0) {
            return;
        }
        if (length == 1) {
            append(block, positions[offset]);
            return;
        }

        ensureCapacity(positionCount + length);

        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) block;
        int rawArrayBase = variableWidthBlock.getRawArrayBase();
        int[] rawOffsets = variableWidthBlock.getRawOffsets();

        // update the offsets and compute the total size
        int initialOffset = offsets[positionCount];
        int totalSize = 0;
        int maxLength = 0;
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            int valueLength = rawOffsets[rawArrayBase + position + 1] - rawOffsets[rawArrayBase + position];
            totalSize += valueLength;
            maxLength = max(maxLength, valueLength);
            offsets[positionCount + i + 1] = initialOffset + totalSize;
        }
        // grow the buffer for the new data
        ensureFreeSpace(totalSize);

        // copy values to buffer
        Slice rawSlice = variableWidthBlock.getRawSlice();
        copyValues(rawSlice.byteArray(), rawSlice.byteArrayOffset(), rawOffsets, rawArrayBase, positions, offset, length, bytes, offsets, positionCount, totalSize, maxLength, COPY_VALUES_IN_CHUNKS);

        // update nulls
        long[] rawValueIsValid = variableWidthBlock.getRawValueIsValid();
        if (rawValueIsValid == null || !hasUnsetBit(rawValueIsValid, rawArrayBase, positions, offset, length)) {
            if (valueIsValid != null) {
                setBits(valueIsValid, 0, positionCount, length);
            }
            hasNonNullValue = true;
        }
        else {
            initializeValidityForFirstNull();
            copyBits(rawValueIsValid, rawArrayBase, positions, offset, valueIsValid, positionCount, length);
            hasNullValue = true;
            hasNonNullValue |= hasSetBit(rawValueIsValid, rawArrayBase, positions, offset, length);
        }
        positionCount += length;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(length * SIZE_IN_BYTES_PER_POSITION + totalSize);
        }
    }

    /// Copies the values at the given positions of a block to `target`. The copy of the value at
    /// `positions[offset + i]` starts at `targetOffsets[targetOffsetsBase + i]`. `totalSize` is the size of the
    /// copied values and `maxLength` the length of the longest one. With `copyInChunks`, values are copied as
    /// whole chunks where that suits their lengths, which is faster where the processor has 128-bit vectors.
    static void copyValues(
            byte[] source,
            int sourceBase,
            int[] sourceOffsets,
            int sourceOffsetsBase,
            int[] positions,
            int offset,
            int length,
            byte[] target,
            int[] targetOffsets,
            int targetOffsetsBase,
            long totalSize,
            int maxLength,
            boolean copyInChunks)
    {
        int copiedBytes = 0;
        if (copyInChunks && !isNearlyRange(positions, offset, length)) {
            copiedBytes = copiedBytes(length, totalSize, maxLength);
        }
        if (copiedBytes == 0) {
            copyRanges(source, sourceBase, sourceOffsets, sourceOffsetsBase, positions, offset, length, target, targetOffsets[targetOffsetsBase]);
            return;
        }

        // a chunk is read and written whole, so a value is copied in chunks only when they fit in both arrays from its start
        int sourceLimit = source.length - copiedBytes - sourceBase;
        int targetLimit = target.length - copiedBytes;
        int firstNotCopied;
        if (copiedBytes <= MAX_COPIED_BYTES) {
            firstNotCopied = copyChunks(source, sourceBase, sourceOffsets, sourceOffsetsBase, positions, offset, length, target, targetOffsets, targetOffsetsBase, copiedBytes, sourceLimit, targetLimit);
        }
        else {
            firstNotCopied = copyLongChunks(source, sourceBase, sourceOffsets, sourceOffsetsBase, positions, offset, length, target, targetOffsets, targetOffsetsBase, copiedBytes, sourceLimit, targetLimit);
        }

        // The values at the end of an array, which are the last ones when the target has no room after the values,
        // and the values that are longer than the bytes copied for them, which can be anywhere
        int firstRemaining = firstNotCopied;
        if (maxLength > copiedBytes) {
            firstRemaining = 0;
        }
        for (int i = firstRemaining; i < length; i++) {
            int position = positions[offset + i];
            int sourceStart = sourceOffsets[sourceOffsetsBase + position];
            int valueLength = sourceOffsets[sourceOffsetsBase + position + 1] - sourceStart;
            int targetStart = targetOffsets[targetOffsetsBase + i];
            if (valueLength > copiedBytes || sourceStart > sourceLimit || targetStart > targetLimit) {
                System.arraycopy(source, sourceBase + sourceStart, target, targetStart, valueLength);
            }
        }
    }

    /// Returns whether the positions are a range of the positions of a block with few gaps, when they are in
    /// increasing order. The values of such positions are next to each other, and copying them as ranges takes few
    /// calls. For positions with more gaps, whether a position follows the one before is a branch that a processor
    /// cannot predict, and copying ranges is slower than copying a fixed number of chunks for every value.
    private static boolean isNearlyRange(int[] positions, int offset, int length)
    {
        long gaps = (long) positions[offset + length - 1] - positions[offset] + 1 - length;
        return gaps <= length >> 5;
    }

    /// Returns the number of bytes to copy in chunks for every one of `length` values, or 0 when the values are
    /// too long, or their lengths too far apart, for chunks to be faster than copying ranges of values.
    private static int copiedBytes(int length, long totalSize, int maxLength)
    {
        if (maxLength <= MAX_COPIED_BYTES) {
            // chunks that cover the longest value
            int copiedBytes = CHUNK_SIZE;
            while (copiedBytes < maxLength) {
                copiedBytes *= 2;
            }
            return copiedBytes;
        }
        if (maxLength <= MAX_COPIED_LONG_BYTES) {
            // Long values of similar lengths: chunks that cover the longest value, when that is at most twice the
            // bytes of the values. A copy of a fixed size has no branch on the length of a value and no call.
            int copiedBytes = (maxLength + MAX_COPIED_BYTES - 1) / MAX_COPIED_BYTES * MAX_COPIED_BYTES;
            if ((long) copiedBytes * length <= 2 * totalSize) {
                return copiedBytes;
            }
        }
        // A few long values among short ones: chunks that cover twice the average length, which is most values of
        // a column. The longer values are copied by themselves afterwards.
        long coveredLength = 2 * totalSize / length;
        if (coveredLength > MAX_COPIED_BYTES) {
            return 0;
        }
        int copiedBytes = CHUNK_SIZE;
        while (copiedBytes < coveredLength) {
            copiedBytes *= 2;
        }
        return copiedBytes;
    }

    /// Copies `copiedBytes` bytes from the start of every value, whatever its length, so that the loop has neither
    /// a branch on the length nor a call. A longer copy writes over the start of the next value, which the copy of
    /// that value then replaces. Returns the index of the first value that was not copied, since its chunks do not
    /// fit in one of the arrays from the start of the value, or `length` when all were copied.
    ///
    /// The method has one loop for every number of chunks: a loop over the chunks of a value inside the loop over
    /// the values keeps the JIT compiler from treating the latter as an innermost loop, which makes it several
    /// times slower.
    private static int copyChunks(
            byte[] source,
            int sourceBase,
            int[] sourceOffsets,
            int sourceOffsetsBase,
            int[] positions,
            int offset,
            int length,
            byte[] target,
            int[] targetOffsets,
            int targetOffsetsBase,
            int copiedBytes,
            int sourceLimit,
            int targetLimit)
    {
        int firstNotCopied = length;
        if (copiedBytes == CHUNK_SIZE) {
            for (int i = 0; i < length; i++) {
                int sourceStart = sourceOffsets[sourceOffsetsBase + positions[offset + i]];
                int targetStart = targetOffsets[targetOffsetsBase + i];
                if (sourceStart <= sourceLimit && targetStart <= targetLimit) {
                    copyChunk(source, sourceBase + sourceStart, target, targetStart);
                }
                else {
                    firstNotCopied = min(firstNotCopied, i);
                }
            }
        }
        else if (copiedBytes == 2 * CHUNK_SIZE) {
            for (int i = 0; i < length; i++) {
                int sourceStart = sourceOffsets[sourceOffsetsBase + positions[offset + i]];
                int targetStart = targetOffsets[targetOffsetsBase + i];
                if (sourceStart <= sourceLimit && targetStart <= targetLimit) {
                    copyChunk(source, sourceBase + sourceStart, target, targetStart);
                    copyChunk(source, sourceBase + sourceStart + CHUNK_SIZE, target, targetStart + CHUNK_SIZE);
                }
                else {
                    firstNotCopied = min(firstNotCopied, i);
                }
            }
        }
        else {
            for (int i = 0; i < length; i++) {
                int sourceStart = sourceOffsets[sourceOffsetsBase + positions[offset + i]];
                int targetStart = targetOffsets[targetOffsetsBase + i];
                if (sourceStart <= sourceLimit && targetStart <= targetLimit) {
                    copyChunk(source, sourceBase + sourceStart, target, targetStart);
                    copyChunk(source, sourceBase + sourceStart + CHUNK_SIZE, target, targetStart + CHUNK_SIZE);
                    copyChunk(source, sourceBase + sourceStart + 2 * CHUNK_SIZE, target, targetStart + 2 * CHUNK_SIZE);
                    copyChunk(source, sourceBase + sourceStart + 3 * CHUNK_SIZE, target, targetStart + 3 * CHUNK_SIZE);
                }
                else {
                    firstNotCopied = min(firstNotCopied, i);
                }
            }
        }
        return firstNotCopied;
    }

    /// Like [#copyChunks], for 8, 12 or 16 chunks, which cover every value that is copied.
    private static int copyLongChunks(
            byte[] source,
            int sourceBase,
            int[] sourceOffsets,
            int sourceOffsetsBase,
            int[] positions,
            int offset,
            int length,
            byte[] target,
            int[] targetOffsets,
            int targetOffsetsBase,
            int copiedBytes,
            int sourceLimit,
            int targetLimit)
    {
        int firstNotCopied = length;
        if (copiedBytes == 8 * CHUNK_SIZE) {
            for (int i = 0; i < length; i++) {
                int sourceStart = sourceOffsets[sourceOffsetsBase + positions[offset + i]];
                int targetStart = targetOffsets[targetOffsetsBase + i];
                if (sourceStart <= sourceLimit && targetStart <= targetLimit) {
                    copyFourChunks(source, sourceBase + sourceStart, target, targetStart);
                    copyFourChunks(source, sourceBase + sourceStart + 4 * CHUNK_SIZE, target, targetStart + 4 * CHUNK_SIZE);
                }
                else {
                    firstNotCopied = min(firstNotCopied, i);
                }
            }
        }
        else if (copiedBytes == 12 * CHUNK_SIZE) {
            for (int i = 0; i < length; i++) {
                int sourceStart = sourceOffsets[sourceOffsetsBase + positions[offset + i]];
                int targetStart = targetOffsets[targetOffsetsBase + i];
                if (sourceStart <= sourceLimit && targetStart <= targetLimit) {
                    copyFourChunks(source, sourceBase + sourceStart, target, targetStart);
                    copyFourChunks(source, sourceBase + sourceStart + 4 * CHUNK_SIZE, target, targetStart + 4 * CHUNK_SIZE);
                    copyFourChunks(source, sourceBase + sourceStart + 8 * CHUNK_SIZE, target, targetStart + 8 * CHUNK_SIZE);
                }
                else {
                    firstNotCopied = min(firstNotCopied, i);
                }
            }
        }
        else {
            for (int i = 0; i < length; i++) {
                int sourceStart = sourceOffsets[sourceOffsetsBase + positions[offset + i]];
                int targetStart = targetOffsets[targetOffsetsBase + i];
                if (sourceStart <= sourceLimit && targetStart <= targetLimit) {
                    copyFourChunks(source, sourceBase + sourceStart, target, targetStart);
                    copyFourChunks(source, sourceBase + sourceStart + 4 * CHUNK_SIZE, target, targetStart + 4 * CHUNK_SIZE);
                    copyFourChunks(source, sourceBase + sourceStart + 8 * CHUNK_SIZE, target, targetStart + 8 * CHUNK_SIZE);
                    copyFourChunks(source, sourceBase + sourceStart + 12 * CHUNK_SIZE, target, targetStart + 12 * CHUNK_SIZE);
                }
                else {
                    firstNotCopied = min(firstNotCopied, i);
                }
            }
        }
        return firstNotCopied;
    }

    private static void copyChunk(byte[] source, int sourceIndex, byte[] target, int targetIndex)
    {
        ByteVector.fromArray(CHUNK, source, sourceIndex).intoArray(target, targetIndex);
    }

    private static void copyFourChunks(byte[] source, int sourceIndex, byte[] target, int targetIndex)
    {
        copyChunk(source, sourceIndex, target, targetIndex);
        copyChunk(source, sourceIndex + CHUNK_SIZE, target, targetIndex + CHUNK_SIZE);
        copyChunk(source, sourceIndex + 2 * CHUNK_SIZE, target, targetIndex + 2 * CHUNK_SIZE);
        copyChunk(source, sourceIndex + 3 * CHUNK_SIZE, target, targetIndex + 3 * CHUNK_SIZE);
    }

    /// Copies the values one after the other from `targetStart`, with a single call for the values of adjacent positions.
    private static void copyRanges(byte[] source, int sourceBase, int[] sourceOffsets, int sourceOffsetsBase, int[] positions, int offset, int length, byte[] target, int targetStart)
    {
        // the values that are not copied yet
        int rangeStart = 0;
        int rangeEnd = 0;
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            int sourceStart = sourceOffsets[sourceOffsetsBase + position];
            if (sourceStart != rangeEnd) {
                System.arraycopy(source, sourceBase + rangeStart, target, targetStart, rangeEnd - rangeStart);
                targetStart += rangeEnd - rangeStart;
                rangeStart = sourceStart;
            }
            rangeEnd = sourceOffsets[sourceOffsetsBase + position + 1];
        }
        System.arraycopy(source, sourceBase + rangeStart, target, targetStart, rangeEnd - rangeStart);
    }

    @Override
    public BlockBuilder appendNull()
    {
        hasNullValue = true;
        entryAdded(0, true);
        return this;
    }

    @Override
    public void resetTo(int position)
    {
        checkIndex(position, positionCount + 1);
        positionCount = position;
    }

    private void entryAdded(int bytesWritten, boolean isNull)
    {
        ensureCapacity(positionCount + 1);

        if (isNull) {
            initializeValidityForFirstNull();
            clear(valueIsValid, 0, positionCount);
        }
        else if (valueIsValid != null) {
            set(valueIsValid, 0, positionCount);
        }
        offsets[positionCount + 1] = offsets[positionCount] + bytesWritten;

        positionCount++;
        hasNonNullValue |= !isNull;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(SIZE_IN_BYTES_PER_POSITION + bytesWritten);
        }
    }

    @Override
    public Block build()
    {
        if (!hasNonNullValue) {
            return RunLengthEncodedBlock.create(NULL_VALUE_BLOCK, positionCount);
        }
        return buildValueBlock();
    }

    @Override
    public VariableWidthBlock buildValueBlock()
    {
        return new VariableWidthBlock(0, positionCount, Slices.wrappedBuffer(bytes, 0, offsets[positionCount]), offsets, hasNullValue ? valueIsValid : null);
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus)
    {
        return new VariableWidthBlockBuilder(blockBuilderStatus, calculateBlockResetSize(positionCount), calculateBlockResetBytes(offsets[positionCount]));
    }

    @Override
    public BlockBuilder newBlockBuilderLike(int expectedEntries, BlockBuilderStatus blockBuilderStatus)
    {
        if (expectedEntries < 0 || expectedEntries > MAX_ARRAY_SIZE) {
            throw new IllegalArgumentException("Invalid expectedEntries: " + expectedEntries);
        }
        int currentSizeInBytes = positionCount == 0 ? positionCount : (getOffset(positionCount) - getOffset(0));
        long scaledBytes = positionCount == 0 ? 0 : (long) currentSizeInBytes * expectedEntries / positionCount;
        // a small sample can be unrepresentative, so limit how far a single reset can grow the estimate
        int expectedBytes = (int) min(min(scaledBytes, 2L * currentSizeInBytes), MAX_ARRAY_SIZE);
        return new VariableWidthBlockBuilder(blockBuilderStatus, expectedEntries, calculateBlockResetBytes(expectedBytes));
    }

    private int getOffset(int position)
    {
        return offsets[position];
    }

    private void ensureCapacity(int capacity)
    {
        if (offsets.length > capacity) {
            return;
        }

        int newSize;
        if (initialized) {
            newSize = calculateNewArraySize(capacity);
        }
        else {
            checkValidArrayGrowth(capacity, initialEntryCount);
            newSize = initialEntryCount;
            initialized = true;
        }
        newSize = max(newSize, capacity);

        if (valueIsValid != null) {
            valueIsValid = Bitmap.ensureCapacity(valueIsValid, newSize);
        }
        offsets = Arrays.copyOf(offsets, newSize + 1);
        updateRetainedSize();
    }

    private boolean initializeValidityForFirstNull()
    {
        if (valueIsValid != null) {
            return false;
        }
        valueIsValid = Bitmap.allocateWords(offsets.length - 1, false);
        setBits(valueIsValid, 0, 0, positionCount);
        updateRetainedSize();
        return true;
    }

    private void ensureFreeSpace(int extraBytesCapacity)
    {
        long requiredSize = (long) offsets[positionCount] + extraBytesCapacity;
        if (requiredSize > MAX_ARRAY_SIZE) {
            throw new TrinoException(
                    GENERIC_INSUFFICIENT_RESOURCES,
                    "Block byte size exceeds limit of " + MAX_ARRAY_SIZE + " bytes");
        }
        if (bytes.length >= requiredSize) {
            return;
        }

        int newSize = calculateNewArraySize((int) requiredSize, initialSliceOutputSize);
        bytes = Arrays.copyOf(bytes, newSize);
        updateRetainedSize();
    }

    private void updateRetainedSize()
    {
        arraysRetainedSizeInBytes = sizeOf(valueIsValid) + sizeOf(offsets) + sizeOf(bytes);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("VariableWidthBlockBuilder{");
        sb.append("positionCount=").append(positionCount);
        sb.append(", size=").append(offsets[positionCount]);
        sb.append('}');
        return sb.toString();
    }
}
