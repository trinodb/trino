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
import jakarta.annotation.Nullable;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorSpecies;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.BlockUtil.MAX_ARRAY_SIZE;
import static io.trino.spi.block.BlockUtil.calculateBlockResetBytes;
import static io.trino.spi.block.BlockUtil.calculateBlockResetSize;
import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static io.trino.spi.block.StridedVariableWidthBlock.INT;
import static io.trino.spi.block.StridedVariableWidthBlock.LONG;
import static io.trino.spi.block.StridedVariableWidthBlock.nullsBytes;
import static io.trino.spi.block.StridedVariableWidthBlock.offsetsBytes;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.checkIndex;

/// Builds a [StridedVariableWidthBlock] in one `byte[]` laid out as `[nullBits | int[cap+1] offsets | values]`.
/// Appending never modifies earlier positions, so a block built mid-stream stays valid as more values are appended.
public final class StridedVariableWidthBlockBuilder
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = instanceSize(StridedVariableWidthBlockBuilder.class);
    private static final ValueBlock NULL_VALUE_BLOCK = new StridedVariableWidthBlockBuilder(1, 0).appendNull().buildValueBlock();

    private static final VectorSpecies<Byte> CHUNK = ByteVector.SPECIES_128;
    private static final int CHUNK_SIZE = CHUNK.vectorByteSize();
    private static final boolean COPY_IN_CHUNKS = ByteVector.SPECIES_PREFERRED.vectorBitSize() >= CHUNK.vectorBitSize();
    private static final int MAX_CHUNKED_CELL = 4 * CHUNK_SIZE;
    private static final int MAX_CHUNKED_VALUE = 16 * CHUNK_SIZE;
    private static final int MIN_SLOT_CAPACITY = 16;

    @Nullable
    private final BlockBuilderStatus blockBuilderStatus;
    private final int expectedEntries;
    private final int expectedBytes;

    private byte[] bytes;
    private int slotCapacity;
    private int offsetsOffset;
    private int valuesOffset;
    private int valueLimit;
    private int nextOffsetByte;
    private int nextValueOffset;

    private int positionCount;
    private boolean hasNull;
    private boolean hasNonNull;
    private int valueEnd;
    private int maxLength;

    public StridedVariableWidthBlockBuilder(int expectedEntries, int expectedBytes)
    {
        this(null, expectedEntries, expectedBytes);
    }

    public StridedVariableWidthBlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytes)
    {
        if (expectedEntries < 0 || expectedBytes < 0) {
            throw new IllegalArgumentException("expectedEntries or expectedBytes is negative");
        }
        this.blockBuilderStatus = blockBuilderStatus;
        this.expectedEntries = expectedEntries;
        this.expectedBytes = expectedBytes;
        this.bytes = new byte[0];
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        if (positionCount == 0) {
            return 0;
        }
        long nullBytes = hasNull ? nullsBytes(positionCount) : 0;
        return nullBytes + (long) Integer.BYTES * positionCount + valueEnd;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(bytes);
    }

    public StridedVariableWidthBlockBuilder writeEntry(Slice value)
    {
        return writeEntry(value.byteArray(), value.byteArrayOffset(), value.length());
    }

    public StridedVariableWidthBlockBuilder writeEntry(byte[] source, int sourceOffset, int length)
    {
        int pos = positionCount;
        int newEnd = valueEnd + length;
        if (pos >= slotCapacity || newEnd > valueLimit) {
            grow(pos + 1, newEnd);
        }
        System.arraycopy(source, sourceOffset, bytes, nextValueOffset, length);
        INT.set(bytes, nextOffsetByte, newEnd);
        nextValueOffset += length;
        nextOffsetByte += Integer.BYTES;
        valueEnd = newEnd;
        positionCount = pos + 1;
        hasNonNull = true;
        maxLength = max(maxLength, length);
        addBytes((long) Integer.BYTES + length);
        return this;
    }

    @Override
    public StridedVariableWidthBlockBuilder appendNull()
    {
        int pos = positionCount;
        if (pos >= slotCapacity) {
            grow(pos + 1, valueEnd);
        }
        INT.set(bytes, nextOffsetByte, valueEnd);
        nextOffsetByte += Integer.BYTES;
        bytes[pos >>> 3] |= (byte) (1 << (pos & 7));
        hasNull = true;
        positionCount = pos + 1;
        addBytes(Integer.BYTES);
        return this;
    }

    @Override
    public void append(ValueBlock block, int position)
    {
        if (block instanceof StridedVariableWidthBlock strided) {
            if (strided.isNull(position)) {
                appendNull();
            }
            else {
                writeEntry(strided.rawBytes(), strided.rawValuesOffset() + strided.getRawSliceOffset(position), strided.getSliceLength(position));
            }
            return;
        }
        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) block;
        if (variableWidthBlock.isNull(position)) {
            appendNull();
        }
        else {
            Slice rawSlice = variableWidthBlock.getRawSlice();
            writeEntry(rawSlice.byteArray(), rawSlice.byteArrayOffset() + variableWidthBlock.getRawSliceOffset(position), variableWidthBlock.getSliceLength(position));
        }
    }

    @Override
    public void appendRepeated(ValueBlock block, int position, int count)
    {
        for (int i = 0; i < count; i++) {
            append(block, position);
        }
    }

    @Override
    public void appendRange(ValueBlock block, int offset, int length)
    {
        checkValidRegion(block.getPositionCount(), offset, length);
        if (length == 0) {
            return;
        }
        if (!(block instanceof StridedVariableWidthBlock strided)) {
            for (int i = 0; i < length; i++) {
                append(block, offset + i);
            }
            return;
        }

        byte[] source = strided.rawBytes();
        int srcBase = strided.rawBasePosition() + offset;
        int srcOffsets = strided.rawOffsetsOffset();
        int firstStart = (int) INT.get(source, srcOffsets + (srcBase << 2));
        int copyBytes = (int) INT.get(source, srcOffsets + ((srcBase + length) << 2)) - firstStart;
        ensure(positionCount + length, valueEnd + copyBytes);
        System.arraycopy(source, strided.rawValuesOffset() + firstStart, bytes, valuesOffset + valueEnd, copyBytes);
        int delta = valueEnd - firstStart;
        if (delta == 0) {
            System.arraycopy(source, srcOffsets + ((srcBase + 1) << 2), bytes, offsetsOffset + ((positionCount + 1) << 2), length << 2);
        }
        else {
            int dstOff = offsetsOffset + ((positionCount + 1) << 2);
            int srcOff = srcOffsets + ((srcBase + 1) << 2);
            for (int i = 0; i < length; i++) {
                INT.set(bytes, dstOff + (i << 2), (int) INT.get(source, srcOff + (i << 2)) + delta);
            }
        }
        copyNullBitsRange(strided, srcBase, length);
        valueEnd += copyBytes;
        positionCount += length;
        nextValueOffset = valuesOffset + valueEnd;
        nextOffsetByte = offsetsOffset + ((positionCount + 1) << 2);
        maxLength = max(maxLength, strided.maxLength());
        hasNonNull = true;
        addBytes(copyBytes + (long) Integer.BYTES * length);
    }

    @Override
    public void appendPositions(ValueBlock block, int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);
        if (length == 0) {
            return;
        }
        if (!(block instanceof StridedVariableWidthBlock strided)) {
            for (int i = 0; i < length; i++) {
                append(block, positions[offset + i]);
            }
            return;
        }

        byte[] source = strided.rawBytes();
        int sourceBase = strided.rawBasePosition();
        int sourceOffsets = strided.rawOffsetsOffset();
        int sourceValues = strided.rawValuesOffset();
        int sourcePositionCount = strided.getPositionCount();

        int copiedBytes = 0;
        if (COPY_IN_CHUNKS && strided.maxLength() <= MAX_CHUNKED_VALUE) {
            copiedBytes = chunkedBytes(strided.maxLength());
            if (copiedBytes > MAX_CHUNKED_CELL && (long) copiedBytes * sourcePositionCount > 2L * strided.valueBytes()) {
                copiedBytes = 0;
            }
        }

        long estimatedBytes = (long) strided.valueBytes() * length / max(1, sourcePositionCount);
        ensure(positionCount + length, toIntExact(valueEnd + estimatedBytes + (estimatedBytes >> 3) + copiedBytes));

        int sourceLimit = source.length - sourceValues - copiedBytes;
        int beforeBytes = valueEnd;
        int copied = 0;
        while (copied < length) {
            int remaining = length - copied;
            int targetLimit = bytes.length - copiedBytes;
            int dstOffsetByte = offsetsOffset + ((positionCount + 1) << 2);
            int count;
            if (copiedBytes == 0) {
                count = copyRanges(
                        source,
                        sourceBase,
                        sourceOffsets,
                        sourceValues,
                        sourcePositionCount,
                        positions,
                        offset + copied,
                        remaining,
                        bytes,
                        dstOffsetByte,
                        valuesOffset,
                        valueEnd,
                        targetLimit);
            }
            else if (copiedBytes <= MAX_CHUNKED_CELL) {
                count = copyChunks(
                        source,
                        sourceBase,
                        sourceOffsets,
                        sourceValues,
                        sourcePositionCount,
                        sourceLimit,
                        positions,
                        offset + copied,
                        remaining,
                        bytes,
                        dstOffsetByte,
                        valuesOffset,
                        valueEnd,
                        targetLimit,
                        copiedBytes);
            }
            else {
                count = copyLongChunks(
                        source,
                        sourceBase,
                        sourceOffsets,
                        sourceValues,
                        sourcePositionCount,
                        sourceLimit,
                        positions,
                        offset + copied,
                        remaining,
                        bytes,
                        dstOffsetByte,
                        valuesOffset,
                        valueEnd,
                        targetLimit,
                        copiedBytes);
            }
            if (count > 0) {
                copyNullBitsPositions(strided, positions, offset + copied, count);
                positionCount += count;
                valueEnd = (int) INT.get(bytes, offsetsOffset + (positionCount << 2));
                nextValueOffset = valuesOffset + valueEnd;
                nextOffsetByte = offsetsOffset + ((positionCount + 1) << 2);
                copied += count;
            }
            if (copied < length) {
                int pos = checkIndex(positions[offset + copied], sourcePositionCount);
                if (strided.isNull(pos)) {
                    appendNull();
                }
                else {
                    long pair = (long) LONG.get(source, sourceOffsets + ((sourceBase + pos) << 2));
                    int start = (int) pair;
                    int valueLength = (int) (pair >>> 32) - start;
                    ensure(positionCount + (length - copied), valueEnd + valueLength + copiedBytes);
                    System.arraycopy(source, sourceValues + start, bytes, nextValueOffset, valueLength);
                    valueEnd += valueLength;
                    nextValueOffset += valueLength;
                    INT.set(bytes, nextOffsetByte, valueEnd);
                    nextOffsetByte += Integer.BYTES;
                    positionCount++;
                }
                copied++;
            }
        }
        maxLength = max(maxLength, strided.maxLength());
        hasNonNull = true;
        addBytes((valueEnd - beforeBytes) + (long) Integer.BYTES * length);
    }

    @Override
    public void resetTo(int position)
    {
        checkIndex(position, positionCount + 1);
        if (hasNull && position < positionCount) {
            clearNullBits(bytes, position, positionCount);
        }
        valueEnd = position == 0 ? 0 : (int) INT.get(bytes, offsetsOffset + (position << 2));
        positionCount = position;
        nextValueOffset = valuesOffset + valueEnd;
        nextOffsetByte = offsetsOffset + ((position + 1) << 2);
    }

    @Override
    public Block build()
    {
        if (!hasNonNull) {
            return RunLengthEncodedBlock.create(NULL_VALUE_BLOCK, positionCount);
        }
        return buildValueBlock();
    }

    @Override
    public StridedVariableWidthBlock buildValueBlock()
    {
        if (positionCount == 0 && bytes.length == 0) {
            return new StridedVariableWidthBlock(new byte[offsetsBytes(0)], 0, 0, false, 0, 0, 0, 0);
        }
        return new StridedVariableWidthBlock(bytes, 0, positionCount, hasNull, 0, offsetsOffset, valuesOffset, maxLength);
    }

    @Override
    public StridedVariableWidthBlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus)
    {
        return new StridedVariableWidthBlockBuilder(blockBuilderStatus, calculateBlockResetSize(positionCount), calculateBlockResetBytes(valueEnd));
    }

    @Override
    public StridedVariableWidthBlockBuilder newBlockBuilderLike(int expectedEntries, BlockBuilderStatus blockBuilderStatus)
    {
        long scaledBytes = positionCount == 0 ? 0 : (long) valueEnd * expectedEntries / positionCount;
        int nextBytes = (int) min(min(scaledBytes, 2L * valueEnd), MAX_ARRAY_SIZE);
        return new StridedVariableWidthBlockBuilder(blockBuilderStatus, expectedEntries, calculateBlockResetBytes(nextBytes));
    }

    @Override
    public String toString()
    {
        return "StridedVariableWidthBlockBuilder{positionCount=" + positionCount + ", size=" + valueEnd + '}';
    }

    private void addBytes(long bytesAdded)
    {
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(toIntExact(bytesAdded));
        }
    }

    private void copyNullBitsRange(StridedVariableWidthBlock source, int srcBase, int length)
    {
        if (!source.mayHaveNull()) {
            return;
        }
        byte[] src = source.rawBytes();
        int srcNulls = source.rawNullsOffset();
        int any = 0;
        for (int i = 0; i < length; i++) {
            int sp = srcBase + i;
            int bit = (src[srcNulls + (sp >>> 3)] >>> (sp & 7)) & 1;
            int dp = positionCount + i;
            bytes[dp >>> 3] |= (byte) (bit << (dp & 7));
            any |= bit;
        }
        if (any != 0) {
            hasNull = true;
        }
    }

    private void copyNullBitsPositions(StridedVariableWidthBlock source, int[] positions, int offset, int length)
    {
        if (!source.mayHaveNull()) {
            return;
        }
        if (StridedVariableWidthBlock.copyNullBitsPacked(source.rawBytes(), source.rawNullsOffset(), source.rawBasePosition(), positions, offset, length, bytes, positionCount)) {
            hasNull = true;
        }
    }

    private static void clearNullBits(byte[] bytes, int from, int to)
    {
        for (int p = from; p < to; p++) {
            bytes[p >>> 3] &= (byte) ~(1 << (p & 7));
        }
    }

    private static int copyChunks(
            byte[] source,
            int sourceBase,
            int sourceOffsets,
            int sourceValues,
            int sourcePositionCount,
            int sourceLimit,
            int[] positions,
            int offset,
            int length,
            byte[] target,
            int targetOffsetByte,
            int targetValuesOffset,
            int targetEnd,
            int targetLimit,
            int copiedBytes)
    {
        int maxValueEnd = targetLimit - targetValuesOffset;
        if (copiedBytes == CHUNK_SIZE) {
            for (int i = 0; i < length; i++) {
                int rawPos = sourceBase + checkIndex(positions[offset + i], sourcePositionCount);
                long pair = (long) LONG.get(source, sourceOffsets + (rawPos << 2));
                int start = (int) pair;
                int vLen = (int) (pair >>> 32) - start;
                if (((maxValueEnd - targetEnd) | (sourceLimit - start) | (copiedBytes - vLen)) < 0) {
                    return i;
                }
                copyChunk(source, sourceValues + start, target, targetValuesOffset + targetEnd);
                targetEnd += vLen;
                INT.set(target, targetOffsetByte + (i << 2), targetEnd);
            }
        }
        else if (copiedBytes == 2 * CHUNK_SIZE) {
            for (int i = 0; i < length; i++) {
                int rawPos = sourceBase + checkIndex(positions[offset + i], sourcePositionCount);
                long pair = (long) LONG.get(source, sourceOffsets + (rawPos << 2));
                int start = (int) pair;
                int vLen = (int) (pair >>> 32) - start;
                if (((maxValueEnd - targetEnd) | (sourceLimit - start) | (copiedBytes - vLen)) < 0) {
                    return i;
                }
                int src = sourceValues + start;
                int dst = targetValuesOffset + targetEnd;
                copyChunk(source, src, target, dst);
                copyChunk(source, src + CHUNK_SIZE, target, dst + CHUNK_SIZE);
                targetEnd += vLen;
                INT.set(target, targetOffsetByte + (i << 2), targetEnd);
            }
        }
        else {
            for (int i = 0; i < length; i++) {
                int rawPos = sourceBase + checkIndex(positions[offset + i], sourcePositionCount);
                long pair = (long) LONG.get(source, sourceOffsets + (rawPos << 2));
                int start = (int) pair;
                int vLen = (int) (pair >>> 32) - start;
                if (((maxValueEnd - targetEnd) | (sourceLimit - start) | (copiedBytes - vLen)) < 0) {
                    return i;
                }
                copyFourChunks(source, sourceValues + start, target, targetValuesOffset + targetEnd);
                targetEnd += vLen;
                INT.set(target, targetOffsetByte + (i << 2), targetEnd);
            }
        }
        return length;
    }

    private static int copyLongChunks(
            byte[] source,
            int sourceBase,
            int sourceOffsets,
            int sourceValues,
            int sourcePositionCount,
            int sourceLimit,
            int[] positions,
            int offset,
            int length,
            byte[] target,
            int targetOffsetByte,
            int targetValuesOffset,
            int targetEnd,
            int targetLimit,
            int copiedBytes)
    {
        int maxValueEnd = targetLimit - targetValuesOffset;
        if (copiedBytes == 8 * CHUNK_SIZE) {
            for (int i = 0; i < length; i++) {
                int rawPos = sourceBase + checkIndex(positions[offset + i], sourcePositionCount);
                long pair = (long) LONG.get(source, sourceOffsets + (rawPos << 2));
                int start = (int) pair;
                int vLen = (int) (pair >>> 32) - start;
                if (((maxValueEnd - targetEnd) | (sourceLimit - start) | (copiedBytes - vLen)) < 0) {
                    return i;
                }
                int src = sourceValues + start;
                int dst = targetValuesOffset + targetEnd;
                copyFourChunks(source, src, target, dst);
                copyFourChunks(source, src + 4 * CHUNK_SIZE, target, dst + 4 * CHUNK_SIZE);
                targetEnd += vLen;
                INT.set(target, targetOffsetByte + (i << 2), targetEnd);
            }
        }
        else if (copiedBytes == 12 * CHUNK_SIZE) {
            for (int i = 0; i < length; i++) {
                int rawPos = sourceBase + checkIndex(positions[offset + i], sourcePositionCount);
                long pair = (long) LONG.get(source, sourceOffsets + (rawPos << 2));
                int start = (int) pair;
                int vLen = (int) (pair >>> 32) - start;
                if (((maxValueEnd - targetEnd) | (sourceLimit - start) | (copiedBytes - vLen)) < 0) {
                    return i;
                }
                int src = sourceValues + start;
                int dst = targetValuesOffset + targetEnd;
                copyFourChunks(source, src, target, dst);
                copyFourChunks(source, src + 4 * CHUNK_SIZE, target, dst + 4 * CHUNK_SIZE);
                copyFourChunks(source, src + 8 * CHUNK_SIZE, target, dst + 8 * CHUNK_SIZE);
                targetEnd += vLen;
                INT.set(target, targetOffsetByte + (i << 2), targetEnd);
            }
        }
        else {
            for (int i = 0; i < length; i++) {
                int rawPos = sourceBase + checkIndex(positions[offset + i], sourcePositionCount);
                long pair = (long) LONG.get(source, sourceOffsets + (rawPos << 2));
                int start = (int) pair;
                int vLen = (int) (pair >>> 32) - start;
                if (((maxValueEnd - targetEnd) | (sourceLimit - start) | (copiedBytes - vLen)) < 0) {
                    return i;
                }
                int src = sourceValues + start;
                int dst = targetValuesOffset + targetEnd;
                copyFourChunks(source, src, target, dst);
                copyFourChunks(source, src + 4 * CHUNK_SIZE, target, dst + 4 * CHUNK_SIZE);
                copyFourChunks(source, src + 8 * CHUNK_SIZE, target, dst + 8 * CHUNK_SIZE);
                copyFourChunks(source, src + 12 * CHUNK_SIZE, target, dst + 12 * CHUNK_SIZE);
                targetEnd += vLen;
                INT.set(target, targetOffsetByte + (i << 2), targetEnd);
            }
        }
        return length;
    }

    private static int copyRanges(
            byte[] source,
            int sourceBase,
            int sourceOffsets,
            int sourceValues,
            int sourcePositionCount,
            int[] positions,
            int offset,
            int length,
            byte[] target,
            int targetOffsetByte,
            int targetValuesOffset,
            int targetEnd,
            int targetLimit)
    {
        int maxValueEnd = targetLimit - targetValuesOffset;
        int rangeStart = 0;
        int rangeEnd = 0;
        int rangeTarget = targetEnd;
        int copied = length;
        for (int i = 0; i < length; i++) {
            int rawPos = sourceBase + checkIndex(positions[offset + i], sourcePositionCount);
            long pair = (long) LONG.get(source, sourceOffsets + (rawPos << 2));
            int sourceStart = (int) pair;
            int vLen = (int) (pair >>> 32) - sourceStart;
            if (targetEnd > maxValueEnd - vLen) {
                copied = i;
                break;
            }
            if (sourceStart != rangeEnd) {
                System.arraycopy(source, sourceValues + rangeStart, target, targetValuesOffset + rangeTarget, rangeEnd - rangeStart);
                rangeTarget = targetEnd;
                rangeStart = sourceStart;
            }
            rangeEnd = sourceStart + vLen;
            targetEnd += vLen;
            INT.set(target, targetOffsetByte + (i << 2), targetEnd);
        }
        System.arraycopy(source, sourceValues + rangeStart, target, targetValuesOffset + rangeTarget, rangeEnd - rangeStart);
        return copied;
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

    private static int chunkedBytes(int length)
    {
        if (length > MAX_CHUNKED_CELL) {
            return (length + MAX_CHUNKED_CELL - 1) / MAX_CHUNKED_CELL * MAX_CHUNKED_CELL;
        }
        int copiedBytes = CHUNK_SIZE;
        while (copiedBytes < length) {
            copiedBytes *= 2;
        }
        return copiedBytes;
    }

    private void ensure(int requiredPositions, int requiredValueBytes)
    {
        if (requiredPositions > slotCapacity || requiredValueBytes > valueLimit) {
            grow(requiredPositions, requiredValueBytes);
        }
    }

    private void grow(int requiredPositions, int requiredValueBytes)
    {
        if (requiredPositions <= slotCapacity && slotCapacity > 0) {
            long currentValueCap = bytes.length - valuesOffset;
            long newValueCap = max(max(requiredValueBytes, expectedBytes), currentValueCap + (currentValueCap >> 1));
            long newSize = (long) valuesOffset + newValueCap;
            if (newSize > Integer.MAX_VALUE - 8) {
                newSize = (long) valuesOffset + requiredValueBytes;
            }
            bytes = Arrays.copyOf(bytes, toIntExact(newSize));
            valueLimit = bytes.length - valuesOffset;
            return;
        }
        int newCap = growCapacity(slotCapacity, requiredPositions, expectedEntries);
        int newNulls = nullsBytes(newCap);
        int newOffsets = newNulls;
        int newValues = newOffsets + offsetsBytes(newCap);
        long currentValueCap = max(0L, (long) bytes.length - valuesOffset);
        long newValueCap = max(max(requiredValueBytes, expectedBytes), currentValueCap + (currentValueCap >> 1));
        long newSize = (long) newValues + newValueCap;
        if (newSize > Integer.MAX_VALUE - 8) {
            newSize = (long) newValues + requiredValueBytes;
        }
        byte[] target = new byte[toIntExact(newSize)];
        if (positionCount > 0) {
            if (hasNull) {
                System.arraycopy(bytes, 0, target, 0, nullsBytes(positionCount));
            }
            System.arraycopy(bytes, offsetsOffset, target, newOffsets, offsetsBytes(positionCount));
            System.arraycopy(bytes, valuesOffset, target, newValues, valueEnd);
        }
        bytes = target;
        slotCapacity = newCap;
        offsetsOffset = newOffsets;
        valuesOffset = newValues;
        valueLimit = target.length - newValues;
        nextValueOffset = newValues + valueEnd;
        nextOffsetByte = newOffsets + ((positionCount + 1) << 2);
    }

    private static int growCapacity(int currentCapacity, int required, int expected)
    {
        long target = max(max(required, expected), max((long) currentCapacity + (currentCapacity >> 1), MIN_SLOT_CAPACITY));
        return toIntExact(min(target, Integer.MAX_VALUE - 64));
    }
}
