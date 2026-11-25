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
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorSpecies;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkValidPosition;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.checkIndex;

/// A [ValueBlock] of variable-width values that keeps its null bits, offsets and values in one `byte[]`:
///
/// ```
/// [ null bits in 8-byte words, 1 = null (only when the block may have nulls)
/// | positionCount + 1 little-endian 4-byte offsets of the values, padded to 8 bytes
/// | bytes of the values ]
/// ```
///
/// The serialized form is a 16-byte header followed by the same three sections. [#wrap] therefore reads a
/// serialized block in place, [#getRegion] is a view of the same array, and [#writeTo] does no work for each value
/// unless the block is a region that does not start at the first position.
public final class StridedVariableWidthBlock
        implements ValueBlock
{
    private static final int INSTANCE_SIZE = instanceSize(StridedVariableWidthBlock.class);

    static final int HEADER_SIZE = 4 * Integer.BYTES;
    static final int MAY_HAVE_NULL_FLAG = 1;

    static final VarHandle INT = MethodHandles.byteArrayViewVarHandle(int[].class, LITTLE_ENDIAN);
    static final VarHandle LONG = MethodHandles.byteArrayViewVarHandle(long[].class, LITTLE_ENDIAN);

    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED;
    private static final VectorSpecies<Integer> INT_SPECIES = IntVector.SPECIES_PREFERRED;
    private static final int INT_LANES = INT_SPECIES.length();

    private static final VectorSpecies<Byte> CHUNK = ByteVector.SPECIES_128;
    private static final int CHUNK_SIZE = CHUNK.vectorByteSize();
    private static final boolean COPY_IN_CHUNKS = ByteVector.SPECIES_PREFERRED.vectorBitSize() >= CHUNK.vectorBitSize();
    private static final int MAX_CHUNKED_CELL = 4 * CHUNK_SIZE;
    private static final int MAX_CHUNKED_VALUE = 16 * CHUNK_SIZE;

    private static final long PRIME64_1 = 0x9E3779B185EBCA87L;
    private static final long PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
    private static final long PRIME64_3 = 0x165667B19E3779F9L;
    private static final long PRIME64_4 = 0x85EBCA77C2B2AE63L;
    private static final long PRIME64_5 = 0x27D4EB2F165667C5L;

    private final byte[] bytes;
    private final Slice slice;
    private final int basePosition;
    private final int positionCount;
    private final boolean mayHaveNull;
    private final int nullsOffset;
    private final int offsetsOffset;
    private final int baseOffsetsOffset;
    private final int valuesOffset;
    private final int maxLength;

    StridedVariableWidthBlock(
            byte[] bytes,
            int basePosition,
            int positionCount,
            boolean mayHaveNull,
            int nullsOffset,
            int offsetsOffset,
            int valuesOffset,
            int maxLength)
    {
        if (basePosition < 0 || positionCount < 0) {
            throw new IllegalArgumentException("basePosition or positionCount is negative");
        }
        int endPos = basePosition + positionCount;
        if (mayHaveNull && (nullsOffset < 0 || (long) nullsOffset + nullsBytes(endPos) > bytes.length)) {
            throw new IllegalArgumentException("null bitmap is outside of the array");
        }
        if (offsetsOffset < 0 || (long) offsetsOffset + ((long) (endPos + 1) << 2) > bytes.length) {
            throw new IllegalArgumentException("offsets are outside of the array");
        }
        if (valuesOffset < 0 || valuesOffset > bytes.length) {
            throw new IllegalArgumentException("values are outside of the array");
        }
        this.bytes = bytes;
        this.slice = Slices.wrappedBuffer(bytes, valuesOffset, bytes.length - valuesOffset);
        this.basePosition = basePosition;
        this.positionCount = positionCount;
        this.mayHaveNull = mayHaveNull;
        this.nullsOffset = nullsOffset;
        this.offsetsOffset = offsetsOffset;
        this.baseOffsetsOffset = offsetsOffset + (basePosition << 2);
        this.valuesOffset = valuesOffset;
        this.maxLength = maxLength;
    }

    /// Returns the block serialized at `offset` of `bytes`. The block references `bytes` with zero copy.
    public static StridedVariableWidthBlock wrap(byte[] bytes, int offset)
    {
        if (offset < 0 || offset > bytes.length - HEADER_SIZE) {
            throw new IllegalArgumentException("serialized block is longer than the array");
        }
        int positionCount = (int) INT.get(bytes, offset);
        int flags = (int) INT.get(bytes, offset + Integer.BYTES);
        int valueBytes = (int) INT.get(bytes, offset + 2 * Integer.BYTES);
        int maxLength = (int) INT.get(bytes, offset + 3 * Integer.BYTES);
        if (positionCount < 0 || valueBytes < 0 || maxLength < 0) {
            throw new IllegalArgumentException("invalid header of a serialized block");
        }
        boolean mayHaveNull = (flags & MAY_HAVE_NULL_FLAG) != 0;
        int cursor = offset + HEADER_SIZE;
        int nullsOffset = cursor;
        if (mayHaveNull) {
            cursor = toIntExact((long) cursor + nullsBytes(positionCount));
        }
        int offsetsOffset = cursor;
        int valuesOffset = toIntExact((long) offsetsOffset + offsetsBytes(positionCount));
        if ((long) valuesOffset + valueBytes > bytes.length) {
            throw new IllegalArgumentException("serialized block is longer than the array");
        }
        return new StridedVariableWidthBlock(bytes, 0, positionCount, mayHaveNull, nullsOffset, offsetsOffset, valuesOffset, maxLength);
    }

    /// Returns the block serialized in `slice`. The block references the bytes of `slice`.
    public static StridedVariableWidthBlock wrap(Slice slice)
    {
        return wrap(slice.byteArray(), slice.byteArrayOffset());
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public boolean mayHaveNull()
    {
        return mayHaveNull;
    }

    @Override
    public boolean hasNull()
    {
        if (!mayHaveNull || positionCount == 0) {
            return false;
        }
        int startBit = basePosition;
        int endBit = basePosition + positionCount;
        int startWord = startBit >>> 6;
        int endWord = (endBit - 1) >>> 6;
        if (startWord == endWord) {
            long word = (long) LONG.get(bytes, nullsOffset + (startWord << 3));
            long mask = (~0L >>> (64 - positionCount)) << (startBit & 63);
            return (word & mask) != 0;
        }
        long firstWord = (long) LONG.get(bytes, nullsOffset + (startWord << 3));
        if ((firstWord & (~0L << (startBit & 63))) != 0) {
            return true;
        }
        for (int wordIndex = startWord + 1; wordIndex < endWord; wordIndex++) {
            if ((long) LONG.get(bytes, nullsOffset + (wordIndex << 3)) != 0) {
                return true;
            }
        }
        int trailingBits = endBit & 63;
        long lastMask = trailingBits == 0 ? ~0L : (1L << trailingBits) - 1;
        long lastWord = (long) LONG.get(bytes, nullsOffset + (endWord << 3));
        return (lastWord & lastMask) != 0;
    }

    @Override
    public boolean isNull(int position)
    {
        checkValidPosition(position, positionCount);
        if (!mayHaveNull) {
            return false;
        }
        int rawPos = basePosition + position;
        return (bytes[nullsOffset + (rawPos >>> 3)] & (1 << (rawPos & 7))) != 0;
    }

    private boolean isNullUnchecked(int rawPos)
    {
        return mayHaveNull && (bytes[nullsOffset + (rawPos >>> 3)] & (1 << (rawPos & 7))) != 0;
    }

    /// Returns `-1L` if `position` is null, otherwise packs `((long) byteOffset << 32) | length` into a single `long`.
    public long valueSpan(int position)
    {
        checkValidPosition(position, positionCount);
        if (isNullUnchecked(basePosition + position)) {
            return -1L;
        }
        int offIdx = baseOffsetsOffset + (position << 2);
        int start = (int) INT.get(bytes, offIdx);
        int length = (int) INT.get(bytes, offIdx + Integer.BYTES) - start;
        return ((long) (valuesOffset + start) << 32) | length;
    }

    public Slice getSlice(int position)
    {
        checkValidPosition(position, positionCount);
        int offIdx = baseOffsetsOffset + (position << 2);
        int start = (int) INT.get(bytes, offIdx);
        int length = (int) INT.get(bytes, offIdx + Integer.BYTES) - start;
        return Slices.wrappedBuffer(bytes, valuesOffset + start, length);
    }

    public int getSliceLength(int position)
    {
        checkValidPosition(position, positionCount);
        int offIdx = baseOffsetsOffset + (position << 2);
        return (int) INT.get(bytes, offIdx + Integer.BYTES) - (int) INT.get(bytes, offIdx);
    }

    public Slice getRawSlice()
    {
        return slice;
    }

    public int getRawSliceOffset(int position)
    {
        checkValidPosition(position, positionCount);
        return (int) INT.get(bytes, baseOffsetsOffset + (position << 2));
    }

    public int compareValue(int position, StridedVariableWidthBlock other, int otherPosition)
    {
        checkValidPosition(position, positionCount);
        checkValidPosition(otherPosition, other.positionCount);
        int idx1 = baseOffsetsOffset + (position << 2);
        int s1 = (int) INT.get(bytes, idx1);
        int len1 = (int) INT.get(bytes, idx1 + Integer.BYTES) - s1;
        int idx2 = other.baseOffsetsOffset + (otherPosition << 2);
        int s2 = (int) INT.get(other.bytes, idx2);
        int len2 = (int) INT.get(other.bytes, idx2 + Integer.BYTES) - s2;
        return compareBytes(bytes, valuesOffset + s1, len1, other.bytes, other.valuesOffset + s2, len2);
    }

    public int compareValue(int position, byte[] otherBytes, int otherOffset, int otherLength)
    {
        checkValidPosition(position, positionCount);
        int idx = baseOffsetsOffset + (position << 2);
        int s = (int) INT.get(bytes, idx);
        return compareBytes(bytes, valuesOffset + s, (int) INT.get(bytes, idx + Integer.BYTES) - s, otherBytes, otherOffset, otherLength);
    }

    public boolean valueEquals(int position, StridedVariableWidthBlock other, int otherPosition)
    {
        checkValidPosition(position, positionCount);
        checkValidPosition(otherPosition, other.positionCount);
        int idx1 = baseOffsetsOffset + (position << 2);
        int s1 = (int) INT.get(bytes, idx1);
        int len1 = (int) INT.get(bytes, idx1 + Integer.BYTES) - s1;
        int idx2 = other.baseOffsetsOffset + (otherPosition << 2);
        int s2 = (int) INT.get(other.bytes, idx2);
        int len2 = (int) INT.get(other.bytes, idx2 + Integer.BYTES) - s2;
        if (len1 != len2) {
            return false;
        }
        if (len1 == 0) {
            return true;
        }
        int off1 = valuesOffset + s1;
        int off2 = other.valuesOffset + s2;
        return Arrays.equals(bytes, off1, off1 + len1, other.bytes, off2, off2 + len1);
    }

    public boolean valueEquals(int position, byte[] otherBytes, int otherOffset, int otherLength)
    {
        checkValidPosition(position, positionCount);
        int idx = baseOffsetsOffset + (position << 2);
        int s = (int) INT.get(bytes, idx);
        int len = (int) INT.get(bytes, idx + Integer.BYTES) - s;
        if (len != otherLength) {
            return false;
        }
        if (len == 0) {
            return true;
        }
        int off = valuesOffset + s;
        return Arrays.equals(bytes, off, off + len, otherBytes, otherOffset, otherOffset + len);
    }

    public long valueHash(int position)
    {
        checkValidPosition(position, positionCount);
        int idx = baseOffsetsOffset + (position << 2);
        int s = (int) INT.get(bytes, idx);
        return XxHash64.hash(bytes, valuesOffset + s, (int) INT.get(bytes, idx + Integer.BYTES) - s);
    }

    public void copyValue(int position, byte[] target, int targetOffset)
    {
        checkValidPosition(position, positionCount);
        int idx = baseOffsetsOffset + (position << 2);
        int s = (int) INT.get(bytes, idx);
        System.arraycopy(bytes, valuesOffset + s, target, targetOffset, (int) INT.get(bytes, idx + Integer.BYTES) - s);
    }

    /// Hashes `length` positions from `positions[offset..offset+length)` into `output[outputOffset..]`,
    /// writing `0L` for null positions.
    public void hashPositions(int[] positions, int offset, int length, long[] output, int outputOffset)
    {
        if (!mayHaveNull) {
            if (maxLength < 32) {
                hashShortPositionsNoNulls(bytes, baseOffsetsOffset, valuesOffset, positions, offset, length, output, outputOffset);
            }
            else {
                hashLongPositionsNoNulls(bytes, baseOffsetsOffset, valuesOffset, positions, offset, length, output, outputOffset);
            }
            return;
        }
        if (maxLength < 32) {
            hashShortPositionsWithNulls(bytes, basePosition, nullsOffset, baseOffsetsOffset, valuesOffset, positions, offset, length, output, outputOffset);
        }
        else {
            hashLongPositionsWithNulls(bytes, basePosition, nullsOffset, baseOffsetsOffset, valuesOffset, positions, offset, length, output, outputOffset);
        }
    }

    private static void hashShortPositionsNoNulls(byte[] data, int baseOff, int valOff, int[] positions, int offset, int length, long[] output, int outputOffset)
    {
        int i = 0;
        int limit = length & ~1;
        for (; i < limit; i += 2) {
            int idx0 = baseOff + (positions[offset + i] << 2);
            int idx1 = baseOff + (positions[offset + i + 1] << 2);
            int s0 = (int) INT.get(data, idx0);
            int e0 = (int) INT.get(data, idx0 + Integer.BYTES);
            int s1 = (int) INT.get(data, idx1);
            int e1 = (int) INT.get(data, idx1 + Integer.BYTES);
            long h0 = hashShortBody(data, valOff + s0, e0 - s0);
            long h1 = hashShortBody(data, valOff + s1, e1 - s1);
            h0 ^= h0 >>> 33;
            h1 ^= h1 >>> 33;
            h0 *= PRIME64_2;
            h1 *= PRIME64_2;
            h0 ^= h0 >>> 29;
            h1 ^= h1 >>> 29;
            h0 *= PRIME64_3;
            h1 *= PRIME64_3;
            output[outputOffset + i] = h0 ^ (h0 >>> 32);
            output[outputOffset + i + 1] = h1 ^ (h1 >>> 32);
        }
        if (i < length) {
            int idx = baseOff + (positions[offset + i] << 2);
            int s = (int) INT.get(data, idx);
            output[outputOffset + i] = finalShuffle(hashShortBody(data, valOff + s, (int) INT.get(data, idx + Integer.BYTES) - s));
        }
    }

    private static void hashLongPositionsNoNulls(byte[] data, int baseOff, int valOff, int[] positions, int offset, int length, long[] output, int outputOffset)
    {
        for (int i = 0; i < length; i++) {
            int idx = baseOff + (positions[offset + i] << 2);
            int s = (int) INT.get(data, idx);
            output[outputOffset + i] = XxHash64.hash(data, valOff + s, (int) INT.get(data, idx + Integer.BYTES) - s);
        }
    }

    private static void hashShortPositionsWithNulls(byte[] data, int base, int nullOff, int baseOff, int valOff, int[] positions, int offset, int length, long[] output, int outputOffset)
    {
        int i = 0;
        int limit = length & ~1;
        for (; i < limit; i += 2) {
            int pos0 = positions[offset + i];
            int pos1 = positions[offset + i + 1];
            int raw0 = base + pos0;
            int raw1 = base + pos1;
            long mask0 = ((data[nullOff + (raw0 >>> 3)] >>> (raw0 & 7)) & 1) - 1L;
            long mask1 = ((data[nullOff + (raw1 >>> 3)] >>> (raw1 & 7)) & 1) - 1L;
            int idx0 = baseOff + (pos0 << 2);
            int idx1 = baseOff + (pos1 << 2);
            int s0 = (int) INT.get(data, idx0);
            int e0 = (int) INT.get(data, idx0 + Integer.BYTES);
            int s1 = (int) INT.get(data, idx1);
            int e1 = (int) INT.get(data, idx1 + Integer.BYTES);
            long h0 = hashShortBody(data, valOff + s0, e0 - s0);
            long h1 = hashShortBody(data, valOff + s1, e1 - s1);
            h0 ^= h0 >>> 33;
            h1 ^= h1 >>> 33;
            h0 *= PRIME64_2;
            h1 *= PRIME64_2;
            h0 ^= h0 >>> 29;
            h1 ^= h1 >>> 29;
            h0 *= PRIME64_3;
            h1 *= PRIME64_3;
            output[outputOffset + i] = (h0 ^ (h0 >>> 32)) & mask0;
            output[outputOffset + i + 1] = (h1 ^ (h1 >>> 32)) & mask1;
        }
        if (i < length) {
            int pos = positions[offset + i];
            int rawPos = base + pos;
            if ((data[nullOff + (rawPos >>> 3)] & (1 << (rawPos & 7))) != 0) {
                output[outputOffset + i] = 0L;
            }
            else {
                int idx = baseOff + (pos << 2);
                int s = (int) INT.get(data, idx);
                output[outputOffset + i] = finalShuffle(hashShortBody(data, valOff + s, (int) INT.get(data, idx + Integer.BYTES) - s));
            }
        }
    }

    private static void hashLongPositionsWithNulls(byte[] data, int base, int nullOff, int baseOff, int valOff, int[] positions, int offset, int length, long[] output, int outputOffset)
    {
        for (int i = 0; i < length; i++) {
            int pos = positions[offset + i];
            int rawPos = base + pos;
            if ((data[nullOff + (rawPos >>> 3)] & (1 << (rawPos & 7))) != 0) {
                output[outputOffset + i] = 0L;
            }
            else {
                int idx = baseOff + (pos << 2);
                int s = (int) INT.get(data, idx);
                output[outputOffset + i] = XxHash64.hash(data, valOff + s, (int) INT.get(data, idx + Integer.BYTES) - s);
            }
        }
    }

    private static long hashShortBody(byte[] data, int off, int len)
    {
        long hash = PRIME64_5 + len;
        int end = off + len;
        while (off <= end - 8) {
            long k = Long.rotateLeft((long) LONG.get(data, off) * PRIME64_2, 31) * PRIME64_1;
            hash = Long.rotateLeft(hash ^ k, 27) * PRIME64_1 + PRIME64_4;
            off += 8;
        }
        if (off <= end - 4) {
            long k = ((int) INT.get(data, off) & 0xFFFF_FFFFL) * PRIME64_1;
            hash = Long.rotateLeft(hash ^ k, 23) * PRIME64_2 + PRIME64_3;
            off += 4;
        }
        while (off < end) {
            hash = Long.rotateLeft(hash ^ ((data[off++] & 0xFFL) * PRIME64_5), 11) * PRIME64_1;
        }
        return hash;
    }

    private static long finalShuffle(long hash)
    {
        hash ^= hash >>> 33;
        hash *= PRIME64_2;
        hash ^= hash >>> 29;
        hash *= PRIME64_3;
        return hash ^ (hash >>> 32);
    }

    /// Filters `selectedPositions[0..count)` in place to those non-null positions whose value equals
    /// `target[targetOffset..targetOffset+targetLength)`. Returns the new matching count.
    public int filterEquals(byte[] target, int targetOffset, int targetLength, int[] selectedPositions, int count)
    {
        if (targetLength > maxLength || count == 0) {
            return 0;
        }
        byte[] data = bytes;
        int base = basePosition;
        int offOff = offsetsOffset;
        int valOff = valuesOffset;
        byte firstByte = targetLength > 0 ? target[targetOffset] : 0;
        int matched = 0;

        if (base == 0 && count == positionCount && selectedPositions[0] == 0 && selectedPositions[count - 1] == count - 1) {
            IntVector targetLenVec = IntVector.broadcast(INT_SPECIES, targetLength);
            int lanes = INT_LANES;
            long allMask = lanes == 64 ? -1L : (1L << lanes) - 1;
            int vectorLimit = count & ~(lanes - 1);
            if (targetLength == 0) {
                int i = 0;
                if (!mayHaveNull) {
                    for (; i < vectorLimit; i += lanes) {
                        IntVector starts = ByteVector.fromArray(BYTE_SPECIES, data, offOff + (i << 2)).reinterpretAsInts();
                        IntVector ends = ByteVector.fromArray(BYTE_SPECIES, data, offOff + ((i + 1) << 2)).reinterpretAsInts();
                        long mask = ends.sub(starts).eq(targetLenVec).toLong();
                        while (mask != 0) {
                            selectedPositions[matched++] = i + Long.numberOfTrailingZeros(mask);
                            mask &= mask - 1;
                        }
                    }
                    for (; i < count; i++) {
                        int idx = offOff + (i << 2);
                        if ((int) INT.get(data, idx + Integer.BYTES) == (int) INT.get(data, idx)) {
                            selectedPositions[matched++] = i;
                        }
                    }
                    return matched;
                }
                int nullOff = nullsOffset;
                for (; i < vectorLimit; i += lanes) {
                    IntVector starts = ByteVector.fromArray(BYTE_SPECIES, data, offOff + (i << 2)).reinterpretAsInts();
                    IntVector ends = ByteVector.fromArray(BYTE_SPECIES, data, offOff + ((i + 1) << 2)).reinterpretAsInts();
                    long nullWord = (long) LONG.get(data, nullOff + ((i >>> 6) << 3));
                    long mask = ends.sub(starts).eq(targetLenVec).toLong() & ~(nullWord >>> i);
                    while (mask != 0) {
                        selectedPositions[matched++] = i + Long.numberOfTrailingZeros(mask);
                        mask &= mask - 1;
                    }
                }
                for (; i < count; i++) {
                    int idx = offOff + (i << 2);
                    if ((int) INT.get(data, idx + Integer.BYTES) == (int) INT.get(data, idx)
                            && ((long) LONG.get(data, nullOff + ((i >>> 6) << 3)) & (1L << i)) == 0) {
                        selectedPositions[matched++] = i;
                    }
                }
                return matched;
            }
            int i = 0;
            if (!mayHaveNull) {
                for (; i < vectorLimit; i += lanes) {
                    IntVector starts = ByteVector.fromArray(BYTE_SPECIES, data, offOff + (i << 2)).reinterpretAsInts();
                    IntVector ends = ByteVector.fromArray(BYTE_SPECIES, data, offOff + ((i + 1) << 2)).reinterpretAsInts();
                    long mask = ends.sub(starts).eq(targetLenVec).toLong();
                    if (mask == 0) {
                        continue;
                    }
                    if (mask == allMask) {
                        int off = valOff + (int) INT.get(data, offOff + (i << 2));
                        for (int lane = 0; lane < lanes; lane++, off += targetLength) {
                            if (data[off] == firstByte && Arrays.equals(data, off, off + targetLength, target, targetOffset, targetOffset + targetLength)) {
                                selectedPositions[matched++] = i + lane;
                            }
                        }
                        continue;
                    }
                    while (mask != 0) {
                        int pos = i + Long.numberOfTrailingZeros(mask);
                        int off = valOff + (int) INT.get(data, offOff + (pos << 2));
                        if (data[off] == firstByte && Arrays.equals(data, off, off + targetLength, target, targetOffset, targetOffset + targetLength)) {
                            selectedPositions[matched++] = pos;
                        }
                        mask &= mask - 1;
                    }
                }
                for (; i < count; i++) {
                    int idx = offOff + (i << 2);
                    int s = (int) INT.get(data, idx);
                    if ((int) INT.get(data, idx + Integer.BYTES) - s == targetLength) {
                        int off = valOff + s;
                        if (data[off] == firstByte && Arrays.equals(data, off, off + targetLength, target, targetOffset, targetOffset + targetLength)) {
                            selectedPositions[matched++] = i;
                        }
                    }
                }
                return matched;
            }
            int nullOff = nullsOffset;
            for (; i < vectorLimit; i += lanes) {
                IntVector starts = ByteVector.fromArray(BYTE_SPECIES, data, offOff + (i << 2)).reinterpretAsInts();
                IntVector ends = ByteVector.fromArray(BYTE_SPECIES, data, offOff + ((i + 1) << 2)).reinterpretAsInts();
                long nullWord = (long) LONG.get(data, nullOff + ((i >>> 6) << 3));
                long mask = ends.sub(starts).eq(targetLenVec).toLong() & ~(nullWord >>> i);
                if (mask == 0) {
                    continue;
                }
                if (mask == allMask) {
                    int off = valOff + (int) INT.get(data, offOff + (i << 2));
                    for (int lane = 0; lane < lanes; lane++, off += targetLength) {
                        if (data[off] == firstByte && Arrays.equals(data, off, off + targetLength, target, targetOffset, targetOffset + targetLength)) {
                            selectedPositions[matched++] = i + lane;
                        }
                    }
                    continue;
                }
                while (mask != 0) {
                    int pos = i + Long.numberOfTrailingZeros(mask);
                    int off = valOff + (int) INT.get(data, offOff + (pos << 2));
                    if (data[off] == firstByte && Arrays.equals(data, off, off + targetLength, target, targetOffset, targetOffset + targetLength)) {
                        selectedPositions[matched++] = pos;
                    }
                    mask &= mask - 1;
                }
            }
            for (; i < count; i++) {
                int idx = offOff + (i << 2);
                int s = (int) INT.get(data, idx);
                if ((int) INT.get(data, idx + Integer.BYTES) - s == targetLength
                        && ((long) LONG.get(data, nullOff + ((i >>> 6) << 3)) & (1L << i)) == 0) {
                    int off = valOff + s;
                    if (data[off] == firstByte && Arrays.equals(data, off, off + targetLength, target, targetOffset, targetOffset + targetLength)) {
                        selectedPositions[matched++] = i;
                    }
                }
            }
            return matched;
        }

        if (!mayHaveNull) {
            for (int i = 0; i < count; i++) {
                int pos = selectedPositions[i];
                long p = (long) LONG.get(data, offOff + ((base + pos) << 2));
                int s = (int) p;
                if ((int) (p >>> 32) - s == targetLength) {
                    int off = valOff + s;
                    if (targetLength == 0 || (data[off] == firstByte && Arrays.equals(data, off, off + targetLength, target, targetOffset, targetOffset + targetLength))) {
                        selectedPositions[matched++] = pos;
                    }
                }
            }
            return matched;
        }
        int nullOff = nullsOffset;
        for (int i = 0; i < count; i++) {
            int pos = selectedPositions[i];
            int rawPos = base + pos;
            long p = (long) LONG.get(data, offOff + (rawPos << 2));
            int s = (int) p;
            if ((int) (p >>> 32) - s == targetLength && ((long) LONG.get(data, nullOff + ((rawPos >>> 6) << 3)) & (1L << rawPos)) == 0) {
                int off = valOff + s;
                if (targetLength == 0 || (data[off] == firstByte && Arrays.equals(data, off, off + targetLength, target, targetOffset, targetOffset + targetLength))) {
                    selectedPositions[matched++] = pos;
                }
            }
        }
        return matched;
    }

    @Override
    public long getSizeInBytes()
    {
        if (positionCount == 0) {
            return 0;
        }
        long nullBytes = mayHaveNull ? nullsBytes(positionCount) : 0;
        return nullBytes + (long) Integer.BYTES * positionCount + valueBytes();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(bytes);
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return getRegion(position, length).getSizeInBytes();
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        if (isNull(position)) {
            return 0;
        }
        return getSliceLength(position);
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        consumer.accept(bytes, sizeOf(bytes));
        consumer.accept(this, getRetainedSizeInBytes() - sizeOf(bytes));
    }

    @Override
    public StridedVariableWidthBlock getSingleValueBlock(int position)
    {
        int len = getSliceLength(position);
        StridedVariableWidthBlockBuilder builder = new StridedVariableWidthBlockBuilder(1, len);
        builder.append(this, position);
        return builder.buildValueBlock();
    }

    @Override
    public Optional<Bitmap> getValidityBitmap()
    {
        if (!mayHaveNull) {
            return Optional.empty();
        }
        int wordCount = Bitmap.wordsForBits(positionCount);
        long[] words = new long[wordCount];
        if (positionCount == 0) {
            return Optional.of(new Bitmap(words, 0, 0));
        }
        int bitOffset = basePosition & 63;
        int startWord = basePosition >>> 6;
        if (bitOffset == 0) {
            for (int i = 0; i < wordCount; i++) {
                words[i] = ~(long) LONG.get(bytes, nullsOffset + ((startWord + i) << 3));
            }
        }
        else {
            int maxSrcWord = (basePosition + positionCount - 1) >>> 6;
            for (int i = 0; i < wordCount; i++) {
                int srcWord = startWord + i;
                long low = (long) LONG.get(bytes, nullsOffset + (srcWord << 3)) >>> bitOffset;
                long high = srcWord + 1 <= maxSrcWord
                        ? (long) LONG.get(bytes, nullsOffset + ((srcWord + 1) << 3)) << (64 - bitOffset)
                        : 0L;
                words[i] = ~(low | high);
            }
        }
        int trailingBits = positionCount & 63;
        if (trailingBits != 0) {
            words[wordCount - 1] &= (1L << trailingBits) - 1;
        }
        return Optional.of(new Bitmap(words, 0, positionCount));
    }

    @Override
    public StridedVariableWidthBlock getRegion(int positionOffset, int length)
    {
        checkValidRegion(positionCount, positionOffset, length);
        return new StridedVariableWidthBlock(bytes, basePosition + positionOffset, length, mayHaveNull, nullsOffset, offsetsOffset, valuesOffset, maxLength);
    }

    @Override
    public StridedVariableWidthBlock copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);
        if (length == 0) {
            return new StridedVariableWidthBlock(new byte[offsetsBytes(0)], 0, 0, false, 0, 0, 0, 0);
        }
        byte[] src = bytes;
        int srcBaseOff = baseOffsetsOffset;
        int srcValOff = valuesOffset;
        int totalBytes = 0;
        int maxLen = 0;
        for (int i = 0; i < length; i++) {
            int pos = checkIndex(positions[offset + i], positionCount);
            long pair = (long) LONG.get(src, srcBaseOff + (pos << 2));
            int len = (int) (pair >>> 32) - (int) pair;
            totalBytes += len;
            maxLen = max(maxLen, len);
        }
        int nullsSize = mayHaveNull ? nullsBytes(length) : 0;
        int dstOffOff = nullsSize;
        int dstValOff = dstOffOff + offsetsBytes(length);
        byte[] dst = new byte[toIntExact((long) dstValOff + totalBytes)];
        boolean copiedHasNull = mayHaveNull && copyNullBitsPacked(src, nullsOffset, basePosition, positions, offset, length, dst, 0);
        int copiedBytes = 0;
        if (COPY_IN_CHUNKS && totalBytes > 0 && !isNearlyRange(positions, offset, length)) {
            copiedBytes = copiedBytesForCopy(length, totalBytes, maxLen);
        }
        if (copiedBytes == 0) {
            copyPositionsRanges(src, srcBaseOff, srcValOff, positions, offset, length, dst, dstOffOff, dstValOff);
        }
        else if (copiedBytes <= MAX_CHUNKED_CELL) {
            copyPositionsChunks(src, srcBaseOff, srcValOff, positions, offset, length, dst, dstOffOff, dstValOff, copiedBytes);
        }
        else {
            copyPositionsLongChunks(src, srcBaseOff, srcValOff, positions, offset, length, dst, dstOffOff, dstValOff, copiedBytes);
        }
        return new StridedVariableWidthBlock(dst, 0, length, copiedHasNull, 0, dstOffOff, dstValOff, maxLen);
    }

    private static boolean isNearlyRange(int[] positions, int offset, int length)
    {
        long gaps = (long) positions[offset + length - 1] - positions[offset] + 1 - length;
        return gaps >= 0 && gaps <= (length >> 5);
    }

    private static int copiedBytesForCopy(int length, long totalBytes, int maxLen)
    {
        if (maxLen <= MAX_CHUNKED_CELL) {
            int copiedBytes = CHUNK_SIZE;
            while (copiedBytes < maxLen) {
                copiedBytes *= 2;
            }
            return copiedBytes;
        }
        if (maxLen <= MAX_CHUNKED_VALUE) {
            int copiedBytes = (maxLen + MAX_CHUNKED_CELL - 1) / MAX_CHUNKED_CELL * MAX_CHUNKED_CELL;
            if ((long) copiedBytes * length <= 2 * totalBytes) {
                return copiedBytes;
            }
        }
        long coveredLength = 2 * totalBytes / length;
        if (coveredLength > MAX_CHUNKED_CELL) {
            return 0;
        }
        int copiedBytes = CHUNK_SIZE;
        while (copiedBytes < coveredLength) {
            copiedBytes *= 2;
        }
        return copiedBytes;
    }

    private static void copyPositionsRanges(
            byte[] src,
            int srcBaseOff,
            int srcValOff,
            int[] positions,
            int offset,
            int length,
            byte[] dst,
            int dstOffOff,
            int dstValOff)
    {
        int dstEnd = 0;
        int rangeStart = 0;
        int rangeEnd = 0;
        int rangeDst = 0;
        for (int i = 0; i < length; i++) {
            int idx = srcBaseOff + (positions[offset + i] << 2);
            int start = (int) INT.get(src, idx);
            int len = (int) INT.get(src, idx + Integer.BYTES) - start;
            if (len != 0) {
                if (start != rangeEnd) {
                    if (rangeEnd > rangeStart) {
                        System.arraycopy(src, srcValOff + rangeStart, dst, dstValOff + rangeDst, rangeEnd - rangeStart);
                    }
                    rangeDst = dstEnd;
                    rangeStart = start;
                }
                rangeEnd = start + len;
                dstEnd += len;
            }
            INT.set(dst, dstOffOff + ((i + 1) << 2), dstEnd);
        }
        if (rangeEnd > rangeStart) {
            System.arraycopy(src, srcValOff + rangeStart, dst, dstValOff + rangeDst, rangeEnd - rangeStart);
        }
    }

    private static void copyPositionsChunks(
            byte[] src,
            int srcBaseOff,
            int srcValOff,
            int[] positions,
            int offset,
            int length,
            byte[] dst,
            int dstOffOff,
            int dstValOff,
            int copiedBytes)
    {
        int sourceLimit = src.length - srcValOff - copiedBytes;
        int targetLimit = dst.length - dstValOff - copiedBytes;
        int dstEnd = 0;
        if (copiedBytes == CHUNK_SIZE) {
            for (int i = 0; i < length; i++) {
                long pair = (long) LONG.get(src, srcBaseOff + (positions[offset + i] << 2));
                int start = (int) pair;
                int len = (int) (pair >>> 32) - start;
                if (dstEnd <= targetLimit && start <= sourceLimit && len <= CHUNK_SIZE) {
                    copyChunk(src, srcValOff + start, dst, dstValOff + dstEnd);
                }
                else if (len > 0) {
                    System.arraycopy(src, srcValOff + start, dst, dstValOff + dstEnd, len);
                }
                dstEnd += len;
                INT.set(dst, dstOffOff + ((i + 1) << 2), dstEnd);
            }
        }
        else if (copiedBytes == 2 * CHUNK_SIZE) {
            for (int i = 0; i < length; i++) {
                long pair = (long) LONG.get(src, srcBaseOff + (positions[offset + i] << 2));
                int start = (int) pair;
                int len = (int) (pair >>> 32) - start;
                if (dstEnd <= targetLimit && start <= sourceLimit && len <= 2 * CHUNK_SIZE) {
                    int s = srcValOff + start;
                    int d = dstValOff + dstEnd;
                    copyChunk(src, s, dst, d);
                    copyChunk(src, s + CHUNK_SIZE, dst, d + CHUNK_SIZE);
                }
                else if (len > 0) {
                    System.arraycopy(src, srcValOff + start, dst, dstValOff + dstEnd, len);
                }
                dstEnd += len;
                INT.set(dst, dstOffOff + ((i + 1) << 2), dstEnd);
            }
        }
        else {
            for (int i = 0; i < length; i++) {
                long pair = (long) LONG.get(src, srcBaseOff + (positions[offset + i] << 2));
                int start = (int) pair;
                int len = (int) (pair >>> 32) - start;
                if (dstEnd <= targetLimit && start <= sourceLimit && len <= 4 * CHUNK_SIZE) {
                    copyFourChunks(src, srcValOff + start, dst, dstValOff + dstEnd);
                }
                else if (len > 0) {
                    System.arraycopy(src, srcValOff + start, dst, dstValOff + dstEnd, len);
                }
                dstEnd += len;
                INT.set(dst, dstOffOff + ((i + 1) << 2), dstEnd);
            }
        }
    }

    private static void copyPositionsLongChunks(
            byte[] src,
            int srcBaseOff,
            int srcValOff,
            int[] positions,
            int offset,
            int length,
            byte[] dst,
            int dstOffOff,
            int dstValOff,
            int copiedBytes)
    {
        int sourceLimit = src.length - srcValOff - copiedBytes;
        int targetLimit = dst.length - dstValOff - copiedBytes;
        int dstEnd = 0;
        if (copiedBytes == 8 * CHUNK_SIZE) {
            for (int i = 0; i < length; i++) {
                long pair = (long) LONG.get(src, srcBaseOff + (positions[offset + i] << 2));
                int start = (int) pair;
                int len = (int) (pair >>> 32) - start;
                if (dstEnd <= targetLimit && start <= sourceLimit && len <= 8 * CHUNK_SIZE) {
                    int s = srcValOff + start;
                    int d = dstValOff + dstEnd;
                    copyFourChunks(src, s, dst, d);
                    copyFourChunks(src, s + 4 * CHUNK_SIZE, dst, d + 4 * CHUNK_SIZE);
                }
                else if (len > 0) {
                    System.arraycopy(src, srcValOff + start, dst, dstValOff + dstEnd, len);
                }
                dstEnd += len;
                INT.set(dst, dstOffOff + ((i + 1) << 2), dstEnd);
            }
        }
        else if (copiedBytes == 12 * CHUNK_SIZE) {
            for (int i = 0; i < length; i++) {
                long pair = (long) LONG.get(src, srcBaseOff + (positions[offset + i] << 2));
                int start = (int) pair;
                int len = (int) (pair >>> 32) - start;
                if (dstEnd <= targetLimit && start <= sourceLimit && len <= 12 * CHUNK_SIZE) {
                    int s = srcValOff + start;
                    int d = dstValOff + dstEnd;
                    copyFourChunks(src, s, dst, d);
                    copyFourChunks(src, s + 4 * CHUNK_SIZE, dst, d + 4 * CHUNK_SIZE);
                    copyFourChunks(src, s + 8 * CHUNK_SIZE, dst, d + 8 * CHUNK_SIZE);
                }
                else if (len > 0) {
                    System.arraycopy(src, srcValOff + start, dst, dstValOff + dstEnd, len);
                }
                dstEnd += len;
                INT.set(dst, dstOffOff + ((i + 1) << 2), dstEnd);
            }
        }
        else {
            for (int i = 0; i < length; i++) {
                long pair = (long) LONG.get(src, srcBaseOff + (positions[offset + i] << 2));
                int start = (int) pair;
                int len = (int) (pair >>> 32) - start;
                if (dstEnd <= targetLimit && start <= sourceLimit && len <= 16 * CHUNK_SIZE) {
                    int s = srcValOff + start;
                    int d = dstValOff + dstEnd;
                    copyFourChunks(src, s, dst, d);
                    copyFourChunks(src, s + 4 * CHUNK_SIZE, dst, d + 4 * CHUNK_SIZE);
                    copyFourChunks(src, s + 8 * CHUNK_SIZE, dst, d + 8 * CHUNK_SIZE);
                    copyFourChunks(src, s + 12 * CHUNK_SIZE, dst, d + 12 * CHUNK_SIZE);
                }
                else if (len > 0) {
                    System.arraycopy(src, srcValOff + start, dst, dstValOff + dstEnd, len);
                }
                dstEnd += len;
                INT.set(dst, dstOffOff + ((i + 1) << 2), dstEnd);
            }
        }
    }

    static boolean copyNullBitsPacked(byte[] src, int srcNullOff, int srcBase, int[] positions, int offset, int length, byte[] dst, int dstPos)
    {
        int any = 0;
        int i = 0;
        while (i < length && ((dstPos + i) & 7) != 0) {
            int p = srcBase + positions[offset + i];
            int bit = (src[srcNullOff + (p >>> 3)] >>> (p & 7)) & 1;
            int dp = dstPos + i;
            dst[dp >>> 3] |= (byte) (bit << (dp & 7));
            any |= bit;
            i++;
        }
        int dstByte = (dstPos + i) >>> 3;
        int limit = length - 7;
        for (; i < limit; i += 8) {
            int p0 = srcBase + positions[offset + i];
            int p1 = srcBase + positions[offset + i + 1];
            int p2 = srcBase + positions[offset + i + 2];
            int p3 = srcBase + positions[offset + i + 3];
            int p4 = srcBase + positions[offset + i + 4];
            int p5 = srcBase + positions[offset + i + 5];
            int p6 = srcBase + positions[offset + i + 6];
            int p7 = srcBase + positions[offset + i + 7];
            int b = ((src[srcNullOff + (p0 >>> 3)] >>> (p0 & 7)) & 1)
                    | (((src[srcNullOff + (p1 >>> 3)] >>> (p1 & 7)) & 1) << 1)
                    | (((src[srcNullOff + (p2 >>> 3)] >>> (p2 & 7)) & 1) << 2)
                    | (((src[srcNullOff + (p3 >>> 3)] >>> (p3 & 7)) & 1) << 3)
                    | (((src[srcNullOff + (p4 >>> 3)] >>> (p4 & 7)) & 1) << 4)
                    | (((src[srcNullOff + (p5 >>> 3)] >>> (p5 & 7)) & 1) << 5)
                    | (((src[srcNullOff + (p6 >>> 3)] >>> (p6 & 7)) & 1) << 6)
                    | (((src[srcNullOff + (p7 >>> 3)] >>> (p7 & 7)) & 1) << 7);
            dst[dstByte++] = (byte) b;
            any |= b;
        }
        if (i < length) {
            int b = 0;
            for (int shift = 0; i < length; i++, shift++) {
                int p = srcBase + positions[offset + i];
                b |= ((src[srcNullOff + (p >>> 3)] >>> (p & 7)) & 1) << shift;
            }
            dst[dstByte] = (byte) b;
            any |= b;
        }
        return any != 0;
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

    @Override
    public StridedVariableWidthBlock copyRegion(int positionOffset, int length)
    {
        StridedVariableWidthBlock region = getRegion(positionOffset, length);
        Slice image = Slices.allocate(region.serializedSizeInBytes());
        region.writeTo(image.getOutput());
        return wrap(image);
    }

    @Override
    public StridedVariableWidthBlock copyWithAppendedNull()
    {
        StridedVariableWidthBlockBuilder builder = new StridedVariableWidthBlockBuilder(positionCount + 1, valueBytes());
        builder.appendRange(this, 0, positionCount);
        builder.appendNull();
        return builder.buildValueBlock();
    }

    public int serializedSizeInBytes()
    {
        boolean writeNulls = hasNull();
        return toIntExact(HEADER_SIZE + (writeNulls ? nullsBytes(positionCount) : 0L) + offsetsBytes(positionCount) + valueBytes());
    }

    public void writeTo(SliceOutput output)
    {
        boolean writeNulls = hasNull();
        int valueBytes = valueBytes();
        int flags = writeNulls ? MAY_HAVE_NULL_FLAG : 0;
        output.writeLong((positionCount & 0xFFFF_FFFFL) | ((long) flags << 32));
        output.writeLong((valueBytes & 0xFFFF_FFFFL) | ((long) maxLength << 32));
        int offBytes = offsetsBytes(positionCount);
        if (basePosition == 0) {
            if (writeNulls) {
                int nullBytes = nullsBytes(positionCount);
                if ((positionCount & 63) == 0 && nullsOffset + nullBytes == offsetsOffset && offsetsOffset + offBytes == valuesOffset) {
                    output.writeBytes(bytes, nullsOffset, nullBytes + offBytes + valueBytes);
                    return;
                }
                writeCanonicalNullBits(output);
            }
            if (offsetsOffset + offBytes == valuesOffset) {
                output.writeBytes(bytes, offsetsOffset, offBytes + valueBytes);
                return;
            }
            output.writeBytes(bytes, offsetsOffset, offBytes);
            output.writeBytes(bytes, valuesOffset, valueBytes);
            return;
        }
        if (writeNulls) {
            writeCanonicalNullBits(output);
        }
        int firstStart = firstStart();
        for (int i = 0; i <= positionCount; i++) {
            output.writeInt((int) INT.get(bytes, offsetsOffset + ((basePosition + i) << 2)) - firstStart);
        }
        int pad = offBytes - (positionCount + 1) * Integer.BYTES;
        if (pad > 0) {
            output.writeZero(pad);
        }
        output.writeBytes(bytes, valuesOffset + firstStart, valueBytes);
    }

    private void writeCanonicalNullBits(SliceOutput output)
    {
        int wordCount = Bitmap.wordsForBits(positionCount);
        int bitOffset = basePosition & 63;
        int startWord = basePosition >>> 6;
        if (bitOffset == 0 && (positionCount & 63) == 0) {
            output.writeBytes(bytes, nullsOffset + (startWord << 3), wordCount << 3);
            return;
        }
        int maxSrcWord = (basePosition + positionCount - 1) >>> 6;
        int trailingBits = positionCount & 63;
        for (int i = 0; i < wordCount; i++) {
            long word;
            if (bitOffset == 0) {
                word = (long) LONG.get(bytes, nullsOffset + ((startWord + i) << 3));
            }
            else {
                int srcWord = startWord + i;
                long low = (long) LONG.get(bytes, nullsOffset + (srcWord << 3)) >>> bitOffset;
                long high = srcWord + 1 <= maxSrcWord
                        ? (long) LONG.get(bytes, nullsOffset + ((srcWord + 1) << 3)) << (64 - bitOffset)
                        : 0L;
                word = low | high;
            }
            if (i == wordCount - 1 && trailingBits != 0) {
                word &= (1L << trailingBits) - 1;
            }
            output.writeLong(word);
        }
    }

    private static int compareBytes(byte[] a, int aOff, int aLen, byte[] b, int bOff, int bLen)
    {
        int minLen = min(aLen, bLen);
        if (minLen > 0) {
            int diff = (a[aOff] & 0xFF) - (b[bOff] & 0xFF);
            if (diff != 0 || minLen == 1) {
                return diff != 0 ? diff : aLen - bLen;
            }
            return Arrays.compareUnsigned(a, aOff + 1, aOff + aLen, b, bOff + 1, bOff + bLen);
        }
        return aLen - bLen;
    }

    byte[] rawBytes()
    {
        return bytes;
    }

    int rawBasePosition()
    {
        return basePosition;
    }

    int rawNullsOffset()
    {
        return nullsOffset;
    }

    int rawOffsetsOffset()
    {
        return offsetsOffset;
    }

    int rawValuesOffset()
    {
        return valuesOffset;
    }

    int maxLength()
    {
        return maxLength;
    }

    int valueBytes()
    {
        if (positionCount == 0) {
            return 0;
        }
        return lastEnd() - firstStart();
    }

    int firstStart()
    {
        if (positionCount == 0) {
            return 0;
        }
        return (int) INT.get(bytes, offsetsOffset + (basePosition << 2));
    }

    int lastEnd()
    {
        if (positionCount == 0) {
            return 0;
        }
        return (int) INT.get(bytes, offsetsOffset + ((basePosition + positionCount) << 2));
    }

    static int nullsBytes(int positions)
    {
        return Bitmap.wordsForBits(positions) << 3;
    }

    static int offsetsBytes(int positions)
    {
        return (((positions + 1) << 2) + 7) & ~7;
    }

    @Override
    public String toString()
    {
        return "StridedVariableWidthBlock{positionCount=" + positionCount + '}';
    }
}
