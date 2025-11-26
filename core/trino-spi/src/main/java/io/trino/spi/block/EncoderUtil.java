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

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import jakarta.annotation.Nullable;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

import static java.util.Objects.checkFromIndexSize;

final class EncoderUtil
{
    private static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;
    private static final VectorSpecies<Integer> INT_SPECIES = IntVector.SPECIES_PREFERRED;
    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED;
    private static final VectorSpecies<Short> SHORT_SPECIES = ShortVector.SPECIES_PREFERRED;

    private EncoderUtil() {}

    /**
     * Append null values for the block as a stream of bits.
     */
    @SuppressWarnings({"NarrowingCompoundAssignment", "ImplicitNumericConversion"})
    public static void encodeNullsAsBits(SliceOutput sliceOutput, @Nullable boolean[] isNull, int offset, int length)
    {
        sliceOutput.writeBoolean(isNull != null);
        if (isNull == null) {
            return;
        }
        // inlined from Objects.checkFromIndexSize
        if ((length | offset) < 0 || length > isNull.length - offset) {
            throw new IndexOutOfBoundsException("Invalid offset: %s, length: %s for size: %s".formatted(offset, length, isNull.length));
        }
        byte[] packedIsNull = new byte[(length + 7) / 8];
        int currentByte = 0;
        for (int position = 0; position < (length & ~0b111); position += 8) {
            byte value = 0;
            value |= (isNull[position + offset] ? 1 : 0) << 7;
            value |= (isNull[position + offset + 1] ? 1 : 0) << 6;
            value |= (isNull[position + offset + 2] ? 1 : 0) << 5;
            value |= (isNull[position + offset + 3] ? 1 : 0) << 4;
            value |= (isNull[position + offset + 4] ? 1 : 0) << 3;
            value |= (isNull[position + offset + 5] ? 1 : 0) << 2;
            value |= (isNull[position + offset + 6] ? 1 : 0) << 1;
            value |= (isNull[position + offset + 7] ? 1 : 0);
            packedIsNull[currentByte++] = value;
        }

        // write last null bits
        if ((length & 0b111) > 0) {
            byte value = 0;
            int mask = 0b1000_0000;
            for (int position = length & ~0b111; position < length; position++) {
                value |= isNull[position + offset] ? mask : 0;
                mask >>>= 1;
            }
            packedIsNull[currentByte++] = value;
        }

        sliceOutput.writeBytes(packedIsNull, 0, currentByte);
    }

    /**
     * Decode the bit stream created by encodeNullsAsBits.
     */
    public static Optional<boolean[]> decodeNullBits(SliceInput sliceInput, int positionCount)
    {
        return Optional.ofNullable(retrieveNullBits(sliceInput, positionCount))
                .map(packedIsNull -> decodeNullBits(packedIsNull, positionCount));
    }

    public static boolean[] decodeNullBits(byte[] packedIsNull, int positionCount)
    {
        // read null bits 8 at a time
        boolean[] valueIsNull = new boolean[positionCount];
        int currentByte = 0;
        for (int position = 0; position < (positionCount & ~0b111); position += 8, currentByte++) {
            byte value = packedIsNull[currentByte];
            valueIsNull[position] = ((value & 0b1000_0000) != 0);
            valueIsNull[position + 1] = ((value & 0b0100_0000) != 0);
            valueIsNull[position + 2] = ((value & 0b0010_0000) != 0);
            valueIsNull[position + 3] = ((value & 0b0001_0000) != 0);
            valueIsNull[position + 4] = ((value & 0b0000_1000) != 0);
            valueIsNull[position + 5] = ((value & 0b0000_0100) != 0);
            valueIsNull[position + 6] = ((value & 0b0000_0010) != 0);
            valueIsNull[position + 7] = ((value & 0b0000_0001) != 0);
        }

        // read last null bits
        if ((positionCount & 0b111) > 0) {
            byte value = packedIsNull[packedIsNull.length - 1];
            int mask = 0b1000_0000;
            for (int position = positionCount & ~0b111; position < positionCount; position++) {
                valueIsNull[position] = ((value & mask) != 0);
                mask >>>= 1;
            }
        }

        return valueIsNull;
    }

    @Nullable
    public static byte[] retrieveNullBits(SliceInput sliceInput, int positionCount)
    {
        if (!sliceInput.readBoolean()) {
            return null;
        }
        try {
            return sliceInput.readNBytes((positionCount + 7) / 8);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static void compactBytesWithNullsVectorized(SliceOutput sliceOutput, byte[] values, boolean[] isNull, int offset, int length)
    {
        checkFromIndexSize(offset, length, values.length);
        checkFromIndexSize(offset, length, isNull.length);
        byte[] compacted = new byte[length];
        int valuesIndex = 0;
        int compactedIndex = 0;
        for (; valuesIndex < BYTE_SPECIES.loopBound(length); valuesIndex += BYTE_SPECIES.length()) {
            VectorMask<Byte> mask = BYTE_SPECIES.loadMask(isNull, valuesIndex + offset).not();
            ByteVector.fromArray(BYTE_SPECIES, values, valuesIndex + offset)
                    .compress(mask)
                    .intoArray(compacted, compactedIndex);
            compactedIndex += mask.trueCount();
        }
        for (; valuesIndex < length; valuesIndex++) {
            compacted[compactedIndex] = values[valuesIndex + offset];
            compactedIndex += isNull[valuesIndex + offset] ? 0 : 1;
        }
        sliceOutput.writeInt(compactedIndex);
        sliceOutput.writeBytes(compacted, 0, compactedIndex);
    }

    static void compactBytesWithNullsScalar(SliceOutput sliceOutput, byte[] values, boolean[] isNull, int offset, int length)
    {
        checkFromIndexSize(offset, length, values.length);
        checkFromIndexSize(offset, length, isNull.length);
        byte[] compacted = new byte[length];
        int compactedIndex = 0;
        for (int i = 0; i < length; i++) {
            compacted[compactedIndex] = values[i + offset];
            compactedIndex += isNull[i + offset] ? 0 : 1;
        }
        sliceOutput.writeInt(compactedIndex);
        sliceOutput.writeBytes(compacted, 0, compactedIndex);
    }

    static void compactShortsWithNullsVectorized(SliceOutput sliceOutput, short[] values, boolean[] isNull, int offset, int length)
    {
        checkFromIndexSize(offset, length, values.length);
        checkFromIndexSize(offset, length, isNull.length);
        short[] compacted = new short[length];
        int valuesIndex = 0;
        int compactedIndex = 0;
        for (; valuesIndex < SHORT_SPECIES.loopBound(length); valuesIndex += SHORT_SPECIES.length()) {
            VectorMask<Short> mask = SHORT_SPECIES.loadMask(isNull, valuesIndex + offset).not();
            ShortVector.fromArray(SHORT_SPECIES, values, valuesIndex + offset)
                    .compress(mask)
                    .intoArray(compacted, compactedIndex);
            compactedIndex += mask.trueCount();
        }
        for (; valuesIndex < length; valuesIndex++) {
            compacted[compactedIndex] = values[valuesIndex + offset];
            compactedIndex += isNull[valuesIndex + offset] ? 0 : 1;
        }
        sliceOutput.writeInt(compactedIndex);
        sliceOutput.writeShorts(compacted, 0, compactedIndex);
    }

    static void compactShortsWithNullsScalar(SliceOutput sliceOutput, short[] values, boolean[] isNull, int offset, int length)
    {
        checkFromIndexSize(offset, length, values.length);
        checkFromIndexSize(offset, length, isNull.length);
        short[] compacted = new short[length];
        int compactedIndex = 0;
        for (int i = 0; i < length; i++) {
            compacted[compactedIndex] = values[i + offset];
            compactedIndex += isNull[i + offset] ? 0 : 1;
        }
        sliceOutput.writeInt(compactedIndex);
        sliceOutput.writeShorts(compacted, 0, compactedIndex);
    }

    static void compactIntsWithNullsVectorized(SliceOutput sliceOutput, int[] values, boolean[] isNull, int offset, int length)
    {
        checkFromIndexSize(offset, length, values.length);
        checkFromIndexSize(offset, length, isNull.length);
        int[] compacted = new int[length];
        int valuesIndex = 0;
        int compactedIndex = 0;
        for (; valuesIndex < INT_SPECIES.loopBound(length); valuesIndex += INT_SPECIES.length()) {
            VectorMask<Integer> mask = INT_SPECIES.loadMask(isNull, valuesIndex + offset).not();
            IntVector.fromArray(INT_SPECIES, values, valuesIndex + offset)
                    .compress(mask)
                    .intoArray(compacted, compactedIndex);
            compactedIndex += mask.trueCount();
        }
        for (; valuesIndex < length; valuesIndex++) {
            compacted[compactedIndex] = values[valuesIndex + offset];
            compactedIndex += isNull[valuesIndex + offset] ? 0 : 1;
        }
        sliceOutput.writeInt(compactedIndex);
        sliceOutput.writeInts(compacted, 0, compactedIndex);
    }

    static void compactIntsWithNullsScalar(SliceOutput sliceOutput, int[] values, boolean[] isNull, int offset, int length)
    {
        checkFromIndexSize(offset, length, values.length);
        checkFromIndexSize(offset, length, isNull.length);
        int[] compacted = new int[length];
        int compactedIndex = 0;
        for (int i = 0; i < length; i++) {
            compacted[compactedIndex] = values[i + offset];
            compactedIndex += isNull[i + offset] ? 0 : 1;
        }
        sliceOutput.writeInt(compactedIndex);
        sliceOutput.writeInts(compacted, 0, compactedIndex);
    }

    static void compactLongsWithNullsVectorized(SliceOutput sliceOutput, long[] values, boolean[] isNull, int offset, int length)
    {
        checkFromIndexSize(offset, length, values.length);
        checkFromIndexSize(offset, length, isNull.length);
        long[] compacted = new long[length];
        int valuesIndex = 0;
        int compactedIndex = 0;
        for (; valuesIndex < LONG_SPECIES.loopBound(length); valuesIndex += LONG_SPECIES.length()) {
            VectorMask<Long> mask = LONG_SPECIES.loadMask(isNull, valuesIndex + offset).not();
            LongVector.fromArray(LONG_SPECIES, values, valuesIndex + offset)
                    .compress(mask)
                    .intoArray(compacted, compactedIndex);
            compactedIndex += mask.trueCount();
        }
        for (; valuesIndex < length; valuesIndex++) {
            compacted[compactedIndex] = values[valuesIndex + offset];
            compactedIndex += isNull[valuesIndex + offset] ? 0 : 1;
        }
        sliceOutput.writeInt(compactedIndex);
        sliceOutput.writeLongs(compacted, 0, compactedIndex);
    }

    static void compactLongsWithNullsScalar(SliceOutput sliceOutput, long[] values, boolean[] isNull, int offset, int length)
    {
        checkFromIndexSize(offset, length, values.length);
        checkFromIndexSize(offset, length, isNull.length);
        long[] compacted = new long[length];
        int compactedIndex = 0;
        for (int i = 0; i < length; i++) {
            compacted[compactedIndex] = values[i + offset];
            compactedIndex += isNull[i + offset] ? 0 : 1;
        }
        sliceOutput.writeInt(compactedIndex);
        sliceOutput.writeLongs(compacted, 0, compactedIndex);
    }
}
