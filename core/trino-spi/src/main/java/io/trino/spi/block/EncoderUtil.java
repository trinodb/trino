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
import jdk.incubator.vector.VectorOperators;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

final class EncoderUtil
{
    private static final ByteVector NULL_BIT_SHIFTS = ByteVector.fromArray(ByteVector.SPECIES_64, new byte[] {7, 6, 5, 4, 3, 2, 1, 0}, 0);

    private EncoderUtil() {}

    /**
     * Append null values for the block as a stream of bits.
     */
    public static void encodeNullsAsBitsScalar(SliceOutput sliceOutput, @Nullable boolean[] isNull, int offset, int length)
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
            int value = ((isNull[position + offset] ? 1 : 0) << 7) |
                    ((isNull[position + offset + 1] ? 1 : 0) << 6) |
                    ((isNull[position + offset + 2] ? 1 : 0) << 5) |
                    ((isNull[position + offset + 3] ? 1 : 0) << 4) |
                    ((isNull[position + offset + 4] ? 1 : 0) << 3) |
                    ((isNull[position + offset + 5] ? 1 : 0) << 2) |
                    ((isNull[position + offset + 6] ? 1 : 0) << 1) |
                    (isNull[position + offset + 7] ? 1 : 0);
            packedIsNull[currentByte++] = (byte) (value & 0xFF);
        }

        // write last null bits
        if ((length & 0b111) > 0) {
            int value = 0;
            int mask = 0b1000_0000;
            for (int position = length & ~0b111; position < length; position++) {
                value |= isNull[position + offset] ? mask : 0;
                mask >>>= 1;
            }
            packedIsNull[currentByte++] = (byte) (value & 0xFF);
        }

        sliceOutput.writeBytes(packedIsNull, 0, currentByte);
    }

    /**
     * Implementation of {@link EncoderUtil#encodeNullsAsBitsScalar(SliceOutput, boolean[], int, int)} using the vector API
     */
    public static void encodeNullsAsBitsVectorized(SliceOutput sliceOutput, @Nullable boolean[] isNull, int offset, int length)
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
        int position = 0;
        while (position < ByteVector.SPECIES_64.loopBound(length)) {
            packedIsNull[currentByte++] = ByteVector.fromBooleanArray(ByteVector.SPECIES_64, isNull, position + offset)
                    .lanewise(VectorOperators.LSHL, NULL_BIT_SHIFTS)
                    .reduceLanes(VectorOperators.OR);
            position += ByteVector.SPECIES_64.length();
        }

        // write last null bits
        if (position < length) {
            int value = 0;
            int mask = 0b1000_0000;
            for (; position < length; position++) {
                value |= isNull[position + offset] ? mask : 0;
                mask >>>= 1;
            }
            packedIsNull[currentByte++] = (byte) (value & 0xFF);
        }

        sliceOutput.writeBytes(packedIsNull, 0, currentByte);
    }

    /**
     * Decode the bit stream created by encodeNullsAsBits.
     */
    public static Optional<boolean[]> decodeNullBitsScalar(SliceInput sliceInput, int positionCount)
    {
        return Optional.ofNullable(retrieveNullBits(sliceInput, positionCount))
                .map(packedIsNull -> decodeNullBitsScalar(packedIsNull, positionCount));
    }

    public static boolean[] decodeNullBitsScalar(byte[] packedIsNull, int positionCount)
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

    /**
     * Implementation of {@link EncoderUtil#decodeNullBitsScalar(SliceInput, int)} that uses the vector API
     */
    public static Optional<boolean[]> decodeNullBitsVectorized(SliceInput sliceInput, int positionCount)
    {
        return Optional.ofNullable(retrieveNullBits(sliceInput, positionCount))
                .map(packedIsNull -> decodeNullBitsVectorized(packedIsNull, positionCount));
    }

    public static boolean[] decodeNullBitsVectorized(byte[] packedIsNull, int positionCount)
    {
        // read null bits 8 at a time
        boolean[] valueIsNull = new boolean[positionCount];
        int currentByte = 0;
        int position = 0;
        while (position < ByteVector.SPECIES_64.loopBound(positionCount)) {
            // equivalent of ((value >>> shift) & 1) != 0
            ByteVector.broadcast(ByteVector.SPECIES_64, packedIsNull[currentByte++])
                    .lanewise(VectorOperators.LSHR, NULL_BIT_SHIFTS)
                    .intoBooleanArray(valueIsNull, position);
            position += ByteVector.SPECIES_64.length();
        }

        // read last null bits
        if (position < positionCount) {
            byte value = packedIsNull[currentByte];
            int mask = 0b1000_0000;
            for (; position < positionCount; position++) {
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
}
