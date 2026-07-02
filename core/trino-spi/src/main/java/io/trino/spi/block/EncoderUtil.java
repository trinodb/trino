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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

import static io.trino.spi.block.Bitmap.getAlignedWord;
import static io.trino.spi.block.Bitmap.lowBitsMask;
import static io.trino.spi.block.Bitmap.wordsForBits;

final class EncoderUtil
{
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

    public static void encodeValidityAsLongs(SliceOutput sliceOutput, @Nullable long[] validity, int offset, int positionCount)
    {
        sliceOutput.writeBoolean(validity != null);
        if (validity == null) {
            return;
        }

        int wordCount = wordsForBits(positionCount);
        for (int wordIndex = 0; wordIndex < wordCount; wordIndex++) {
            int position = wordIndex << 6;
            long word = getAlignedWord(validity, offset, position);
            int remaining = positionCount - position;
            if (remaining < Long.SIZE) {
                word &= lowBitsMask(remaining);
            }
            sliceOutput.writeLong(word);
        }
    }

    @Nullable
    public static long[] decodeValidityAsLongs(SliceInput sliceInput, int positionCount)
    {
        if (!sliceInput.readBoolean()) {
            return null;
        }

        long[] validity = new long[wordsForBits(positionCount)];
        sliceInput.readLongs(validity);
        return validity;
    }
}
