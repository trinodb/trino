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
package io.trino.parquet.reader.decoders;

import io.trino.parquet.reader.SimpleSliceInputStream;

import static com.google.common.base.Preconditions.checkArgument;

public final class ByteBitUnpackers
{
    private static final ByteBitUnpacker[] UNPACKERS = {
            new Unpacker1(),
            new Unpacker2(),
            new Unpacker3(),
            new Unpacker4(),
            new Unpacker5(),
            new Unpacker6(),
            new Unpacker7(),
            new Unpacker8(),
            new Unpacker9()};

    // Byte unpacker also exists for the out-of-range 9 value.
    // This unpacker truncates the most significant bit of the resulted numbers.
    // This is due to the fact that deltas may require more than 8 bits to be stored.
    // E.g. Values -100, 100, -100 are stored as deltas 200, -200 which is a span of 400,
    // far exceeding the 8-byte capabilities.
    // However, since the Trino type is Tinyint we have a certainty
    // that the resulting value fits into 8-byte number and the most significant
    // delta bit, when being '1' would cause positive overflow. Knowing that the value
    // fits into 8-bit value we can assume that there has been negative overflow
    // previously and the result is correct
    // Example:
    // Values: -100, 100, -100
    // First value: -100
    // Deltas: 200, -200
    // Minimum delta: -200
    // Normalized deltas: 400 , 0
    // Calculating first value: -100 + -200 (negative overflow) + 400 (positive overflow) = 100
    // Calculating first value without first bit: -100 + -200 + 144 (truncated) = -156
    // And finally the resulting value truncation: (byte) -156 == 100
    public static ByteBitUnpacker getByteBitUnpacker(int bitWidth)
    {
        checkArgument(bitWidth > 0 && bitWidth <= 9, "bitWidth %s should be in the range 1-9", bitWidth);
        return UNPACKERS[bitWidth - 1];
    }

    private ByteBitUnpackers()
    {
    }

    private static final class Unpacker1
            implements ByteBitUnpacker
    {
        @Override
        public void unpack(byte[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(byte[] output, int outputOffset, SimpleSliceInputStream input)
        {
            int v0 = input.readInt();
            output[outputOffset] = (byte) (v0 & 0b1L);
            output[outputOffset + 1] = (byte) ((v0 >>> 1) & 0b1L);
            output[outputOffset + 2] = (byte) ((v0 >>> 2) & 0b1L);
            output[outputOffset + 3] = (byte) ((v0 >>> 3) & 0b1L);
            output[outputOffset + 4] = (byte) ((v0 >>> 4) & 0b1L);
            output[outputOffset + 5] = (byte) ((v0 >>> 5) & 0b1L);
            output[outputOffset + 6] = (byte) ((v0 >>> 6) & 0b1L);
            output[outputOffset + 7] = (byte) ((v0 >>> 7) & 0b1L);
            output[outputOffset + 8] = (byte) ((v0 >>> 8) & 0b1L);
            output[outputOffset + 9] = (byte) ((v0 >>> 9) & 0b1L);
            output[outputOffset + 10] = (byte) ((v0 >>> 10) & 0b1L);
            output[outputOffset + 11] = (byte) ((v0 >>> 11) & 0b1L);
            output[outputOffset + 12] = (byte) ((v0 >>> 12) & 0b1L);
            output[outputOffset + 13] = (byte) ((v0 >>> 13) & 0b1L);
            output[outputOffset + 14] = (byte) ((v0 >>> 14) & 0b1L);
            output[outputOffset + 15] = (byte) ((v0 >>> 15) & 0b1L);
            output[outputOffset + 16] = (byte) ((v0 >>> 16) & 0b1L);
            output[outputOffset + 17] = (byte) ((v0 >>> 17) & 0b1L);
            output[outputOffset + 18] = (byte) ((v0 >>> 18) & 0b1L);
            output[outputOffset + 19] = (byte) ((v0 >>> 19) & 0b1L);
            output[outputOffset + 20] = (byte) ((v0 >>> 20) & 0b1L);
            output[outputOffset + 21] = (byte) ((v0 >>> 21) & 0b1L);
            output[outputOffset + 22] = (byte) ((v0 >>> 22) & 0b1L);
            output[outputOffset + 23] = (byte) ((v0 >>> 23) & 0b1L);
            output[outputOffset + 24] = (byte) ((v0 >>> 24) & 0b1L);
            output[outputOffset + 25] = (byte) ((v0 >>> 25) & 0b1L);
            output[outputOffset + 26] = (byte) ((v0 >>> 26) & 0b1L);
            output[outputOffset + 27] = (byte) ((v0 >>> 27) & 0b1L);
            output[outputOffset + 28] = (byte) ((v0 >>> 28) & 0b1L);
            output[outputOffset + 29] = (byte) ((v0 >>> 29) & 0b1L);
            output[outputOffset + 30] = (byte) ((v0 >>> 30) & 0b1L);
            output[outputOffset + 31] = (byte) ((v0 >>> 31) & 0b1L);
        }
    }

    private static final class Unpacker2
            implements ByteBitUnpacker
    {
        @Override
        public void unpack(byte[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(byte[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            output[outputOffset] = (byte) (v0 & 0b11L);
            output[outputOffset + 1] = (byte) ((v0 >>> 2) & 0b11L);
            output[outputOffset + 2] = (byte) ((v0 >>> 4) & 0b11L);
            output[outputOffset + 3] = (byte) ((v0 >>> 6) & 0b11L);
            output[outputOffset + 4] = (byte) ((v0 >>> 8) & 0b11L);
            output[outputOffset + 5] = (byte) ((v0 >>> 10) & 0b11L);
            output[outputOffset + 6] = (byte) ((v0 >>> 12) & 0b11L);
            output[outputOffset + 7] = (byte) ((v0 >>> 14) & 0b11L);
            output[outputOffset + 8] = (byte) ((v0 >>> 16) & 0b11L);
            output[outputOffset + 9] = (byte) ((v0 >>> 18) & 0b11L);
            output[outputOffset + 10] = (byte) ((v0 >>> 20) & 0b11L);
            output[outputOffset + 11] = (byte) ((v0 >>> 22) & 0b11L);
            output[outputOffset + 12] = (byte) ((v0 >>> 24) & 0b11L);
            output[outputOffset + 13] = (byte) ((v0 >>> 26) & 0b11L);
            output[outputOffset + 14] = (byte) ((v0 >>> 28) & 0b11L);
            output[outputOffset + 15] = (byte) ((v0 >>> 30) & 0b11L);
            output[outputOffset + 16] = (byte) ((v0 >>> 32) & 0b11L);
            output[outputOffset + 17] = (byte) ((v0 >>> 34) & 0b11L);
            output[outputOffset + 18] = (byte) ((v0 >>> 36) & 0b11L);
            output[outputOffset + 19] = (byte) ((v0 >>> 38) & 0b11L);
            output[outputOffset + 20] = (byte) ((v0 >>> 40) & 0b11L);
            output[outputOffset + 21] = (byte) ((v0 >>> 42) & 0b11L);
            output[outputOffset + 22] = (byte) ((v0 >>> 44) & 0b11L);
            output[outputOffset + 23] = (byte) ((v0 >>> 46) & 0b11L);
            output[outputOffset + 24] = (byte) ((v0 >>> 48) & 0b11L);
            output[outputOffset + 25] = (byte) ((v0 >>> 50) & 0b11L);
            output[outputOffset + 26] = (byte) ((v0 >>> 52) & 0b11L);
            output[outputOffset + 27] = (byte) ((v0 >>> 54) & 0b11L);
            output[outputOffset + 28] = (byte) ((v0 >>> 56) & 0b11L);
            output[outputOffset + 29] = (byte) ((v0 >>> 58) & 0b11L);
            output[outputOffset + 30] = (byte) ((v0 >>> 60) & 0b11L);
            output[outputOffset + 31] = (byte) ((v0 >>> 62) & 0b11L);
        }
    }

    private static final class Unpacker3
            implements ByteBitUnpacker
    {
        @Override
        public void unpack(byte[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(byte[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            int v1 = input.readInt();
            output[outputOffset] = (byte) (v0 & 0b111L);
            output[outputOffset + 1] = (byte) ((v0 >>> 3) & 0b111L);
            output[outputOffset + 2] = (byte) ((v0 >>> 6) & 0b111L);
            output[outputOffset + 3] = (byte) ((v0 >>> 9) & 0b111L);
            output[outputOffset + 4] = (byte) ((v0 >>> 12) & 0b111L);
            output[outputOffset + 5] = (byte) ((v0 >>> 15) & 0b111L);
            output[outputOffset + 6] = (byte) ((v0 >>> 18) & 0b111L);
            output[outputOffset + 7] = (byte) ((v0 >>> 21) & 0b111L);
            output[outputOffset + 8] = (byte) ((v0 >>> 24) & 0b111L);
            output[outputOffset + 9] = (byte) ((v0 >>> 27) & 0b111L);
            output[outputOffset + 10] = (byte) ((v0 >>> 30) & 0b111L);
            output[outputOffset + 11] = (byte) ((v0 >>> 33) & 0b111L);
            output[outputOffset + 12] = (byte) ((v0 >>> 36) & 0b111L);
            output[outputOffset + 13] = (byte) ((v0 >>> 39) & 0b111L);
            output[outputOffset + 14] = (byte) ((v0 >>> 42) & 0b111L);
            output[outputOffset + 15] = (byte) ((v0 >>> 45) & 0b111L);
            output[outputOffset + 16] = (byte) ((v0 >>> 48) & 0b111L);
            output[outputOffset + 17] = (byte) ((v0 >>> 51) & 0b111L);
            output[outputOffset + 18] = (byte) ((v0 >>> 54) & 0b111L);
            output[outputOffset + 19] = (byte) ((v0 >>> 57) & 0b111L);
            output[outputOffset + 20] = (byte) ((v0 >>> 60) & 0b111L);
            output[outputOffset + 21] = (byte) (((v0 >>> 63) & 0b1L) | ((v1 & 0b11L) << 1));
            output[outputOffset + 22] = (byte) ((v1 >>> 2) & 0b111L);
            output[outputOffset + 23] = (byte) ((v1 >>> 5) & 0b111L);
            output[outputOffset + 24] = (byte) ((v1 >>> 8) & 0b111L);
            output[outputOffset + 25] = (byte) ((v1 >>> 11) & 0b111L);
            output[outputOffset + 26] = (byte) ((v1 >>> 14) & 0b111L);
            output[outputOffset + 27] = (byte) ((v1 >>> 17) & 0b111L);
            output[outputOffset + 28] = (byte) ((v1 >>> 20) & 0b111L);
            output[outputOffset + 29] = (byte) ((v1 >>> 23) & 0b111L);
            output[outputOffset + 30] = (byte) ((v1 >>> 26) & 0b111L);
            output[outputOffset + 31] = (byte) ((v1 >>> 29) & 0b111L);
        }
    }

    private static final class Unpacker4
            implements ByteBitUnpacker
    {
        @Override
        public void unpack(byte[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(byte[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            output[outputOffset] = (byte) (v0 & 0b1111L);
            output[outputOffset + 1] = (byte) ((v0 >>> 4) & 0b1111L);
            output[outputOffset + 2] = (byte) ((v0 >>> 8) & 0b1111L);
            output[outputOffset + 3] = (byte) ((v0 >>> 12) & 0b1111L);
            output[outputOffset + 4] = (byte) ((v0 >>> 16) & 0b1111L);
            output[outputOffset + 5] = (byte) ((v0 >>> 20) & 0b1111L);
            output[outputOffset + 6] = (byte) ((v0 >>> 24) & 0b1111L);
            output[outputOffset + 7] = (byte) ((v0 >>> 28) & 0b1111L);
            output[outputOffset + 8] = (byte) ((v0 >>> 32) & 0b1111L);
            output[outputOffset + 9] = (byte) ((v0 >>> 36) & 0b1111L);
            output[outputOffset + 10] = (byte) ((v0 >>> 40) & 0b1111L);
            output[outputOffset + 11] = (byte) ((v0 >>> 44) & 0b1111L);
            output[outputOffset + 12] = (byte) ((v0 >>> 48) & 0b1111L);
            output[outputOffset + 13] = (byte) ((v0 >>> 52) & 0b1111L);
            output[outputOffset + 14] = (byte) ((v0 >>> 56) & 0b1111L);
            output[outputOffset + 15] = (byte) ((v0 >>> 60) & 0b1111L);
            output[outputOffset + 16] = (byte) (v1 & 0b1111L);
            output[outputOffset + 17] = (byte) ((v1 >>> 4) & 0b1111L);
            output[outputOffset + 18] = (byte) ((v1 >>> 8) & 0b1111L);
            output[outputOffset + 19] = (byte) ((v1 >>> 12) & 0b1111L);
            output[outputOffset + 20] = (byte) ((v1 >>> 16) & 0b1111L);
            output[outputOffset + 21] = (byte) ((v1 >>> 20) & 0b1111L);
            output[outputOffset + 22] = (byte) ((v1 >>> 24) & 0b1111L);
            output[outputOffset + 23] = (byte) ((v1 >>> 28) & 0b1111L);
            output[outputOffset + 24] = (byte) ((v1 >>> 32) & 0b1111L);
            output[outputOffset + 25] = (byte) ((v1 >>> 36) & 0b1111L);
            output[outputOffset + 26] = (byte) ((v1 >>> 40) & 0b1111L);
            output[outputOffset + 27] = (byte) ((v1 >>> 44) & 0b1111L);
            output[outputOffset + 28] = (byte) ((v1 >>> 48) & 0b1111L);
            output[outputOffset + 29] = (byte) ((v1 >>> 52) & 0b1111L);
            output[outputOffset + 30] = (byte) ((v1 >>> 56) & 0b1111L);
            output[outputOffset + 31] = (byte) ((v1 >>> 60) & 0b1111L);
        }
    }

    private static final class Unpacker5
            implements ByteBitUnpacker
    {
        @Override
        public void unpack(byte[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(byte[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            int v2 = input.readInt();
            output[outputOffset] = (byte) (v0 & 0b11111L);
            output[outputOffset + 1] = (byte) ((v0 >>> 5) & 0b11111L);
            output[outputOffset + 2] = (byte) ((v0 >>> 10) & 0b11111L);
            output[outputOffset + 3] = (byte) ((v0 >>> 15) & 0b11111L);
            output[outputOffset + 4] = (byte) ((v0 >>> 20) & 0b11111L);
            output[outputOffset + 5] = (byte) ((v0 >>> 25) & 0b11111L);
            output[outputOffset + 6] = (byte) ((v0 >>> 30) & 0b11111L);
            output[outputOffset + 7] = (byte) ((v0 >>> 35) & 0b11111L);
            output[outputOffset + 8] = (byte) ((v0 >>> 40) & 0b11111L);
            output[outputOffset + 9] = (byte) ((v0 >>> 45) & 0b11111L);
            output[outputOffset + 10] = (byte) ((v0 >>> 50) & 0b11111L);
            output[outputOffset + 11] = (byte) ((v0 >>> 55) & 0b11111L);
            output[outputOffset + 12] = (byte) (((v0 >>> 60) & 0b1111L) | ((v1 & 0b1L) << 4));
            output[outputOffset + 13] = (byte) ((v1 >>> 1) & 0b11111L);
            output[outputOffset + 14] = (byte) ((v1 >>> 6) & 0b11111L);
            output[outputOffset + 15] = (byte) ((v1 >>> 11) & 0b11111L);
            output[outputOffset + 16] = (byte) ((v1 >>> 16) & 0b11111L);
            output[outputOffset + 17] = (byte) ((v1 >>> 21) & 0b11111L);
            output[outputOffset + 18] = (byte) ((v1 >>> 26) & 0b11111L);
            output[outputOffset + 19] = (byte) ((v1 >>> 31) & 0b11111L);
            output[outputOffset + 20] = (byte) ((v1 >>> 36) & 0b11111L);
            output[outputOffset + 21] = (byte) ((v1 >>> 41) & 0b11111L);
            output[outputOffset + 22] = (byte) ((v1 >>> 46) & 0b11111L);
            output[outputOffset + 23] = (byte) ((v1 >>> 51) & 0b11111L);
            output[outputOffset + 24] = (byte) ((v1 >>> 56) & 0b11111L);
            output[outputOffset + 25] = (byte) (((v1 >>> 61) & 0b111L) | ((v2 & 0b11L) << 3));
            output[outputOffset + 26] = (byte) ((v2 >>> 2) & 0b11111L);
            output[outputOffset + 27] = (byte) ((v2 >>> 7) & 0b11111L);
            output[outputOffset + 28] = (byte) ((v2 >>> 12) & 0b11111L);
            output[outputOffset + 29] = (byte) ((v2 >>> 17) & 0b11111L);
            output[outputOffset + 30] = (byte) ((v2 >>> 22) & 0b11111L);
            output[outputOffset + 31] = (byte) ((v2 >>> 27) & 0b11111L);
        }
    }

    private static final class Unpacker6
            implements ByteBitUnpacker
    {
        @Override
        public void unpack(byte[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(byte[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            output[outputOffset] = (byte) (v0 & 0b111111L);
            output[outputOffset + 1] = (byte) ((v0 >>> 6) & 0b111111L);
            output[outputOffset + 2] = (byte) ((v0 >>> 12) & 0b111111L);
            output[outputOffset + 3] = (byte) ((v0 >>> 18) & 0b111111L);
            output[outputOffset + 4] = (byte) ((v0 >>> 24) & 0b111111L);
            output[outputOffset + 5] = (byte) ((v0 >>> 30) & 0b111111L);
            output[outputOffset + 6] = (byte) ((v0 >>> 36) & 0b111111L);
            output[outputOffset + 7] = (byte) ((v0 >>> 42) & 0b111111L);
            output[outputOffset + 8] = (byte) ((v0 >>> 48) & 0b111111L);
            output[outputOffset + 9] = (byte) ((v0 >>> 54) & 0b111111L);
            output[outputOffset + 10] = (byte) (((v0 >>> 60) & 0b1111L) | ((v1 & 0b11L) << 4));
            output[outputOffset + 11] = (byte) ((v1 >>> 2) & 0b111111L);
            output[outputOffset + 12] = (byte) ((v1 >>> 8) & 0b111111L);
            output[outputOffset + 13] = (byte) ((v1 >>> 14) & 0b111111L);
            output[outputOffset + 14] = (byte) ((v1 >>> 20) & 0b111111L);
            output[outputOffset + 15] = (byte) ((v1 >>> 26) & 0b111111L);
            output[outputOffset + 16] = (byte) ((v1 >>> 32) & 0b111111L);
            output[outputOffset + 17] = (byte) ((v1 >>> 38) & 0b111111L);
            output[outputOffset + 18] = (byte) ((v1 >>> 44) & 0b111111L);
            output[outputOffset + 19] = (byte) ((v1 >>> 50) & 0b111111L);
            output[outputOffset + 20] = (byte) ((v1 >>> 56) & 0b111111L);
            output[outputOffset + 21] = (byte) (((v1 >>> 62) & 0b11L) | ((v2 & 0b1111L) << 2));
            output[outputOffset + 22] = (byte) ((v2 >>> 4) & 0b111111L);
            output[outputOffset + 23] = (byte) ((v2 >>> 10) & 0b111111L);
            output[outputOffset + 24] = (byte) ((v2 >>> 16) & 0b111111L);
            output[outputOffset + 25] = (byte) ((v2 >>> 22) & 0b111111L);
            output[outputOffset + 26] = (byte) ((v2 >>> 28) & 0b111111L);
            output[outputOffset + 27] = (byte) ((v2 >>> 34) & 0b111111L);
            output[outputOffset + 28] = (byte) ((v2 >>> 40) & 0b111111L);
            output[outputOffset + 29] = (byte) ((v2 >>> 46) & 0b111111L);
            output[outputOffset + 30] = (byte) ((v2 >>> 52) & 0b111111L);
            output[outputOffset + 31] = (byte) ((v2 >>> 58) & 0b111111L);
        }
    }

    private static final class Unpacker7
            implements ByteBitUnpacker
    {
        @Override
        public void unpack(byte[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(byte[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            int v3 = input.readInt();
            output[outputOffset] = (byte) (v0 & 0b1111111L);
            output[outputOffset + 1] = (byte) ((v0 >>> 7) & 0b1111111L);
            output[outputOffset + 2] = (byte) ((v0 >>> 14) & 0b1111111L);
            output[outputOffset + 3] = (byte) ((v0 >>> 21) & 0b1111111L);
            output[outputOffset + 4] = (byte) ((v0 >>> 28) & 0b1111111L);
            output[outputOffset + 5] = (byte) ((v0 >>> 35) & 0b1111111L);
            output[outputOffset + 6] = (byte) ((v0 >>> 42) & 0b1111111L);
            output[outputOffset + 7] = (byte) ((v0 >>> 49) & 0b1111111L);
            output[outputOffset + 8] = (byte) ((v0 >>> 56) & 0b1111111L);
            output[outputOffset + 9] = (byte) (((v0 >>> 63) & 0b1L) | ((v1 & 0b111111L) << 1));
            output[outputOffset + 10] = (byte) ((v1 >>> 6) & 0b1111111L);
            output[outputOffset + 11] = (byte) ((v1 >>> 13) & 0b1111111L);
            output[outputOffset + 12] = (byte) ((v1 >>> 20) & 0b1111111L);
            output[outputOffset + 13] = (byte) ((v1 >>> 27) & 0b1111111L);
            output[outputOffset + 14] = (byte) ((v1 >>> 34) & 0b1111111L);
            output[outputOffset + 15] = (byte) ((v1 >>> 41) & 0b1111111L);
            output[outputOffset + 16] = (byte) ((v1 >>> 48) & 0b1111111L);
            output[outputOffset + 17] = (byte) ((v1 >>> 55) & 0b1111111L);
            output[outputOffset + 18] = (byte) (((v1 >>> 62) & 0b11L) | ((v2 & 0b11111L) << 2));
            output[outputOffset + 19] = (byte) ((v2 >>> 5) & 0b1111111L);
            output[outputOffset + 20] = (byte) ((v2 >>> 12) & 0b1111111L);
            output[outputOffset + 21] = (byte) ((v2 >>> 19) & 0b1111111L);
            output[outputOffset + 22] = (byte) ((v2 >>> 26) & 0b1111111L);
            output[outputOffset + 23] = (byte) ((v2 >>> 33) & 0b1111111L);
            output[outputOffset + 24] = (byte) ((v2 >>> 40) & 0b1111111L);
            output[outputOffset + 25] = (byte) ((v2 >>> 47) & 0b1111111L);
            output[outputOffset + 26] = (byte) ((v2 >>> 54) & 0b1111111L);
            output[outputOffset + 27] = (byte) (((v2 >>> 61) & 0b111L) | ((v3 & 0b1111L) << 3));
            output[outputOffset + 28] = (byte) ((v3 >>> 4) & 0b1111111L);
            output[outputOffset + 29] = (byte) ((v3 >>> 11) & 0b1111111L);
            output[outputOffset + 30] = (byte) ((v3 >>> 18) & 0b1111111L);
            output[outputOffset + 31] = (byte) ((v3 >>> 25) & 0b1111111L);
        }
    }

    private static final class Unpacker8
            implements ByteBitUnpacker
    {
        @Override
        public void unpack(byte[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            input.readBytes(output, outputOffset, length);
        }
    }

    private static final class Unpacker9
            implements ByteBitUnpacker
    {
        @Override
        public void unpack(byte[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(byte[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            int v4 = input.readInt();
            output[outputOffset] = (byte) v0;
            output[outputOffset + 1] = (byte) ((v0 >>> 9) & 0b111111111L);
            output[outputOffset + 2] = (byte) ((v0 >>> 18) & 0b111111111L);
            output[outputOffset + 3] = (byte) ((v0 >>> 27) & 0b111111111L);
            output[outputOffset + 4] = (byte) ((v0 >>> 36) & 0b111111111L);
            output[outputOffset + 5] = (byte) ((v0 >>> 45) & 0b111111111L);
            output[outputOffset + 6] = (byte) ((v0 >>> 54) & 0b111111111L);
            output[outputOffset + 7] = (byte) ((v0 >>> 63) | ((v1 & 0b11111111L) << 1));
            output[outputOffset + 8] = (byte) ((v1 >>> 8) & 0b111111111L);
            output[outputOffset + 9] = (byte) ((v1 >>> 17) & 0b111111111L);
            output[outputOffset + 10] = (byte) ((v1 >>> 26) & 0b111111111L);
            output[outputOffset + 11] = (byte) ((v1 >>> 35) & 0b111111111L);
            output[outputOffset + 12] = (byte) ((v1 >>> 44) & 0b111111111L);
            output[outputOffset + 13] = (byte) ((v1 >>> 53) & 0b111111111L);
            output[outputOffset + 14] = (byte) ((v1 >>> 62) | ((v2 & 0b1111111L) << 2));
            output[outputOffset + 15] = (byte) ((v2 >>> 7) & 0b111111111L);
            output[outputOffset + 16] = (byte) ((v2 >>> 16) & 0b111111111L);
            output[outputOffset + 17] = (byte) ((v2 >>> 25) & 0b111111111L);
            output[outputOffset + 18] = (byte) ((v2 >>> 34) & 0b111111111L);
            output[outputOffset + 19] = (byte) ((v2 >>> 43) & 0b111111111L);
            output[outputOffset + 20] = (byte) ((v2 >>> 52) & 0b111111111L);
            output[outputOffset + 21] = (byte) ((v2 >>> 61) | ((v3 & 0b111111L) << 3));
            output[outputOffset + 22] = (byte) ((v3 >>> 6) & 0b111111111L);
            output[outputOffset + 23] = (byte) ((v3 >>> 15) & 0b111111111L);
            output[outputOffset + 24] = (byte) ((v3 >>> 24) & 0b111111111L);
            output[outputOffset + 25] = (byte) ((v3 >>> 33) & 0b111111111L);
            output[outputOffset + 26] = (byte) ((v3 >>> 42) & 0b111111111L);
            output[outputOffset + 27] = (byte) ((v3 >>> 51) & 0b111111111L);
            output[outputOffset + 28] = (byte) ((v3 >>> 60) | ((v4 & 0b11111L) << 4));
            output[outputOffset + 29] = (byte) ((v4 >>> 5) & 0b111111111L);
            output[outputOffset + 30] = (byte) ((v4 >>> 14) & 0b111111111L);
            output[outputOffset + 31] = (byte) ((v4 >>> 23) & 0b111111111L);
        }
    }
}
