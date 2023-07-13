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

import io.airlift.slice.Slices;
import io.trino.parquet.reader.SimpleSliceInputStream;

import static com.google.common.base.Preconditions.checkArgument;

public final class ShortBitUnpackers
{
    private static final ShortBitUnpacker[] UNPACKERS = {
            new Unpacker1(),
            new Unpacker2(),
            new Unpacker3(),
            new Unpacker4(),
            new Unpacker5(),
            new Unpacker6(),
            new Unpacker7(),
            new Unpacker8(),
            new Unpacker9(),
            new Unpacker10(),
            new Unpacker11(),
            new Unpacker12(),
            new Unpacker13(),
            new Unpacker14(),
            new Unpacker15(),
            new Unpacker16(),
            new Unpacker17()};

    // Short unpacker also exists for the out-of-range 17 value.
    // This unpacker truncates the most significant bit of the resulted numbers.
    // This is due to the fact that deltas may require more than 16 bits to be stored.
    public static ShortBitUnpacker getShortBitUnpacker(int bitWidth)
    {
        checkArgument(bitWidth > 0 && bitWidth <= 17, "bitWidth %s should be in the range 1-17", bitWidth);
        return UNPACKERS[bitWidth - 1];
    }

    private ShortBitUnpackers() {}

    private static final class Unpacker1
            implements ShortBitUnpacker
    {
        @Override
        public void unpack(short[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(short[] output, int outputOffset, SimpleSliceInputStream input)
        {
            int v0 = input.readInt();
            output[outputOffset] = (short) (v0 & 0b1L);
            output[outputOffset + 1] = (short) ((v0 >>> 1) & 0b1L);
            output[outputOffset + 2] = (short) ((v0 >>> 2) & 0b1L);
            output[outputOffset + 3] = (short) ((v0 >>> 3) & 0b1L);
            output[outputOffset + 4] = (short) ((v0 >>> 4) & 0b1L);
            output[outputOffset + 5] = (short) ((v0 >>> 5) & 0b1L);
            output[outputOffset + 6] = (short) ((v0 >>> 6) & 0b1L);
            output[outputOffset + 7] = (short) ((v0 >>> 7) & 0b1L);
            output[outputOffset + 8] = (short) ((v0 >>> 8) & 0b1L);
            output[outputOffset + 9] = (short) ((v0 >>> 9) & 0b1L);
            output[outputOffset + 10] = (short) ((v0 >>> 10) & 0b1L);
            output[outputOffset + 11] = (short) ((v0 >>> 11) & 0b1L);
            output[outputOffset + 12] = (short) ((v0 >>> 12) & 0b1L);
            output[outputOffset + 13] = (short) ((v0 >>> 13) & 0b1L);
            output[outputOffset + 14] = (short) ((v0 >>> 14) & 0b1L);
            output[outputOffset + 15] = (short) ((v0 >>> 15) & 0b1L);
            output[outputOffset + 16] = (short) ((v0 >>> 16) & 0b1L);
            output[outputOffset + 17] = (short) ((v0 >>> 17) & 0b1L);
            output[outputOffset + 18] = (short) ((v0 >>> 18) & 0b1L);
            output[outputOffset + 19] = (short) ((v0 >>> 19) & 0b1L);
            output[outputOffset + 20] = (short) ((v0 >>> 20) & 0b1L);
            output[outputOffset + 21] = (short) ((v0 >>> 21) & 0b1L);
            output[outputOffset + 22] = (short) ((v0 >>> 22) & 0b1L);
            output[outputOffset + 23] = (short) ((v0 >>> 23) & 0b1L);
            output[outputOffset + 24] = (short) ((v0 >>> 24) & 0b1L);
            output[outputOffset + 25] = (short) ((v0 >>> 25) & 0b1L);
            output[outputOffset + 26] = (short) ((v0 >>> 26) & 0b1L);
            output[outputOffset + 27] = (short) ((v0 >>> 27) & 0b1L);
            output[outputOffset + 28] = (short) ((v0 >>> 28) & 0b1L);
            output[outputOffset + 29] = (short) ((v0 >>> 29) & 0b1L);
            output[outputOffset + 30] = (short) ((v0 >>> 30) & 0b1L);
            output[outputOffset + 31] = (short) ((v0 >>> 31) & 0b1L);
        }
    }

    private static final class Unpacker2
            implements ShortBitUnpacker
    {
        @Override
        public void unpack(short[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(short[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            output[outputOffset] = (short) (v0 & 0b11L);
            output[outputOffset + 1] = (short) ((v0 >>> 2) & 0b11L);
            output[outputOffset + 2] = (short) ((v0 >>> 4) & 0b11L);
            output[outputOffset + 3] = (short) ((v0 >>> 6) & 0b11L);
            output[outputOffset + 4] = (short) ((v0 >>> 8) & 0b11L);
            output[outputOffset + 5] = (short) ((v0 >>> 10) & 0b11L);
            output[outputOffset + 6] = (short) ((v0 >>> 12) & 0b11L);
            output[outputOffset + 7] = (short) ((v0 >>> 14) & 0b11L);
            output[outputOffset + 8] = (short) ((v0 >>> 16) & 0b11L);
            output[outputOffset + 9] = (short) ((v0 >>> 18) & 0b11L);
            output[outputOffset + 10] = (short) ((v0 >>> 20) & 0b11L);
            output[outputOffset + 11] = (short) ((v0 >>> 22) & 0b11L);
            output[outputOffset + 12] = (short) ((v0 >>> 24) & 0b11L);
            output[outputOffset + 13] = (short) ((v0 >>> 26) & 0b11L);
            output[outputOffset + 14] = (short) ((v0 >>> 28) & 0b11L);
            output[outputOffset + 15] = (short) ((v0 >>> 30) & 0b11L);
            output[outputOffset + 16] = (short) ((v0 >>> 32) & 0b11L);
            output[outputOffset + 17] = (short) ((v0 >>> 34) & 0b11L);
            output[outputOffset + 18] = (short) ((v0 >>> 36) & 0b11L);
            output[outputOffset + 19] = (short) ((v0 >>> 38) & 0b11L);
            output[outputOffset + 20] = (short) ((v0 >>> 40) & 0b11L);
            output[outputOffset + 21] = (short) ((v0 >>> 42) & 0b11L);
            output[outputOffset + 22] = (short) ((v0 >>> 44) & 0b11L);
            output[outputOffset + 23] = (short) ((v0 >>> 46) & 0b11L);
            output[outputOffset + 24] = (short) ((v0 >>> 48) & 0b11L);
            output[outputOffset + 25] = (short) ((v0 >>> 50) & 0b11L);
            output[outputOffset + 26] = (short) ((v0 >>> 52) & 0b11L);
            output[outputOffset + 27] = (short) ((v0 >>> 54) & 0b11L);
            output[outputOffset + 28] = (short) ((v0 >>> 56) & 0b11L);
            output[outputOffset + 29] = (short) ((v0 >>> 58) & 0b11L);
            output[outputOffset + 30] = (short) ((v0 >>> 60) & 0b11L);
            output[outputOffset + 31] = (short) ((v0 >>> 62) & 0b11L);
        }
    }

    private static final class Unpacker3
            implements ShortBitUnpacker
    {
        @Override
        public void unpack(short[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(short[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            int v1 = input.readInt();
            output[outputOffset] = (short) (v0 & 0b111L);
            output[outputOffset + 1] = (short) ((v0 >>> 3) & 0b111L);
            output[outputOffset + 2] = (short) ((v0 >>> 6) & 0b111L);
            output[outputOffset + 3] = (short) ((v0 >>> 9) & 0b111L);
            output[outputOffset + 4] = (short) ((v0 >>> 12) & 0b111L);
            output[outputOffset + 5] = (short) ((v0 >>> 15) & 0b111L);
            output[outputOffset + 6] = (short) ((v0 >>> 18) & 0b111L);
            output[outputOffset + 7] = (short) ((v0 >>> 21) & 0b111L);
            output[outputOffset + 8] = (short) ((v0 >>> 24) & 0b111L);
            output[outputOffset + 9] = (short) ((v0 >>> 27) & 0b111L);
            output[outputOffset + 10] = (short) ((v0 >>> 30) & 0b111L);
            output[outputOffset + 11] = (short) ((v0 >>> 33) & 0b111L);
            output[outputOffset + 12] = (short) ((v0 >>> 36) & 0b111L);
            output[outputOffset + 13] = (short) ((v0 >>> 39) & 0b111L);
            output[outputOffset + 14] = (short) ((v0 >>> 42) & 0b111L);
            output[outputOffset + 15] = (short) ((v0 >>> 45) & 0b111L);
            output[outputOffset + 16] = (short) ((v0 >>> 48) & 0b111L);
            output[outputOffset + 17] = (short) ((v0 >>> 51) & 0b111L);
            output[outputOffset + 18] = (short) ((v0 >>> 54) & 0b111L);
            output[outputOffset + 19] = (short) ((v0 >>> 57) & 0b111L);
            output[outputOffset + 20] = (short) ((v0 >>> 60) & 0b111L);
            output[outputOffset + 21] = (short) (((v0 >>> 63) & 0b1L) | ((v1 & 0b11L) << 1));
            output[outputOffset + 22] = (short) ((v1 >>> 2) & 0b111L);
            output[outputOffset + 23] = (short) ((v1 >>> 5) & 0b111L);
            output[outputOffset + 24] = (short) ((v1 >>> 8) & 0b111L);
            output[outputOffset + 25] = (short) ((v1 >>> 11) & 0b111L);
            output[outputOffset + 26] = (short) ((v1 >>> 14) & 0b111L);
            output[outputOffset + 27] = (short) ((v1 >>> 17) & 0b111L);
            output[outputOffset + 28] = (short) ((v1 >>> 20) & 0b111L);
            output[outputOffset + 29] = (short) ((v1 >>> 23) & 0b111L);
            output[outputOffset + 30] = (short) ((v1 >>> 26) & 0b111L);
            output[outputOffset + 31] = (short) ((v1 >>> 29) & 0b111L);
        }
    }

    private static final class Unpacker4
            implements ShortBitUnpacker
    {
        @Override
        public void unpack(short[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(short[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            output[outputOffset] = (short) (v0 & 0b1111L);
            output[outputOffset + 1] = (short) ((v0 >>> 4) & 0b1111L);
            output[outputOffset + 2] = (short) ((v0 >>> 8) & 0b1111L);
            output[outputOffset + 3] = (short) ((v0 >>> 12) & 0b1111L);
            output[outputOffset + 4] = (short) ((v0 >>> 16) & 0b1111L);
            output[outputOffset + 5] = (short) ((v0 >>> 20) & 0b1111L);
            output[outputOffset + 6] = (short) ((v0 >>> 24) & 0b1111L);
            output[outputOffset + 7] = (short) ((v0 >>> 28) & 0b1111L);
            output[outputOffset + 8] = (short) ((v0 >>> 32) & 0b1111L);
            output[outputOffset + 9] = (short) ((v0 >>> 36) & 0b1111L);
            output[outputOffset + 10] = (short) ((v0 >>> 40) & 0b1111L);
            output[outputOffset + 11] = (short) ((v0 >>> 44) & 0b1111L);
            output[outputOffset + 12] = (short) ((v0 >>> 48) & 0b1111L);
            output[outputOffset + 13] = (short) ((v0 >>> 52) & 0b1111L);
            output[outputOffset + 14] = (short) ((v0 >>> 56) & 0b1111L);
            output[outputOffset + 15] = (short) ((v0 >>> 60) & 0b1111L);
            output[outputOffset + 16] = (short) (v1 & 0b1111L);
            output[outputOffset + 17] = (short) ((v1 >>> 4) & 0b1111L);
            output[outputOffset + 18] = (short) ((v1 >>> 8) & 0b1111L);
            output[outputOffset + 19] = (short) ((v1 >>> 12) & 0b1111L);
            output[outputOffset + 20] = (short) ((v1 >>> 16) & 0b1111L);
            output[outputOffset + 21] = (short) ((v1 >>> 20) & 0b1111L);
            output[outputOffset + 22] = (short) ((v1 >>> 24) & 0b1111L);
            output[outputOffset + 23] = (short) ((v1 >>> 28) & 0b1111L);
            output[outputOffset + 24] = (short) ((v1 >>> 32) & 0b1111L);
            output[outputOffset + 25] = (short) ((v1 >>> 36) & 0b1111L);
            output[outputOffset + 26] = (short) ((v1 >>> 40) & 0b1111L);
            output[outputOffset + 27] = (short) ((v1 >>> 44) & 0b1111L);
            output[outputOffset + 28] = (short) ((v1 >>> 48) & 0b1111L);
            output[outputOffset + 29] = (short) ((v1 >>> 52) & 0b1111L);
            output[outputOffset + 30] = (short) ((v1 >>> 56) & 0b1111L);
            output[outputOffset + 31] = (short) ((v1 >>> 60) & 0b1111L);
        }
    }

    private static final class Unpacker5
            implements ShortBitUnpacker
    {
        @Override
        public void unpack(short[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(short[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            int v2 = input.readInt();
            output[outputOffset] = (short) (v0 & 0b11111L);
            output[outputOffset + 1] = (short) ((v0 >>> 5) & 0b11111L);
            output[outputOffset + 2] = (short) ((v0 >>> 10) & 0b11111L);
            output[outputOffset + 3] = (short) ((v0 >>> 15) & 0b11111L);
            output[outputOffset + 4] = (short) ((v0 >>> 20) & 0b11111L);
            output[outputOffset + 5] = (short) ((v0 >>> 25) & 0b11111L);
            output[outputOffset + 6] = (short) ((v0 >>> 30) & 0b11111L);
            output[outputOffset + 7] = (short) ((v0 >>> 35) & 0b11111L);
            output[outputOffset + 8] = (short) ((v0 >>> 40) & 0b11111L);
            output[outputOffset + 9] = (short) ((v0 >>> 45) & 0b11111L);
            output[outputOffset + 10] = (short) ((v0 >>> 50) & 0b11111L);
            output[outputOffset + 11] = (short) ((v0 >>> 55) & 0b11111L);
            output[outputOffset + 12] = (short) (((v0 >>> 60) & 0b1111L) | ((v1 & 0b1L) << 4));
            output[outputOffset + 13] = (short) ((v1 >>> 1) & 0b11111L);
            output[outputOffset + 14] = (short) ((v1 >>> 6) & 0b11111L);
            output[outputOffset + 15] = (short) ((v1 >>> 11) & 0b11111L);
            output[outputOffset + 16] = (short) ((v1 >>> 16) & 0b11111L);
            output[outputOffset + 17] = (short) ((v1 >>> 21) & 0b11111L);
            output[outputOffset + 18] = (short) ((v1 >>> 26) & 0b11111L);
            output[outputOffset + 19] = (short) ((v1 >>> 31) & 0b11111L);
            output[outputOffset + 20] = (short) ((v1 >>> 36) & 0b11111L);
            output[outputOffset + 21] = (short) ((v1 >>> 41) & 0b11111L);
            output[outputOffset + 22] = (short) ((v1 >>> 46) & 0b11111L);
            output[outputOffset + 23] = (short) ((v1 >>> 51) & 0b11111L);
            output[outputOffset + 24] = (short) ((v1 >>> 56) & 0b11111L);
            output[outputOffset + 25] = (short) (((v1 >>> 61) & 0b111L) | ((v2 & 0b11L) << 3));
            output[outputOffset + 26] = (short) ((v2 >>> 2) & 0b11111L);
            output[outputOffset + 27] = (short) ((v2 >>> 7) & 0b11111L);
            output[outputOffset + 28] = (short) ((v2 >>> 12) & 0b11111L);
            output[outputOffset + 29] = (short) ((v2 >>> 17) & 0b11111L);
            output[outputOffset + 30] = (short) ((v2 >>> 22) & 0b11111L);
            output[outputOffset + 31] = (short) ((v2 >>> 27) & 0b11111L);
        }
    }

    private static final class Unpacker6
            implements ShortBitUnpacker
    {
        @Override
        public void unpack(short[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(short[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            output[outputOffset] = (short) (v0 & 0b111111L);
            output[outputOffset + 1] = (short) ((v0 >>> 6) & 0b111111L);
            output[outputOffset + 2] = (short) ((v0 >>> 12) & 0b111111L);
            output[outputOffset + 3] = (short) ((v0 >>> 18) & 0b111111L);
            output[outputOffset + 4] = (short) ((v0 >>> 24) & 0b111111L);
            output[outputOffset + 5] = (short) ((v0 >>> 30) & 0b111111L);
            output[outputOffset + 6] = (short) ((v0 >>> 36) & 0b111111L);
            output[outputOffset + 7] = (short) ((v0 >>> 42) & 0b111111L);
            output[outputOffset + 8] = (short) ((v0 >>> 48) & 0b111111L);
            output[outputOffset + 9] = (short) ((v0 >>> 54) & 0b111111L);
            output[outputOffset + 10] = (short) (((v0 >>> 60) & 0b1111L) | ((v1 & 0b11L) << 4));
            output[outputOffset + 11] = (short) ((v1 >>> 2) & 0b111111L);
            output[outputOffset + 12] = (short) ((v1 >>> 8) & 0b111111L);
            output[outputOffset + 13] = (short) ((v1 >>> 14) & 0b111111L);
            output[outputOffset + 14] = (short) ((v1 >>> 20) & 0b111111L);
            output[outputOffset + 15] = (short) ((v1 >>> 26) & 0b111111L);
            output[outputOffset + 16] = (short) ((v1 >>> 32) & 0b111111L);
            output[outputOffset + 17] = (short) ((v1 >>> 38) & 0b111111L);
            output[outputOffset + 18] = (short) ((v1 >>> 44) & 0b111111L);
            output[outputOffset + 19] = (short) ((v1 >>> 50) & 0b111111L);
            output[outputOffset + 20] = (short) ((v1 >>> 56) & 0b111111L);
            output[outputOffset + 21] = (short) (((v1 >>> 62) & 0b11L) | ((v2 & 0b1111L) << 2));
            output[outputOffset + 22] = (short) ((v2 >>> 4) & 0b111111L);
            output[outputOffset + 23] = (short) ((v2 >>> 10) & 0b111111L);
            output[outputOffset + 24] = (short) ((v2 >>> 16) & 0b111111L);
            output[outputOffset + 25] = (short) ((v2 >>> 22) & 0b111111L);
            output[outputOffset + 26] = (short) ((v2 >>> 28) & 0b111111L);
            output[outputOffset + 27] = (short) ((v2 >>> 34) & 0b111111L);
            output[outputOffset + 28] = (short) ((v2 >>> 40) & 0b111111L);
            output[outputOffset + 29] = (short) ((v2 >>> 46) & 0b111111L);
            output[outputOffset + 30] = (short) ((v2 >>> 52) & 0b111111L);
            output[outputOffset + 31] = (short) ((v2 >>> 58) & 0b111111L);
        }
    }

    private static final class Unpacker7
            implements ShortBitUnpacker
    {
        @Override
        public void unpack(short[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(short[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            int v3 = input.readInt();
            output[outputOffset] = (short) (v0 & 0b1111111L);
            output[outputOffset + 1] = (short) ((v0 >>> 7) & 0b1111111L);
            output[outputOffset + 2] = (short) ((v0 >>> 14) & 0b1111111L);
            output[outputOffset + 3] = (short) ((v0 >>> 21) & 0b1111111L);
            output[outputOffset + 4] = (short) ((v0 >>> 28) & 0b1111111L);
            output[outputOffset + 5] = (short) ((v0 >>> 35) & 0b1111111L);
            output[outputOffset + 6] = (short) ((v0 >>> 42) & 0b1111111L);
            output[outputOffset + 7] = (short) ((v0 >>> 49) & 0b1111111L);
            output[outputOffset + 8] = (short) ((v0 >>> 56) & 0b1111111L);
            output[outputOffset + 9] = (short) (((v0 >>> 63) & 0b1L) | ((v1 & 0b111111L) << 1));
            output[outputOffset + 10] = (short) ((v1 >>> 6) & 0b1111111L);
            output[outputOffset + 11] = (short) ((v1 >>> 13) & 0b1111111L);
            output[outputOffset + 12] = (short) ((v1 >>> 20) & 0b1111111L);
            output[outputOffset + 13] = (short) ((v1 >>> 27) & 0b1111111L);
            output[outputOffset + 14] = (short) ((v1 >>> 34) & 0b1111111L);
            output[outputOffset + 15] = (short) ((v1 >>> 41) & 0b1111111L);
            output[outputOffset + 16] = (short) ((v1 >>> 48) & 0b1111111L);
            output[outputOffset + 17] = (short) ((v1 >>> 55) & 0b1111111L);
            output[outputOffset + 18] = (short) (((v1 >>> 62) & 0b11L) | ((v2 & 0b11111L) << 2));
            output[outputOffset + 19] = (short) ((v2 >>> 5) & 0b1111111L);
            output[outputOffset + 20] = (short) ((v2 >>> 12) & 0b1111111L);
            output[outputOffset + 21] = (short) ((v2 >>> 19) & 0b1111111L);
            output[outputOffset + 22] = (short) ((v2 >>> 26) & 0b1111111L);
            output[outputOffset + 23] = (short) ((v2 >>> 33) & 0b1111111L);
            output[outputOffset + 24] = (short) ((v2 >>> 40) & 0b1111111L);
            output[outputOffset + 25] = (short) ((v2 >>> 47) & 0b1111111L);
            output[outputOffset + 26] = (short) ((v2 >>> 54) & 0b1111111L);
            output[outputOffset + 27] = (short) (((v2 >>> 61) & 0b111L) | ((v3 & 0b1111L) << 3));
            output[outputOffset + 28] = (short) ((v3 >>> 4) & 0b1111111L);
            output[outputOffset + 29] = (short) ((v3 >>> 11) & 0b1111111L);
            output[outputOffset + 30] = (short) ((v3 >>> 18) & 0b1111111L);
            output[outputOffset + 31] = (short) ((v3 >>> 25) & 0b1111111L);
        }
    }

    private static final class Unpacker8
            implements ShortBitUnpacker
    {
        @Override
        public void unpack(short[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(short[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            output[outputOffset] = (short) (v0 & 0b11111111L);
            output[outputOffset + 1] = (short) ((v0 >>> 8) & 0b11111111L);
            output[outputOffset + 2] = (short) ((v0 >>> 16) & 0b11111111L);
            output[outputOffset + 3] = (short) ((v0 >>> 24) & 0b11111111L);
            output[outputOffset + 4] = (short) ((v0 >>> 32) & 0b11111111L);
            output[outputOffset + 5] = (short) ((v0 >>> 40) & 0b11111111L);
            output[outputOffset + 6] = (short) ((v0 >>> 48) & 0b11111111L);
            output[outputOffset + 7] = (short) ((v0 >>> 56) & 0b11111111L);
            output[outputOffset + 8] = (short) (v1 & 0b11111111L);
            output[outputOffset + 9] = (short) ((v1 >>> 8) & 0b11111111L);
            output[outputOffset + 10] = (short) ((v1 >>> 16) & 0b11111111L);
            output[outputOffset + 11] = (short) ((v1 >>> 24) & 0b11111111L);
            output[outputOffset + 12] = (short) ((v1 >>> 32) & 0b11111111L);
            output[outputOffset + 13] = (short) ((v1 >>> 40) & 0b11111111L);
            output[outputOffset + 14] = (short) ((v1 >>> 48) & 0b11111111L);
            output[outputOffset + 15] = (short) ((v1 >>> 56) & 0b11111111L);
            output[outputOffset + 16] = (short) (v2 & 0b11111111L);
            output[outputOffset + 17] = (short) ((v2 >>> 8) & 0b11111111L);
            output[outputOffset + 18] = (short) ((v2 >>> 16) & 0b11111111L);
            output[outputOffset + 19] = (short) ((v2 >>> 24) & 0b11111111L);
            output[outputOffset + 20] = (short) ((v2 >>> 32) & 0b11111111L);
            output[outputOffset + 21] = (short) ((v2 >>> 40) & 0b11111111L);
            output[outputOffset + 22] = (short) ((v2 >>> 48) & 0b11111111L);
            output[outputOffset + 23] = (short) ((v2 >>> 56) & 0b11111111L);
            output[outputOffset + 24] = (short) (v3 & 0b11111111L);
            output[outputOffset + 25] = (short) ((v3 >>> 8) & 0b11111111L);
            output[outputOffset + 26] = (short) ((v3 >>> 16) & 0b11111111L);
            output[outputOffset + 27] = (short) ((v3 >>> 24) & 0b11111111L);
            output[outputOffset + 28] = (short) ((v3 >>> 32) & 0b11111111L);
            output[outputOffset + 29] = (short) ((v3 >>> 40) & 0b11111111L);
            output[outputOffset + 30] = (short) ((v3 >>> 48) & 0b11111111L);
            output[outputOffset + 31] = (short) ((v3 >>> 56) & 0b11111111L);
        }
    }

    private static final class Unpacker9
            implements ShortBitUnpacker
    {
        @Override
        public void unpack(short[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(short[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            int v4 = input.readInt();
            output[outputOffset] = (short) (v0 & 0b111111111L);
            output[outputOffset + 1] = (short) ((v0 >>> 9) & 0b111111111L);
            output[outputOffset + 2] = (short) ((v0 >>> 18) & 0b111111111L);
            output[outputOffset + 3] = (short) ((v0 >>> 27) & 0b111111111L);
            output[outputOffset + 4] = (short) ((v0 >>> 36) & 0b111111111L);
            output[outputOffset + 5] = (short) ((v0 >>> 45) & 0b111111111L);
            output[outputOffset + 6] = (short) ((v0 >>> 54) & 0b111111111L);
            output[outputOffset + 7] = (short) (((v0 >>> 63) & 0b1L) | ((v1 & 0b11111111L) << 1));
            output[outputOffset + 8] = (short) ((v1 >>> 8) & 0b111111111L);
            output[outputOffset + 9] = (short) ((v1 >>> 17) & 0b111111111L);
            output[outputOffset + 10] = (short) ((v1 >>> 26) & 0b111111111L);
            output[outputOffset + 11] = (short) ((v1 >>> 35) & 0b111111111L);
            output[outputOffset + 12] = (short) ((v1 >>> 44) & 0b111111111L);
            output[outputOffset + 13] = (short) ((v1 >>> 53) & 0b111111111L);
            output[outputOffset + 14] = (short) (((v1 >>> 62) & 0b11L) | ((v2 & 0b1111111L) << 2));
            output[outputOffset + 15] = (short) ((v2 >>> 7) & 0b111111111L);
            output[outputOffset + 16] = (short) ((v2 >>> 16) & 0b111111111L);
            output[outputOffset + 17] = (short) ((v2 >>> 25) & 0b111111111L);
            output[outputOffset + 18] = (short) ((v2 >>> 34) & 0b111111111L);
            output[outputOffset + 19] = (short) ((v2 >>> 43) & 0b111111111L);
            output[outputOffset + 20] = (short) ((v2 >>> 52) & 0b111111111L);
            output[outputOffset + 21] = (short) (((v2 >>> 61) & 0b111L) | ((v3 & 0b111111L) << 3));
            output[outputOffset + 22] = (short) ((v3 >>> 6) & 0b111111111L);
            output[outputOffset + 23] = (short) ((v3 >>> 15) & 0b111111111L);
            output[outputOffset + 24] = (short) ((v3 >>> 24) & 0b111111111L);
            output[outputOffset + 25] = (short) ((v3 >>> 33) & 0b111111111L);
            output[outputOffset + 26] = (short) ((v3 >>> 42) & 0b111111111L);
            output[outputOffset + 27] = (short) ((v3 >>> 51) & 0b111111111L);
            output[outputOffset + 28] = (short) (((v3 >>> 60) & 0b1111L) | ((v4 & 0b11111L) << 4));
            output[outputOffset + 29] = (short) ((v4 >>> 5) & 0b111111111L);
            output[outputOffset + 30] = (short) ((v4 >>> 14) & 0b111111111L);
            output[outputOffset + 31] = (short) ((v4 >>> 23) & 0b111111111L);
        }
    }

    private static final class Unpacker10
            implements ShortBitUnpacker
    {
        @Override
        public void unpack(short[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(short[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            output[outputOffset] = (short) (v0 & 0b1111111111L);
            output[outputOffset + 1] = (short) ((v0 >>> 10) & 0b1111111111L);
            output[outputOffset + 2] = (short) ((v0 >>> 20) & 0b1111111111L);
            output[outputOffset + 3] = (short) ((v0 >>> 30) & 0b1111111111L);
            output[outputOffset + 4] = (short) ((v0 >>> 40) & 0b1111111111L);
            output[outputOffset + 5] = (short) ((v0 >>> 50) & 0b1111111111L);
            output[outputOffset + 6] = (short) (((v0 >>> 60) & 0b1111L) | ((v1 & 0b111111L) << 4));
            output[outputOffset + 7] = (short) ((v1 >>> 6) & 0b1111111111L);
            output[outputOffset + 8] = (short) ((v1 >>> 16) & 0b1111111111L);
            output[outputOffset + 9] = (short) ((v1 >>> 26) & 0b1111111111L);
            output[outputOffset + 10] = (short) ((v1 >>> 36) & 0b1111111111L);
            output[outputOffset + 11] = (short) ((v1 >>> 46) & 0b1111111111L);
            output[outputOffset + 12] = (short) (((v1 >>> 56) & 0b11111111L) | ((v2 & 0b11L) << 8));
            output[outputOffset + 13] = (short) ((v2 >>> 2) & 0b1111111111L);
            output[outputOffset + 14] = (short) ((v2 >>> 12) & 0b1111111111L);
            output[outputOffset + 15] = (short) ((v2 >>> 22) & 0b1111111111L);
            output[outputOffset + 16] = (short) ((v2 >>> 32) & 0b1111111111L);
            output[outputOffset + 17] = (short) ((v2 >>> 42) & 0b1111111111L);
            output[outputOffset + 18] = (short) ((v2 >>> 52) & 0b1111111111L);
            output[outputOffset + 19] = (short) (((v2 >>> 62) & 0b11L) | ((v3 & 0b11111111L) << 2));
            output[outputOffset + 20] = (short) ((v3 >>> 8) & 0b1111111111L);
            output[outputOffset + 21] = (short) ((v3 >>> 18) & 0b1111111111L);
            output[outputOffset + 22] = (short) ((v3 >>> 28) & 0b1111111111L);
            output[outputOffset + 23] = (short) ((v3 >>> 38) & 0b1111111111L);
            output[outputOffset + 24] = (short) ((v3 >>> 48) & 0b1111111111L);
            output[outputOffset + 25] = (short) (((v3 >>> 58) & 0b111111L) | ((v4 & 0b1111L) << 6));
            output[outputOffset + 26] = (short) ((v4 >>> 4) & 0b1111111111L);
            output[outputOffset + 27] = (short) ((v4 >>> 14) & 0b1111111111L);
            output[outputOffset + 28] = (short) ((v4 >>> 24) & 0b1111111111L);
            output[outputOffset + 29] = (short) ((v4 >>> 34) & 0b1111111111L);
            output[outputOffset + 30] = (short) ((v4 >>> 44) & 0b1111111111L);
            output[outputOffset + 31] = (short) ((v4 >>> 54) & 0b1111111111L);
        }
    }

    private static final class Unpacker11
            implements ShortBitUnpacker
    {
        @Override
        public void unpack(short[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(short[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            int v5 = input.readInt();
            output[outputOffset] = (short) (v0 & 0b11111111111L);
            output[outputOffset + 1] = (short) ((v0 >>> 11) & 0b11111111111L);
            output[outputOffset + 2] = (short) ((v0 >>> 22) & 0b11111111111L);
            output[outputOffset + 3] = (short) ((v0 >>> 33) & 0b11111111111L);
            output[outputOffset + 4] = (short) ((v0 >>> 44) & 0b11111111111L);
            output[outputOffset + 5] = (short) (((v0 >>> 55) & 0b111111111L) | ((v1 & 0b11L) << 9));
            output[outputOffset + 6] = (short) ((v1 >>> 2) & 0b11111111111L);
            output[outputOffset + 7] = (short) ((v1 >>> 13) & 0b11111111111L);
            output[outputOffset + 8] = (short) ((v1 >>> 24) & 0b11111111111L);
            output[outputOffset + 9] = (short) ((v1 >>> 35) & 0b11111111111L);
            output[outputOffset + 10] = (short) ((v1 >>> 46) & 0b11111111111L);
            output[outputOffset + 11] = (short) (((v1 >>> 57) & 0b1111111L) | ((v2 & 0b1111L) << 7));
            output[outputOffset + 12] = (short) ((v2 >>> 4) & 0b11111111111L);
            output[outputOffset + 13] = (short) ((v2 >>> 15) & 0b11111111111L);
            output[outputOffset + 14] = (short) ((v2 >>> 26) & 0b11111111111L);
            output[outputOffset + 15] = (short) ((v2 >>> 37) & 0b11111111111L);
            output[outputOffset + 16] = (short) ((v2 >>> 48) & 0b11111111111L);
            output[outputOffset + 17] = (short) (((v2 >>> 59) & 0b11111L) | ((v3 & 0b111111L) << 5));
            output[outputOffset + 18] = (short) ((v3 >>> 6) & 0b11111111111L);
            output[outputOffset + 19] = (short) ((v3 >>> 17) & 0b11111111111L);
            output[outputOffset + 20] = (short) ((v3 >>> 28) & 0b11111111111L);
            output[outputOffset + 21] = (short) ((v3 >>> 39) & 0b11111111111L);
            output[outputOffset + 22] = (short) ((v3 >>> 50) & 0b11111111111L);
            output[outputOffset + 23] = (short) (((v3 >>> 61) & 0b111L) | ((v4 & 0b11111111L) << 3));
            output[outputOffset + 24] = (short) ((v4 >>> 8) & 0b11111111111L);
            output[outputOffset + 25] = (short) ((v4 >>> 19) & 0b11111111111L);
            output[outputOffset + 26] = (short) ((v4 >>> 30) & 0b11111111111L);
            output[outputOffset + 27] = (short) ((v4 >>> 41) & 0b11111111111L);
            output[outputOffset + 28] = (short) ((v4 >>> 52) & 0b11111111111L);
            output[outputOffset + 29] = (short) (((v4 >>> 63) & 0b1L) | ((v5 & 0b1111111111L) << 1));
            output[outputOffset + 30] = (short) ((v5 >>> 10) & 0b11111111111L);
            output[outputOffset + 31] = (short) ((v5 >>> 21) & 0b11111111111L);
        }
    }

    private static final class Unpacker12
            implements ShortBitUnpacker
    {
        @Override
        public void unpack(short[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(short[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            output[outputOffset] = (short) (v0 & 0b111111111111L);
            output[outputOffset + 1] = (short) ((v0 >>> 12) & 0b111111111111L);
            output[outputOffset + 2] = (short) ((v0 >>> 24) & 0b111111111111L);
            output[outputOffset + 3] = (short) ((v0 >>> 36) & 0b111111111111L);
            output[outputOffset + 4] = (short) ((v0 >>> 48) & 0b111111111111L);
            output[outputOffset + 5] = (short) (((v0 >>> 60) & 0b1111L) | ((v1 & 0b11111111L) << 4));
            output[outputOffset + 6] = (short) ((v1 >>> 8) & 0b111111111111L);
            output[outputOffset + 7] = (short) ((v1 >>> 20) & 0b111111111111L);
            output[outputOffset + 8] = (short) ((v1 >>> 32) & 0b111111111111L);
            output[outputOffset + 9] = (short) ((v1 >>> 44) & 0b111111111111L);
            output[outputOffset + 10] = (short) (((v1 >>> 56) & 0b11111111L) | ((v2 & 0b1111L) << 8));
            output[outputOffset + 11] = (short) ((v2 >>> 4) & 0b111111111111L);
            output[outputOffset + 12] = (short) ((v2 >>> 16) & 0b111111111111L);
            output[outputOffset + 13] = (short) ((v2 >>> 28) & 0b111111111111L);
            output[outputOffset + 14] = (short) ((v2 >>> 40) & 0b111111111111L);
            output[outputOffset + 15] = (short) ((v2 >>> 52) & 0b111111111111L);
            output[outputOffset + 16] = (short) (v3 & 0b111111111111L);
            output[outputOffset + 17] = (short) ((v3 >>> 12) & 0b111111111111L);
            output[outputOffset + 18] = (short) ((v3 >>> 24) & 0b111111111111L);
            output[outputOffset + 19] = (short) ((v3 >>> 36) & 0b111111111111L);
            output[outputOffset + 20] = (short) ((v3 >>> 48) & 0b111111111111L);
            output[outputOffset + 21] = (short) (((v3 >>> 60) & 0b1111L) | ((v4 & 0b11111111L) << 4));
            output[outputOffset + 22] = (short) ((v4 >>> 8) & 0b111111111111L);
            output[outputOffset + 23] = (short) ((v4 >>> 20) & 0b111111111111L);
            output[outputOffset + 24] = (short) ((v4 >>> 32) & 0b111111111111L);
            output[outputOffset + 25] = (short) ((v4 >>> 44) & 0b111111111111L);
            output[outputOffset + 26] = (short) (((v4 >>> 56) & 0b11111111L) | ((v5 & 0b1111L) << 8));
            output[outputOffset + 27] = (short) ((v5 >>> 4) & 0b111111111111L);
            output[outputOffset + 28] = (short) ((v5 >>> 16) & 0b111111111111L);
            output[outputOffset + 29] = (short) ((v5 >>> 28) & 0b111111111111L);
            output[outputOffset + 30] = (short) ((v5 >>> 40) & 0b111111111111L);
            output[outputOffset + 31] = (short) ((v5 >>> 52) & 0b111111111111L);
        }
    }

    private static final class Unpacker13
            implements ShortBitUnpacker
    {
        @Override
        public void unpack(short[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(short[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            int v6 = input.readInt();
            output[outputOffset] = (short) (v0 & 0b1111111111111L);
            output[outputOffset + 1] = (short) ((v0 >>> 13) & 0b1111111111111L);
            output[outputOffset + 2] = (short) ((v0 >>> 26) & 0b1111111111111L);
            output[outputOffset + 3] = (short) ((v0 >>> 39) & 0b1111111111111L);
            output[outputOffset + 4] = (short) (((v0 >>> 52) & 0b111111111111L) | ((v1 & 0b1L) << 12));
            output[outputOffset + 5] = (short) ((v1 >>> 1) & 0b1111111111111L);
            output[outputOffset + 6] = (short) ((v1 >>> 14) & 0b1111111111111L);
            output[outputOffset + 7] = (short) ((v1 >>> 27) & 0b1111111111111L);
            output[outputOffset + 8] = (short) ((v1 >>> 40) & 0b1111111111111L);
            output[outputOffset + 9] = (short) (((v1 >>> 53) & 0b11111111111L) | ((v2 & 0b11L) << 11));
            output[outputOffset + 10] = (short) ((v2 >>> 2) & 0b1111111111111L);
            output[outputOffset + 11] = (short) ((v2 >>> 15) & 0b1111111111111L);
            output[outputOffset + 12] = (short) ((v2 >>> 28) & 0b1111111111111L);
            output[outputOffset + 13] = (short) ((v2 >>> 41) & 0b1111111111111L);
            output[outputOffset + 14] = (short) (((v2 >>> 54) & 0b1111111111L) | ((v3 & 0b111L) << 10));
            output[outputOffset + 15] = (short) ((v3 >>> 3) & 0b1111111111111L);
            output[outputOffset + 16] = (short) ((v3 >>> 16) & 0b1111111111111L);
            output[outputOffset + 17] = (short) ((v3 >>> 29) & 0b1111111111111L);
            output[outputOffset + 18] = (short) ((v3 >>> 42) & 0b1111111111111L);
            output[outputOffset + 19] = (short) (((v3 >>> 55) & 0b111111111L) | ((v4 & 0b1111L) << 9));
            output[outputOffset + 20] = (short) ((v4 >>> 4) & 0b1111111111111L);
            output[outputOffset + 21] = (short) ((v4 >>> 17) & 0b1111111111111L);
            output[outputOffset + 22] = (short) ((v4 >>> 30) & 0b1111111111111L);
            output[outputOffset + 23] = (short) ((v4 >>> 43) & 0b1111111111111L);
            output[outputOffset + 24] = (short) (((v4 >>> 56) & 0b11111111L) | ((v5 & 0b11111L) << 8));
            output[outputOffset + 25] = (short) ((v5 >>> 5) & 0b1111111111111L);
            output[outputOffset + 26] = (short) ((v5 >>> 18) & 0b1111111111111L);
            output[outputOffset + 27] = (short) ((v5 >>> 31) & 0b1111111111111L);
            output[outputOffset + 28] = (short) ((v5 >>> 44) & 0b1111111111111L);
            output[outputOffset + 29] = (short) (((v5 >>> 57) & 0b1111111L) | ((v6 & 0b111111L) << 7));
            output[outputOffset + 30] = (short) ((v6 >>> 6) & 0b1111111111111L);
            output[outputOffset + 31] = (short) ((v6 >>> 19) & 0b1111111111111L);
        }
    }

    private static final class Unpacker14
            implements ShortBitUnpacker
    {
        @Override
        public void unpack(short[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(short[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            output[outputOffset] = (short) (v0 & 0b11111111111111L);
            output[outputOffset + 1] = (short) ((v0 >>> 14) & 0b11111111111111L);
            output[outputOffset + 2] = (short) ((v0 >>> 28) & 0b11111111111111L);
            output[outputOffset + 3] = (short) ((v0 >>> 42) & 0b11111111111111L);
            output[outputOffset + 4] = (short) (((v0 >>> 56) & 0b11111111L) | ((v1 & 0b111111L) << 8));
            output[outputOffset + 5] = (short) ((v1 >>> 6) & 0b11111111111111L);
            output[outputOffset + 6] = (short) ((v1 >>> 20) & 0b11111111111111L);
            output[outputOffset + 7] = (short) ((v1 >>> 34) & 0b11111111111111L);
            output[outputOffset + 8] = (short) ((v1 >>> 48) & 0b11111111111111L);
            output[outputOffset + 9] = (short) (((v1 >>> 62) & 0b11L) | ((v2 & 0b111111111111L) << 2));
            output[outputOffset + 10] = (short) ((v2 >>> 12) & 0b11111111111111L);
            output[outputOffset + 11] = (short) ((v2 >>> 26) & 0b11111111111111L);
            output[outputOffset + 12] = (short) ((v2 >>> 40) & 0b11111111111111L);
            output[outputOffset + 13] = (short) (((v2 >>> 54) & 0b1111111111L) | ((v3 & 0b1111L) << 10));
            output[outputOffset + 14] = (short) ((v3 >>> 4) & 0b11111111111111L);
            output[outputOffset + 15] = (short) ((v3 >>> 18) & 0b11111111111111L);
            output[outputOffset + 16] = (short) ((v3 >>> 32) & 0b11111111111111L);
            output[outputOffset + 17] = (short) ((v3 >>> 46) & 0b11111111111111L);
            output[outputOffset + 18] = (short) (((v3 >>> 60) & 0b1111L) | ((v4 & 0b1111111111L) << 4));
            output[outputOffset + 19] = (short) ((v4 >>> 10) & 0b11111111111111L);
            output[outputOffset + 20] = (short) ((v4 >>> 24) & 0b11111111111111L);
            output[outputOffset + 21] = (short) ((v4 >>> 38) & 0b11111111111111L);
            output[outputOffset + 22] = (short) (((v4 >>> 52) & 0b111111111111L) | ((v5 & 0b11L) << 12));
            output[outputOffset + 23] = (short) ((v5 >>> 2) & 0b11111111111111L);
            output[outputOffset + 24] = (short) ((v5 >>> 16) & 0b11111111111111L);
            output[outputOffset + 25] = (short) ((v5 >>> 30) & 0b11111111111111L);
            output[outputOffset + 26] = (short) ((v5 >>> 44) & 0b11111111111111L);
            output[outputOffset + 27] = (short) (((v5 >>> 58) & 0b111111L) | ((v6 & 0b11111111L) << 6));
            output[outputOffset + 28] = (short) ((v6 >>> 8) & 0b11111111111111L);
            output[outputOffset + 29] = (short) ((v6 >>> 22) & 0b11111111111111L);
            output[outputOffset + 30] = (short) ((v6 >>> 36) & 0b11111111111111L);
            output[outputOffset + 31] = (short) ((v6 >>> 50) & 0b11111111111111L);
        }
    }

    private static final class Unpacker15
            implements ShortBitUnpacker
    {
        @Override
        public void unpack(short[] output, int outputOffset, SimpleSliceInputStream input,
                int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(short[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            int v7 = input.readInt();
            output[outputOffset] = (short) (v0 & 0b111111111111111L);
            output[outputOffset + 1] = (short) ((v0 >>> 15) & 0b111111111111111L);
            output[outputOffset + 2] = (short) ((v0 >>> 30) & 0b111111111111111L);
            output[outputOffset + 3] = (short) ((v0 >>> 45) & 0b111111111111111L);
            output[outputOffset + 4] = (short) (((v0 >>> 60) & 0b1111L) | ((v1 & 0b11111111111L) << 4));
            output[outputOffset + 5] = (short) ((v1 >>> 11) & 0b111111111111111L);
            output[outputOffset + 6] = (short) ((v1 >>> 26) & 0b111111111111111L);
            output[outputOffset + 7] = (short) ((v1 >>> 41) & 0b111111111111111L);
            output[outputOffset + 8] = (short) (((v1 >>> 56) & 0b11111111L) | ((v2 & 0b1111111L) << 8));
            output[outputOffset + 9] = (short) ((v2 >>> 7) & 0b111111111111111L);
            output[outputOffset + 10] = (short) ((v2 >>> 22) & 0b111111111111111L);
            output[outputOffset + 11] = (short) ((v2 >>> 37) & 0b111111111111111L);
            output[outputOffset + 12] = (short) (((v2 >>> 52) & 0b111111111111L) | ((v3 & 0b111L) << 12));
            output[outputOffset + 13] = (short) ((v3 >>> 3) & 0b111111111111111L);
            output[outputOffset + 14] = (short) ((v3 >>> 18) & 0b111111111111111L);
            output[outputOffset + 15] = (short) ((v3 >>> 33) & 0b111111111111111L);
            output[outputOffset + 16] = (short) ((v3 >>> 48) & 0b111111111111111L);
            output[outputOffset + 17] = (short) (((v3 >>> 63) & 0b1L) | ((v4 & 0b11111111111111L) << 1));
            output[outputOffset + 18] = (short) ((v4 >>> 14) & 0b111111111111111L);
            output[outputOffset + 19] = (short) ((v4 >>> 29) & 0b111111111111111L);
            output[outputOffset + 20] = (short) ((v4 >>> 44) & 0b111111111111111L);
            output[outputOffset + 21] = (short) (((v4 >>> 59) & 0b11111L) | ((v5 & 0b1111111111L) << 5));
            output[outputOffset + 22] = (short) ((v5 >>> 10) & 0b111111111111111L);
            output[outputOffset + 23] = (short) ((v5 >>> 25) & 0b111111111111111L);
            output[outputOffset + 24] = (short) ((v5 >>> 40) & 0b111111111111111L);
            output[outputOffset + 25] = (short) (((v5 >>> 55) & 0b111111111L) | ((v6 & 0b111111L) << 9));
            output[outputOffset + 26] = (short) ((v6 >>> 6) & 0b111111111111111L);
            output[outputOffset + 27] = (short) ((v6 >>> 21) & 0b111111111111111L);
            output[outputOffset + 28] = (short) ((v6 >>> 36) & 0b111111111111111L);
            output[outputOffset + 29] = (short) (((v6 >>> 51) & 0b1111111111111L) | ((v7 & 0b11L) << 13));
            output[outputOffset + 30] = (short) ((v7 >>> 2) & 0b111111111111111L);
            output[outputOffset + 31] = (short) ((v7 >>> 17) & 0b111111111111111L);
        }
    }

    private static final class Unpacker16
            implements ShortBitUnpacker
    {
        @Override
        public void unpack(short[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            input.readBytes(Slices.wrappedShortArray(output, outputOffset, length), 0, length * Short.BYTES);
        }
    }

    private static final class Unpacker17
            implements ShortBitUnpacker
    {
        @Override
        public void unpack(short[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(short[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            int v8 = input.readInt();
            output[outputOffset] = (short) (v0 & 0b11111111111111111L);
            output[outputOffset + 1] = (short) ((v0 >>> 17) & 0b11111111111111111L);
            output[outputOffset + 2] = (short) ((v0 >>> 34) & 0b11111111111111111L);
            output[outputOffset + 3] = (short) (((v0 >>> 51) & 0b1111111111111L) | ((v1 & 0b1111L) << 13));
            output[outputOffset + 4] = (short) ((v1 >>> 4) & 0b11111111111111111L);
            output[outputOffset + 5] = (short) ((v1 >>> 21) & 0b11111111111111111L);
            output[outputOffset + 6] = (short) ((v1 >>> 38) & 0b11111111111111111L);
            output[outputOffset + 7] = (short) (((v1 >>> 55) & 0b111111111L) | ((v2 & 0b11111111L) << 9));
            output[outputOffset + 8] = (short) ((v2 >>> 8) & 0b11111111111111111L);
            output[outputOffset + 9] = (short) ((v2 >>> 25) & 0b11111111111111111L);
            output[outputOffset + 10] = (short) ((v2 >>> 42) & 0b11111111111111111L);
            output[outputOffset + 11] = (short) (((v2 >>> 59) & 0b11111L) | ((v3 & 0b111111111111L) << 5));
            output[outputOffset + 12] = (short) ((v3 >>> 12) & 0b11111111111111111L);
            output[outputOffset + 13] = (short) ((v3 >>> 29) & 0b11111111111111111L);
            output[outputOffset + 14] = (short) ((v3 >>> 46) & 0b11111111111111111L);
            output[outputOffset + 15] = (short) (((v3 >>> 63) & 0b1L) | ((v4 & 0b1111111111111111L) << 1));
            output[outputOffset + 16] = (short) ((v4 >>> 16) & 0b11111111111111111L);
            output[outputOffset + 17] = (short) ((v4 >>> 33) & 0b11111111111111111L);
            output[outputOffset + 18] = (short) (((v4 >>> 50) & 0b11111111111111L) | ((v5 & 0b111L) << 14));
            output[outputOffset + 19] = (short) ((v5 >>> 3) & 0b11111111111111111L);
            output[outputOffset + 20] = (short) ((v5 >>> 20) & 0b11111111111111111L);
            output[outputOffset + 21] = (short) ((v5 >>> 37) & 0b11111111111111111L);
            output[outputOffset + 22] = (short) (((v5 >>> 54) & 0b1111111111L) | ((v6 & 0b1111111L) << 10));
            output[outputOffset + 23] = (short) ((v6 >>> 7) & 0b11111111111111111L);
            output[outputOffset + 24] = (short) ((v6 >>> 24) & 0b11111111111111111L);
            output[outputOffset + 25] = (short) ((v6 >>> 41) & 0b11111111111111111L);
            output[outputOffset + 26] = (short) (((v6 >>> 58) & 0b111111L) | ((v7 & 0b11111111111L) << 6));
            output[outputOffset + 27] = (short) ((v7 >>> 11) & 0b11111111111111111L);
            output[outputOffset + 28] = (short) ((v7 >>> 28) & 0b11111111111111111L);
            output[outputOffset + 29] = (short) ((v7 >>> 45) & 0b11111111111111111L);
            output[outputOffset + 30] = (short) (((v7 >>> 62) & 0b11L) | ((v8 & 0b111111111111111L) << 2));
            output[outputOffset + 31] = (short) ((v8 >>> 15) & 0b11111111111111111L);
        }
    }
}
