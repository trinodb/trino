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

public final class LongBitUnpackers
{
    private static final LongBitUnpacker[] UNPACKERS = {
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
            new Unpacker17(),
            new Unpacker18(),
            new Unpacker19(),
            new Unpacker20(),
            new Unpacker21(),
            new Unpacker22(),
            new Unpacker23(),
            new Unpacker24(),
            new Unpacker25(),
            new Unpacker26(),
            new Unpacker27(),
            new Unpacker28(),
            new Unpacker29(),
            new Unpacker30(),
            new Unpacker31(),
            new Unpacker32(),
            new Unpacker33(),
            new Unpacker34(),
            new Unpacker35(),
            new Unpacker36(),
            new Unpacker37(),
            new Unpacker38(),
            new Unpacker39(),
            new Unpacker40(),
            new Unpacker41(),
            new Unpacker42(),
            new Unpacker43(),
            new Unpacker44(),
            new Unpacker45(),
            new Unpacker46(),
            new Unpacker47(),
            new Unpacker48(),
            new Unpacker49(),
            new Unpacker50(),
            new Unpacker51(),
            new Unpacker52(),
            new Unpacker53(),
            new Unpacker54(),
            new Unpacker55(),
            new Unpacker56(),
            new Unpacker57(),
            new Unpacker58(),
            new Unpacker59(),
            new Unpacker60(),
            new Unpacker61(),
            new Unpacker62(),
            new Unpacker63(),
            new Unpacker64()};

    public static LongBitUnpacker getLongBitUnpacker(int bitWidth)
    {
        checkArgument(bitWidth > 0 && bitWidth <= Long.SIZE, "bitWidth %s should be in the range 1-64", bitWidth);
        return UNPACKERS[bitWidth - 1];
    }

    private LongBitUnpackers()
    {
    }

    private static final class Unpacker1
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            int v0 = input.readInt();
            output[outputOffset] = v0 & 0b1L;
            output[outputOffset + 1] = (v0 >>> 1) & 0b1L;
            output[outputOffset + 2] = (v0 >>> 2) & 0b1L;
            output[outputOffset + 3] = (v0 >>> 3) & 0b1L;
            output[outputOffset + 4] = (v0 >>> 4) & 0b1L;
            output[outputOffset + 5] = (v0 >>> 5) & 0b1L;
            output[outputOffset + 6] = (v0 >>> 6) & 0b1L;
            output[outputOffset + 7] = (v0 >>> 7) & 0b1L;
            output[outputOffset + 8] = (v0 >>> 8) & 0b1L;
            output[outputOffset + 9] = (v0 >>> 9) & 0b1L;
            output[outputOffset + 10] = (v0 >>> 10) & 0b1L;
            output[outputOffset + 11] = (v0 >>> 11) & 0b1L;
            output[outputOffset + 12] = (v0 >>> 12) & 0b1L;
            output[outputOffset + 13] = (v0 >>> 13) & 0b1L;
            output[outputOffset + 14] = (v0 >>> 14) & 0b1L;
            output[outputOffset + 15] = (v0 >>> 15) & 0b1L;
            output[outputOffset + 16] = (v0 >>> 16) & 0b1L;
            output[outputOffset + 17] = (v0 >>> 17) & 0b1L;
            output[outputOffset + 18] = (v0 >>> 18) & 0b1L;
            output[outputOffset + 19] = (v0 >>> 19) & 0b1L;
            output[outputOffset + 20] = (v0 >>> 20) & 0b1L;
            output[outputOffset + 21] = (v0 >>> 21) & 0b1L;
            output[outputOffset + 22] = (v0 >>> 22) & 0b1L;
            output[outputOffset + 23] = (v0 >>> 23) & 0b1L;
            output[outputOffset + 24] = (v0 >>> 24) & 0b1L;
            output[outputOffset + 25] = (v0 >>> 25) & 0b1L;
            output[outputOffset + 26] = (v0 >>> 26) & 0b1L;
            output[outputOffset + 27] = (v0 >>> 27) & 0b1L;
            output[outputOffset + 28] = (v0 >>> 28) & 0b1L;
            output[outputOffset + 29] = (v0 >>> 29) & 0b1L;
            output[outputOffset + 30] = (v0 >>> 30) & 0b1L;
            output[outputOffset + 31] = (v0 >>> 31) & 0b1L;
        }
    }

    private static final class Unpacker2
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            output[outputOffset] = v0 & 0b11L;
            output[outputOffset + 1] = (v0 >>> 2) & 0b11L;
            output[outputOffset + 2] = (v0 >>> 4) & 0b11L;
            output[outputOffset + 3] = (v0 >>> 6) & 0b11L;
            output[outputOffset + 4] = (v0 >>> 8) & 0b11L;
            output[outputOffset + 5] = (v0 >>> 10) & 0b11L;
            output[outputOffset + 6] = (v0 >>> 12) & 0b11L;
            output[outputOffset + 7] = (v0 >>> 14) & 0b11L;
            output[outputOffset + 8] = (v0 >>> 16) & 0b11L;
            output[outputOffset + 9] = (v0 >>> 18) & 0b11L;
            output[outputOffset + 10] = (v0 >>> 20) & 0b11L;
            output[outputOffset + 11] = (v0 >>> 22) & 0b11L;
            output[outputOffset + 12] = (v0 >>> 24) & 0b11L;
            output[outputOffset + 13] = (v0 >>> 26) & 0b11L;
            output[outputOffset + 14] = (v0 >>> 28) & 0b11L;
            output[outputOffset + 15] = (v0 >>> 30) & 0b11L;
            output[outputOffset + 16] = (v0 >>> 32) & 0b11L;
            output[outputOffset + 17] = (v0 >>> 34) & 0b11L;
            output[outputOffset + 18] = (v0 >>> 36) & 0b11L;
            output[outputOffset + 19] = (v0 >>> 38) & 0b11L;
            output[outputOffset + 20] = (v0 >>> 40) & 0b11L;
            output[outputOffset + 21] = (v0 >>> 42) & 0b11L;
            output[outputOffset + 22] = (v0 >>> 44) & 0b11L;
            output[outputOffset + 23] = (v0 >>> 46) & 0b11L;
            output[outputOffset + 24] = (v0 >>> 48) & 0b11L;
            output[outputOffset + 25] = (v0 >>> 50) & 0b11L;
            output[outputOffset + 26] = (v0 >>> 52) & 0b11L;
            output[outputOffset + 27] = (v0 >>> 54) & 0b11L;
            output[outputOffset + 28] = (v0 >>> 56) & 0b11L;
            output[outputOffset + 29] = (v0 >>> 58) & 0b11L;
            output[outputOffset + 30] = (v0 >>> 60) & 0b11L;
            output[outputOffset + 31] = (v0 >>> 62) & 0b11L;
        }
    }

    private static final class Unpacker3
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            int v1 = input.readInt();
            output[outputOffset] = v0 & 0b111L;
            output[outputOffset + 1] = (v0 >>> 3) & 0b111L;
            output[outputOffset + 2] = (v0 >>> 6) & 0b111L;
            output[outputOffset + 3] = (v0 >>> 9) & 0b111L;
            output[outputOffset + 4] = (v0 >>> 12) & 0b111L;
            output[outputOffset + 5] = (v0 >>> 15) & 0b111L;
            output[outputOffset + 6] = (v0 >>> 18) & 0b111L;
            output[outputOffset + 7] = (v0 >>> 21) & 0b111L;
            output[outputOffset + 8] = (v0 >>> 24) & 0b111L;
            output[outputOffset + 9] = (v0 >>> 27) & 0b111L;
            output[outputOffset + 10] = (v0 >>> 30) & 0b111L;
            output[outputOffset + 11] = (v0 >>> 33) & 0b111L;
            output[outputOffset + 12] = (v0 >>> 36) & 0b111L;
            output[outputOffset + 13] = (v0 >>> 39) & 0b111L;
            output[outputOffset + 14] = (v0 >>> 42) & 0b111L;
            output[outputOffset + 15] = (v0 >>> 45) & 0b111L;
            output[outputOffset + 16] = (v0 >>> 48) & 0b111L;
            output[outputOffset + 17] = (v0 >>> 51) & 0b111L;
            output[outputOffset + 18] = (v0 >>> 54) & 0b111L;
            output[outputOffset + 19] = (v0 >>> 57) & 0b111L;
            output[outputOffset + 20] = (v0 >>> 60) & 0b111L;
            output[outputOffset + 21] = ((v0 >>> 63) & 0b1L) | ((v1 & 0b11L) << 1);
            output[outputOffset + 22] = (v1 >>> 2) & 0b111L;
            output[outputOffset + 23] = (v1 >>> 5) & 0b111L;
            output[outputOffset + 24] = (v1 >>> 8) & 0b111L;
            output[outputOffset + 25] = (v1 >>> 11) & 0b111L;
            output[outputOffset + 26] = (v1 >>> 14) & 0b111L;
            output[outputOffset + 27] = (v1 >>> 17) & 0b111L;
            output[outputOffset + 28] = (v1 >>> 20) & 0b111L;
            output[outputOffset + 29] = (v1 >>> 23) & 0b111L;
            output[outputOffset + 30] = (v1 >>> 26) & 0b111L;
            output[outputOffset + 31] = (v1 >>> 29) & 0b111L;
        }
    }

    private static final class Unpacker4
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            output[outputOffset] = v0 & 0b1111L;
            output[outputOffset + 1] = (v0 >>> 4) & 0b1111L;
            output[outputOffset + 2] = (v0 >>> 8) & 0b1111L;
            output[outputOffset + 3] = (v0 >>> 12) & 0b1111L;
            output[outputOffset + 4] = (v0 >>> 16) & 0b1111L;
            output[outputOffset + 5] = (v0 >>> 20) & 0b1111L;
            output[outputOffset + 6] = (v0 >>> 24) & 0b1111L;
            output[outputOffset + 7] = (v0 >>> 28) & 0b1111L;
            output[outputOffset + 8] = (v0 >>> 32) & 0b1111L;
            output[outputOffset + 9] = (v0 >>> 36) & 0b1111L;
            output[outputOffset + 10] = (v0 >>> 40) & 0b1111L;
            output[outputOffset + 11] = (v0 >>> 44) & 0b1111L;
            output[outputOffset + 12] = (v0 >>> 48) & 0b1111L;
            output[outputOffset + 13] = (v0 >>> 52) & 0b1111L;
            output[outputOffset + 14] = (v0 >>> 56) & 0b1111L;
            output[outputOffset + 15] = (v0 >>> 60) & 0b1111L;
            output[outputOffset + 16] = v1 & 0b1111L;
            output[outputOffset + 17] = (v1 >>> 4) & 0b1111L;
            output[outputOffset + 18] = (v1 >>> 8) & 0b1111L;
            output[outputOffset + 19] = (v1 >>> 12) & 0b1111L;
            output[outputOffset + 20] = (v1 >>> 16) & 0b1111L;
            output[outputOffset + 21] = (v1 >>> 20) & 0b1111L;
            output[outputOffset + 22] = (v1 >>> 24) & 0b1111L;
            output[outputOffset + 23] = (v1 >>> 28) & 0b1111L;
            output[outputOffset + 24] = (v1 >>> 32) & 0b1111L;
            output[outputOffset + 25] = (v1 >>> 36) & 0b1111L;
            output[outputOffset + 26] = (v1 >>> 40) & 0b1111L;
            output[outputOffset + 27] = (v1 >>> 44) & 0b1111L;
            output[outputOffset + 28] = (v1 >>> 48) & 0b1111L;
            output[outputOffset + 29] = (v1 >>> 52) & 0b1111L;
            output[outputOffset + 30] = (v1 >>> 56) & 0b1111L;
            output[outputOffset + 31] = (v1 >>> 60) & 0b1111L;
        }
    }

    private static final class Unpacker5
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            int v2 = input.readInt();
            output[outputOffset] = v0 & 0b11111L;
            output[outputOffset + 1] = (v0 >>> 5) & 0b11111L;
            output[outputOffset + 2] = (v0 >>> 10) & 0b11111L;
            output[outputOffset + 3] = (v0 >>> 15) & 0b11111L;
            output[outputOffset + 4] = (v0 >>> 20) & 0b11111L;
            output[outputOffset + 5] = (v0 >>> 25) & 0b11111L;
            output[outputOffset + 6] = (v0 >>> 30) & 0b11111L;
            output[outputOffset + 7] = (v0 >>> 35) & 0b11111L;
            output[outputOffset + 8] = (v0 >>> 40) & 0b11111L;
            output[outputOffset + 9] = (v0 >>> 45) & 0b11111L;
            output[outputOffset + 10] = (v0 >>> 50) & 0b11111L;
            output[outputOffset + 11] = (v0 >>> 55) & 0b11111L;
            output[outputOffset + 12] = ((v0 >>> 60) & 0b1111L) | ((v1 & 0b1L) << 4);
            output[outputOffset + 13] = (v1 >>> 1) & 0b11111L;
            output[outputOffset + 14] = (v1 >>> 6) & 0b11111L;
            output[outputOffset + 15] = (v1 >>> 11) & 0b11111L;
            output[outputOffset + 16] = (v1 >>> 16) & 0b11111L;
            output[outputOffset + 17] = (v1 >>> 21) & 0b11111L;
            output[outputOffset + 18] = (v1 >>> 26) & 0b11111L;
            output[outputOffset + 19] = (v1 >>> 31) & 0b11111L;
            output[outputOffset + 20] = (v1 >>> 36) & 0b11111L;
            output[outputOffset + 21] = (v1 >>> 41) & 0b11111L;
            output[outputOffset + 22] = (v1 >>> 46) & 0b11111L;
            output[outputOffset + 23] = (v1 >>> 51) & 0b11111L;
            output[outputOffset + 24] = (v1 >>> 56) & 0b11111L;
            output[outputOffset + 25] = ((v1 >>> 61) & 0b111L) | ((v2 & 0b11L) << 3);
            output[outputOffset + 26] = (v2 >>> 2) & 0b11111L;
            output[outputOffset + 27] = (v2 >>> 7) & 0b11111L;
            output[outputOffset + 28] = (v2 >>> 12) & 0b11111L;
            output[outputOffset + 29] = (v2 >>> 17) & 0b11111L;
            output[outputOffset + 30] = (v2 >>> 22) & 0b11111L;
            output[outputOffset + 31] = (v2 >>> 27) & 0b11111L;
        }
    }

    private static final class Unpacker6
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            output[outputOffset] = v0 & 0b111111L;
            output[outputOffset + 1] = (v0 >>> 6) & 0b111111L;
            output[outputOffset + 2] = (v0 >>> 12) & 0b111111L;
            output[outputOffset + 3] = (v0 >>> 18) & 0b111111L;
            output[outputOffset + 4] = (v0 >>> 24) & 0b111111L;
            output[outputOffset + 5] = (v0 >>> 30) & 0b111111L;
            output[outputOffset + 6] = (v0 >>> 36) & 0b111111L;
            output[outputOffset + 7] = (v0 >>> 42) & 0b111111L;
            output[outputOffset + 8] = (v0 >>> 48) & 0b111111L;
            output[outputOffset + 9] = (v0 >>> 54) & 0b111111L;
            output[outputOffset + 10] = ((v0 >>> 60) & 0b1111L) | ((v1 & 0b11L) << 4);
            output[outputOffset + 11] = (v1 >>> 2) & 0b111111L;
            output[outputOffset + 12] = (v1 >>> 8) & 0b111111L;
            output[outputOffset + 13] = (v1 >>> 14) & 0b111111L;
            output[outputOffset + 14] = (v1 >>> 20) & 0b111111L;
            output[outputOffset + 15] = (v1 >>> 26) & 0b111111L;
            output[outputOffset + 16] = (v1 >>> 32) & 0b111111L;
            output[outputOffset + 17] = (v1 >>> 38) & 0b111111L;
            output[outputOffset + 18] = (v1 >>> 44) & 0b111111L;
            output[outputOffset + 19] = (v1 >>> 50) & 0b111111L;
            output[outputOffset + 20] = (v1 >>> 56) & 0b111111L;
            output[outputOffset + 21] = ((v1 >>> 62) & 0b11L) | ((v2 & 0b1111L) << 2);
            output[outputOffset + 22] = (v2 >>> 4) & 0b111111L;
            output[outputOffset + 23] = (v2 >>> 10) & 0b111111L;
            output[outputOffset + 24] = (v2 >>> 16) & 0b111111L;
            output[outputOffset + 25] = (v2 >>> 22) & 0b111111L;
            output[outputOffset + 26] = (v2 >>> 28) & 0b111111L;
            output[outputOffset + 27] = (v2 >>> 34) & 0b111111L;
            output[outputOffset + 28] = (v2 >>> 40) & 0b111111L;
            output[outputOffset + 29] = (v2 >>> 46) & 0b111111L;
            output[outputOffset + 30] = (v2 >>> 52) & 0b111111L;
            output[outputOffset + 31] = (v2 >>> 58) & 0b111111L;
        }
    }

    private static final class Unpacker7
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            int v3 = input.readInt();
            output[outputOffset] = v0 & 0b1111111L;
            output[outputOffset + 1] = (v0 >>> 7) & 0b1111111L;
            output[outputOffset + 2] = (v0 >>> 14) & 0b1111111L;
            output[outputOffset + 3] = (v0 >>> 21) & 0b1111111L;
            output[outputOffset + 4] = (v0 >>> 28) & 0b1111111L;
            output[outputOffset + 5] = (v0 >>> 35) & 0b1111111L;
            output[outputOffset + 6] = (v0 >>> 42) & 0b1111111L;
            output[outputOffset + 7] = (v0 >>> 49) & 0b1111111L;
            output[outputOffset + 8] = (v0 >>> 56) & 0b1111111L;
            output[outputOffset + 9] = ((v0 >>> 63) & 0b1L) | ((v1 & 0b111111L) << 1);
            output[outputOffset + 10] = (v1 >>> 6) & 0b1111111L;
            output[outputOffset + 11] = (v1 >>> 13) & 0b1111111L;
            output[outputOffset + 12] = (v1 >>> 20) & 0b1111111L;
            output[outputOffset + 13] = (v1 >>> 27) & 0b1111111L;
            output[outputOffset + 14] = (v1 >>> 34) & 0b1111111L;
            output[outputOffset + 15] = (v1 >>> 41) & 0b1111111L;
            output[outputOffset + 16] = (v1 >>> 48) & 0b1111111L;
            output[outputOffset + 17] = (v1 >>> 55) & 0b1111111L;
            output[outputOffset + 18] = ((v1 >>> 62) & 0b11L) | ((v2 & 0b11111L) << 2);
            output[outputOffset + 19] = (v2 >>> 5) & 0b1111111L;
            output[outputOffset + 20] = (v2 >>> 12) & 0b1111111L;
            output[outputOffset + 21] = (v2 >>> 19) & 0b1111111L;
            output[outputOffset + 22] = (v2 >>> 26) & 0b1111111L;
            output[outputOffset + 23] = (v2 >>> 33) & 0b1111111L;
            output[outputOffset + 24] = (v2 >>> 40) & 0b1111111L;
            output[outputOffset + 25] = (v2 >>> 47) & 0b1111111L;
            output[outputOffset + 26] = (v2 >>> 54) & 0b1111111L;
            output[outputOffset + 27] = ((v2 >>> 61) & 0b111L) | ((v3 & 0b1111L) << 3);
            output[outputOffset + 28] = (v3 >>> 4) & 0b1111111L;
            output[outputOffset + 29] = (v3 >>> 11) & 0b1111111L;
            output[outputOffset + 30] = (v3 >>> 18) & 0b1111111L;
            output[outputOffset + 31] = (v3 >>> 25) & 0b1111111L;
        }
    }

    private static final class Unpacker8
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            output[outputOffset] = v0 & 0b11111111L;
            output[outputOffset + 1] = (v0 >>> 8) & 0b11111111L;
            output[outputOffset + 2] = (v0 >>> 16) & 0b11111111L;
            output[outputOffset + 3] = (v0 >>> 24) & 0b11111111L;
            output[outputOffset + 4] = (v0 >>> 32) & 0b11111111L;
            output[outputOffset + 5] = (v0 >>> 40) & 0b11111111L;
            output[outputOffset + 6] = (v0 >>> 48) & 0b11111111L;
            output[outputOffset + 7] = (v0 >>> 56) & 0b11111111L;
            output[outputOffset + 8] = v1 & 0b11111111L;
            output[outputOffset + 9] = (v1 >>> 8) & 0b11111111L;
            output[outputOffset + 10] = (v1 >>> 16) & 0b11111111L;
            output[outputOffset + 11] = (v1 >>> 24) & 0b11111111L;
            output[outputOffset + 12] = (v1 >>> 32) & 0b11111111L;
            output[outputOffset + 13] = (v1 >>> 40) & 0b11111111L;
            output[outputOffset + 14] = (v1 >>> 48) & 0b11111111L;
            output[outputOffset + 15] = (v1 >>> 56) & 0b11111111L;
            output[outputOffset + 16] = v2 & 0b11111111L;
            output[outputOffset + 17] = (v2 >>> 8) & 0b11111111L;
            output[outputOffset + 18] = (v2 >>> 16) & 0b11111111L;
            output[outputOffset + 19] = (v2 >>> 24) & 0b11111111L;
            output[outputOffset + 20] = (v2 >>> 32) & 0b11111111L;
            output[outputOffset + 21] = (v2 >>> 40) & 0b11111111L;
            output[outputOffset + 22] = (v2 >>> 48) & 0b11111111L;
            output[outputOffset + 23] = (v2 >>> 56) & 0b11111111L;
            output[outputOffset + 24] = v3 & 0b11111111L;
            output[outputOffset + 25] = (v3 >>> 8) & 0b11111111L;
            output[outputOffset + 26] = (v3 >>> 16) & 0b11111111L;
            output[outputOffset + 27] = (v3 >>> 24) & 0b11111111L;
            output[outputOffset + 28] = (v3 >>> 32) & 0b11111111L;
            output[outputOffset + 29] = (v3 >>> 40) & 0b11111111L;
            output[outputOffset + 30] = (v3 >>> 48) & 0b11111111L;
            output[outputOffset + 31] = (v3 >>> 56) & 0b11111111L;
        }
    }

    private static final class Unpacker9
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            int v4 = input.readInt();
            output[outputOffset] = v0 & 0b111111111L;
            output[outputOffset + 1] = (v0 >>> 9) & 0b111111111L;
            output[outputOffset + 2] = (v0 >>> 18) & 0b111111111L;
            output[outputOffset + 3] = (v0 >>> 27) & 0b111111111L;
            output[outputOffset + 4] = (v0 >>> 36) & 0b111111111L;
            output[outputOffset + 5] = (v0 >>> 45) & 0b111111111L;
            output[outputOffset + 6] = (v0 >>> 54) & 0b111111111L;
            output[outputOffset + 7] = ((v0 >>> 63) & 0b1L) | ((v1 & 0b11111111L) << 1);
            output[outputOffset + 8] = (v1 >>> 8) & 0b111111111L;
            output[outputOffset + 9] = (v1 >>> 17) & 0b111111111L;
            output[outputOffset + 10] = (v1 >>> 26) & 0b111111111L;
            output[outputOffset + 11] = (v1 >>> 35) & 0b111111111L;
            output[outputOffset + 12] = (v1 >>> 44) & 0b111111111L;
            output[outputOffset + 13] = (v1 >>> 53) & 0b111111111L;
            output[outputOffset + 14] = ((v1 >>> 62) & 0b11L) | ((v2 & 0b1111111L) << 2);
            output[outputOffset + 15] = (v2 >>> 7) & 0b111111111L;
            output[outputOffset + 16] = (v2 >>> 16) & 0b111111111L;
            output[outputOffset + 17] = (v2 >>> 25) & 0b111111111L;
            output[outputOffset + 18] = (v2 >>> 34) & 0b111111111L;
            output[outputOffset + 19] = (v2 >>> 43) & 0b111111111L;
            output[outputOffset + 20] = (v2 >>> 52) & 0b111111111L;
            output[outputOffset + 21] = ((v2 >>> 61) & 0b111L) | ((v3 & 0b111111L) << 3);
            output[outputOffset + 22] = (v3 >>> 6) & 0b111111111L;
            output[outputOffset + 23] = (v3 >>> 15) & 0b111111111L;
            output[outputOffset + 24] = (v3 >>> 24) & 0b111111111L;
            output[outputOffset + 25] = (v3 >>> 33) & 0b111111111L;
            output[outputOffset + 26] = (v3 >>> 42) & 0b111111111L;
            output[outputOffset + 27] = (v3 >>> 51) & 0b111111111L;
            output[outputOffset + 28] = ((v3 >>> 60) & 0b1111L) | ((v4 & 0b11111L) << 4);
            output[outputOffset + 29] = (v4 >>> 5) & 0b111111111L;
            output[outputOffset + 30] = (v4 >>> 14) & 0b111111111L;
            output[outputOffset + 31] = (v4 >>> 23) & 0b111111111L;
        }
    }

    private static final class Unpacker10
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            output[outputOffset] = v0 & 0b1111111111L;
            output[outputOffset + 1] = (v0 >>> 10) & 0b1111111111L;
            output[outputOffset + 2] = (v0 >>> 20) & 0b1111111111L;
            output[outputOffset + 3] = (v0 >>> 30) & 0b1111111111L;
            output[outputOffset + 4] = (v0 >>> 40) & 0b1111111111L;
            output[outputOffset + 5] = (v0 >>> 50) & 0b1111111111L;
            output[outputOffset + 6] = ((v0 >>> 60) & 0b1111L) | ((v1 & 0b111111L) << 4);
            output[outputOffset + 7] = (v1 >>> 6) & 0b1111111111L;
            output[outputOffset + 8] = (v1 >>> 16) & 0b1111111111L;
            output[outputOffset + 9] = (v1 >>> 26) & 0b1111111111L;
            output[outputOffset + 10] = (v1 >>> 36) & 0b1111111111L;
            output[outputOffset + 11] = (v1 >>> 46) & 0b1111111111L;
            output[outputOffset + 12] = ((v1 >>> 56) & 0b11111111L) | ((v2 & 0b11L) << 8);
            output[outputOffset + 13] = (v2 >>> 2) & 0b1111111111L;
            output[outputOffset + 14] = (v2 >>> 12) & 0b1111111111L;
            output[outputOffset + 15] = (v2 >>> 22) & 0b1111111111L;
            output[outputOffset + 16] = (v2 >>> 32) & 0b1111111111L;
            output[outputOffset + 17] = (v2 >>> 42) & 0b1111111111L;
            output[outputOffset + 18] = (v2 >>> 52) & 0b1111111111L;
            output[outputOffset + 19] = ((v2 >>> 62) & 0b11L) | ((v3 & 0b11111111L) << 2);
            output[outputOffset + 20] = (v3 >>> 8) & 0b1111111111L;
            output[outputOffset + 21] = (v3 >>> 18) & 0b1111111111L;
            output[outputOffset + 22] = (v3 >>> 28) & 0b1111111111L;
            output[outputOffset + 23] = (v3 >>> 38) & 0b1111111111L;
            output[outputOffset + 24] = (v3 >>> 48) & 0b1111111111L;
            output[outputOffset + 25] = ((v3 >>> 58) & 0b111111L) | ((v4 & 0b1111L) << 6);
            output[outputOffset + 26] = (v4 >>> 4) & 0b1111111111L;
            output[outputOffset + 27] = (v4 >>> 14) & 0b1111111111L;
            output[outputOffset + 28] = (v4 >>> 24) & 0b1111111111L;
            output[outputOffset + 29] = (v4 >>> 34) & 0b1111111111L;
            output[outputOffset + 30] = (v4 >>> 44) & 0b1111111111L;
            output[outputOffset + 31] = (v4 >>> 54) & 0b1111111111L;
        }
    }

    private static final class Unpacker11
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            int v5 = input.readInt();
            output[outputOffset] = v0 & 0b11111111111L;
            output[outputOffset + 1] = (v0 >>> 11) & 0b11111111111L;
            output[outputOffset + 2] = (v0 >>> 22) & 0b11111111111L;
            output[outputOffset + 3] = (v0 >>> 33) & 0b11111111111L;
            output[outputOffset + 4] = (v0 >>> 44) & 0b11111111111L;
            output[outputOffset + 5] = ((v0 >>> 55) & 0b111111111L) | ((v1 & 0b11L) << 9);
            output[outputOffset + 6] = (v1 >>> 2) & 0b11111111111L;
            output[outputOffset + 7] = (v1 >>> 13) & 0b11111111111L;
            output[outputOffset + 8] = (v1 >>> 24) & 0b11111111111L;
            output[outputOffset + 9] = (v1 >>> 35) & 0b11111111111L;
            output[outputOffset + 10] = (v1 >>> 46) & 0b11111111111L;
            output[outputOffset + 11] = ((v1 >>> 57) & 0b1111111L) | ((v2 & 0b1111L) << 7);
            output[outputOffset + 12] = (v2 >>> 4) & 0b11111111111L;
            output[outputOffset + 13] = (v2 >>> 15) & 0b11111111111L;
            output[outputOffset + 14] = (v2 >>> 26) & 0b11111111111L;
            output[outputOffset + 15] = (v2 >>> 37) & 0b11111111111L;
            output[outputOffset + 16] = (v2 >>> 48) & 0b11111111111L;
            output[outputOffset + 17] = ((v2 >>> 59) & 0b11111L) | ((v3 & 0b111111L) << 5);
            output[outputOffset + 18] = (v3 >>> 6) & 0b11111111111L;
            output[outputOffset + 19] = (v3 >>> 17) & 0b11111111111L;
            output[outputOffset + 20] = (v3 >>> 28) & 0b11111111111L;
            output[outputOffset + 21] = (v3 >>> 39) & 0b11111111111L;
            output[outputOffset + 22] = (v3 >>> 50) & 0b11111111111L;
            output[outputOffset + 23] = ((v3 >>> 61) & 0b111L) | ((v4 & 0b11111111L) << 3);
            output[outputOffset + 24] = (v4 >>> 8) & 0b11111111111L;
            output[outputOffset + 25] = (v4 >>> 19) & 0b11111111111L;
            output[outputOffset + 26] = (v4 >>> 30) & 0b11111111111L;
            output[outputOffset + 27] = (v4 >>> 41) & 0b11111111111L;
            output[outputOffset + 28] = (v4 >>> 52) & 0b11111111111L;
            output[outputOffset + 29] = ((v4 >>> 63) & 0b1L) | ((v5 & 0b1111111111L) << 1);
            output[outputOffset + 30] = (v5 >>> 10) & 0b11111111111L;
            output[outputOffset + 31] = (v5 >>> 21) & 0b11111111111L;
        }
    }

    private static final class Unpacker12
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            output[outputOffset] = v0 & 0b111111111111L;
            output[outputOffset + 1] = (v0 >>> 12) & 0b111111111111L;
            output[outputOffset + 2] = (v0 >>> 24) & 0b111111111111L;
            output[outputOffset + 3] = (v0 >>> 36) & 0b111111111111L;
            output[outputOffset + 4] = (v0 >>> 48) & 0b111111111111L;
            output[outputOffset + 5] = ((v0 >>> 60) & 0b1111L) | ((v1 & 0b11111111L) << 4);
            output[outputOffset + 6] = (v1 >>> 8) & 0b111111111111L;
            output[outputOffset + 7] = (v1 >>> 20) & 0b111111111111L;
            output[outputOffset + 8] = (v1 >>> 32) & 0b111111111111L;
            output[outputOffset + 9] = (v1 >>> 44) & 0b111111111111L;
            output[outputOffset + 10] = ((v1 >>> 56) & 0b11111111L) | ((v2 & 0b1111L) << 8);
            output[outputOffset + 11] = (v2 >>> 4) & 0b111111111111L;
            output[outputOffset + 12] = (v2 >>> 16) & 0b111111111111L;
            output[outputOffset + 13] = (v2 >>> 28) & 0b111111111111L;
            output[outputOffset + 14] = (v2 >>> 40) & 0b111111111111L;
            output[outputOffset + 15] = (v2 >>> 52) & 0b111111111111L;
            output[outputOffset + 16] = v3 & 0b111111111111L;
            output[outputOffset + 17] = (v3 >>> 12) & 0b111111111111L;
            output[outputOffset + 18] = (v3 >>> 24) & 0b111111111111L;
            output[outputOffset + 19] = (v3 >>> 36) & 0b111111111111L;
            output[outputOffset + 20] = (v3 >>> 48) & 0b111111111111L;
            output[outputOffset + 21] = ((v3 >>> 60) & 0b1111L) | ((v4 & 0b11111111L) << 4);
            output[outputOffset + 22] = (v4 >>> 8) & 0b111111111111L;
            output[outputOffset + 23] = (v4 >>> 20) & 0b111111111111L;
            output[outputOffset + 24] = (v4 >>> 32) & 0b111111111111L;
            output[outputOffset + 25] = (v4 >>> 44) & 0b111111111111L;
            output[outputOffset + 26] = ((v4 >>> 56) & 0b11111111L) | ((v5 & 0b1111L) << 8);
            output[outputOffset + 27] = (v5 >>> 4) & 0b111111111111L;
            output[outputOffset + 28] = (v5 >>> 16) & 0b111111111111L;
            output[outputOffset + 29] = (v5 >>> 28) & 0b111111111111L;
            output[outputOffset + 30] = (v5 >>> 40) & 0b111111111111L;
            output[outputOffset + 31] = (v5 >>> 52) & 0b111111111111L;
        }
    }

    private static final class Unpacker13
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            int v6 = input.readInt();
            output[outputOffset] = v0 & 0b1111111111111L;
            output[outputOffset + 1] = (v0 >>> 13) & 0b1111111111111L;
            output[outputOffset + 2] = (v0 >>> 26) & 0b1111111111111L;
            output[outputOffset + 3] = (v0 >>> 39) & 0b1111111111111L;
            output[outputOffset + 4] = ((v0 >>> 52) & 0b111111111111L) | ((v1 & 0b1L) << 12);
            output[outputOffset + 5] = (v1 >>> 1) & 0b1111111111111L;
            output[outputOffset + 6] = (v1 >>> 14) & 0b1111111111111L;
            output[outputOffset + 7] = (v1 >>> 27) & 0b1111111111111L;
            output[outputOffset + 8] = (v1 >>> 40) & 0b1111111111111L;
            output[outputOffset + 9] = ((v1 >>> 53) & 0b11111111111L) | ((v2 & 0b11L) << 11);
            output[outputOffset + 10] = (v2 >>> 2) & 0b1111111111111L;
            output[outputOffset + 11] = (v2 >>> 15) & 0b1111111111111L;
            output[outputOffset + 12] = (v2 >>> 28) & 0b1111111111111L;
            output[outputOffset + 13] = (v2 >>> 41) & 0b1111111111111L;
            output[outputOffset + 14] = ((v2 >>> 54) & 0b1111111111L) | ((v3 & 0b111L) << 10);
            output[outputOffset + 15] = (v3 >>> 3) & 0b1111111111111L;
            output[outputOffset + 16] = (v3 >>> 16) & 0b1111111111111L;
            output[outputOffset + 17] = (v3 >>> 29) & 0b1111111111111L;
            output[outputOffset + 18] = (v3 >>> 42) & 0b1111111111111L;
            output[outputOffset + 19] = ((v3 >>> 55) & 0b111111111L) | ((v4 & 0b1111L) << 9);
            output[outputOffset + 20] = (v4 >>> 4) & 0b1111111111111L;
            output[outputOffset + 21] = (v4 >>> 17) & 0b1111111111111L;
            output[outputOffset + 22] = (v4 >>> 30) & 0b1111111111111L;
            output[outputOffset + 23] = (v4 >>> 43) & 0b1111111111111L;
            output[outputOffset + 24] = ((v4 >>> 56) & 0b11111111L) | ((v5 & 0b11111L) << 8);
            output[outputOffset + 25] = (v5 >>> 5) & 0b1111111111111L;
            output[outputOffset + 26] = (v5 >>> 18) & 0b1111111111111L;
            output[outputOffset + 27] = (v5 >>> 31) & 0b1111111111111L;
            output[outputOffset + 28] = (v5 >>> 44) & 0b1111111111111L;
            output[outputOffset + 29] = ((v5 >>> 57) & 0b1111111L) | ((v6 & 0b111111L) << 7);
            output[outputOffset + 30] = (v6 >>> 6) & 0b1111111111111L;
            output[outputOffset + 31] = (v6 >>> 19) & 0b1111111111111L;
        }
    }

    private static final class Unpacker14
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            output[outputOffset] = v0 & 0b11111111111111L;
            output[outputOffset + 1] = (v0 >>> 14) & 0b11111111111111L;
            output[outputOffset + 2] = (v0 >>> 28) & 0b11111111111111L;
            output[outputOffset + 3] = (v0 >>> 42) & 0b11111111111111L;
            output[outputOffset + 4] = ((v0 >>> 56) & 0b11111111L) | ((v1 & 0b111111L) << 8);
            output[outputOffset + 5] = (v1 >>> 6) & 0b11111111111111L;
            output[outputOffset + 6] = (v1 >>> 20) & 0b11111111111111L;
            output[outputOffset + 7] = (v1 >>> 34) & 0b11111111111111L;
            output[outputOffset + 8] = (v1 >>> 48) & 0b11111111111111L;
            output[outputOffset + 9] = ((v1 >>> 62) & 0b11L) | ((v2 & 0b111111111111L) << 2);
            output[outputOffset + 10] = (v2 >>> 12) & 0b11111111111111L;
            output[outputOffset + 11] = (v2 >>> 26) & 0b11111111111111L;
            output[outputOffset + 12] = (v2 >>> 40) & 0b11111111111111L;
            output[outputOffset + 13] = ((v2 >>> 54) & 0b1111111111L) | ((v3 & 0b1111L) << 10);
            output[outputOffset + 14] = (v3 >>> 4) & 0b11111111111111L;
            output[outputOffset + 15] = (v3 >>> 18) & 0b11111111111111L;
            output[outputOffset + 16] = (v3 >>> 32) & 0b11111111111111L;
            output[outputOffset + 17] = (v3 >>> 46) & 0b11111111111111L;
            output[outputOffset + 18] = ((v3 >>> 60) & 0b1111L) | ((v4 & 0b1111111111L) << 4);
            output[outputOffset + 19] = (v4 >>> 10) & 0b11111111111111L;
            output[outputOffset + 20] = (v4 >>> 24) & 0b11111111111111L;
            output[outputOffset + 21] = (v4 >>> 38) & 0b11111111111111L;
            output[outputOffset + 22] = ((v4 >>> 52) & 0b111111111111L) | ((v5 & 0b11L) << 12);
            output[outputOffset + 23] = (v5 >>> 2) & 0b11111111111111L;
            output[outputOffset + 24] = (v5 >>> 16) & 0b11111111111111L;
            output[outputOffset + 25] = (v5 >>> 30) & 0b11111111111111L;
            output[outputOffset + 26] = (v5 >>> 44) & 0b11111111111111L;
            output[outputOffset + 27] = ((v5 >>> 58) & 0b111111L) | ((v6 & 0b11111111L) << 6);
            output[outputOffset + 28] = (v6 >>> 8) & 0b11111111111111L;
            output[outputOffset + 29] = (v6 >>> 22) & 0b11111111111111L;
            output[outputOffset + 30] = (v6 >>> 36) & 0b11111111111111L;
            output[outputOffset + 31] = (v6 >>> 50) & 0b11111111111111L;
        }
    }

    private static final class Unpacker15
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            int v7 = input.readInt();
            output[outputOffset] = v0 & 0b111111111111111L;
            output[outputOffset + 1] = (v0 >>> 15) & 0b111111111111111L;
            output[outputOffset + 2] = (v0 >>> 30) & 0b111111111111111L;
            output[outputOffset + 3] = (v0 >>> 45) & 0b111111111111111L;
            output[outputOffset + 4] = ((v0 >>> 60) & 0b1111L) | ((v1 & 0b11111111111L) << 4);
            output[outputOffset + 5] = (v1 >>> 11) & 0b111111111111111L;
            output[outputOffset + 6] = (v1 >>> 26) & 0b111111111111111L;
            output[outputOffset + 7] = (v1 >>> 41) & 0b111111111111111L;
            output[outputOffset + 8] = ((v1 >>> 56) & 0b11111111L) | ((v2 & 0b1111111L) << 8);
            output[outputOffset + 9] = (v2 >>> 7) & 0b111111111111111L;
            output[outputOffset + 10] = (v2 >>> 22) & 0b111111111111111L;
            output[outputOffset + 11] = (v2 >>> 37) & 0b111111111111111L;
            output[outputOffset + 12] = ((v2 >>> 52) & 0b111111111111L) | ((v3 & 0b111L) << 12);
            output[outputOffset + 13] = (v3 >>> 3) & 0b111111111111111L;
            output[outputOffset + 14] = (v3 >>> 18) & 0b111111111111111L;
            output[outputOffset + 15] = (v3 >>> 33) & 0b111111111111111L;
            output[outputOffset + 16] = (v3 >>> 48) & 0b111111111111111L;
            output[outputOffset + 17] = ((v3 >>> 63) & 0b1L) | ((v4 & 0b11111111111111L) << 1);
            output[outputOffset + 18] = (v4 >>> 14) & 0b111111111111111L;
            output[outputOffset + 19] = (v4 >>> 29) & 0b111111111111111L;
            output[outputOffset + 20] = (v4 >>> 44) & 0b111111111111111L;
            output[outputOffset + 21] = ((v4 >>> 59) & 0b11111L) | ((v5 & 0b1111111111L) << 5);
            output[outputOffset + 22] = (v5 >>> 10) & 0b111111111111111L;
            output[outputOffset + 23] = (v5 >>> 25) & 0b111111111111111L;
            output[outputOffset + 24] = (v5 >>> 40) & 0b111111111111111L;
            output[outputOffset + 25] = ((v5 >>> 55) & 0b111111111L) | ((v6 & 0b111111L) << 9);
            output[outputOffset + 26] = (v6 >>> 6) & 0b111111111111111L;
            output[outputOffset + 27] = (v6 >>> 21) & 0b111111111111111L;
            output[outputOffset + 28] = (v6 >>> 36) & 0b111111111111111L;
            output[outputOffset + 29] = ((v6 >>> 51) & 0b1111111111111L) | ((v7 & 0b11L) << 13);
            output[outputOffset + 30] = (v7 >>> 2) & 0b111111111111111L;
            output[outputOffset + 31] = (v7 >>> 17) & 0b111111111111111L;
        }
    }

    private static final class Unpacker16
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            output[outputOffset] = v0 & 0b1111111111111111L;
            output[outputOffset + 1] = (v0 >>> 16) & 0b1111111111111111L;
            output[outputOffset + 2] = (v0 >>> 32) & 0b1111111111111111L;
            output[outputOffset + 3] = (v0 >>> 48) & 0b1111111111111111L;
            output[outputOffset + 4] = v1 & 0b1111111111111111L;
            output[outputOffset + 5] = (v1 >>> 16) & 0b1111111111111111L;
            output[outputOffset + 6] = (v1 >>> 32) & 0b1111111111111111L;
            output[outputOffset + 7] = (v1 >>> 48) & 0b1111111111111111L;
            output[outputOffset + 8] = v2 & 0b1111111111111111L;
            output[outputOffset + 9] = (v2 >>> 16) & 0b1111111111111111L;
            output[outputOffset + 10] = (v2 >>> 32) & 0b1111111111111111L;
            output[outputOffset + 11] = (v2 >>> 48) & 0b1111111111111111L;
            output[outputOffset + 12] = v3 & 0b1111111111111111L;
            output[outputOffset + 13] = (v3 >>> 16) & 0b1111111111111111L;
            output[outputOffset + 14] = (v3 >>> 32) & 0b1111111111111111L;
            output[outputOffset + 15] = (v3 >>> 48) & 0b1111111111111111L;
            output[outputOffset + 16] = v4 & 0b1111111111111111L;
            output[outputOffset + 17] = (v4 >>> 16) & 0b1111111111111111L;
            output[outputOffset + 18] = (v4 >>> 32) & 0b1111111111111111L;
            output[outputOffset + 19] = (v4 >>> 48) & 0b1111111111111111L;
            output[outputOffset + 20] = v5 & 0b1111111111111111L;
            output[outputOffset + 21] = (v5 >>> 16) & 0b1111111111111111L;
            output[outputOffset + 22] = (v5 >>> 32) & 0b1111111111111111L;
            output[outputOffset + 23] = (v5 >>> 48) & 0b1111111111111111L;
            output[outputOffset + 24] = v6 & 0b1111111111111111L;
            output[outputOffset + 25] = (v6 >>> 16) & 0b1111111111111111L;
            output[outputOffset + 26] = (v6 >>> 32) & 0b1111111111111111L;
            output[outputOffset + 27] = (v6 >>> 48) & 0b1111111111111111L;
            output[outputOffset + 28] = v7 & 0b1111111111111111L;
            output[outputOffset + 29] = (v7 >>> 16) & 0b1111111111111111L;
            output[outputOffset + 30] = (v7 >>> 32) & 0b1111111111111111L;
            output[outputOffset + 31] = (v7 >>> 48) & 0b1111111111111111L;
        }
    }

    private static final class Unpacker17
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
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
            output[outputOffset] = v0 & 0b11111111111111111L;
            output[outputOffset + 1] = (v0 >>> 17) & 0b11111111111111111L;
            output[outputOffset + 2] = (v0 >>> 34) & 0b11111111111111111L;
            output[outputOffset + 3] = ((v0 >>> 51) & 0b1111111111111L) | ((v1 & 0b1111L) << 13);
            output[outputOffset + 4] = (v1 >>> 4) & 0b11111111111111111L;
            output[outputOffset + 5] = (v1 >>> 21) & 0b11111111111111111L;
            output[outputOffset + 6] = (v1 >>> 38) & 0b11111111111111111L;
            output[outputOffset + 7] = ((v1 >>> 55) & 0b111111111L) | ((v2 & 0b11111111L) << 9);
            output[outputOffset + 8] = (v2 >>> 8) & 0b11111111111111111L;
            output[outputOffset + 9] = (v2 >>> 25) & 0b11111111111111111L;
            output[outputOffset + 10] = (v2 >>> 42) & 0b11111111111111111L;
            output[outputOffset + 11] = ((v2 >>> 59) & 0b11111L) | ((v3 & 0b111111111111L) << 5);
            output[outputOffset + 12] = (v3 >>> 12) & 0b11111111111111111L;
            output[outputOffset + 13] = (v3 >>> 29) & 0b11111111111111111L;
            output[outputOffset + 14] = (v3 >>> 46) & 0b11111111111111111L;
            output[outputOffset + 15] = ((v3 >>> 63) & 0b1L) | ((v4 & 0b1111111111111111L) << 1);
            output[outputOffset + 16] = (v4 >>> 16) & 0b11111111111111111L;
            output[outputOffset + 17] = (v4 >>> 33) & 0b11111111111111111L;
            output[outputOffset + 18] = ((v4 >>> 50) & 0b11111111111111L) | ((v5 & 0b111L) << 14);
            output[outputOffset + 19] = (v5 >>> 3) & 0b11111111111111111L;
            output[outputOffset + 20] = (v5 >>> 20) & 0b11111111111111111L;
            output[outputOffset + 21] = (v5 >>> 37) & 0b11111111111111111L;
            output[outputOffset + 22] = ((v5 >>> 54) & 0b1111111111L) | ((v6 & 0b1111111L) << 10);
            output[outputOffset + 23] = (v6 >>> 7) & 0b11111111111111111L;
            output[outputOffset + 24] = (v6 >>> 24) & 0b11111111111111111L;
            output[outputOffset + 25] = (v6 >>> 41) & 0b11111111111111111L;
            output[outputOffset + 26] = ((v6 >>> 58) & 0b111111L) | ((v7 & 0b11111111111L) << 6);
            output[outputOffset + 27] = (v7 >>> 11) & 0b11111111111111111L;
            output[outputOffset + 28] = (v7 >>> 28) & 0b11111111111111111L;
            output[outputOffset + 29] = (v7 >>> 45) & 0b11111111111111111L;
            output[outputOffset + 30] = ((v7 >>> 62) & 0b11L) | ((v8 & 0b111111111111111L) << 2);
            output[outputOffset + 31] = (v8 >>> 15) & 0b11111111111111111L;
        }
    }

    private static final class Unpacker18
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            output[outputOffset] = v0 & 0b111111111111111111L;
            output[outputOffset + 1] = (v0 >>> 18) & 0b111111111111111111L;
            output[outputOffset + 2] = (v0 >>> 36) & 0b111111111111111111L;
            output[outputOffset + 3] = ((v0 >>> 54) & 0b1111111111L) | ((v1 & 0b11111111L) << 10);
            output[outputOffset + 4] = (v1 >>> 8) & 0b111111111111111111L;
            output[outputOffset + 5] = (v1 >>> 26) & 0b111111111111111111L;
            output[outputOffset + 6] = (v1 >>> 44) & 0b111111111111111111L;
            output[outputOffset + 7] = ((v1 >>> 62) & 0b11L) | ((v2 & 0b1111111111111111L) << 2);
            output[outputOffset + 8] = (v2 >>> 16) & 0b111111111111111111L;
            output[outputOffset + 9] = (v2 >>> 34) & 0b111111111111111111L;
            output[outputOffset + 10] = ((v2 >>> 52) & 0b111111111111L) | ((v3 & 0b111111L) << 12);
            output[outputOffset + 11] = (v3 >>> 6) & 0b111111111111111111L;
            output[outputOffset + 12] = (v3 >>> 24) & 0b111111111111111111L;
            output[outputOffset + 13] = (v3 >>> 42) & 0b111111111111111111L;
            output[outputOffset + 14] = ((v3 >>> 60) & 0b1111L) | ((v4 & 0b11111111111111L) << 4);
            output[outputOffset + 15] = (v4 >>> 14) & 0b111111111111111111L;
            output[outputOffset + 16] = (v4 >>> 32) & 0b111111111111111111L;
            output[outputOffset + 17] = ((v4 >>> 50) & 0b11111111111111L) | ((v5 & 0b1111L) << 14);
            output[outputOffset + 18] = (v5 >>> 4) & 0b111111111111111111L;
            output[outputOffset + 19] = (v5 >>> 22) & 0b111111111111111111L;
            output[outputOffset + 20] = (v5 >>> 40) & 0b111111111111111111L;
            output[outputOffset + 21] = ((v5 >>> 58) & 0b111111L) | ((v6 & 0b111111111111L) << 6);
            output[outputOffset + 22] = (v6 >>> 12) & 0b111111111111111111L;
            output[outputOffset + 23] = (v6 >>> 30) & 0b111111111111111111L;
            output[outputOffset + 24] = ((v6 >>> 48) & 0b1111111111111111L) | ((v7 & 0b11L) << 16);
            output[outputOffset + 25] = (v7 >>> 2) & 0b111111111111111111L;
            output[outputOffset + 26] = (v7 >>> 20) & 0b111111111111111111L;
            output[outputOffset + 27] = (v7 >>> 38) & 0b111111111111111111L;
            output[outputOffset + 28] = ((v7 >>> 56) & 0b11111111L) | ((v8 & 0b1111111111L) << 8);
            output[outputOffset + 29] = (v8 >>> 10) & 0b111111111111111111L;
            output[outputOffset + 30] = (v8 >>> 28) & 0b111111111111111111L;
            output[outputOffset + 31] = (v8 >>> 46) & 0b111111111111111111L;
        }
    }

    private static final class Unpacker19
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            int v9 = input.readInt();
            output[outputOffset] = v0 & 0b1111111111111111111L;
            output[outputOffset + 1] = (v0 >>> 19) & 0b1111111111111111111L;
            output[outputOffset + 2] = (v0 >>> 38) & 0b1111111111111111111L;
            output[outputOffset + 3] = ((v0 >>> 57) & 0b1111111L) | ((v1 & 0b111111111111L) << 7);
            output[outputOffset + 4] = (v1 >>> 12) & 0b1111111111111111111L;
            output[outputOffset + 5] = (v1 >>> 31) & 0b1111111111111111111L;
            output[outputOffset + 6] = ((v1 >>> 50) & 0b11111111111111L) | ((v2 & 0b11111L) << 14);
            output[outputOffset + 7] = (v2 >>> 5) & 0b1111111111111111111L;
            output[outputOffset + 8] = (v2 >>> 24) & 0b1111111111111111111L;
            output[outputOffset + 9] = (v2 >>> 43) & 0b1111111111111111111L;
            output[outputOffset + 10] = ((v2 >>> 62) & 0b11L) | ((v3 & 0b11111111111111111L) << 2);
            output[outputOffset + 11] = (v3 >>> 17) & 0b1111111111111111111L;
            output[outputOffset + 12] = (v3 >>> 36) & 0b1111111111111111111L;
            output[outputOffset + 13] = ((v3 >>> 55) & 0b111111111L) | ((v4 & 0b1111111111L) << 9);
            output[outputOffset + 14] = (v4 >>> 10) & 0b1111111111111111111L;
            output[outputOffset + 15] = (v4 >>> 29) & 0b1111111111111111111L;
            output[outputOffset + 16] = ((v4 >>> 48) & 0b1111111111111111L) | ((v5 & 0b111L) << 16);
            output[outputOffset + 17] = (v5 >>> 3) & 0b1111111111111111111L;
            output[outputOffset + 18] = (v5 >>> 22) & 0b1111111111111111111L;
            output[outputOffset + 19] = (v5 >>> 41) & 0b1111111111111111111L;
            output[outputOffset + 20] = ((v5 >>> 60) & 0b1111L) | ((v6 & 0b111111111111111L) << 4);
            output[outputOffset + 21] = (v6 >>> 15) & 0b1111111111111111111L;
            output[outputOffset + 22] = (v6 >>> 34) & 0b1111111111111111111L;
            output[outputOffset + 23] = ((v6 >>> 53) & 0b11111111111L) | ((v7 & 0b11111111L) << 11);
            output[outputOffset + 24] = (v7 >>> 8) & 0b1111111111111111111L;
            output[outputOffset + 25] = (v7 >>> 27) & 0b1111111111111111111L;
            output[outputOffset + 26] = ((v7 >>> 46) & 0b111111111111111111L) | ((v8 & 0b1L) << 18);
            output[outputOffset + 27] = (v8 >>> 1) & 0b1111111111111111111L;
            output[outputOffset + 28] = (v8 >>> 20) & 0b1111111111111111111L;
            output[outputOffset + 29] = (v8 >>> 39) & 0b1111111111111111111L;
            output[outputOffset + 30] = ((v8 >>> 58) & 0b111111L) | ((v9 & 0b1111111111111L) << 6);
            output[outputOffset + 31] = (v9 >>> 13) & 0b1111111111111111111L;
        }
    }

    private static final class Unpacker20
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            output[outputOffset] = v0 & 0b11111111111111111111L;
            output[outputOffset + 1] = (v0 >>> 20) & 0b11111111111111111111L;
            output[outputOffset + 2] = (v0 >>> 40) & 0b11111111111111111111L;
            output[outputOffset + 3] = ((v0 >>> 60) & 0b1111L) | ((v1 & 0b1111111111111111L) << 4);
            output[outputOffset + 4] = (v1 >>> 16) & 0b11111111111111111111L;
            output[outputOffset + 5] = (v1 >>> 36) & 0b11111111111111111111L;
            output[outputOffset + 6] = ((v1 >>> 56) & 0b11111111L) | ((v2 & 0b111111111111L) << 8);
            output[outputOffset + 7] = (v2 >>> 12) & 0b11111111111111111111L;
            output[outputOffset + 8] = (v2 >>> 32) & 0b11111111111111111111L;
            output[outputOffset + 9] = ((v2 >>> 52) & 0b111111111111L) | ((v3 & 0b11111111L) << 12);
            output[outputOffset + 10] = (v3 >>> 8) & 0b11111111111111111111L;
            output[outputOffset + 11] = (v3 >>> 28) & 0b11111111111111111111L;
            output[outputOffset + 12] = ((v3 >>> 48) & 0b1111111111111111L) | ((v4 & 0b1111L) << 16);
            output[outputOffset + 13] = (v4 >>> 4) & 0b11111111111111111111L;
            output[outputOffset + 14] = (v4 >>> 24) & 0b11111111111111111111L;
            output[outputOffset + 15] = (v4 >>> 44) & 0b11111111111111111111L;
            output[outputOffset + 16] = v5 & 0b11111111111111111111L;
            output[outputOffset + 17] = (v5 >>> 20) & 0b11111111111111111111L;
            output[outputOffset + 18] = (v5 >>> 40) & 0b11111111111111111111L;
            output[outputOffset + 19] = ((v5 >>> 60) & 0b1111L) | ((v6 & 0b1111111111111111L) << 4);
            output[outputOffset + 20] = (v6 >>> 16) & 0b11111111111111111111L;
            output[outputOffset + 21] = (v6 >>> 36) & 0b11111111111111111111L;
            output[outputOffset + 22] = ((v6 >>> 56) & 0b11111111L) | ((v7 & 0b111111111111L) << 8);
            output[outputOffset + 23] = (v7 >>> 12) & 0b11111111111111111111L;
            output[outputOffset + 24] = (v7 >>> 32) & 0b11111111111111111111L;
            output[outputOffset + 25] = ((v7 >>> 52) & 0b111111111111L) | ((v8 & 0b11111111L) << 12);
            output[outputOffset + 26] = (v8 >>> 8) & 0b11111111111111111111L;
            output[outputOffset + 27] = (v8 >>> 28) & 0b11111111111111111111L;
            output[outputOffset + 28] = ((v8 >>> 48) & 0b1111111111111111L) | ((v9 & 0b1111L) << 16);
            output[outputOffset + 29] = (v9 >>> 4) & 0b11111111111111111111L;
            output[outputOffset + 30] = (v9 >>> 24) & 0b11111111111111111111L;
            output[outputOffset + 31] = (v9 >>> 44) & 0b11111111111111111111L;
        }
    }

    private static final class Unpacker21
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            int v10 = input.readInt();
            output[outputOffset] = v0 & 0b111111111111111111111L;
            output[outputOffset + 1] = (v0 >>> 21) & 0b111111111111111111111L;
            output[outputOffset + 2] = (v0 >>> 42) & 0b111111111111111111111L;
            output[outputOffset + 3] = ((v0 >>> 63) & 0b1L) | ((v1 & 0b11111111111111111111L) << 1);
            output[outputOffset + 4] = (v1 >>> 20) & 0b111111111111111111111L;
            output[outputOffset + 5] = (v1 >>> 41) & 0b111111111111111111111L;
            output[outputOffset + 6] = ((v1 >>> 62) & 0b11L) | ((v2 & 0b1111111111111111111L) << 2);
            output[outputOffset + 7] = (v2 >>> 19) & 0b111111111111111111111L;
            output[outputOffset + 8] = (v2 >>> 40) & 0b111111111111111111111L;
            output[outputOffset + 9] = ((v2 >>> 61) & 0b111L) | ((v3 & 0b111111111111111111L) << 3);
            output[outputOffset + 10] = (v3 >>> 18) & 0b111111111111111111111L;
            output[outputOffset + 11] = (v3 >>> 39) & 0b111111111111111111111L;
            output[outputOffset + 12] = ((v3 >>> 60) & 0b1111L) | ((v4 & 0b11111111111111111L) << 4);
            output[outputOffset + 13] = (v4 >>> 17) & 0b111111111111111111111L;
            output[outputOffset + 14] = (v4 >>> 38) & 0b111111111111111111111L;
            output[outputOffset + 15] = ((v4 >>> 59) & 0b11111L) | ((v5 & 0b1111111111111111L) << 5);
            output[outputOffset + 16] = (v5 >>> 16) & 0b111111111111111111111L;
            output[outputOffset + 17] = (v5 >>> 37) & 0b111111111111111111111L;
            output[outputOffset + 18] = ((v5 >>> 58) & 0b111111L) | ((v6 & 0b111111111111111L) << 6);
            output[outputOffset + 19] = (v6 >>> 15) & 0b111111111111111111111L;
            output[outputOffset + 20] = (v6 >>> 36) & 0b111111111111111111111L;
            output[outputOffset + 21] = ((v6 >>> 57) & 0b1111111L) | ((v7 & 0b11111111111111L) << 7);
            output[outputOffset + 22] = (v7 >>> 14) & 0b111111111111111111111L;
            output[outputOffset + 23] = (v7 >>> 35) & 0b111111111111111111111L;
            output[outputOffset + 24] = ((v7 >>> 56) & 0b11111111L) | ((v8 & 0b1111111111111L) << 8);
            output[outputOffset + 25] = (v8 >>> 13) & 0b111111111111111111111L;
            output[outputOffset + 26] = (v8 >>> 34) & 0b111111111111111111111L;
            output[outputOffset + 27] = ((v8 >>> 55) & 0b111111111L) | ((v9 & 0b111111111111L) << 9);
            output[outputOffset + 28] = (v9 >>> 12) & 0b111111111111111111111L;
            output[outputOffset + 29] = (v9 >>> 33) & 0b111111111111111111111L;
            output[outputOffset + 30] = ((v9 >>> 54) & 0b1111111111L) | ((v10 & 0b11111111111L) << 10);
            output[outputOffset + 31] = (v10 >>> 11) & 0b111111111111111111111L;
        }
    }

    private static final class Unpacker22
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            output[outputOffset] = v0 & 0b1111111111111111111111L;
            output[outputOffset + 1] = (v0 >>> 22) & 0b1111111111111111111111L;
            output[outputOffset + 2] = ((v0 >>> 44) & 0b11111111111111111111L) | ((v1 & 0b11L) << 20);
            output[outputOffset + 3] = (v1 >>> 2) & 0b1111111111111111111111L;
            output[outputOffset + 4] = (v1 >>> 24) & 0b1111111111111111111111L;
            output[outputOffset + 5] = ((v1 >>> 46) & 0b111111111111111111L) | ((v2 & 0b1111L) << 18);
            output[outputOffset + 6] = (v2 >>> 4) & 0b1111111111111111111111L;
            output[outputOffset + 7] = (v2 >>> 26) & 0b1111111111111111111111L;
            output[outputOffset + 8] = ((v2 >>> 48) & 0b1111111111111111L) | ((v3 & 0b111111L) << 16);
            output[outputOffset + 9] = (v3 >>> 6) & 0b1111111111111111111111L;
            output[outputOffset + 10] = (v3 >>> 28) & 0b1111111111111111111111L;
            output[outputOffset + 11] = ((v3 >>> 50) & 0b11111111111111L) | ((v4 & 0b11111111L) << 14);
            output[outputOffset + 12] = (v4 >>> 8) & 0b1111111111111111111111L;
            output[outputOffset + 13] = (v4 >>> 30) & 0b1111111111111111111111L;
            output[outputOffset + 14] = ((v4 >>> 52) & 0b111111111111L) | ((v5 & 0b1111111111L) << 12);
            output[outputOffset + 15] = (v5 >>> 10) & 0b1111111111111111111111L;
            output[outputOffset + 16] = (v5 >>> 32) & 0b1111111111111111111111L;
            output[outputOffset + 17] = ((v5 >>> 54) & 0b1111111111L) | ((v6 & 0b111111111111L) << 10);
            output[outputOffset + 18] = (v6 >>> 12) & 0b1111111111111111111111L;
            output[outputOffset + 19] = (v6 >>> 34) & 0b1111111111111111111111L;
            output[outputOffset + 20] = ((v6 >>> 56) & 0b11111111L) | ((v7 & 0b11111111111111L) << 8);
            output[outputOffset + 21] = (v7 >>> 14) & 0b1111111111111111111111L;
            output[outputOffset + 22] = (v7 >>> 36) & 0b1111111111111111111111L;
            output[outputOffset + 23] = ((v7 >>> 58) & 0b111111L) | ((v8 & 0b1111111111111111L) << 6);
            output[outputOffset + 24] = (v8 >>> 16) & 0b1111111111111111111111L;
            output[outputOffset + 25] = (v8 >>> 38) & 0b1111111111111111111111L;
            output[outputOffset + 26] = ((v8 >>> 60) & 0b1111L) | ((v9 & 0b111111111111111111L) << 4);
            output[outputOffset + 27] = (v9 >>> 18) & 0b1111111111111111111111L;
            output[outputOffset + 28] = (v9 >>> 40) & 0b1111111111111111111111L;
            output[outputOffset + 29] = ((v9 >>> 62) & 0b11L) | ((v10 & 0b11111111111111111111L) << 2);
            output[outputOffset + 30] = (v10 >>> 20) & 0b1111111111111111111111L;
            output[outputOffset + 31] = (v10 >>> 42) & 0b1111111111111111111111L;
        }
    }

    private static final class Unpacker23
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            int v11 = input.readInt();
            output[outputOffset] = v0 & 0b11111111111111111111111L;
            output[outputOffset + 1] = (v0 >>> 23) & 0b11111111111111111111111L;
            output[outputOffset + 2] = ((v0 >>> 46) & 0b111111111111111111L) | ((v1 & 0b11111L) << 18);
            output[outputOffset + 3] = (v1 >>> 5) & 0b11111111111111111111111L;
            output[outputOffset + 4] = (v1 >>> 28) & 0b11111111111111111111111L;
            output[outputOffset + 5] = ((v1 >>> 51) & 0b1111111111111L) | ((v2 & 0b1111111111L) << 13);
            output[outputOffset + 6] = (v2 >>> 10) & 0b11111111111111111111111L;
            output[outputOffset + 7] = (v2 >>> 33) & 0b11111111111111111111111L;
            output[outputOffset + 8] = ((v2 >>> 56) & 0b11111111L) | ((v3 & 0b111111111111111L) << 8);
            output[outputOffset + 9] = (v3 >>> 15) & 0b11111111111111111111111L;
            output[outputOffset + 10] = (v3 >>> 38) & 0b11111111111111111111111L;
            output[outputOffset + 11] = ((v3 >>> 61) & 0b111L) | ((v4 & 0b11111111111111111111L) << 3);
            output[outputOffset + 12] = (v4 >>> 20) & 0b11111111111111111111111L;
            output[outputOffset + 13] = ((v4 >>> 43) & 0b111111111111111111111L) | ((v5 & 0b11L) << 21);
            output[outputOffset + 14] = (v5 >>> 2) & 0b11111111111111111111111L;
            output[outputOffset + 15] = (v5 >>> 25) & 0b11111111111111111111111L;
            output[outputOffset + 16] = ((v5 >>> 48) & 0b1111111111111111L) | ((v6 & 0b1111111L) << 16);
            output[outputOffset + 17] = (v6 >>> 7) & 0b11111111111111111111111L;
            output[outputOffset + 18] = (v6 >>> 30) & 0b11111111111111111111111L;
            output[outputOffset + 19] = ((v6 >>> 53) & 0b11111111111L) | ((v7 & 0b111111111111L) << 11);
            output[outputOffset + 20] = (v7 >>> 12) & 0b11111111111111111111111L;
            output[outputOffset + 21] = (v7 >>> 35) & 0b11111111111111111111111L;
            output[outputOffset + 22] = ((v7 >>> 58) & 0b111111L) | ((v8 & 0b11111111111111111L) << 6);
            output[outputOffset + 23] = (v8 >>> 17) & 0b11111111111111111111111L;
            output[outputOffset + 24] = (v8 >>> 40) & 0b11111111111111111111111L;
            output[outputOffset + 25] = ((v8 >>> 63) & 0b1L) | ((v9 & 0b1111111111111111111111L) << 1);
            output[outputOffset + 26] = (v9 >>> 22) & 0b11111111111111111111111L;
            output[outputOffset + 27] = ((v9 >>> 45) & 0b1111111111111111111L) | ((v10 & 0b1111L) << 19);
            output[outputOffset + 28] = (v10 >>> 4) & 0b11111111111111111111111L;
            output[outputOffset + 29] = (v10 >>> 27) & 0b11111111111111111111111L;
            output[outputOffset + 30] = ((v10 >>> 50) & 0b11111111111111L) | ((v11 & 0b111111111L) << 14);
            output[outputOffset + 31] = (v11 >>> 9) & 0b11111111111111111111111L;
        }
    }

    private static final class Unpacker24
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            output[outputOffset] = v0 & 0b111111111111111111111111L;
            output[outputOffset + 1] = (v0 >>> 24) & 0b111111111111111111111111L;
            output[outputOffset + 2] = ((v0 >>> 48) & 0b1111111111111111L) | ((v1 & 0b11111111L) << 16);
            output[outputOffset + 3] = (v1 >>> 8) & 0b111111111111111111111111L;
            output[outputOffset + 4] = (v1 >>> 32) & 0b111111111111111111111111L;
            output[outputOffset + 5] = ((v1 >>> 56) & 0b11111111L) | ((v2 & 0b1111111111111111L) << 8);
            output[outputOffset + 6] = (v2 >>> 16) & 0b111111111111111111111111L;
            output[outputOffset + 7] = (v2 >>> 40) & 0b111111111111111111111111L;
            output[outputOffset + 8] = v3 & 0b111111111111111111111111L;
            output[outputOffset + 9] = (v3 >>> 24) & 0b111111111111111111111111L;
            output[outputOffset + 10] = ((v3 >>> 48) & 0b1111111111111111L) | ((v4 & 0b11111111L) << 16);
            output[outputOffset + 11] = (v4 >>> 8) & 0b111111111111111111111111L;
            output[outputOffset + 12] = (v4 >>> 32) & 0b111111111111111111111111L;
            output[outputOffset + 13] = ((v4 >>> 56) & 0b11111111L) | ((v5 & 0b1111111111111111L) << 8);
            output[outputOffset + 14] = (v5 >>> 16) & 0b111111111111111111111111L;
            output[outputOffset + 15] = (v5 >>> 40) & 0b111111111111111111111111L;
            output[outputOffset + 16] = v6 & 0b111111111111111111111111L;
            output[outputOffset + 17] = (v6 >>> 24) & 0b111111111111111111111111L;
            output[outputOffset + 18] = ((v6 >>> 48) & 0b1111111111111111L) | ((v7 & 0b11111111L) << 16);
            output[outputOffset + 19] = (v7 >>> 8) & 0b111111111111111111111111L;
            output[outputOffset + 20] = (v7 >>> 32) & 0b111111111111111111111111L;
            output[outputOffset + 21] = ((v7 >>> 56) & 0b11111111L) | ((v8 & 0b1111111111111111L) << 8);
            output[outputOffset + 22] = (v8 >>> 16) & 0b111111111111111111111111L;
            output[outputOffset + 23] = (v8 >>> 40) & 0b111111111111111111111111L;
            output[outputOffset + 24] = v9 & 0b111111111111111111111111L;
            output[outputOffset + 25] = (v9 >>> 24) & 0b111111111111111111111111L;
            output[outputOffset + 26] = ((v9 >>> 48) & 0b1111111111111111L) | ((v10 & 0b11111111L) << 16);
            output[outputOffset + 27] = (v10 >>> 8) & 0b111111111111111111111111L;
            output[outputOffset + 28] = (v10 >>> 32) & 0b111111111111111111111111L;
            output[outputOffset + 29] = ((v10 >>> 56) & 0b11111111L) | ((v11 & 0b1111111111111111L) << 8);
            output[outputOffset + 30] = (v11 >>> 16) & 0b111111111111111111111111L;
            output[outputOffset + 31] = (v11 >>> 40) & 0b111111111111111111111111L;
        }
    }

    private static final class Unpacker25
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            int v12 = input.readInt();
            output[outputOffset] = v0 & 0b1111111111111111111111111L;
            output[outputOffset + 1] = (v0 >>> 25) & 0b1111111111111111111111111L;
            output[outputOffset + 2] = ((v0 >>> 50) & 0b11111111111111L) | ((v1 & 0b11111111111L) << 14);
            output[outputOffset + 3] = (v1 >>> 11) & 0b1111111111111111111111111L;
            output[outputOffset + 4] = (v1 >>> 36) & 0b1111111111111111111111111L;
            output[outputOffset + 5] = ((v1 >>> 61) & 0b111L) | ((v2 & 0b1111111111111111111111L) << 3);
            output[outputOffset + 6] = (v2 >>> 22) & 0b1111111111111111111111111L;
            output[outputOffset + 7] = ((v2 >>> 47) & 0b11111111111111111L) | ((v3 & 0b11111111L) << 17);
            output[outputOffset + 8] = (v3 >>> 8) & 0b1111111111111111111111111L;
            output[outputOffset + 9] = (v3 >>> 33) & 0b1111111111111111111111111L;
            output[outputOffset + 10] = ((v3 >>> 58) & 0b111111L) | ((v4 & 0b1111111111111111111L) << 6);
            output[outputOffset + 11] = (v4 >>> 19) & 0b1111111111111111111111111L;
            output[outputOffset + 12] = ((v4 >>> 44) & 0b11111111111111111111L) | ((v5 & 0b11111L) << 20);
            output[outputOffset + 13] = (v5 >>> 5) & 0b1111111111111111111111111L;
            output[outputOffset + 14] = (v5 >>> 30) & 0b1111111111111111111111111L;
            output[outputOffset + 15] = ((v5 >>> 55) & 0b111111111L) | ((v6 & 0b1111111111111111L) << 9);
            output[outputOffset + 16] = (v6 >>> 16) & 0b1111111111111111111111111L;
            output[outputOffset + 17] = ((v6 >>> 41) & 0b11111111111111111111111L) | ((v7 & 0b11L) << 23);
            output[outputOffset + 18] = (v7 >>> 2) & 0b1111111111111111111111111L;
            output[outputOffset + 19] = (v7 >>> 27) & 0b1111111111111111111111111L;
            output[outputOffset + 20] = ((v7 >>> 52) & 0b111111111111L) | ((v8 & 0b1111111111111L) << 12);
            output[outputOffset + 21] = (v8 >>> 13) & 0b1111111111111111111111111L;
            output[outputOffset + 22] = (v8 >>> 38) & 0b1111111111111111111111111L;
            output[outputOffset + 23] = ((v8 >>> 63) & 0b1L) | ((v9 & 0b111111111111111111111111L) << 1);
            output[outputOffset + 24] = (v9 >>> 24) & 0b1111111111111111111111111L;
            output[outputOffset + 25] = ((v9 >>> 49) & 0b111111111111111L) | ((v10 & 0b1111111111L) << 15);
            output[outputOffset + 26] = (v10 >>> 10) & 0b1111111111111111111111111L;
            output[outputOffset + 27] = (v10 >>> 35) & 0b1111111111111111111111111L;
            output[outputOffset + 28] = ((v10 >>> 60) & 0b1111L) | ((v11 & 0b111111111111111111111L) << 4);
            output[outputOffset + 29] = (v11 >>> 21) & 0b1111111111111111111111111L;
            output[outputOffset + 30] = ((v11 >>> 46) & 0b111111111111111111L) | ((v12 & 0b1111111L) << 18);
            output[outputOffset + 31] = (v12 >>> 7) & 0b1111111111111111111111111L;
        }
    }

    private static final class Unpacker26
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            output[outputOffset] = v0 & 0b11111111111111111111111111L;
            output[outputOffset + 1] = (v0 >>> 26) & 0b11111111111111111111111111L;
            output[outputOffset + 2] = ((v0 >>> 52) & 0b111111111111L) | ((v1 & 0b11111111111111L) << 12);
            output[outputOffset + 3] = (v1 >>> 14) & 0b11111111111111111111111111L;
            output[outputOffset + 4] = ((v1 >>> 40) & 0b111111111111111111111111L) | ((v2 & 0b11L) << 24);
            output[outputOffset + 5] = (v2 >>> 2) & 0b11111111111111111111111111L;
            output[outputOffset + 6] = (v2 >>> 28) & 0b11111111111111111111111111L;
            output[outputOffset + 7] = ((v2 >>> 54) & 0b1111111111L) | ((v3 & 0b1111111111111111L) << 10);
            output[outputOffset + 8] = (v3 >>> 16) & 0b11111111111111111111111111L;
            output[outputOffset + 9] = ((v3 >>> 42) & 0b1111111111111111111111L) | ((v4 & 0b1111L) << 22);
            output[outputOffset + 10] = (v4 >>> 4) & 0b11111111111111111111111111L;
            output[outputOffset + 11] = (v4 >>> 30) & 0b11111111111111111111111111L;
            output[outputOffset + 12] = ((v4 >>> 56) & 0b11111111L) | ((v5 & 0b111111111111111111L) << 8);
            output[outputOffset + 13] = (v5 >>> 18) & 0b11111111111111111111111111L;
            output[outputOffset + 14] = ((v5 >>> 44) & 0b11111111111111111111L) | ((v6 & 0b111111L) << 20);
            output[outputOffset + 15] = (v6 >>> 6) & 0b11111111111111111111111111L;
            output[outputOffset + 16] = (v6 >>> 32) & 0b11111111111111111111111111L;
            output[outputOffset + 17] = ((v6 >>> 58) & 0b111111L) | ((v7 & 0b11111111111111111111L) << 6);
            output[outputOffset + 18] = (v7 >>> 20) & 0b11111111111111111111111111L;
            output[outputOffset + 19] = ((v7 >>> 46) & 0b111111111111111111L) | ((v8 & 0b11111111L) << 18);
            output[outputOffset + 20] = (v8 >>> 8) & 0b11111111111111111111111111L;
            output[outputOffset + 21] = (v8 >>> 34) & 0b11111111111111111111111111L;
            output[outputOffset + 22] = ((v8 >>> 60) & 0b1111L) | ((v9 & 0b1111111111111111111111L) << 4);
            output[outputOffset + 23] = (v9 >>> 22) & 0b11111111111111111111111111L;
            output[outputOffset + 24] = ((v9 >>> 48) & 0b1111111111111111L) | ((v10 & 0b1111111111L) << 16);
            output[outputOffset + 25] = (v10 >>> 10) & 0b11111111111111111111111111L;
            output[outputOffset + 26] = (v10 >>> 36) & 0b11111111111111111111111111L;
            output[outputOffset + 27] = ((v10 >>> 62) & 0b11L) | ((v11 & 0b111111111111111111111111L) << 2);
            output[outputOffset + 28] = (v11 >>> 24) & 0b11111111111111111111111111L;
            output[outputOffset + 29] = ((v11 >>> 50) & 0b11111111111111L) | ((v12 & 0b111111111111L) << 14);
            output[outputOffset + 30] = (v12 >>> 12) & 0b11111111111111111111111111L;
            output[outputOffset + 31] = (v12 >>> 38) & 0b11111111111111111111111111L;
        }
    }

    private static final class Unpacker27
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            int v13 = input.readInt();
            output[outputOffset] = v0 & 0b111111111111111111111111111L;
            output[outputOffset + 1] = (v0 >>> 27) & 0b111111111111111111111111111L;
            output[outputOffset + 2] = ((v0 >>> 54) & 0b1111111111L) | ((v1 & 0b11111111111111111L) << 10);
            output[outputOffset + 3] = (v1 >>> 17) & 0b111111111111111111111111111L;
            output[outputOffset + 4] = ((v1 >>> 44) & 0b11111111111111111111L) | ((v2 & 0b1111111L) << 20);
            output[outputOffset + 5] = (v2 >>> 7) & 0b111111111111111111111111111L;
            output[outputOffset + 6] = (v2 >>> 34) & 0b111111111111111111111111111L;
            output[outputOffset + 7] = ((v2 >>> 61) & 0b111L) | ((v3 & 0b111111111111111111111111L) << 3);
            output[outputOffset + 8] = (v3 >>> 24) & 0b111111111111111111111111111L;
            output[outputOffset + 9] = ((v3 >>> 51) & 0b1111111111111L) | ((v4 & 0b11111111111111L) << 13);
            output[outputOffset + 10] = (v4 >>> 14) & 0b111111111111111111111111111L;
            output[outputOffset + 11] = ((v4 >>> 41) & 0b11111111111111111111111L) | ((v5 & 0b1111L) << 23);
            output[outputOffset + 12] = (v5 >>> 4) & 0b111111111111111111111111111L;
            output[outputOffset + 13] = (v5 >>> 31) & 0b111111111111111111111111111L;
            output[outputOffset + 14] = ((v5 >>> 58) & 0b111111L) | ((v6 & 0b111111111111111111111L) << 6);
            output[outputOffset + 15] = (v6 >>> 21) & 0b111111111111111111111111111L;
            output[outputOffset + 16] = ((v6 >>> 48) & 0b1111111111111111L) | ((v7 & 0b11111111111L) << 16);
            output[outputOffset + 17] = (v7 >>> 11) & 0b111111111111111111111111111L;
            output[outputOffset + 18] = ((v7 >>> 38) & 0b11111111111111111111111111L) | ((v8 & 0b1L) << 26);
            output[outputOffset + 19] = (v8 >>> 1) & 0b111111111111111111111111111L;
            output[outputOffset + 20] = (v8 >>> 28) & 0b111111111111111111111111111L;
            output[outputOffset + 21] = ((v8 >>> 55) & 0b111111111L) | ((v9 & 0b111111111111111111L) << 9);
            output[outputOffset + 22] = (v9 >>> 18) & 0b111111111111111111111111111L;
            output[outputOffset + 23] = ((v9 >>> 45) & 0b1111111111111111111L) | ((v10 & 0b11111111L) << 19);
            output[outputOffset + 24] = (v10 >>> 8) & 0b111111111111111111111111111L;
            output[outputOffset + 25] = (v10 >>> 35) & 0b111111111111111111111111111L;
            output[outputOffset + 26] = ((v10 >>> 62) & 0b11L) | ((v11 & 0b1111111111111111111111111L) << 2);
            output[outputOffset + 27] = (v11 >>> 25) & 0b111111111111111111111111111L;
            output[outputOffset + 28] = ((v11 >>> 52) & 0b111111111111L) | ((v12 & 0b111111111111111L) << 12);
            output[outputOffset + 29] = (v12 >>> 15) & 0b111111111111111111111111111L;
            output[outputOffset + 30] = ((v12 >>> 42) & 0b1111111111111111111111L) | ((v13 & 0b11111L) << 22);
            output[outputOffset + 31] = (v13 >>> 5) & 0b111111111111111111111111111L;
        }
    }

    private static final class Unpacker28
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            output[outputOffset] = v0 & 0b1111111111111111111111111111L;
            output[outputOffset + 1] = (v0 >>> 28) & 0b1111111111111111111111111111L;
            output[outputOffset + 2] = ((v0 >>> 56) & 0b11111111L) | ((v1 & 0b11111111111111111111L) << 8);
            output[outputOffset + 3] = (v1 >>> 20) & 0b1111111111111111111111111111L;
            output[outputOffset + 4] = ((v1 >>> 48) & 0b1111111111111111L) | ((v2 & 0b111111111111L) << 16);
            output[outputOffset + 5] = (v2 >>> 12) & 0b1111111111111111111111111111L;
            output[outputOffset + 6] = ((v2 >>> 40) & 0b111111111111111111111111L) | ((v3 & 0b1111L) << 24);
            output[outputOffset + 7] = (v3 >>> 4) & 0b1111111111111111111111111111L;
            output[outputOffset + 8] = (v3 >>> 32) & 0b1111111111111111111111111111L;
            output[outputOffset + 9] = ((v3 >>> 60) & 0b1111L) | ((v4 & 0b111111111111111111111111L) << 4);
            output[outputOffset + 10] = (v4 >>> 24) & 0b1111111111111111111111111111L;
            output[outputOffset + 11] = ((v4 >>> 52) & 0b111111111111L) | ((v5 & 0b1111111111111111L) << 12);
            output[outputOffset + 12] = (v5 >>> 16) & 0b1111111111111111111111111111L;
            output[outputOffset + 13] = ((v5 >>> 44) & 0b11111111111111111111L) | ((v6 & 0b11111111L) << 20);
            output[outputOffset + 14] = (v6 >>> 8) & 0b1111111111111111111111111111L;
            output[outputOffset + 15] = (v6 >>> 36) & 0b1111111111111111111111111111L;
            output[outputOffset + 16] = v7 & 0b1111111111111111111111111111L;
            output[outputOffset + 17] = (v7 >>> 28) & 0b1111111111111111111111111111L;
            output[outputOffset + 18] = ((v7 >>> 56) & 0b11111111L) | ((v8 & 0b11111111111111111111L) << 8);
            output[outputOffset + 19] = (v8 >>> 20) & 0b1111111111111111111111111111L;
            output[outputOffset + 20] = ((v8 >>> 48) & 0b1111111111111111L) | ((v9 & 0b111111111111L) << 16);
            output[outputOffset + 21] = (v9 >>> 12) & 0b1111111111111111111111111111L;
            output[outputOffset + 22] = ((v9 >>> 40) & 0b111111111111111111111111L) | ((v10 & 0b1111L) << 24);
            output[outputOffset + 23] = (v10 >>> 4) & 0b1111111111111111111111111111L;
            output[outputOffset + 24] = (v10 >>> 32) & 0b1111111111111111111111111111L;
            output[outputOffset + 25] = ((v10 >>> 60) & 0b1111L) | ((v11 & 0b111111111111111111111111L) << 4);
            output[outputOffset + 26] = (v11 >>> 24) & 0b1111111111111111111111111111L;
            output[outputOffset + 27] = ((v11 >>> 52) & 0b111111111111L) | ((v12 & 0b1111111111111111L) << 12);
            output[outputOffset + 28] = (v12 >>> 16) & 0b1111111111111111111111111111L;
            output[outputOffset + 29] = ((v12 >>> 44) & 0b11111111111111111111L) | ((v13 & 0b11111111L) << 20);
            output[outputOffset + 30] = (v13 >>> 8) & 0b1111111111111111111111111111L;
            output[outputOffset + 31] = (v13 >>> 36) & 0b1111111111111111111111111111L;
        }
    }

    private static final class Unpacker29
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            int v14 = input.readInt();
            output[outputOffset] = v0 & 0b11111111111111111111111111111L;
            output[outputOffset + 1] = (v0 >>> 29) & 0b11111111111111111111111111111L;
            output[outputOffset + 2] = ((v0 >>> 58) & 0b111111L) | ((v1 & 0b11111111111111111111111L) << 6);
            output[outputOffset + 3] = (v1 >>> 23) & 0b11111111111111111111111111111L;
            output[outputOffset + 4] = ((v1 >>> 52) & 0b111111111111L) | ((v2 & 0b11111111111111111L) << 12);
            output[outputOffset + 5] = (v2 >>> 17) & 0b11111111111111111111111111111L;
            output[outputOffset + 6] = ((v2 >>> 46) & 0b111111111111111111L) | ((v3 & 0b11111111111L) << 18);
            output[outputOffset + 7] = (v3 >>> 11) & 0b11111111111111111111111111111L;
            output[outputOffset + 8] = ((v3 >>> 40) & 0b111111111111111111111111L) | ((v4 & 0b11111L) << 24);
            output[outputOffset + 9] = (v4 >>> 5) & 0b11111111111111111111111111111L;
            output[outputOffset + 10] = (v4 >>> 34) & 0b11111111111111111111111111111L;
            output[outputOffset + 11] = ((v4 >>> 63) & 0b1L) | ((v5 & 0b1111111111111111111111111111L) << 1);
            output[outputOffset + 12] = (v5 >>> 28) & 0b11111111111111111111111111111L;
            output[outputOffset + 13] = ((v5 >>> 57) & 0b1111111L) | ((v6 & 0b1111111111111111111111L) << 7);
            output[outputOffset + 14] = (v6 >>> 22) & 0b11111111111111111111111111111L;
            output[outputOffset + 15] = ((v6 >>> 51) & 0b1111111111111L) | ((v7 & 0b1111111111111111L) << 13);
            output[outputOffset + 16] = (v7 >>> 16) & 0b11111111111111111111111111111L;
            output[outputOffset + 17] = ((v7 >>> 45) & 0b1111111111111111111L) | ((v8 & 0b1111111111L) << 19);
            output[outputOffset + 18] = (v8 >>> 10) & 0b11111111111111111111111111111L;
            output[outputOffset + 19] = ((v8 >>> 39) & 0b1111111111111111111111111L) | ((v9 & 0b1111L) << 25);
            output[outputOffset + 20] = (v9 >>> 4) & 0b11111111111111111111111111111L;
            output[outputOffset + 21] = (v9 >>> 33) & 0b11111111111111111111111111111L;
            output[outputOffset + 22] = ((v9 >>> 62) & 0b11L) | ((v10 & 0b111111111111111111111111111L) << 2);
            output[outputOffset + 23] = (v10 >>> 27) & 0b11111111111111111111111111111L;
            output[outputOffset + 24] = ((v10 >>> 56) & 0b11111111L) | ((v11 & 0b111111111111111111111L) << 8);
            output[outputOffset + 25] = (v11 >>> 21) & 0b11111111111111111111111111111L;
            output[outputOffset + 26] = ((v11 >>> 50) & 0b11111111111111L) | ((v12 & 0b111111111111111L) << 14);
            output[outputOffset + 27] = (v12 >>> 15) & 0b11111111111111111111111111111L;
            output[outputOffset + 28] = ((v12 >>> 44) & 0b11111111111111111111L) | ((v13 & 0b111111111L) << 20);
            output[outputOffset + 29] = (v13 >>> 9) & 0b11111111111111111111111111111L;
            output[outputOffset + 30] = ((v13 >>> 38) & 0b11111111111111111111111111L) | ((v14 & 0b111L) << 26);
            output[outputOffset + 31] = (v14 >>> 3) & 0b11111111111111111111111111111L;
        }
    }

    private static final class Unpacker30
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            output[outputOffset] = v0 & 0b111111111111111111111111111111L;
            output[outputOffset + 1] = (v0 >>> 30) & 0b111111111111111111111111111111L;
            output[outputOffset + 2] = ((v0 >>> 60) & 0b1111L) | ((v1 & 0b11111111111111111111111111L) << 4);
            output[outputOffset + 3] = (v1 >>> 26) & 0b111111111111111111111111111111L;
            output[outputOffset + 4] = ((v1 >>> 56) & 0b11111111L) | ((v2 & 0b1111111111111111111111L) << 8);
            output[outputOffset + 5] = (v2 >>> 22) & 0b111111111111111111111111111111L;
            output[outputOffset + 6] = ((v2 >>> 52) & 0b111111111111L) | ((v3 & 0b111111111111111111L) << 12);
            output[outputOffset + 7] = (v3 >>> 18) & 0b111111111111111111111111111111L;
            output[outputOffset + 8] = ((v3 >>> 48) & 0b1111111111111111L) | ((v4 & 0b11111111111111L) << 16);
            output[outputOffset + 9] = (v4 >>> 14) & 0b111111111111111111111111111111L;
            output[outputOffset + 10] = ((v4 >>> 44) & 0b11111111111111111111L) | ((v5 & 0b1111111111L) << 20);
            output[outputOffset + 11] = (v5 >>> 10) & 0b111111111111111111111111111111L;
            output[outputOffset + 12] = ((v5 >>> 40) & 0b111111111111111111111111L) | ((v6 & 0b111111L) << 24);
            output[outputOffset + 13] = (v6 >>> 6) & 0b111111111111111111111111111111L;
            output[outputOffset + 14] = ((v6 >>> 36) & 0b1111111111111111111111111111L) | ((v7 & 0b11L) << 28);
            output[outputOffset + 15] = (v7 >>> 2) & 0b111111111111111111111111111111L;
            output[outputOffset + 16] = (v7 >>> 32) & 0b111111111111111111111111111111L;
            output[outputOffset + 17] = ((v7 >>> 62) & 0b11L) | ((v8 & 0b1111111111111111111111111111L) << 2);
            output[outputOffset + 18] = (v8 >>> 28) & 0b111111111111111111111111111111L;
            output[outputOffset + 19] = ((v8 >>> 58) & 0b111111L) | ((v9 & 0b111111111111111111111111L) << 6);
            output[outputOffset + 20] = (v9 >>> 24) & 0b111111111111111111111111111111L;
            output[outputOffset + 21] = ((v9 >>> 54) & 0b1111111111L) | ((v10 & 0b11111111111111111111L) << 10);
            output[outputOffset + 22] = (v10 >>> 20) & 0b111111111111111111111111111111L;
            output[outputOffset + 23] = ((v10 >>> 50) & 0b11111111111111L) | ((v11 & 0b1111111111111111L) << 14);
            output[outputOffset + 24] = (v11 >>> 16) & 0b111111111111111111111111111111L;
            output[outputOffset + 25] = ((v11 >>> 46) & 0b111111111111111111L) | ((v12 & 0b111111111111L) << 18);
            output[outputOffset + 26] = (v12 >>> 12) & 0b111111111111111111111111111111L;
            output[outputOffset + 27] = ((v12 >>> 42) & 0b1111111111111111111111L) | ((v13 & 0b11111111L) << 22);
            output[outputOffset + 28] = (v13 >>> 8) & 0b111111111111111111111111111111L;
            output[outputOffset + 29] = ((v13 >>> 38) & 0b11111111111111111111111111L) | ((v14 & 0b1111L) << 26);
            output[outputOffset + 30] = (v14 >>> 4) & 0b111111111111111111111111111111L;
            output[outputOffset + 31] = (v14 >>> 34) & 0b111111111111111111111111111111L;
        }
    }

    private static final class Unpacker31
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            int v15 = input.readInt();
            output[outputOffset] = v0 & 0b1111111111111111111111111111111L;
            output[outputOffset + 1] = (v0 >>> 31) & 0b1111111111111111111111111111111L;
            output[outputOffset + 2] = ((v0 >>> 62) & 0b11L) | ((v1 & 0b11111111111111111111111111111L) << 2);
            output[outputOffset + 3] = (v1 >>> 29) & 0b1111111111111111111111111111111L;
            output[outputOffset + 4] = ((v1 >>> 60) & 0b1111L) | ((v2 & 0b111111111111111111111111111L) << 4);
            output[outputOffset + 5] = (v2 >>> 27) & 0b1111111111111111111111111111111L;
            output[outputOffset + 6] = ((v2 >>> 58) & 0b111111L) | ((v3 & 0b1111111111111111111111111L) << 6);
            output[outputOffset + 7] = (v3 >>> 25) & 0b1111111111111111111111111111111L;
            output[outputOffset + 8] = ((v3 >>> 56) & 0b11111111L) | ((v4 & 0b11111111111111111111111L) << 8);
            output[outputOffset + 9] = (v4 >>> 23) & 0b1111111111111111111111111111111L;
            output[outputOffset + 10] = ((v4 >>> 54) & 0b1111111111L) | ((v5 & 0b111111111111111111111L) << 10);
            output[outputOffset + 11] = (v5 >>> 21) & 0b1111111111111111111111111111111L;
            output[outputOffset + 12] = ((v5 >>> 52) & 0b111111111111L) | ((v6 & 0b1111111111111111111L) << 12);
            output[outputOffset + 13] = (v6 >>> 19) & 0b1111111111111111111111111111111L;
            output[outputOffset + 14] = ((v6 >>> 50) & 0b11111111111111L) | ((v7 & 0b11111111111111111L) << 14);
            output[outputOffset + 15] = (v7 >>> 17) & 0b1111111111111111111111111111111L;
            output[outputOffset + 16] = ((v7 >>> 48) & 0b1111111111111111L) | ((v8 & 0b111111111111111L) << 16);
            output[outputOffset + 17] = (v8 >>> 15) & 0b1111111111111111111111111111111L;
            output[outputOffset + 18] = ((v8 >>> 46) & 0b111111111111111111L) | ((v9 & 0b1111111111111L) << 18);
            output[outputOffset + 19] = (v9 >>> 13) & 0b1111111111111111111111111111111L;
            output[outputOffset + 20] = ((v9 >>> 44) & 0b11111111111111111111L) | ((v10 & 0b11111111111L) << 20);
            output[outputOffset + 21] = (v10 >>> 11) & 0b1111111111111111111111111111111L;
            output[outputOffset + 22] = ((v10 >>> 42) & 0b1111111111111111111111L) | ((v11 & 0b111111111L) << 22);
            output[outputOffset + 23] = (v11 >>> 9) & 0b1111111111111111111111111111111L;
            output[outputOffset + 24] = ((v11 >>> 40) & 0b111111111111111111111111L) | ((v12 & 0b1111111L) << 24);
            output[outputOffset + 25] = (v12 >>> 7) & 0b1111111111111111111111111111111L;
            output[outputOffset + 26] = ((v12 >>> 38) & 0b11111111111111111111111111L) | ((v13 & 0b11111L) << 26);
            output[outputOffset + 27] = (v13 >>> 5) & 0b1111111111111111111111111111111L;
            output[outputOffset + 28] = ((v13 >>> 36) & 0b1111111111111111111111111111L) | ((v14 & 0b111L) << 28);
            output[outputOffset + 29] = (v14 >>> 3) & 0b1111111111111111111111111111111L;
            output[outputOffset + 30] = ((v14 >>> 34) & 0b111111111111111111111111111111L) | ((v15 & 0b1L) << 30);
            output[outputOffset + 31] = (v15 >>> 1) & 0b1111111111111111111111111111111L;
        }
    }

    private static final class Unpacker32
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            output[outputOffset] = v0 & 0b11111111111111111111111111111111L;
            output[outputOffset + 1] = (v0 >>> 32) & 0b11111111111111111111111111111111L;
            output[outputOffset + 2] = v1 & 0b11111111111111111111111111111111L;
            output[outputOffset + 3] = (v1 >>> 32) & 0b11111111111111111111111111111111L;
            output[outputOffset + 4] = v2 & 0b11111111111111111111111111111111L;
            output[outputOffset + 5] = (v2 >>> 32) & 0b11111111111111111111111111111111L;
            output[outputOffset + 6] = v3 & 0b11111111111111111111111111111111L;
            output[outputOffset + 7] = (v3 >>> 32) & 0b11111111111111111111111111111111L;
            output[outputOffset + 8] = v4 & 0b11111111111111111111111111111111L;
            output[outputOffset + 9] = (v4 >>> 32) & 0b11111111111111111111111111111111L;
            output[outputOffset + 10] = v5 & 0b11111111111111111111111111111111L;
            output[outputOffset + 11] = (v5 >>> 32) & 0b11111111111111111111111111111111L;
            output[outputOffset + 12] = v6 & 0b11111111111111111111111111111111L;
            output[outputOffset + 13] = (v6 >>> 32) & 0b11111111111111111111111111111111L;
            output[outputOffset + 14] = v7 & 0b11111111111111111111111111111111L;
            output[outputOffset + 15] = (v7 >>> 32) & 0b11111111111111111111111111111111L;
            output[outputOffset + 16] = v8 & 0b11111111111111111111111111111111L;
            output[outputOffset + 17] = (v8 >>> 32) & 0b11111111111111111111111111111111L;
            output[outputOffset + 18] = v9 & 0b11111111111111111111111111111111L;
            output[outputOffset + 19] = (v9 >>> 32) & 0b11111111111111111111111111111111L;
            output[outputOffset + 20] = v10 & 0b11111111111111111111111111111111L;
            output[outputOffset + 21] = (v10 >>> 32) & 0b11111111111111111111111111111111L;
            output[outputOffset + 22] = v11 & 0b11111111111111111111111111111111L;
            output[outputOffset + 23] = (v11 >>> 32) & 0b11111111111111111111111111111111L;
            output[outputOffset + 24] = v12 & 0b11111111111111111111111111111111L;
            output[outputOffset + 25] = (v12 >>> 32) & 0b11111111111111111111111111111111L;
            output[outputOffset + 26] = v13 & 0b11111111111111111111111111111111L;
            output[outputOffset + 27] = (v13 >>> 32) & 0b11111111111111111111111111111111L;
            output[outputOffset + 28] = v14 & 0b11111111111111111111111111111111L;
            output[outputOffset + 29] = (v14 >>> 32) & 0b11111111111111111111111111111111L;
            output[outputOffset + 30] = v15 & 0b11111111111111111111111111111111L;
            output[outputOffset + 31] = (v15 >>> 32) & 0b11111111111111111111111111111111L;
        }
    }

    private static final class Unpacker33
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            int v16 = input.readInt();
            output[outputOffset] = v0 & 0b111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 33) & 0b1111111111111111111111111111111L) | ((v1 & 0b11L) << 31);
            output[outputOffset + 2] = (v1 >>> 2) & 0b111111111111111111111111111111111L;
            output[outputOffset + 3] = ((v1 >>> 35) & 0b11111111111111111111111111111L) | ((v2 & 0b1111L) << 29);
            output[outputOffset + 4] = (v2 >>> 4) & 0b111111111111111111111111111111111L;
            output[outputOffset + 5] = ((v2 >>> 37) & 0b111111111111111111111111111L) | ((v3 & 0b111111L) << 27);
            output[outputOffset + 6] = (v3 >>> 6) & 0b111111111111111111111111111111111L;
            output[outputOffset + 7] = ((v3 >>> 39) & 0b1111111111111111111111111L) | ((v4 & 0b11111111L) << 25);
            output[outputOffset + 8] = (v4 >>> 8) & 0b111111111111111111111111111111111L;
            output[outputOffset + 9] = ((v4 >>> 41) & 0b11111111111111111111111L) | ((v5 & 0b1111111111L) << 23);
            output[outputOffset + 10] = (v5 >>> 10) & 0b111111111111111111111111111111111L;
            output[outputOffset + 11] = ((v5 >>> 43) & 0b111111111111111111111L) | ((v6 & 0b111111111111L) << 21);
            output[outputOffset + 12] = (v6 >>> 12) & 0b111111111111111111111111111111111L;
            output[outputOffset + 13] = ((v6 >>> 45) & 0b1111111111111111111L) | ((v7 & 0b11111111111111L) << 19);
            output[outputOffset + 14] = (v7 >>> 14) & 0b111111111111111111111111111111111L;
            output[outputOffset + 15] = ((v7 >>> 47) & 0b11111111111111111L) | ((v8 & 0b1111111111111111L) << 17);
            output[outputOffset + 16] = (v8 >>> 16) & 0b111111111111111111111111111111111L;
            output[outputOffset + 17] = ((v8 >>> 49) & 0b111111111111111L) | ((v9 & 0b111111111111111111L) << 15);
            output[outputOffset + 18] = (v9 >>> 18) & 0b111111111111111111111111111111111L;
            output[outputOffset + 19] = ((v9 >>> 51) & 0b1111111111111L) | ((v10 & 0b11111111111111111111L) << 13);
            output[outputOffset + 20] = (v10 >>> 20) & 0b111111111111111111111111111111111L;
            output[outputOffset + 21] = ((v10 >>> 53) & 0b11111111111L) | ((v11 & 0b1111111111111111111111L) << 11);
            output[outputOffset + 22] = (v11 >>> 22) & 0b111111111111111111111111111111111L;
            output[outputOffset + 23] = ((v11 >>> 55) & 0b111111111L) | ((v12 & 0b111111111111111111111111L) << 9);
            output[outputOffset + 24] = (v12 >>> 24) & 0b111111111111111111111111111111111L;
            output[outputOffset + 25] = ((v12 >>> 57) & 0b1111111L) | ((v13 & 0b11111111111111111111111111L) << 7);
            output[outputOffset + 26] = (v13 >>> 26) & 0b111111111111111111111111111111111L;
            output[outputOffset + 27] = ((v13 >>> 59) & 0b11111L) | ((v14 & 0b1111111111111111111111111111L) << 5);
            output[outputOffset + 28] = (v14 >>> 28) & 0b111111111111111111111111111111111L;
            output[outputOffset + 29] = ((v14 >>> 61) & 0b111L) | ((v15 & 0b111111111111111111111111111111L) << 3);
            output[outputOffset + 30] = (v15 >>> 30) & 0b111111111111111111111111111111111L;
            output[outputOffset + 31] = ((v15 >>> 63) & 0b1L) | ((v16 & 0b11111111111111111111111111111111L) << 1);
        }
    }

    private static final class Unpacker34
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            output[outputOffset] = v0 & 0b1111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 34) & 0b111111111111111111111111111111L) | ((v1 & 0b1111L) << 30);
            output[outputOffset + 2] = (v1 >>> 4) & 0b1111111111111111111111111111111111L;
            output[outputOffset + 3] = ((v1 >>> 38) & 0b11111111111111111111111111L) | ((v2 & 0b11111111L) << 26);
            output[outputOffset + 4] = (v2 >>> 8) & 0b1111111111111111111111111111111111L;
            output[outputOffset + 5] = ((v2 >>> 42) & 0b1111111111111111111111L) | ((v3 & 0b111111111111L) << 22);
            output[outputOffset + 6] = (v3 >>> 12) & 0b1111111111111111111111111111111111L;
            output[outputOffset + 7] = ((v3 >>> 46) & 0b111111111111111111L) | ((v4 & 0b1111111111111111L) << 18);
            output[outputOffset + 8] = (v4 >>> 16) & 0b1111111111111111111111111111111111L;
            output[outputOffset + 9] = ((v4 >>> 50) & 0b11111111111111L) | ((v5 & 0b11111111111111111111L) << 14);
            output[outputOffset + 10] = (v5 >>> 20) & 0b1111111111111111111111111111111111L;
            output[outputOffset + 11] = ((v5 >>> 54) & 0b1111111111L) | ((v6 & 0b111111111111111111111111L) << 10);
            output[outputOffset + 12] = (v6 >>> 24) & 0b1111111111111111111111111111111111L;
            output[outputOffset + 13] = ((v6 >>> 58) & 0b111111L) | ((v7 & 0b1111111111111111111111111111L) << 6);
            output[outputOffset + 14] = (v7 >>> 28) & 0b1111111111111111111111111111111111L;
            output[outputOffset + 15] = ((v7 >>> 62) & 0b11L) | ((v8 & 0b11111111111111111111111111111111L) << 2);
            output[outputOffset + 16] = ((v8 >>> 32) & 0b11111111111111111111111111111111L) | ((v9 & 0b11L) << 32);
            output[outputOffset + 17] = (v9 >>> 2) & 0b1111111111111111111111111111111111L;
            output[outputOffset + 18] = ((v9 >>> 36) & 0b1111111111111111111111111111L) | ((v10 & 0b111111L) << 28);
            output[outputOffset + 19] = (v10 >>> 6) & 0b1111111111111111111111111111111111L;
            output[outputOffset + 20] = ((v10 >>> 40) & 0b111111111111111111111111L) | ((v11 & 0b1111111111L) << 24);
            output[outputOffset + 21] = (v11 >>> 10) & 0b1111111111111111111111111111111111L;
            output[outputOffset + 22] = ((v11 >>> 44) & 0b11111111111111111111L) | ((v12 & 0b11111111111111L) << 20);
            output[outputOffset + 23] = (v12 >>> 14) & 0b1111111111111111111111111111111111L;
            output[outputOffset + 24] = ((v12 >>> 48) & 0b1111111111111111L) | ((v13 & 0b111111111111111111L) << 16);
            output[outputOffset + 25] = (v13 >>> 18) & 0b1111111111111111111111111111111111L;
            output[outputOffset + 26] = ((v13 >>> 52) & 0b111111111111L) | ((v14 & 0b1111111111111111111111L) << 12);
            output[outputOffset + 27] = (v14 >>> 22) & 0b1111111111111111111111111111111111L;
            output[outputOffset + 28] = ((v14 >>> 56) & 0b11111111L) | ((v15 & 0b11111111111111111111111111L) << 8);
            output[outputOffset + 29] = (v15 >>> 26) & 0b1111111111111111111111111111111111L;
            output[outputOffset + 30] = ((v15 >>> 60) & 0b1111L) | ((v16 & 0b111111111111111111111111111111L) << 4);
            output[outputOffset + 31] = (v16 >>> 30) & 0b1111111111111111111111111111111111L;
        }
    }

    private static final class Unpacker35
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            int v17 = input.readInt();
            output[outputOffset] = v0 & 0b11111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 35) & 0b11111111111111111111111111111L) | ((v1 & 0b111111L) << 29);
            output[outputOffset + 2] = (v1 >>> 6) & 0b11111111111111111111111111111111111L;
            output[outputOffset + 3] = ((v1 >>> 41) & 0b11111111111111111111111L) | ((v2 & 0b111111111111L) << 23);
            output[outputOffset + 4] = (v2 >>> 12) & 0b11111111111111111111111111111111111L;
            output[outputOffset + 5] = ((v2 >>> 47) & 0b11111111111111111L) | ((v3 & 0b111111111111111111L) << 17);
            output[outputOffset + 6] = (v3 >>> 18) & 0b11111111111111111111111111111111111L;
            output[outputOffset + 7] = ((v3 >>> 53) & 0b11111111111L) | ((v4 & 0b111111111111111111111111L) << 11);
            output[outputOffset + 8] = (v4 >>> 24) & 0b11111111111111111111111111111111111L;
            output[outputOffset + 9] = ((v4 >>> 59) & 0b11111L) | ((v5 & 0b111111111111111111111111111111L) << 5);
            output[outputOffset + 10] = ((v5 >>> 30) & 0b1111111111111111111111111111111111L) | ((v6 & 0b1L) << 34);
            output[outputOffset + 11] = (v6 >>> 1) & 0b11111111111111111111111111111111111L;
            output[outputOffset + 12] = ((v6 >>> 36) & 0b1111111111111111111111111111L) | ((v7 & 0b1111111L) << 28);
            output[outputOffset + 13] = (v7 >>> 7) & 0b11111111111111111111111111111111111L;
            output[outputOffset + 14] = ((v7 >>> 42) & 0b1111111111111111111111L) | ((v8 & 0b1111111111111L) << 22);
            output[outputOffset + 15] = (v8 >>> 13) & 0b11111111111111111111111111111111111L;
            output[outputOffset + 16] = ((v8 >>> 48) & 0b1111111111111111L) | ((v9 & 0b1111111111111111111L) << 16);
            output[outputOffset + 17] = (v9 >>> 19) & 0b11111111111111111111111111111111111L;
            output[outputOffset + 18] = ((v9 >>> 54) & 0b1111111111L) | ((v10 & 0b1111111111111111111111111L) << 10);
            output[outputOffset + 19] = (v10 >>> 25) & 0b11111111111111111111111111111111111L;
            output[outputOffset + 20] = ((v10 >>> 60) & 0b1111L) | ((v11 & 0b1111111111111111111111111111111L) << 4);
            output[outputOffset + 21] = ((v11 >>> 31) & 0b111111111111111111111111111111111L) | ((v12 & 0b11L) << 33);
            output[outputOffset + 22] = (v12 >>> 2) & 0b11111111111111111111111111111111111L;
            output[outputOffset + 23] = ((v12 >>> 37) & 0b111111111111111111111111111L) | ((v13 & 0b11111111L) << 27);
            output[outputOffset + 24] = (v13 >>> 8) & 0b11111111111111111111111111111111111L;
            output[outputOffset + 25] = ((v13 >>> 43) & 0b111111111111111111111L) | ((v14 & 0b11111111111111L) << 21);
            output[outputOffset + 26] = (v14 >>> 14) & 0b11111111111111111111111111111111111L;
            output[outputOffset + 27] = ((v14 >>> 49) & 0b111111111111111L) | ((v15 & 0b11111111111111111111L) << 15);
            output[outputOffset + 28] = (v15 >>> 20) & 0b11111111111111111111111111111111111L;
            output[outputOffset + 29] = ((v15 >>> 55) & 0b111111111L) | ((v16 & 0b11111111111111111111111111L) << 9);
            output[outputOffset + 30] = (v16 >>> 26) & 0b11111111111111111111111111111111111L;
            output[outputOffset + 31] = ((v16 >>> 61) & 0b111L) | ((v17 & 0b11111111111111111111111111111111L) << 3);
        }
    }

    private static final class Unpacker36
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            output[outputOffset] = v0 & 0b111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 36) & 0b1111111111111111111111111111L) | ((v1 & 0b11111111L) << 28);
            output[outputOffset + 2] = (v1 >>> 8) & 0b111111111111111111111111111111111111L;
            output[outputOffset + 3] = ((v1 >>> 44) & 0b11111111111111111111L) | ((v2 & 0b1111111111111111L) << 20);
            output[outputOffset + 4] = (v2 >>> 16) & 0b111111111111111111111111111111111111L;
            output[outputOffset + 5] = ((v2 >>> 52) & 0b111111111111L) | ((v3 & 0b111111111111111111111111L) << 12);
            output[outputOffset + 6] = (v3 >>> 24) & 0b111111111111111111111111111111111111L;
            output[outputOffset + 7] = ((v3 >>> 60) & 0b1111L) | ((v4 & 0b11111111111111111111111111111111L) << 4);
            output[outputOffset + 8] = ((v4 >>> 32) & 0b11111111111111111111111111111111L) | ((v5 & 0b1111L) << 32);
            output[outputOffset + 9] = (v5 >>> 4) & 0b111111111111111111111111111111111111L;
            output[outputOffset + 10] = ((v5 >>> 40) & 0b111111111111111111111111L) | ((v6 & 0b111111111111L) << 24);
            output[outputOffset + 11] = (v6 >>> 12) & 0b111111111111111111111111111111111111L;
            output[outputOffset + 12] = ((v6 >>> 48) & 0b1111111111111111L) | ((v7 & 0b11111111111111111111L) << 16);
            output[outputOffset + 13] = (v7 >>> 20) & 0b111111111111111111111111111111111111L;
            output[outputOffset + 14] = ((v7 >>> 56) & 0b11111111L) | ((v8 & 0b1111111111111111111111111111L) << 8);
            output[outputOffset + 15] = (v8 >>> 28) & 0b111111111111111111111111111111111111L;
            output[outputOffset + 16] = v9 & 0b111111111111111111111111111111111111L;
            output[outputOffset + 17] = ((v9 >>> 36) & 0b1111111111111111111111111111L) | ((v10 & 0b11111111L) << 28);
            output[outputOffset + 18] = (v10 >>> 8) & 0b111111111111111111111111111111111111L;
            output[outputOffset + 19] = ((v10 >>> 44) & 0b11111111111111111111L) | ((v11 & 0b1111111111111111L) << 20);
            output[outputOffset + 20] = (v11 >>> 16) & 0b111111111111111111111111111111111111L;
            output[outputOffset + 21] = ((v11 >>> 52) & 0b111111111111L) | ((v12 & 0b111111111111111111111111L) << 12);
            output[outputOffset + 22] = (v12 >>> 24) & 0b111111111111111111111111111111111111L;
            output[outputOffset + 23] = ((v12 >>> 60) & 0b1111L) | ((v13 & 0b11111111111111111111111111111111L) << 4);
            output[outputOffset + 24] = ((v13 >>> 32) & 0b11111111111111111111111111111111L) | ((v14 & 0b1111L) << 32);
            output[outputOffset + 25] = (v14 >>> 4) & 0b111111111111111111111111111111111111L;
            output[outputOffset + 26] = ((v14 >>> 40) & 0b111111111111111111111111L) | ((v15 & 0b111111111111L) << 24);
            output[outputOffset + 27] = (v15 >>> 12) & 0b111111111111111111111111111111111111L;
            output[outputOffset + 28] = ((v15 >>> 48) & 0b1111111111111111L) | ((v16 & 0b11111111111111111111L) << 16);
            output[outputOffset + 29] = (v16 >>> 20) & 0b111111111111111111111111111111111111L;
            output[outputOffset + 30] = ((v16 >>> 56) & 0b11111111L) | ((v17 & 0b1111111111111111111111111111L) << 8);
            output[outputOffset + 31] = (v17 >>> 28) & 0b111111111111111111111111111111111111L;
        }
    }

    private static final class Unpacker37
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            int v18 = input.readInt();
            output[outputOffset] = v0 & 0b1111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 37) & 0b111111111111111111111111111L) | ((v1 & 0b1111111111L) << 27);
            output[outputOffset + 2] = (v1 >>> 10) & 0b1111111111111111111111111111111111111L;
            output[outputOffset + 3] = ((v1 >>> 47) & 0b11111111111111111L) | ((v2 & 0b11111111111111111111L) << 17);
            output[outputOffset + 4] = (v2 >>> 20) & 0b1111111111111111111111111111111111111L;
            output[outputOffset + 5] = ((v2 >>> 57) & 0b1111111L) | ((v3 & 0b111111111111111111111111111111L) << 7);
            output[outputOffset + 6] = ((v3 >>> 30) & 0b1111111111111111111111111111111111L) | ((v4 & 0b111L) << 34);
            output[outputOffset + 7] = (v4 >>> 3) & 0b1111111111111111111111111111111111111L;
            output[outputOffset + 8] = ((v4 >>> 40) & 0b111111111111111111111111L) | ((v5 & 0b1111111111111L) << 24);
            output[outputOffset + 9] = (v5 >>> 13) & 0b1111111111111111111111111111111111111L;
            output[outputOffset + 10] = ((v5 >>> 50) & 0b11111111111111L) | ((v6 & 0b11111111111111111111111L) << 14);
            output[outputOffset + 11] = (v6 >>> 23) & 0b1111111111111111111111111111111111111L;
            output[outputOffset + 12] = ((v6 >>> 60) & 0b1111L) | ((v7 & 0b111111111111111111111111111111111L) << 4);
            output[outputOffset + 13] = ((v7 >>> 33) & 0b1111111111111111111111111111111L) | ((v8 & 0b111111L) << 31);
            output[outputOffset + 14] = (v8 >>> 6) & 0b1111111111111111111111111111111111111L;
            output[outputOffset + 15] = ((v8 >>> 43) & 0b111111111111111111111L) | ((v9 & 0b1111111111111111L) << 21);
            output[outputOffset + 16] = (v9 >>> 16) & 0b1111111111111111111111111111111111111L;
            output[outputOffset + 17] = ((v9 >>> 53) & 0b11111111111L) | ((v10 & 0b11111111111111111111111111L) << 11);
            output[outputOffset + 18] = (v10 >>> 26) & 0b1111111111111111111111111111111111111L;
            output[outputOffset + 19] = ((v10 >>> 63) & 0b1L) | ((v11 & 0b111111111111111111111111111111111111L) << 1);
            output[outputOffset + 20] = ((v11 >>> 36) & 0b1111111111111111111111111111L) | ((v12 & 0b111111111L) << 28);
            output[outputOffset + 21] = (v12 >>> 9) & 0b1111111111111111111111111111111111111L;
            output[outputOffset + 22] = ((v12 >>> 46) & 0b111111111111111111L) | ((v13 & 0b1111111111111111111L) << 18);
            output[outputOffset + 23] = (v13 >>> 19) & 0b1111111111111111111111111111111111111L;
            output[outputOffset + 24] = ((v13 >>> 56) & 0b11111111L) | ((v14 & 0b11111111111111111111111111111L) << 8);
            output[outputOffset + 25] = ((v14 >>> 29) & 0b11111111111111111111111111111111111L) | ((v15 & 0b11L) << 35);
            output[outputOffset + 26] = (v15 >>> 2) & 0b1111111111111111111111111111111111111L;
            output[outputOffset + 27] = ((v15 >>> 39) & 0b1111111111111111111111111L) | ((v16 & 0b111111111111L) << 25);
            output[outputOffset + 28] = (v16 >>> 12) & 0b1111111111111111111111111111111111111L;
            output[outputOffset + 29] = ((v16 >>> 49) & 0b111111111111111L) | ((v17 & 0b1111111111111111111111L) << 15);
            output[outputOffset + 30] = (v17 >>> 22) & 0b1111111111111111111111111111111111111L;
            output[outputOffset + 31] = ((v17 >>> 59) & 0b11111L) | ((v18 & 0b11111111111111111111111111111111L) << 5);
        }
    }

    private static final class Unpacker38
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            long v18 = input.readLong();
            output[outputOffset] = v0 & 0b11111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 38) & 0b11111111111111111111111111L) | ((v1 & 0b111111111111L) << 26);
            output[outputOffset + 2] = (v1 >>> 12) & 0b11111111111111111111111111111111111111L;
            output[outputOffset + 3] = ((v1 >>> 50) & 0b11111111111111L) | ((v2 & 0b111111111111111111111111L) << 14);
            output[outputOffset + 4] = (v2 >>> 24) & 0b11111111111111111111111111111111111111L;
            output[outputOffset + 5] = ((v2 >>> 62) & 0b11L) | ((v3 & 0b111111111111111111111111111111111111L) << 2);
            output[outputOffset + 6] = ((v3 >>> 36) & 0b1111111111111111111111111111L) | ((v4 & 0b1111111111L) << 28);
            output[outputOffset + 7] = (v4 >>> 10) & 0b11111111111111111111111111111111111111L;
            output[outputOffset + 8] = ((v4 >>> 48) & 0b1111111111111111L) | ((v5 & 0b1111111111111111111111L) << 16);
            output[outputOffset + 9] = (v5 >>> 22) & 0b11111111111111111111111111111111111111L;
            output[outputOffset + 10] = ((v5 >>> 60) & 0b1111L) | ((v6 & 0b1111111111111111111111111111111111L) << 4);
            output[outputOffset + 11] = ((v6 >>> 34) & 0b111111111111111111111111111111L) | ((v7 & 0b11111111L) << 30);
            output[outputOffset + 12] = (v7 >>> 8) & 0b11111111111111111111111111111111111111L;
            output[outputOffset + 13] = ((v7 >>> 46) & 0b111111111111111111L) | ((v8 & 0b11111111111111111111L) << 18);
            output[outputOffset + 14] = (v8 >>> 20) & 0b11111111111111111111111111111111111111L;
            output[outputOffset + 15] = ((v8 >>> 58) & 0b111111L) | ((v9 & 0b11111111111111111111111111111111L) << 6);
            output[outputOffset + 16] = ((v9 >>> 32) & 0b11111111111111111111111111111111L) | ((v10 & 0b111111L) << 32);
            output[outputOffset + 17] = (v10 >>> 6) & 0b11111111111111111111111111111111111111L;
            output[outputOffset + 18] = ((v10 >>> 44) & 0b11111111111111111111L) | ((v11 & 0b111111111111111111L) << 20);
            output[outputOffset + 19] = (v11 >>> 18) & 0b11111111111111111111111111111111111111L;
            output[outputOffset + 20] = ((v11 >>> 56) & 0b11111111L) | ((v12 & 0b111111111111111111111111111111L) << 8);
            output[outputOffset + 21] = ((v12 >>> 30) & 0b1111111111111111111111111111111111L) | ((v13 & 0b1111L) << 34);
            output[outputOffset + 22] = (v13 >>> 4) & 0b11111111111111111111111111111111111111L;
            output[outputOffset + 23] = ((v13 >>> 42) & 0b1111111111111111111111L) | ((v14 & 0b1111111111111111L) << 22);
            output[outputOffset + 24] = (v14 >>> 16) & 0b11111111111111111111111111111111111111L;
            output[outputOffset + 25] = ((v14 >>> 54) & 0b1111111111L) | ((v15 & 0b1111111111111111111111111111L) << 10);
            output[outputOffset + 26] = ((v15 >>> 28) & 0b111111111111111111111111111111111111L) | ((v16 & 0b11L) << 36);
            output[outputOffset + 27] = (v16 >>> 2) & 0b11111111111111111111111111111111111111L;
            output[outputOffset + 28] = ((v16 >>> 40) & 0b111111111111111111111111L) | ((v17 & 0b11111111111111L) << 24);
            output[outputOffset + 29] = (v17 >>> 14) & 0b11111111111111111111111111111111111111L;
            output[outputOffset + 30] = ((v17 >>> 52) & 0b111111111111L) | ((v18 & 0b11111111111111111111111111L) << 12);
            output[outputOffset + 31] = (v18 >>> 26) & 0b11111111111111111111111111111111111111L;
        }
    }

    private static final class Unpacker39
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            long v18 = input.readLong();
            int v19 = input.readInt();
            output[outputOffset] = v0 & 0b111111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 39) & 0b1111111111111111111111111L) | ((v1 & 0b11111111111111L) << 25);
            output[outputOffset + 2] = (v1 >>> 14) & 0b111111111111111111111111111111111111111L;
            output[outputOffset + 3] = ((v1 >>> 53) & 0b11111111111L) | ((v2 & 0b1111111111111111111111111111L) << 11);
            output[outputOffset + 4] = ((v2 >>> 28) & 0b111111111111111111111111111111111111L) | ((v3 & 0b111L) << 36);
            output[outputOffset + 5] = (v3 >>> 3) & 0b111111111111111111111111111111111111111L;
            output[outputOffset + 6] = ((v3 >>> 42) & 0b1111111111111111111111L) | ((v4 & 0b11111111111111111L) << 22);
            output[outputOffset + 7] = (v4 >>> 17) & 0b111111111111111111111111111111111111111L;
            output[outputOffset + 8] = ((v4 >>> 56) & 0b11111111L) | ((v5 & 0b1111111111111111111111111111111L) << 8);
            output[outputOffset + 9] = ((v5 >>> 31) & 0b111111111111111111111111111111111L) | ((v6 & 0b111111L) << 33);
            output[outputOffset + 10] = (v6 >>> 6) & 0b111111111111111111111111111111111111111L;
            output[outputOffset + 11] = ((v6 >>> 45) & 0b1111111111111111111L) | ((v7 & 0b11111111111111111111L) << 19);
            output[outputOffset + 12] = (v7 >>> 20) & 0b111111111111111111111111111111111111111L;
            output[outputOffset + 13] = ((v7 >>> 59) & 0b11111L) | ((v8 & 0b1111111111111111111111111111111111L) << 5);
            output[outputOffset + 14] = ((v8 >>> 34) & 0b111111111111111111111111111111L) | ((v9 & 0b111111111L) << 30);
            output[outputOffset + 15] = (v9 >>> 9) & 0b111111111111111111111111111111111111111L;
            output[outputOffset + 16] = ((v9 >>> 48) & 0b1111111111111111L) | ((v10 & 0b11111111111111111111111L) << 16);
            output[outputOffset + 17] = (v10 >>> 23) & 0b111111111111111111111111111111111111111L;
            output[outputOffset + 18] = ((v10 >>> 62) & 0b11L) | ((v11 & 0b1111111111111111111111111111111111111L) << 2);
            output[outputOffset + 19] = ((v11 >>> 37) & 0b111111111111111111111111111L) | ((v12 & 0b111111111111L) << 27);
            output[outputOffset + 20] = (v12 >>> 12) & 0b111111111111111111111111111111111111111L;
            output[outputOffset + 21] = ((v12 >>> 51) & 0b1111111111111L) | ((v13 & 0b11111111111111111111111111L) << 13);
            output[outputOffset + 22] = ((v13 >>> 26) & 0b11111111111111111111111111111111111111L) | ((v14 & 0b1L) << 38);
            output[outputOffset + 23] = (v14 >>> 1) & 0b111111111111111111111111111111111111111L;
            output[outputOffset + 24] = ((v14 >>> 40) & 0b111111111111111111111111L) | ((v15 & 0b111111111111111L) << 24);
            output[outputOffset + 25] = (v15 >>> 15) & 0b111111111111111111111111111111111111111L;
            output[outputOffset + 26] = ((v15 >>> 54) & 0b1111111111L) | ((v16 & 0b11111111111111111111111111111L) << 10);
            output[outputOffset + 27] = ((v16 >>> 29) & 0b11111111111111111111111111111111111L) | ((v17 & 0b1111L) << 35);
            output[outputOffset + 28] = (v17 >>> 4) & 0b111111111111111111111111111111111111111L;
            output[outputOffset + 29] = ((v17 >>> 43) & 0b111111111111111111111L) | ((v18 & 0b111111111111111111L) << 21);
            output[outputOffset + 30] = (v18 >>> 18) & 0b111111111111111111111111111111111111111L;
            output[outputOffset + 31] = ((v18 >>> 57) & 0b1111111L) | ((v19 & 0b11111111111111111111111111111111L) << 7);
        }
    }

    private static final class Unpacker40
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            long v18 = input.readLong();
            long v19 = input.readLong();
            output[outputOffset] = v0 & 0b1111111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 40) & 0b111111111111111111111111L) | ((v1 & 0b1111111111111111L) << 24);
            output[outputOffset + 2] = (v1 >>> 16) & 0b1111111111111111111111111111111111111111L;
            output[outputOffset + 3] = ((v1 >>> 56) & 0b11111111L) | ((v2 & 0b11111111111111111111111111111111L) << 8);
            output[outputOffset + 4] = ((v2 >>> 32) & 0b11111111111111111111111111111111L) | ((v3 & 0b11111111L) << 32);
            output[outputOffset + 5] = (v3 >>> 8) & 0b1111111111111111111111111111111111111111L;
            output[outputOffset + 6] = ((v3 >>> 48) & 0b1111111111111111L) | ((v4 & 0b111111111111111111111111L) << 16);
            output[outputOffset + 7] = (v4 >>> 24) & 0b1111111111111111111111111111111111111111L;
            output[outputOffset + 8] = v5 & 0b1111111111111111111111111111111111111111L;
            output[outputOffset + 9] = ((v5 >>> 40) & 0b111111111111111111111111L) | ((v6 & 0b1111111111111111L) << 24);
            output[outputOffset + 10] = (v6 >>> 16) & 0b1111111111111111111111111111111111111111L;
            output[outputOffset + 11] = ((v6 >>> 56) & 0b11111111L) | ((v7 & 0b11111111111111111111111111111111L) << 8);
            output[outputOffset + 12] = ((v7 >>> 32) & 0b11111111111111111111111111111111L) | ((v8 & 0b11111111L) << 32);
            output[outputOffset + 13] = (v8 >>> 8) & 0b1111111111111111111111111111111111111111L;
            output[outputOffset + 14] = ((v8 >>> 48) & 0b1111111111111111L) | ((v9 & 0b111111111111111111111111L) << 16);
            output[outputOffset + 15] = (v9 >>> 24) & 0b1111111111111111111111111111111111111111L;
            output[outputOffset + 16] = v10 & 0b1111111111111111111111111111111111111111L;
            output[outputOffset + 17] = ((v10 >>> 40) & 0b111111111111111111111111L) | ((v11 & 0b1111111111111111L) << 24);
            output[outputOffset + 18] = (v11 >>> 16) & 0b1111111111111111111111111111111111111111L;
            output[outputOffset + 19] = ((v11 >>> 56) & 0b11111111L) | ((v12 & 0b11111111111111111111111111111111L) << 8);
            output[outputOffset + 20] = ((v12 >>> 32) & 0b11111111111111111111111111111111L) | ((v13 & 0b11111111L) << 32);
            output[outputOffset + 21] = (v13 >>> 8) & 0b1111111111111111111111111111111111111111L;
            output[outputOffset + 22] = ((v13 >>> 48) & 0b1111111111111111L) | ((v14 & 0b111111111111111111111111L) << 16);
            output[outputOffset + 23] = (v14 >>> 24) & 0b1111111111111111111111111111111111111111L;
            output[outputOffset + 24] = v15 & 0b1111111111111111111111111111111111111111L;
            output[outputOffset + 25] = ((v15 >>> 40) & 0b111111111111111111111111L) | ((v16 & 0b1111111111111111L) << 24);
            output[outputOffset + 26] = (v16 >>> 16) & 0b1111111111111111111111111111111111111111L;
            output[outputOffset + 27] = ((v16 >>> 56) & 0b11111111L) | ((v17 & 0b11111111111111111111111111111111L) << 8);
            output[outputOffset + 28] = ((v17 >>> 32) & 0b11111111111111111111111111111111L) | ((v18 & 0b11111111L) << 32);
            output[outputOffset + 29] = (v18 >>> 8) & 0b1111111111111111111111111111111111111111L;
            output[outputOffset + 30] = ((v18 >>> 48) & 0b1111111111111111L) | ((v19 & 0b111111111111111111111111L) << 16);
            output[outputOffset + 31] = (v19 >>> 24) & 0b1111111111111111111111111111111111111111L;
        }
    }

    private static final class Unpacker41
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            long v18 = input.readLong();
            long v19 = input.readLong();
            int v20 = input.readInt();
            output[outputOffset] = v0 & 0b11111111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 41) & 0b11111111111111111111111L) | ((v1 & 0b111111111111111111L) << 23);
            output[outputOffset + 2] = (v1 >>> 18) & 0b11111111111111111111111111111111111111111L;
            output[outputOffset + 3] = ((v1 >>> 59) & 0b11111L) | ((v2 & 0b111111111111111111111111111111111111L) << 5);
            output[outputOffset + 4] = ((v2 >>> 36) & 0b1111111111111111111111111111L) | ((v3 & 0b1111111111111L) << 28);
            output[outputOffset + 5] = (v3 >>> 13) & 0b11111111111111111111111111111111111111111L;
            output[outputOffset + 6] = ((v3 >>> 54) & 0b1111111111L) | ((v4 & 0b1111111111111111111111111111111L) << 10);
            output[outputOffset + 7] = ((v4 >>> 31) & 0b111111111111111111111111111111111L) | ((v5 & 0b11111111L) << 33);
            output[outputOffset + 8] = (v5 >>> 8) & 0b11111111111111111111111111111111111111111L;
            output[outputOffset + 9] = ((v5 >>> 49) & 0b111111111111111L) | ((v6 & 0b11111111111111111111111111L) << 15);
            output[outputOffset + 10] = ((v6 >>> 26) & 0b11111111111111111111111111111111111111L) | ((v7 & 0b111L) << 38);
            output[outputOffset + 11] = (v7 >>> 3) & 0b11111111111111111111111111111111111111111L;
            output[outputOffset + 12] = ((v7 >>> 44) & 0b11111111111111111111L) | ((v8 & 0b111111111111111111111L) << 20);
            output[outputOffset + 13] = (v8 >>> 21) & 0b11111111111111111111111111111111111111111L;
            output[outputOffset + 14] = ((v8 >>> 62) & 0b11L) | ((v9 & 0b111111111111111111111111111111111111111L) << 2);
            output[outputOffset + 15] = ((v9 >>> 39) & 0b1111111111111111111111111L) | ((v10 & 0b1111111111111111L) << 25);
            output[outputOffset + 16] = (v10 >>> 16) & 0b11111111111111111111111111111111111111111L;
            output[outputOffset + 17] = ((v10 >>> 57) & 0b1111111L) | ((v11 & 0b1111111111111111111111111111111111L) << 7);
            output[outputOffset + 18] = ((v11 >>> 34) & 0b111111111111111111111111111111L) | ((v12 & 0b11111111111L) << 30);
            output[outputOffset + 19] = (v12 >>> 11) & 0b11111111111111111111111111111111111111111L;
            output[outputOffset + 20] = ((v12 >>> 52) & 0b111111111111L) | ((v13 & 0b11111111111111111111111111111L) << 12);
            output[outputOffset + 21] = ((v13 >>> 29) & 0b11111111111111111111111111111111111L) | ((v14 & 0b111111L) << 35);
            output[outputOffset + 22] = (v14 >>> 6) & 0b11111111111111111111111111111111111111111L;
            output[outputOffset + 23] = ((v14 >>> 47) & 0b11111111111111111L) | ((v15 & 0b111111111111111111111111L) << 17);
            output[outputOffset + 24] = ((v15 >>> 24) & 0b1111111111111111111111111111111111111111L) | ((v16 & 0b1L) << 40);
            output[outputOffset + 25] = (v16 >>> 1) & 0b11111111111111111111111111111111111111111L;
            output[outputOffset + 26] = ((v16 >>> 42) & 0b1111111111111111111111L) | ((v17 & 0b1111111111111111111L) << 22);
            output[outputOffset + 27] = (v17 >>> 19) & 0b11111111111111111111111111111111111111111L;
            output[outputOffset + 28] = ((v17 >>> 60) & 0b1111L) | ((v18 & 0b1111111111111111111111111111111111111L) << 4);
            output[outputOffset + 29] = ((v18 >>> 37) & 0b111111111111111111111111111L) | ((v19 & 0b11111111111111L) << 27);
            output[outputOffset + 30] = (v19 >>> 14) & 0b11111111111111111111111111111111111111111L;
            output[outputOffset + 31] = ((v19 >>> 55) & 0b111111111L) | ((v20 & 0b11111111111111111111111111111111L) << 9);
        }
    }

    private static final class Unpacker42
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            long v18 = input.readLong();
            long v19 = input.readLong();
            long v20 = input.readLong();
            output[outputOffset] = v0 & 0b111111111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 42) & 0b1111111111111111111111L) | ((v1 & 0b11111111111111111111L) << 22);
            output[outputOffset + 2] = (v1 >>> 20) & 0b111111111111111111111111111111111111111111L;
            output[outputOffset + 3] = ((v1 >>> 62) & 0b11L) | ((v2 & 0b1111111111111111111111111111111111111111L) << 2);
            output[outputOffset + 4] = ((v2 >>> 40) & 0b111111111111111111111111L) | ((v3 & 0b111111111111111111L) << 24);
            output[outputOffset + 5] = (v3 >>> 18) & 0b111111111111111111111111111111111111111111L;
            output[outputOffset + 6] = ((v3 >>> 60) & 0b1111L) | ((v4 & 0b11111111111111111111111111111111111111L) << 4);
            output[outputOffset + 7] = ((v4 >>> 38) & 0b11111111111111111111111111L) | ((v5 & 0b1111111111111111L) << 26);
            output[outputOffset + 8] = (v5 >>> 16) & 0b111111111111111111111111111111111111111111L;
            output[outputOffset + 9] = ((v5 >>> 58) & 0b111111L) | ((v6 & 0b111111111111111111111111111111111111L) << 6);
            output[outputOffset + 10] = ((v6 >>> 36) & 0b1111111111111111111111111111L) | ((v7 & 0b11111111111111L) << 28);
            output[outputOffset + 11] = (v7 >>> 14) & 0b111111111111111111111111111111111111111111L;
            output[outputOffset + 12] = ((v7 >>> 56) & 0b11111111L) | ((v8 & 0b1111111111111111111111111111111111L) << 8);
            output[outputOffset + 13] = ((v8 >>> 34) & 0b111111111111111111111111111111L) | ((v9 & 0b111111111111L) << 30);
            output[outputOffset + 14] = (v9 >>> 12) & 0b111111111111111111111111111111111111111111L;
            output[outputOffset + 15] = ((v9 >>> 54) & 0b1111111111L) | ((v10 & 0b11111111111111111111111111111111L) << 10);
            output[outputOffset + 16] = ((v10 >>> 32) & 0b11111111111111111111111111111111L) | ((v11 & 0b1111111111L) << 32);
            output[outputOffset + 17] = (v11 >>> 10) & 0b111111111111111111111111111111111111111111L;
            output[outputOffset + 18] = ((v11 >>> 52) & 0b111111111111L) | ((v12 & 0b111111111111111111111111111111L) << 12);
            output[outputOffset + 19] = ((v12 >>> 30) & 0b1111111111111111111111111111111111L) | ((v13 & 0b11111111L) << 34);
            output[outputOffset + 20] = (v13 >>> 8) & 0b111111111111111111111111111111111111111111L;
            output[outputOffset + 21] = ((v13 >>> 50) & 0b11111111111111L) | ((v14 & 0b1111111111111111111111111111L) << 14);
            output[outputOffset + 22] = ((v14 >>> 28) & 0b111111111111111111111111111111111111L) | ((v15 & 0b111111L) << 36);
            output[outputOffset + 23] = (v15 >>> 6) & 0b111111111111111111111111111111111111111111L;
            output[outputOffset + 24] = ((v15 >>> 48) & 0b1111111111111111L) | ((v16 & 0b11111111111111111111111111L) << 16);
            output[outputOffset + 25] = ((v16 >>> 26) & 0b11111111111111111111111111111111111111L) | ((v17 & 0b1111L) << 38);
            output[outputOffset + 26] = (v17 >>> 4) & 0b111111111111111111111111111111111111111111L;
            output[outputOffset + 27] = ((v17 >>> 46) & 0b111111111111111111L) | ((v18 & 0b111111111111111111111111L) << 18);
            output[outputOffset + 28] = ((v18 >>> 24) & 0b1111111111111111111111111111111111111111L) | ((v19 & 0b11L) << 40);
            output[outputOffset + 29] = (v19 >>> 2) & 0b111111111111111111111111111111111111111111L;
            output[outputOffset + 30] = ((v19 >>> 44) & 0b11111111111111111111L) | ((v20 & 0b1111111111111111111111L) << 20);
            output[outputOffset + 31] = (v20 >>> 22) & 0b111111111111111111111111111111111111111111L;
        }
    }

    private static final class Unpacker43
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            long v18 = input.readLong();
            long v19 = input.readLong();
            long v20 = input.readLong();
            int v21 = input.readInt();
            output[outputOffset] = v0 & 0b1111111111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 43) & 0b111111111111111111111L) | ((v1 & 0b1111111111111111111111L) << 21);
            output[outputOffset + 2] = ((v1 >>> 22) & 0b111111111111111111111111111111111111111111L) | ((v2 & 0b1L) << 42);
            output[outputOffset + 3] = (v2 >>> 1) & 0b1111111111111111111111111111111111111111111L;
            output[outputOffset + 4] = ((v2 >>> 44) & 0b11111111111111111111L) | ((v3 & 0b11111111111111111111111L) << 20);
            output[outputOffset + 5] = ((v3 >>> 23) & 0b11111111111111111111111111111111111111111L) | ((v4 & 0b11L) << 41);
            output[outputOffset + 6] = (v4 >>> 2) & 0b1111111111111111111111111111111111111111111L;
            output[outputOffset + 7] = ((v4 >>> 45) & 0b1111111111111111111L) | ((v5 & 0b111111111111111111111111L) << 19);
            output[outputOffset + 8] = ((v5 >>> 24) & 0b1111111111111111111111111111111111111111L) | ((v6 & 0b111L) << 40);
            output[outputOffset + 9] = (v6 >>> 3) & 0b1111111111111111111111111111111111111111111L;
            output[outputOffset + 10] = ((v6 >>> 46) & 0b111111111111111111L) | ((v7 & 0b1111111111111111111111111L) << 18);
            output[outputOffset + 11] = ((v7 >>> 25) & 0b111111111111111111111111111111111111111L) | ((v8 & 0b1111L) << 39);
            output[outputOffset + 12] = (v8 >>> 4) & 0b1111111111111111111111111111111111111111111L;
            output[outputOffset + 13] = ((v8 >>> 47) & 0b11111111111111111L) | ((v9 & 0b11111111111111111111111111L) << 17);
            output[outputOffset + 14] = ((v9 >>> 26) & 0b11111111111111111111111111111111111111L) | ((v10 & 0b11111L) << 38);
            output[outputOffset + 15] = (v10 >>> 5) & 0b1111111111111111111111111111111111111111111L;
            output[outputOffset + 16] = ((v10 >>> 48) & 0b1111111111111111L) | ((v11 & 0b111111111111111111111111111L) << 16);
            output[outputOffset + 17] = ((v11 >>> 27) & 0b1111111111111111111111111111111111111L) | ((v12 & 0b111111L) << 37);
            output[outputOffset + 18] = (v12 >>> 6) & 0b1111111111111111111111111111111111111111111L;
            output[outputOffset + 19] = ((v12 >>> 49) & 0b111111111111111L) | ((v13 & 0b1111111111111111111111111111L) << 15);
            output[outputOffset + 20] = ((v13 >>> 28) & 0b111111111111111111111111111111111111L) | ((v14 & 0b1111111L) << 36);
            output[outputOffset + 21] = (v14 >>> 7) & 0b1111111111111111111111111111111111111111111L;
            output[outputOffset + 22] = ((v14 >>> 50) & 0b11111111111111L) | ((v15 & 0b11111111111111111111111111111L) << 14);
            output[outputOffset + 23] = ((v15 >>> 29) & 0b11111111111111111111111111111111111L) | ((v16 & 0b11111111L) << 35);
            output[outputOffset + 24] = (v16 >>> 8) & 0b1111111111111111111111111111111111111111111L;
            output[outputOffset + 25] = ((v16 >>> 51) & 0b1111111111111L) | ((v17 & 0b111111111111111111111111111111L) << 13);
            output[outputOffset + 26] = ((v17 >>> 30) & 0b1111111111111111111111111111111111L) | ((v18 & 0b111111111L) << 34);
            output[outputOffset + 27] = (v18 >>> 9) & 0b1111111111111111111111111111111111111111111L;
            output[outputOffset + 28] = ((v18 >>> 52) & 0b111111111111L) | ((v19 & 0b1111111111111111111111111111111L) << 12);
            output[outputOffset + 29] = ((v19 >>> 31) & 0b111111111111111111111111111111111L) | ((v20 & 0b1111111111L) << 33);
            output[outputOffset + 30] = (v20 >>> 10) & 0b1111111111111111111111111111111111111111111L;
            output[outputOffset + 31] = ((v20 >>> 53) & 0b11111111111L) | ((v21 & 0b11111111111111111111111111111111L) << 11);
        }
    }

    private static final class Unpacker44
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            long v18 = input.readLong();
            long v19 = input.readLong();
            long v20 = input.readLong();
            long v21 = input.readLong();
            output[outputOffset] = v0 & 0b11111111111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 44) & 0b11111111111111111111L) | ((v1 & 0b111111111111111111111111L) << 20);
            output[outputOffset + 2] = ((v1 >>> 24) & 0b1111111111111111111111111111111111111111L) | ((v2 & 0b1111L) << 40);
            output[outputOffset + 3] = (v2 >>> 4) & 0b11111111111111111111111111111111111111111111L;
            output[outputOffset + 4] = ((v2 >>> 48) & 0b1111111111111111L) | ((v3 & 0b1111111111111111111111111111L) << 16);
            output[outputOffset + 5] = ((v3 >>> 28) & 0b111111111111111111111111111111111111L) | ((v4 & 0b11111111L) << 36);
            output[outputOffset + 6] = (v4 >>> 8) & 0b11111111111111111111111111111111111111111111L;
            output[outputOffset + 7] = ((v4 >>> 52) & 0b111111111111L) | ((v5 & 0b11111111111111111111111111111111L) << 12);
            output[outputOffset + 8] = ((v5 >>> 32) & 0b11111111111111111111111111111111L) | ((v6 & 0b111111111111L) << 32);
            output[outputOffset + 9] = (v6 >>> 12) & 0b11111111111111111111111111111111111111111111L;
            output[outputOffset + 10] = ((v6 >>> 56) & 0b11111111L) | ((v7 & 0b111111111111111111111111111111111111L) << 8);
            output[outputOffset + 11] = ((v7 >>> 36) & 0b1111111111111111111111111111L) | ((v8 & 0b1111111111111111L) << 28);
            output[outputOffset + 12] = (v8 >>> 16) & 0b11111111111111111111111111111111111111111111L;
            output[outputOffset + 13] = ((v8 >>> 60) & 0b1111L) | ((v9 & 0b1111111111111111111111111111111111111111L) << 4);
            output[outputOffset + 14] = ((v9 >>> 40) & 0b111111111111111111111111L) | ((v10 & 0b11111111111111111111L) << 24);
            output[outputOffset + 15] = (v10 >>> 20) & 0b11111111111111111111111111111111111111111111L;
            output[outputOffset + 16] = v11 & 0b11111111111111111111111111111111111111111111L;
            output[outputOffset + 17] = ((v11 >>> 44) & 0b11111111111111111111L) | ((v12 & 0b111111111111111111111111L) << 20);
            output[outputOffset + 18] = ((v12 >>> 24) & 0b1111111111111111111111111111111111111111L) | ((v13 & 0b1111L) << 40);
            output[outputOffset + 19] = (v13 >>> 4) & 0b11111111111111111111111111111111111111111111L;
            output[outputOffset + 20] = ((v13 >>> 48) & 0b1111111111111111L) | ((v14 & 0b1111111111111111111111111111L) << 16);
            output[outputOffset + 21] = ((v14 >>> 28) & 0b111111111111111111111111111111111111L) | ((v15 & 0b11111111L) << 36);
            output[outputOffset + 22] = (v15 >>> 8) & 0b11111111111111111111111111111111111111111111L;
            output[outputOffset + 23] = ((v15 >>> 52) & 0b111111111111L) | ((v16 & 0b11111111111111111111111111111111L) << 12);
            output[outputOffset + 24] = ((v16 >>> 32) & 0b11111111111111111111111111111111L) | ((v17 & 0b111111111111L) << 32);
            output[outputOffset + 25] = (v17 >>> 12) & 0b11111111111111111111111111111111111111111111L;
            output[outputOffset + 26] = ((v17 >>> 56) & 0b11111111L) | ((v18 & 0b111111111111111111111111111111111111L) << 8);
            output[outputOffset + 27] = ((v18 >>> 36) & 0b1111111111111111111111111111L) | ((v19 & 0b1111111111111111L) << 28);
            output[outputOffset + 28] = (v19 >>> 16) & 0b11111111111111111111111111111111111111111111L;
            output[outputOffset + 29] = ((v19 >>> 60) & 0b1111L) | ((v20 & 0b1111111111111111111111111111111111111111L) << 4);
            output[outputOffset + 30] = ((v20 >>> 40) & 0b111111111111111111111111L) | ((v21 & 0b11111111111111111111L) << 24);
            output[outputOffset + 31] = (v21 >>> 20) & 0b11111111111111111111111111111111111111111111L;
        }
    }

    private static final class Unpacker45
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            long v18 = input.readLong();
            long v19 = input.readLong();
            long v20 = input.readLong();
            long v21 = input.readLong();
            int v22 = input.readInt();
            output[outputOffset] = v0 & 0b111111111111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 45) & 0b1111111111111111111L) | ((v1 & 0b11111111111111111111111111L) << 19);
            output[outputOffset + 2] = ((v1 >>> 26) & 0b11111111111111111111111111111111111111L) | ((v2 & 0b1111111L) << 38);
            output[outputOffset + 3] = (v2 >>> 7) & 0b111111111111111111111111111111111111111111111L;
            output[outputOffset + 4] = ((v2 >>> 52) & 0b111111111111L) | ((v3 & 0b111111111111111111111111111111111L) << 12);
            output[outputOffset + 5] = ((v3 >>> 33) & 0b1111111111111111111111111111111L) | ((v4 & 0b11111111111111L) << 31);
            output[outputOffset + 6] = (v4 >>> 14) & 0b111111111111111111111111111111111111111111111L;
            output[outputOffset + 7] = ((v4 >>> 59) & 0b11111L) | ((v5 & 0b1111111111111111111111111111111111111111L) << 5);
            output[outputOffset + 8] = ((v5 >>> 40) & 0b111111111111111111111111L) | ((v6 & 0b111111111111111111111L) << 24);
            output[outputOffset + 9] = ((v6 >>> 21) & 0b1111111111111111111111111111111111111111111L) | ((v7 & 0b11L) << 43);
            output[outputOffset + 10] = (v7 >>> 2) & 0b111111111111111111111111111111111111111111111L;
            output[outputOffset + 11] = ((v7 >>> 47) & 0b11111111111111111L) | ((v8 & 0b1111111111111111111111111111L) << 17);
            output[outputOffset + 12] = ((v8 >>> 28) & 0b111111111111111111111111111111111111L) | ((v9 & 0b111111111L) << 36);
            output[outputOffset + 13] = (v9 >>> 9) & 0b111111111111111111111111111111111111111111111L;
            output[outputOffset + 14] = ((v9 >>> 54) & 0b1111111111L) | ((v10 & 0b11111111111111111111111111111111111L) << 10);
            output[outputOffset + 15] = ((v10 >>> 35) & 0b11111111111111111111111111111L) | ((v11 & 0b1111111111111111L) << 29);
            output[outputOffset + 16] = (v11 >>> 16) & 0b111111111111111111111111111111111111111111111L;
            output[outputOffset + 17] = ((v11 >>> 61) & 0b111L) | ((v12 & 0b111111111111111111111111111111111111111111L) << 3);
            output[outputOffset + 18] = ((v12 >>> 42) & 0b1111111111111111111111L) | ((v13 & 0b11111111111111111111111L) << 22);
            output[outputOffset + 19] = ((v13 >>> 23) & 0b11111111111111111111111111111111111111111L) | ((v14 & 0b1111L) << 41);
            output[outputOffset + 20] = (v14 >>> 4) & 0b111111111111111111111111111111111111111111111L;
            output[outputOffset + 21] = ((v14 >>> 49) & 0b111111111111111L) | ((v15 & 0b111111111111111111111111111111L) << 15);
            output[outputOffset + 22] = ((v15 >>> 30) & 0b1111111111111111111111111111111111L) | ((v16 & 0b11111111111L) << 34);
            output[outputOffset + 23] = (v16 >>> 11) & 0b111111111111111111111111111111111111111111111L;
            output[outputOffset + 24] = ((v16 >>> 56) & 0b11111111L) | ((v17 & 0b1111111111111111111111111111111111111L) << 8);
            output[outputOffset + 25] = ((v17 >>> 37) & 0b111111111111111111111111111L) | ((v18 & 0b111111111111111111L) << 27);
            output[outputOffset + 26] = (v18 >>> 18) & 0b111111111111111111111111111111111111111111111L;
            output[outputOffset + 27] = ((v18 >>> 63) & 0b1L) | ((v19 & 0b11111111111111111111111111111111111111111111L) << 1);
            output[outputOffset + 28] = ((v19 >>> 44) & 0b11111111111111111111L) | ((v20 & 0b1111111111111111111111111L) << 20);
            output[outputOffset + 29] = ((v20 >>> 25) & 0b111111111111111111111111111111111111111L) | ((v21 & 0b111111L) << 39);
            output[outputOffset + 30] = (v21 >>> 6) & 0b111111111111111111111111111111111111111111111L;
            output[outputOffset + 31] = ((v21 >>> 51) & 0b1111111111111L) | ((v22 & 0b11111111111111111111111111111111L) << 13);
        }
    }

    private static final class Unpacker46
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            long v18 = input.readLong();
            long v19 = input.readLong();
            long v20 = input.readLong();
            long v21 = input.readLong();
            long v22 = input.readLong();
            output[outputOffset] = v0 & 0b1111111111111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 46) & 0b111111111111111111L) | ((v1 & 0b1111111111111111111111111111L) << 18);
            output[outputOffset + 2] = ((v1 >>> 28) & 0b111111111111111111111111111111111111L) | ((v2 & 0b1111111111L) << 36);
            output[outputOffset + 3] = (v2 >>> 10) & 0b1111111111111111111111111111111111111111111111L;
            output[outputOffset + 4] = ((v2 >>> 56) & 0b11111111L) | ((v3 & 0b11111111111111111111111111111111111111L) << 8);
            output[outputOffset + 5] = ((v3 >>> 38) & 0b11111111111111111111111111L) | ((v4 & 0b11111111111111111111L) << 26);
            output[outputOffset + 6] = ((v4 >>> 20) & 0b11111111111111111111111111111111111111111111L) | ((v5 & 0b11L) << 44);
            output[outputOffset + 7] = (v5 >>> 2) & 0b1111111111111111111111111111111111111111111111L;
            output[outputOffset + 8] = ((v5 >>> 48) & 0b1111111111111111L) | ((v6 & 0b111111111111111111111111111111L) << 16);
            output[outputOffset + 9] = ((v6 >>> 30) & 0b1111111111111111111111111111111111L) | ((v7 & 0b111111111111L) << 34);
            output[outputOffset + 10] = (v7 >>> 12) & 0b1111111111111111111111111111111111111111111111L;
            output[outputOffset + 11] = ((v7 >>> 58) & 0b111111L) | ((v8 & 0b1111111111111111111111111111111111111111L) << 6);
            output[outputOffset + 12] = ((v8 >>> 40) & 0b111111111111111111111111L) | ((v9 & 0b1111111111111111111111L) << 24);
            output[outputOffset + 13] = ((v9 >>> 22) & 0b111111111111111111111111111111111111111111L) | ((v10 & 0b1111L) << 42);
            output[outputOffset + 14] = (v10 >>> 4) & 0b1111111111111111111111111111111111111111111111L;
            output[outputOffset + 15] = ((v10 >>> 50) & 0b11111111111111L) | ((v11 & 0b11111111111111111111111111111111L) << 14);
            output[outputOffset + 16] = ((v11 >>> 32) & 0b11111111111111111111111111111111L) | ((v12 & 0b11111111111111L) << 32);
            output[outputOffset + 17] = (v12 >>> 14) & 0b1111111111111111111111111111111111111111111111L;
            output[outputOffset + 18] = ((v12 >>> 60) & 0b1111L) | ((v13 & 0b111111111111111111111111111111111111111111L) << 4);
            output[outputOffset + 19] = ((v13 >>> 42) & 0b1111111111111111111111L) | ((v14 & 0b111111111111111111111111L) << 22);
            output[outputOffset + 20] = ((v14 >>> 24) & 0b1111111111111111111111111111111111111111L) | ((v15 & 0b111111L) << 40);
            output[outputOffset + 21] = (v15 >>> 6) & 0b1111111111111111111111111111111111111111111111L;
            output[outputOffset + 22] = ((v15 >>> 52) & 0b111111111111L) | ((v16 & 0b1111111111111111111111111111111111L) << 12);
            output[outputOffset + 23] = ((v16 >>> 34) & 0b111111111111111111111111111111L) | ((v17 & 0b1111111111111111L) << 30);
            output[outputOffset + 24] = (v17 >>> 16) & 0b1111111111111111111111111111111111111111111111L;
            output[outputOffset + 25] = ((v17 >>> 62) & 0b11L) | ((v18 & 0b11111111111111111111111111111111111111111111L) << 2);
            output[outputOffset + 26] = ((v18 >>> 44) & 0b11111111111111111111L) | ((v19 & 0b11111111111111111111111111L) << 20);
            output[outputOffset + 27] = ((v19 >>> 26) & 0b11111111111111111111111111111111111111L) | ((v20 & 0b11111111L) << 38);
            output[outputOffset + 28] = (v20 >>> 8) & 0b1111111111111111111111111111111111111111111111L;
            output[outputOffset + 29] = ((v20 >>> 54) & 0b1111111111L) | ((v21 & 0b111111111111111111111111111111111111L) << 10);
            output[outputOffset + 30] = ((v21 >>> 36) & 0b1111111111111111111111111111L) | ((v22 & 0b111111111111111111L) << 28);
            output[outputOffset + 31] = (v22 >>> 18) & 0b1111111111111111111111111111111111111111111111L;
        }
    }

    private static final class Unpacker47
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            long v18 = input.readLong();
            long v19 = input.readLong();
            long v20 = input.readLong();
            long v21 = input.readLong();
            long v22 = input.readLong();
            int v23 = input.readInt();
            output[outputOffset] = v0 & 0b11111111111111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 47) & 0b11111111111111111L) | ((v1 & 0b111111111111111111111111111111L) << 17);
            output[outputOffset + 2] = ((v1 >>> 30) & 0b1111111111111111111111111111111111L) | ((v2 & 0b1111111111111L) << 34);
            output[outputOffset + 3] = (v2 >>> 13) & 0b11111111111111111111111111111111111111111111111L;
            output[outputOffset + 4] = ((v2 >>> 60) & 0b1111L) | ((v3 & 0b1111111111111111111111111111111111111111111L) << 4);
            output[outputOffset + 5] = ((v3 >>> 43) & 0b111111111111111111111L) | ((v4 & 0b11111111111111111111111111L) << 21);
            output[outputOffset + 6] = ((v4 >>> 26) & 0b11111111111111111111111111111111111111L) | ((v5 & 0b111111111L) << 38);
            output[outputOffset + 7] = (v5 >>> 9) & 0b11111111111111111111111111111111111111111111111L;
            output[outputOffset + 8] = ((v5 >>> 56) & 0b11111111L) | ((v6 & 0b111111111111111111111111111111111111111L) << 8);
            output[outputOffset + 9] = ((v6 >>> 39) & 0b1111111111111111111111111L) | ((v7 & 0b1111111111111111111111L) << 25);
            output[outputOffset + 10] = ((v7 >>> 22) & 0b111111111111111111111111111111111111111111L) | ((v8 & 0b11111L) << 42);
            output[outputOffset + 11] = (v8 >>> 5) & 0b11111111111111111111111111111111111111111111111L;
            output[outputOffset + 12] = ((v8 >>> 52) & 0b111111111111L) | ((v9 & 0b11111111111111111111111111111111111L) << 12);
            output[outputOffset + 13] = ((v9 >>> 35) & 0b11111111111111111111111111111L) | ((v10 & 0b111111111111111111L) << 29);
            output[outputOffset + 14] = ((v10 >>> 18) & 0b1111111111111111111111111111111111111111111111L) | ((v11 & 0b1L) << 46);
            output[outputOffset + 15] = (v11 >>> 1) & 0b11111111111111111111111111111111111111111111111L;
            output[outputOffset + 16] = ((v11 >>> 48) & 0b1111111111111111L) | ((v12 & 0b1111111111111111111111111111111L) << 16);
            output[outputOffset + 17] = ((v12 >>> 31) & 0b111111111111111111111111111111111L) | ((v13 & 0b11111111111111L) << 33);
            output[outputOffset + 18] = (v13 >>> 14) & 0b11111111111111111111111111111111111111111111111L;
            output[outputOffset + 19] = ((v13 >>> 61) & 0b111L) | ((v14 & 0b11111111111111111111111111111111111111111111L) << 3);
            output[outputOffset + 20] = ((v14 >>> 44) & 0b11111111111111111111L) | ((v15 & 0b111111111111111111111111111L) << 20);
            output[outputOffset + 21] = ((v15 >>> 27) & 0b1111111111111111111111111111111111111L) | ((v16 & 0b1111111111L) << 37);
            output[outputOffset + 22] = (v16 >>> 10) & 0b11111111111111111111111111111111111111111111111L;
            output[outputOffset + 23] = ((v16 >>> 57) & 0b1111111L) | ((v17 & 0b1111111111111111111111111111111111111111L) << 7);
            output[outputOffset + 24] = ((v17 >>> 40) & 0b111111111111111111111111L) | ((v18 & 0b11111111111111111111111L) << 24);
            output[outputOffset + 25] = ((v18 >>> 23) & 0b11111111111111111111111111111111111111111L) | ((v19 & 0b111111L) << 41);
            output[outputOffset + 26] = (v19 >>> 6) & 0b11111111111111111111111111111111111111111111111L;
            output[outputOffset + 27] = ((v19 >>> 53) & 0b11111111111L) | ((v20 & 0b111111111111111111111111111111111111L) << 11);
            output[outputOffset + 28] = ((v20 >>> 36) & 0b1111111111111111111111111111L) | ((v21 & 0b1111111111111111111L) << 28);
            output[outputOffset + 29] = ((v21 >>> 19) & 0b111111111111111111111111111111111111111111111L) | ((v22 & 0b11L) << 45);
            output[outputOffset + 30] = (v22 >>> 2) & 0b11111111111111111111111111111111111111111111111L;
            output[outputOffset + 31] = ((v22 >>> 49) & 0b111111111111111L) | ((v23 & 0b11111111111111111111111111111111L) << 15);
        }
    }

    private static final class Unpacker48
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            long v18 = input.readLong();
            long v19 = input.readLong();
            long v20 = input.readLong();
            long v21 = input.readLong();
            long v22 = input.readLong();
            long v23 = input.readLong();
            output[outputOffset] = v0 & 0b111111111111111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 48) & 0b1111111111111111L) | ((v1 & 0b11111111111111111111111111111111L) << 16);
            output[outputOffset + 2] = ((v1 >>> 32) & 0b11111111111111111111111111111111L) | ((v2 & 0b1111111111111111L) << 32);
            output[outputOffset + 3] = (v2 >>> 16) & 0b111111111111111111111111111111111111111111111111L;
            output[outputOffset + 4] = v3 & 0b111111111111111111111111111111111111111111111111L;
            output[outputOffset + 5] = ((v3 >>> 48) & 0b1111111111111111L) | ((v4 & 0b11111111111111111111111111111111L) << 16);
            output[outputOffset + 6] = ((v4 >>> 32) & 0b11111111111111111111111111111111L) | ((v5 & 0b1111111111111111L) << 32);
            output[outputOffset + 7] = (v5 >>> 16) & 0b111111111111111111111111111111111111111111111111L;
            output[outputOffset + 8] = v6 & 0b111111111111111111111111111111111111111111111111L;
            output[outputOffset + 9] = ((v6 >>> 48) & 0b1111111111111111L) | ((v7 & 0b11111111111111111111111111111111L) << 16);
            output[outputOffset + 10] = ((v7 >>> 32) & 0b11111111111111111111111111111111L) | ((v8 & 0b1111111111111111L) << 32);
            output[outputOffset + 11] = (v8 >>> 16) & 0b111111111111111111111111111111111111111111111111L;
            output[outputOffset + 12] = v9 & 0b111111111111111111111111111111111111111111111111L;
            output[outputOffset + 13] = ((v9 >>> 48) & 0b1111111111111111L) | ((v10 & 0b11111111111111111111111111111111L) << 16);
            output[outputOffset + 14] = ((v10 >>> 32) & 0b11111111111111111111111111111111L) | ((v11 & 0b1111111111111111L) << 32);
            output[outputOffset + 15] = (v11 >>> 16) & 0b111111111111111111111111111111111111111111111111L;
            output[outputOffset + 16] = v12 & 0b111111111111111111111111111111111111111111111111L;
            output[outputOffset + 17] = ((v12 >>> 48) & 0b1111111111111111L) | ((v13 & 0b11111111111111111111111111111111L) << 16);
            output[outputOffset + 18] = ((v13 >>> 32) & 0b11111111111111111111111111111111L) | ((v14 & 0b1111111111111111L) << 32);
            output[outputOffset + 19] = (v14 >>> 16) & 0b111111111111111111111111111111111111111111111111L;
            output[outputOffset + 20] = v15 & 0b111111111111111111111111111111111111111111111111L;
            output[outputOffset + 21] = ((v15 >>> 48) & 0b1111111111111111L) | ((v16 & 0b11111111111111111111111111111111L) << 16);
            output[outputOffset + 22] = ((v16 >>> 32) & 0b11111111111111111111111111111111L) | ((v17 & 0b1111111111111111L) << 32);
            output[outputOffset + 23] = (v17 >>> 16) & 0b111111111111111111111111111111111111111111111111L;
            output[outputOffset + 24] = v18 & 0b111111111111111111111111111111111111111111111111L;
            output[outputOffset + 25] = ((v18 >>> 48) & 0b1111111111111111L) | ((v19 & 0b11111111111111111111111111111111L) << 16);
            output[outputOffset + 26] = ((v19 >>> 32) & 0b11111111111111111111111111111111L) | ((v20 & 0b1111111111111111L) << 32);
            output[outputOffset + 27] = (v20 >>> 16) & 0b111111111111111111111111111111111111111111111111L;
            output[outputOffset + 28] = v21 & 0b111111111111111111111111111111111111111111111111L;
            output[outputOffset + 29] = ((v21 >>> 48) & 0b1111111111111111L) | ((v22 & 0b11111111111111111111111111111111L) << 16);
            output[outputOffset + 30] = ((v22 >>> 32) & 0b11111111111111111111111111111111L) | ((v23 & 0b1111111111111111L) << 32);
            output[outputOffset + 31] = (v23 >>> 16) & 0b111111111111111111111111111111111111111111111111L;
        }
    }

    private static final class Unpacker49
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            long v18 = input.readLong();
            long v19 = input.readLong();
            long v20 = input.readLong();
            long v21 = input.readLong();
            long v22 = input.readLong();
            long v23 = input.readLong();
            int v24 = input.readInt();
            output[outputOffset] = v0 & 0b1111111111111111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 49) & 0b111111111111111L) | ((v1 & 0b1111111111111111111111111111111111L) << 15);
            output[outputOffset + 2] = ((v1 >>> 34) & 0b111111111111111111111111111111L) | ((v2 & 0b1111111111111111111L) << 30);
            output[outputOffset + 3] = ((v2 >>> 19) & 0b111111111111111111111111111111111111111111111L) | ((v3 & 0b1111L) << 45);
            output[outputOffset + 4] = (v3 >>> 4) & 0b1111111111111111111111111111111111111111111111111L;
            output[outputOffset + 5] = ((v3 >>> 53) & 0b11111111111L) | ((v4 & 0b11111111111111111111111111111111111111L) << 11);
            output[outputOffset + 6] = ((v4 >>> 38) & 0b11111111111111111111111111L) | ((v5 & 0b11111111111111111111111L) << 26);
            output[outputOffset + 7] = ((v5 >>> 23) & 0b11111111111111111111111111111111111111111L) | ((v6 & 0b11111111L) << 41);
            output[outputOffset + 8] = (v6 >>> 8) & 0b1111111111111111111111111111111111111111111111111L;
            output[outputOffset + 9] = ((v6 >>> 57) & 0b1111111L) | ((v7 & 0b111111111111111111111111111111111111111111L) << 7);
            output[outputOffset + 10] = ((v7 >>> 42) & 0b1111111111111111111111L) | ((v8 & 0b111111111111111111111111111L) << 22);
            output[outputOffset + 11] = ((v8 >>> 27) & 0b1111111111111111111111111111111111111L) | ((v9 & 0b111111111111L) << 37);
            output[outputOffset + 12] = (v9 >>> 12) & 0b1111111111111111111111111111111111111111111111111L;
            output[outputOffset + 13] = ((v9 >>> 61) & 0b111L) | ((v10 & 0b1111111111111111111111111111111111111111111111L) << 3);
            output[outputOffset + 14] = ((v10 >>> 46) & 0b111111111111111111L) | ((v11 & 0b1111111111111111111111111111111L) << 18);
            output[outputOffset + 15] = ((v11 >>> 31) & 0b111111111111111111111111111111111L) | ((v12 & 0b1111111111111111L) << 33);
            output[outputOffset + 16] = ((v12 >>> 16) & 0b111111111111111111111111111111111111111111111111L) | ((v13 & 0b1L) << 48);
            output[outputOffset + 17] = (v13 >>> 1) & 0b1111111111111111111111111111111111111111111111111L;
            output[outputOffset + 18] = ((v13 >>> 50) & 0b11111111111111L) | ((v14 & 0b11111111111111111111111111111111111L) << 14);
            output[outputOffset + 19] = ((v14 >>> 35) & 0b11111111111111111111111111111L) | ((v15 & 0b11111111111111111111L) << 29);
            output[outputOffset + 20] = ((v15 >>> 20) & 0b11111111111111111111111111111111111111111111L) | ((v16 & 0b11111L) << 44);
            output[outputOffset + 21] = (v16 >>> 5) & 0b1111111111111111111111111111111111111111111111111L;
            output[outputOffset + 22] = ((v16 >>> 54) & 0b1111111111L) | ((v17 & 0b111111111111111111111111111111111111111L) << 10);
            output[outputOffset + 23] = ((v17 >>> 39) & 0b1111111111111111111111111L) | ((v18 & 0b111111111111111111111111L) << 25);
            output[outputOffset + 24] = ((v18 >>> 24) & 0b1111111111111111111111111111111111111111L) | ((v19 & 0b111111111L) << 40);
            output[outputOffset + 25] = (v19 >>> 9) & 0b1111111111111111111111111111111111111111111111111L;
            output[outputOffset + 26] = ((v19 >>> 58) & 0b111111L) | ((v20 & 0b1111111111111111111111111111111111111111111L) << 6);
            output[outputOffset + 27] = ((v20 >>> 43) & 0b111111111111111111111L) | ((v21 & 0b1111111111111111111111111111L) << 21);
            output[outputOffset + 28] = ((v21 >>> 28) & 0b111111111111111111111111111111111111L) | ((v22 & 0b1111111111111L) << 36);
            output[outputOffset + 29] = (v22 >>> 13) & 0b1111111111111111111111111111111111111111111111111L;
            output[outputOffset + 30] = ((v22 >>> 62) & 0b11L) | ((v23 & 0b11111111111111111111111111111111111111111111111L) << 2);
            output[outputOffset + 31] = ((v23 >>> 47) & 0b11111111111111111L) | ((v24 & 0b11111111111111111111111111111111L) << 17);
        }
    }

    private static final class Unpacker50
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            long v18 = input.readLong();
            long v19 = input.readLong();
            long v20 = input.readLong();
            long v21 = input.readLong();
            long v22 = input.readLong();
            long v23 = input.readLong();
            long v24 = input.readLong();
            output[outputOffset] = v0 & 0b11111111111111111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 50) & 0b11111111111111L) | ((v1 & 0b111111111111111111111111111111111111L) << 14);
            output[outputOffset + 2] = ((v1 >>> 36) & 0b1111111111111111111111111111L) | ((v2 & 0b1111111111111111111111L) << 28);
            output[outputOffset + 3] = ((v2 >>> 22) & 0b111111111111111111111111111111111111111111L) | ((v3 & 0b11111111L) << 42);
            output[outputOffset + 4] = (v3 >>> 8) & 0b11111111111111111111111111111111111111111111111111L;
            output[outputOffset + 5] = ((v3 >>> 58) & 0b111111L) | ((v4 & 0b11111111111111111111111111111111111111111111L) << 6);
            output[outputOffset + 6] = ((v4 >>> 44) & 0b11111111111111111111L) | ((v5 & 0b111111111111111111111111111111L) << 20);
            output[outputOffset + 7] = ((v5 >>> 30) & 0b1111111111111111111111111111111111L) | ((v6 & 0b1111111111111111L) << 34);
            output[outputOffset + 8] = ((v6 >>> 16) & 0b111111111111111111111111111111111111111111111111L) | ((v7 & 0b11L) << 48);
            output[outputOffset + 9] = (v7 >>> 2) & 0b11111111111111111111111111111111111111111111111111L;
            output[outputOffset + 10] = ((v7 >>> 52) & 0b111111111111L) | ((v8 & 0b11111111111111111111111111111111111111L) << 12);
            output[outputOffset + 11] = ((v8 >>> 38) & 0b11111111111111111111111111L) | ((v9 & 0b111111111111111111111111L) << 26);
            output[outputOffset + 12] = ((v9 >>> 24) & 0b1111111111111111111111111111111111111111L) | ((v10 & 0b1111111111L) << 40);
            output[outputOffset + 13] = (v10 >>> 10) & 0b11111111111111111111111111111111111111111111111111L;
            output[outputOffset + 14] = ((v10 >>> 60) & 0b1111L) | ((v11 & 0b1111111111111111111111111111111111111111111111L) << 4);
            output[outputOffset + 15] = ((v11 >>> 46) & 0b111111111111111111L) | ((v12 & 0b11111111111111111111111111111111L) << 18);
            output[outputOffset + 16] = ((v12 >>> 32) & 0b11111111111111111111111111111111L) | ((v13 & 0b111111111111111111L) << 32);
            output[outputOffset + 17] = ((v13 >>> 18) & 0b1111111111111111111111111111111111111111111111L) | ((v14 & 0b1111L) << 46);
            output[outputOffset + 18] = (v14 >>> 4) & 0b11111111111111111111111111111111111111111111111111L;
            output[outputOffset + 19] = ((v14 >>> 54) & 0b1111111111L) | ((v15 & 0b1111111111111111111111111111111111111111L) << 10);
            output[outputOffset + 20] = ((v15 >>> 40) & 0b111111111111111111111111L) | ((v16 & 0b11111111111111111111111111L) << 24);
            output[outputOffset + 21] = ((v16 >>> 26) & 0b11111111111111111111111111111111111111L) | ((v17 & 0b111111111111L) << 38);
            output[outputOffset + 22] = (v17 >>> 12) & 0b11111111111111111111111111111111111111111111111111L;
            output[outputOffset + 23] = ((v17 >>> 62) & 0b11L) | ((v18 & 0b111111111111111111111111111111111111111111111111L) << 2);
            output[outputOffset + 24] = ((v18 >>> 48) & 0b1111111111111111L) | ((v19 & 0b1111111111111111111111111111111111L) << 16);
            output[outputOffset + 25] = ((v19 >>> 34) & 0b111111111111111111111111111111L) | ((v20 & 0b11111111111111111111L) << 30);
            output[outputOffset + 26] = ((v20 >>> 20) & 0b11111111111111111111111111111111111111111111L) | ((v21 & 0b111111L) << 44);
            output[outputOffset + 27] = (v21 >>> 6) & 0b11111111111111111111111111111111111111111111111111L;
            output[outputOffset + 28] = ((v21 >>> 56) & 0b11111111L) | ((v22 & 0b111111111111111111111111111111111111111111L) << 8);
            output[outputOffset + 29] = ((v22 >>> 42) & 0b1111111111111111111111L) | ((v23 & 0b1111111111111111111111111111L) << 22);
            output[outputOffset + 30] = ((v23 >>> 28) & 0b111111111111111111111111111111111111L) | ((v24 & 0b11111111111111L) << 36);
            output[outputOffset + 31] = (v24 >>> 14) & 0b11111111111111111111111111111111111111111111111111L;
        }
    }

    private static final class Unpacker51
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            long v18 = input.readLong();
            long v19 = input.readLong();
            long v20 = input.readLong();
            long v21 = input.readLong();
            long v22 = input.readLong();
            long v23 = input.readLong();
            long v24 = input.readLong();
            int v25 = input.readInt();
            output[outputOffset] = v0 & 0b111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 51) & 0b1111111111111L) | ((v1 & 0b11111111111111111111111111111111111111L) << 13);
            output[outputOffset + 2] = ((v1 >>> 38) & 0b11111111111111111111111111L) | ((v2 & 0b1111111111111111111111111L) << 26);
            output[outputOffset + 3] = ((v2 >>> 25) & 0b111111111111111111111111111111111111111L) | ((v3 & 0b111111111111L) << 39);
            output[outputOffset + 4] = (v3 >>> 12) & 0b111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 5] = ((v3 >>> 63) & 0b1L) | ((v4 & 0b11111111111111111111111111111111111111111111111111L) << 1);
            output[outputOffset + 6] = ((v4 >>> 50) & 0b11111111111111L) | ((v5 & 0b1111111111111111111111111111111111111L) << 14);
            output[outputOffset + 7] = ((v5 >>> 37) & 0b111111111111111111111111111L) | ((v6 & 0b111111111111111111111111L) << 27);
            output[outputOffset + 8] = ((v6 >>> 24) & 0b1111111111111111111111111111111111111111L) | ((v7 & 0b11111111111L) << 40);
            output[outputOffset + 9] = (v7 >>> 11) & 0b111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 10] = ((v7 >>> 62) & 0b11L) | ((v8 & 0b1111111111111111111111111111111111111111111111111L) << 2);
            output[outputOffset + 11] = ((v8 >>> 49) & 0b111111111111111L) | ((v9 & 0b111111111111111111111111111111111111L) << 15);
            output[outputOffset + 12] = ((v9 >>> 36) & 0b1111111111111111111111111111L) | ((v10 & 0b11111111111111111111111L) << 28);
            output[outputOffset + 13] = ((v10 >>> 23) & 0b11111111111111111111111111111111111111111L) | ((v11 & 0b1111111111L) << 41);
            output[outputOffset + 14] = (v11 >>> 10) & 0b111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 15] = ((v11 >>> 61) & 0b111L) | ((v12 & 0b111111111111111111111111111111111111111111111111L) << 3);
            output[outputOffset + 16] = ((v12 >>> 48) & 0b1111111111111111L) | ((v13 & 0b11111111111111111111111111111111111L) << 16);
            output[outputOffset + 17] = ((v13 >>> 35) & 0b11111111111111111111111111111L) | ((v14 & 0b1111111111111111111111L) << 29);
            output[outputOffset + 18] = ((v14 >>> 22) & 0b111111111111111111111111111111111111111111L) | ((v15 & 0b111111111L) << 42);
            output[outputOffset + 19] = (v15 >>> 9) & 0b111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 20] = ((v15 >>> 60) & 0b1111L) | ((v16 & 0b11111111111111111111111111111111111111111111111L) << 4);
            output[outputOffset + 21] = ((v16 >>> 47) & 0b11111111111111111L) | ((v17 & 0b1111111111111111111111111111111111L) << 17);
            output[outputOffset + 22] = ((v17 >>> 34) & 0b111111111111111111111111111111L) | ((v18 & 0b111111111111111111111L) << 30);
            output[outputOffset + 23] = ((v18 >>> 21) & 0b1111111111111111111111111111111111111111111L) | ((v19 & 0b11111111L) << 43);
            output[outputOffset + 24] = (v19 >>> 8) & 0b111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 25] = ((v19 >>> 59) & 0b11111L) | ((v20 & 0b1111111111111111111111111111111111111111111111L) << 5);
            output[outputOffset + 26] = ((v20 >>> 46) & 0b111111111111111111L) | ((v21 & 0b111111111111111111111111111111111L) << 18);
            output[outputOffset + 27] = ((v21 >>> 33) & 0b1111111111111111111111111111111L) | ((v22 & 0b11111111111111111111L) << 31);
            output[outputOffset + 28] = ((v22 >>> 20) & 0b11111111111111111111111111111111111111111111L) | ((v23 & 0b1111111L) << 44);
            output[outputOffset + 29] = (v23 >>> 7) & 0b111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 30] = ((v23 >>> 58) & 0b111111L) | ((v24 & 0b111111111111111111111111111111111111111111111L) << 6);
            output[outputOffset + 31] = ((v24 >>> 45) & 0b1111111111111111111L) | ((v25 & 0b11111111111111111111111111111111L) << 19);
        }
    }

    private static final class Unpacker52
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            long v18 = input.readLong();
            long v19 = input.readLong();
            long v20 = input.readLong();
            long v21 = input.readLong();
            long v22 = input.readLong();
            long v23 = input.readLong();
            long v24 = input.readLong();
            long v25 = input.readLong();
            output[outputOffset] = v0 & 0b1111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 52) & 0b111111111111L) | ((v1 & 0b1111111111111111111111111111111111111111L) << 12);
            output[outputOffset + 2] = ((v1 >>> 40) & 0b111111111111111111111111L) | ((v2 & 0b1111111111111111111111111111L) << 24);
            output[outputOffset + 3] = ((v2 >>> 28) & 0b111111111111111111111111111111111111L) | ((v3 & 0b1111111111111111L) << 36);
            output[outputOffset + 4] = ((v3 >>> 16) & 0b111111111111111111111111111111111111111111111111L) | ((v4 & 0b1111L) << 48);
            output[outputOffset + 5] = (v4 >>> 4) & 0b1111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 6] = ((v4 >>> 56) & 0b11111111L) | ((v5 & 0b11111111111111111111111111111111111111111111L) << 8);
            output[outputOffset + 7] = ((v5 >>> 44) & 0b11111111111111111111L) | ((v6 & 0b11111111111111111111111111111111L) << 20);
            output[outputOffset + 8] = ((v6 >>> 32) & 0b11111111111111111111111111111111L) | ((v7 & 0b11111111111111111111L) << 32);
            output[outputOffset + 9] = ((v7 >>> 20) & 0b11111111111111111111111111111111111111111111L) | ((v8 & 0b11111111L) << 44);
            output[outputOffset + 10] = (v8 >>> 8) & 0b1111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 11] = ((v8 >>> 60) & 0b1111L) | ((v9 & 0b111111111111111111111111111111111111111111111111L) << 4);
            output[outputOffset + 12] = ((v9 >>> 48) & 0b1111111111111111L) | ((v10 & 0b111111111111111111111111111111111111L) << 16);
            output[outputOffset + 13] = ((v10 >>> 36) & 0b1111111111111111111111111111L) | ((v11 & 0b111111111111111111111111L) << 28);
            output[outputOffset + 14] = ((v11 >>> 24) & 0b1111111111111111111111111111111111111111L) | ((v12 & 0b111111111111L) << 40);
            output[outputOffset + 15] = (v12 >>> 12) & 0b1111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 16] = v13 & 0b1111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 17] = ((v13 >>> 52) & 0b111111111111L) | ((v14 & 0b1111111111111111111111111111111111111111L) << 12);
            output[outputOffset + 18] = ((v14 >>> 40) & 0b111111111111111111111111L) | ((v15 & 0b1111111111111111111111111111L) << 24);
            output[outputOffset + 19] = ((v15 >>> 28) & 0b111111111111111111111111111111111111L) | ((v16 & 0b1111111111111111L) << 36);
            output[outputOffset + 20] = ((v16 >>> 16) & 0b111111111111111111111111111111111111111111111111L) | ((v17 & 0b1111L) << 48);
            output[outputOffset + 21] = (v17 >>> 4) & 0b1111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 22] = ((v17 >>> 56) & 0b11111111L) | ((v18 & 0b11111111111111111111111111111111111111111111L) << 8);
            output[outputOffset + 23] = ((v18 >>> 44) & 0b11111111111111111111L) | ((v19 & 0b11111111111111111111111111111111L) << 20);
            output[outputOffset + 24] = ((v19 >>> 32) & 0b11111111111111111111111111111111L) | ((v20 & 0b11111111111111111111L) << 32);
            output[outputOffset + 25] = ((v20 >>> 20) & 0b11111111111111111111111111111111111111111111L) | ((v21 & 0b11111111L) << 44);
            output[outputOffset + 26] = (v21 >>> 8) & 0b1111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 27] = ((v21 >>> 60) & 0b1111L) | ((v22 & 0b111111111111111111111111111111111111111111111111L) << 4);
            output[outputOffset + 28] = ((v22 >>> 48) & 0b1111111111111111L) | ((v23 & 0b111111111111111111111111111111111111L) << 16);
            output[outputOffset + 29] = ((v23 >>> 36) & 0b1111111111111111111111111111L) | ((v24 & 0b111111111111111111111111L) << 28);
            output[outputOffset + 30] = ((v24 >>> 24) & 0b1111111111111111111111111111111111111111L) | ((v25 & 0b111111111111L) << 40);
            output[outputOffset + 31] = (v25 >>> 12) & 0b1111111111111111111111111111111111111111111111111111L;
        }
    }

    private static final class Unpacker53
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            long v18 = input.readLong();
            long v19 = input.readLong();
            long v20 = input.readLong();
            long v21 = input.readLong();
            long v22 = input.readLong();
            long v23 = input.readLong();
            long v24 = input.readLong();
            long v25 = input.readLong();
            int v26 = input.readInt();
            output[outputOffset] = v0 & 0b11111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 53) & 0b11111111111L) | ((v1 & 0b111111111111111111111111111111111111111111L) << 11);
            output[outputOffset + 2] = ((v1 >>> 42) & 0b1111111111111111111111L) | ((v2 & 0b1111111111111111111111111111111L) << 22);
            output[outputOffset + 3] = ((v2 >>> 31) & 0b111111111111111111111111111111111L) | ((v3 & 0b11111111111111111111L) << 33);
            output[outputOffset + 4] = ((v3 >>> 20) & 0b11111111111111111111111111111111111111111111L) | ((v4 & 0b111111111L) << 44);
            output[outputOffset + 5] = (v4 >>> 9) & 0b11111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 6] = ((v4 >>> 62) & 0b11L) | ((v5 & 0b111111111111111111111111111111111111111111111111111L) << 2);
            output[outputOffset + 7] = ((v5 >>> 51) & 0b1111111111111L) | ((v6 & 0b1111111111111111111111111111111111111111L) << 13);
            output[outputOffset + 8] = ((v6 >>> 40) & 0b111111111111111111111111L) | ((v7 & 0b11111111111111111111111111111L) << 24);
            output[outputOffset + 9] = ((v7 >>> 29) & 0b11111111111111111111111111111111111L) | ((v8 & 0b111111111111111111L) << 35);
            output[outputOffset + 10] = ((v8 >>> 18) & 0b1111111111111111111111111111111111111111111111L) | ((v9 & 0b1111111L) << 46);
            output[outputOffset + 11] = (v9 >>> 7) & 0b11111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 12] = ((v9 >>> 60) & 0b1111L) | ((v10 & 0b1111111111111111111111111111111111111111111111111L) << 4);
            output[outputOffset + 13] = ((v10 >>> 49) & 0b111111111111111L) | ((v11 & 0b11111111111111111111111111111111111111L) << 15);
            output[outputOffset + 14] = ((v11 >>> 38) & 0b11111111111111111111111111L) | ((v12 & 0b111111111111111111111111111L) << 26);
            output[outputOffset + 15] = ((v12 >>> 27) & 0b1111111111111111111111111111111111111L) | ((v13 & 0b1111111111111111L) << 37);
            output[outputOffset + 16] = ((v13 >>> 16) & 0b111111111111111111111111111111111111111111111111L) | ((v14 & 0b11111L) << 48);
            output[outputOffset + 17] = (v14 >>> 5) & 0b11111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 18] = ((v14 >>> 58) & 0b111111L) | ((v15 & 0b11111111111111111111111111111111111111111111111L) << 6);
            output[outputOffset + 19] = ((v15 >>> 47) & 0b11111111111111111L) | ((v16 & 0b111111111111111111111111111111111111L) << 17);
            output[outputOffset + 20] = ((v16 >>> 36) & 0b1111111111111111111111111111L) | ((v17 & 0b1111111111111111111111111L) << 28);
            output[outputOffset + 21] = ((v17 >>> 25) & 0b111111111111111111111111111111111111111L) | ((v18 & 0b11111111111111L) << 39);
            output[outputOffset + 22] = ((v18 >>> 14) & 0b11111111111111111111111111111111111111111111111111L) | ((v19 & 0b111L) << 50);
            output[outputOffset + 23] = (v19 >>> 3) & 0b11111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 24] = ((v19 >>> 56) & 0b11111111L) | ((v20 & 0b111111111111111111111111111111111111111111111L) << 8);
            output[outputOffset + 25] = ((v20 >>> 45) & 0b1111111111111111111L) | ((v21 & 0b1111111111111111111111111111111111L) << 19);
            output[outputOffset + 26] = ((v21 >>> 34) & 0b111111111111111111111111111111L) | ((v22 & 0b11111111111111111111111L) << 30);
            output[outputOffset + 27] = ((v22 >>> 23) & 0b11111111111111111111111111111111111111111L) | ((v23 & 0b111111111111L) << 41);
            output[outputOffset + 28] = ((v23 >>> 12) & 0b1111111111111111111111111111111111111111111111111111L) | ((v24 & 0b1L) << 52);
            output[outputOffset + 29] = (v24 >>> 1) & 0b11111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 30] = ((v24 >>> 54) & 0b1111111111L) | ((v25 & 0b1111111111111111111111111111111111111111111L) << 10);
            output[outputOffset + 31] = ((v25 >>> 43) & 0b111111111111111111111L) | ((v26 & 0b11111111111111111111111111111111L) << 21);
        }
    }

    private static final class Unpacker54
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            long v18 = input.readLong();
            long v19 = input.readLong();
            long v20 = input.readLong();
            long v21 = input.readLong();
            long v22 = input.readLong();
            long v23 = input.readLong();
            long v24 = input.readLong();
            long v25 = input.readLong();
            long v26 = input.readLong();
            output[outputOffset] = v0 & 0b111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 54) & 0b1111111111L) | ((v1 & 0b11111111111111111111111111111111111111111111L) << 10);
            output[outputOffset + 2] = ((v1 >>> 44) & 0b11111111111111111111L) | ((v2 & 0b1111111111111111111111111111111111L) << 20);
            output[outputOffset + 3] = ((v2 >>> 34) & 0b111111111111111111111111111111L) | ((v3 & 0b111111111111111111111111L) << 30);
            output[outputOffset + 4] = ((v3 >>> 24) & 0b1111111111111111111111111111111111111111L) | ((v4 & 0b11111111111111L) << 40);
            output[outputOffset + 5] = ((v4 >>> 14) & 0b11111111111111111111111111111111111111111111111111L) | ((v5 & 0b1111L) << 50);
            output[outputOffset + 6] = (v5 >>> 4) & 0b111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 7] = ((v5 >>> 58) & 0b111111L) | ((v6 & 0b111111111111111111111111111111111111111111111111L) << 6);
            output[outputOffset + 8] = ((v6 >>> 48) & 0b1111111111111111L) | ((v7 & 0b11111111111111111111111111111111111111L) << 16);
            output[outputOffset + 9] = ((v7 >>> 38) & 0b11111111111111111111111111L) | ((v8 & 0b1111111111111111111111111111L) << 26);
            output[outputOffset + 10] = ((v8 >>> 28) & 0b111111111111111111111111111111111111L) | ((v9 & 0b111111111111111111L) << 36);
            output[outputOffset + 11] = ((v9 >>> 18) & 0b1111111111111111111111111111111111111111111111L) | ((v10 & 0b11111111L) << 46);
            output[outputOffset + 12] = (v10 >>> 8) & 0b111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 13] = ((v10 >>> 62) & 0b11L) | ((v11 & 0b1111111111111111111111111111111111111111111111111111L) << 2);
            output[outputOffset + 14] = ((v11 >>> 52) & 0b111111111111L) | ((v12 & 0b111111111111111111111111111111111111111111L) << 12);
            output[outputOffset + 15] = ((v12 >>> 42) & 0b1111111111111111111111L) | ((v13 & 0b11111111111111111111111111111111L) << 22);
            output[outputOffset + 16] = ((v13 >>> 32) & 0b11111111111111111111111111111111L) | ((v14 & 0b1111111111111111111111L) << 32);
            output[outputOffset + 17] = ((v14 >>> 22) & 0b111111111111111111111111111111111111111111L) | ((v15 & 0b111111111111L) << 42);
            output[outputOffset + 18] = ((v15 >>> 12) & 0b1111111111111111111111111111111111111111111111111111L) | ((v16 & 0b11L) << 52);
            output[outputOffset + 19] = (v16 >>> 2) & 0b111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 20] = ((v16 >>> 56) & 0b11111111L) | ((v17 & 0b1111111111111111111111111111111111111111111111L) << 8);
            output[outputOffset + 21] = ((v17 >>> 46) & 0b111111111111111111L) | ((v18 & 0b111111111111111111111111111111111111L) << 18);
            output[outputOffset + 22] = ((v18 >>> 36) & 0b1111111111111111111111111111L) | ((v19 & 0b11111111111111111111111111L) << 28);
            output[outputOffset + 23] = ((v19 >>> 26) & 0b11111111111111111111111111111111111111L) | ((v20 & 0b1111111111111111L) << 38);
            output[outputOffset + 24] = ((v20 >>> 16) & 0b111111111111111111111111111111111111111111111111L) | ((v21 & 0b111111L) << 48);
            output[outputOffset + 25] = (v21 >>> 6) & 0b111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 26] = ((v21 >>> 60) & 0b1111L) | ((v22 & 0b11111111111111111111111111111111111111111111111111L) << 4);
            output[outputOffset + 27] = ((v22 >>> 50) & 0b11111111111111L) | ((v23 & 0b1111111111111111111111111111111111111111L) << 14);
            output[outputOffset + 28] = ((v23 >>> 40) & 0b111111111111111111111111L) | ((v24 & 0b111111111111111111111111111111L) << 24);
            output[outputOffset + 29] = ((v24 >>> 30) & 0b1111111111111111111111111111111111L) | ((v25 & 0b11111111111111111111L) << 34);
            output[outputOffset + 30] = ((v25 >>> 20) & 0b11111111111111111111111111111111111111111111L) | ((v26 & 0b1111111111L) << 44);
            output[outputOffset + 31] = (v26 >>> 10) & 0b111111111111111111111111111111111111111111111111111111L;
        }
    }

    private static final class Unpacker55
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            long v18 = input.readLong();
            long v19 = input.readLong();
            long v20 = input.readLong();
            long v21 = input.readLong();
            long v22 = input.readLong();
            long v23 = input.readLong();
            long v24 = input.readLong();
            long v25 = input.readLong();
            long v26 = input.readLong();
            int v27 = input.readInt();
            output[outputOffset] = v0 & 0b1111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 55) & 0b111111111L) | ((v1 & 0b1111111111111111111111111111111111111111111111L) << 9);
            output[outputOffset + 2] = ((v1 >>> 46) & 0b111111111111111111L) | ((v2 & 0b1111111111111111111111111111111111111L) << 18);
            output[outputOffset + 3] = ((v2 >>> 37) & 0b111111111111111111111111111L) | ((v3 & 0b1111111111111111111111111111L) << 27);
            output[outputOffset + 4] = ((v3 >>> 28) & 0b111111111111111111111111111111111111L) | ((v4 & 0b1111111111111111111L) << 36);
            output[outputOffset + 5] = ((v4 >>> 19) & 0b111111111111111111111111111111111111111111111L) | ((v5 & 0b1111111111L) << 45);
            output[outputOffset + 6] = ((v5 >>> 10) & 0b111111111111111111111111111111111111111111111111111111L) | ((v6 & 0b1L) << 54);
            output[outputOffset + 7] = (v6 >>> 1) & 0b1111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 8] = ((v6 >>> 56) & 0b11111111L) | ((v7 & 0b11111111111111111111111111111111111111111111111L) << 8);
            output[outputOffset + 9] = ((v7 >>> 47) & 0b11111111111111111L) | ((v8 & 0b11111111111111111111111111111111111111L) << 17);
            output[outputOffset + 10] = ((v8 >>> 38) & 0b11111111111111111111111111L) | ((v9 & 0b11111111111111111111111111111L) << 26);
            output[outputOffset + 11] = ((v9 >>> 29) & 0b11111111111111111111111111111111111L) | ((v10 & 0b11111111111111111111L) << 35);
            output[outputOffset + 12] = ((v10 >>> 20) & 0b11111111111111111111111111111111111111111111L) | ((v11 & 0b11111111111L) << 44);
            output[outputOffset + 13] = ((v11 >>> 11) & 0b11111111111111111111111111111111111111111111111111111L) | ((v12 & 0b11L) << 53);
            output[outputOffset + 14] = (v12 >>> 2) & 0b1111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 15] = ((v12 >>> 57) & 0b1111111L) | ((v13 & 0b111111111111111111111111111111111111111111111111L) << 7);
            output[outputOffset + 16] = ((v13 >>> 48) & 0b1111111111111111L) | ((v14 & 0b111111111111111111111111111111111111111L) << 16);
            output[outputOffset + 17] = ((v14 >>> 39) & 0b1111111111111111111111111L) | ((v15 & 0b111111111111111111111111111111L) << 25);
            output[outputOffset + 18] = ((v15 >>> 30) & 0b1111111111111111111111111111111111L) | ((v16 & 0b111111111111111111111L) << 34);
            output[outputOffset + 19] = ((v16 >>> 21) & 0b1111111111111111111111111111111111111111111L) | ((v17 & 0b111111111111L) << 43);
            output[outputOffset + 20] = ((v17 >>> 12) & 0b1111111111111111111111111111111111111111111111111111L) | ((v18 & 0b111L) << 52);
            output[outputOffset + 21] = (v18 >>> 3) & 0b1111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 22] = ((v18 >>> 58) & 0b111111L) | ((v19 & 0b1111111111111111111111111111111111111111111111111L) << 6);
            output[outputOffset + 23] = ((v19 >>> 49) & 0b111111111111111L) | ((v20 & 0b1111111111111111111111111111111111111111L) << 15);
            output[outputOffset + 24] = ((v20 >>> 40) & 0b111111111111111111111111L) | ((v21 & 0b1111111111111111111111111111111L) << 24);
            output[outputOffset + 25] = ((v21 >>> 31) & 0b111111111111111111111111111111111L) | ((v22 & 0b1111111111111111111111L) << 33);
            output[outputOffset + 26] = ((v22 >>> 22) & 0b111111111111111111111111111111111111111111L) | ((v23 & 0b1111111111111L) << 42);
            output[outputOffset + 27] = ((v23 >>> 13) & 0b111111111111111111111111111111111111111111111111111L) | ((v24 & 0b1111L) << 51);
            output[outputOffset + 28] = (v24 >>> 4) & 0b1111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 29] = ((v24 >>> 59) & 0b11111L) | ((v25 & 0b11111111111111111111111111111111111111111111111111L) << 5);
            output[outputOffset + 30] = ((v25 >>> 50) & 0b11111111111111L) | ((v26 & 0b11111111111111111111111111111111111111111L) << 14);
            output[outputOffset + 31] = ((v26 >>> 41) & 0b11111111111111111111111L) | ((v27 & 0b11111111111111111111111111111111L) << 23);
        }
    }

    private static final class Unpacker56
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            long v18 = input.readLong();
            long v19 = input.readLong();
            long v20 = input.readLong();
            long v21 = input.readLong();
            long v22 = input.readLong();
            long v23 = input.readLong();
            long v24 = input.readLong();
            long v25 = input.readLong();
            long v26 = input.readLong();
            long v27 = input.readLong();
            output[outputOffset] = v0 & 0b11111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 56) & 0b11111111L) | ((v1 & 0b111111111111111111111111111111111111111111111111L) << 8);
            output[outputOffset + 2] = ((v1 >>> 48) & 0b1111111111111111L) | ((v2 & 0b1111111111111111111111111111111111111111L) << 16);
            output[outputOffset + 3] = ((v2 >>> 40) & 0b111111111111111111111111L) | ((v3 & 0b11111111111111111111111111111111L) << 24);
            output[outputOffset + 4] = ((v3 >>> 32) & 0b11111111111111111111111111111111L) | ((v4 & 0b111111111111111111111111L) << 32);
            output[outputOffset + 5] = ((v4 >>> 24) & 0b1111111111111111111111111111111111111111L) | ((v5 & 0b1111111111111111L) << 40);
            output[outputOffset + 6] = ((v5 >>> 16) & 0b111111111111111111111111111111111111111111111111L) | ((v6 & 0b11111111L) << 48);
            output[outputOffset + 7] = (v6 >>> 8) & 0b11111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 8] = v7 & 0b11111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 9] = ((v7 >>> 56) & 0b11111111L) | ((v8 & 0b111111111111111111111111111111111111111111111111L) << 8);
            output[outputOffset + 10] = ((v8 >>> 48) & 0b1111111111111111L) | ((v9 & 0b1111111111111111111111111111111111111111L) << 16);
            output[outputOffset + 11] = ((v9 >>> 40) & 0b111111111111111111111111L) | ((v10 & 0b11111111111111111111111111111111L) << 24);
            output[outputOffset + 12] = ((v10 >>> 32) & 0b11111111111111111111111111111111L) | ((v11 & 0b111111111111111111111111L) << 32);
            output[outputOffset + 13] = ((v11 >>> 24) & 0b1111111111111111111111111111111111111111L) | ((v12 & 0b1111111111111111L) << 40);
            output[outputOffset + 14] = ((v12 >>> 16) & 0b111111111111111111111111111111111111111111111111L) | ((v13 & 0b11111111L) << 48);
            output[outputOffset + 15] = (v13 >>> 8) & 0b11111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 16] = v14 & 0b11111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 17] = ((v14 >>> 56) & 0b11111111L) | ((v15 & 0b111111111111111111111111111111111111111111111111L) << 8);
            output[outputOffset + 18] = ((v15 >>> 48) & 0b1111111111111111L) | ((v16 & 0b1111111111111111111111111111111111111111L) << 16);
            output[outputOffset + 19] = ((v16 >>> 40) & 0b111111111111111111111111L) | ((v17 & 0b11111111111111111111111111111111L) << 24);
            output[outputOffset + 20] = ((v17 >>> 32) & 0b11111111111111111111111111111111L) | ((v18 & 0b111111111111111111111111L) << 32);
            output[outputOffset + 21] = ((v18 >>> 24) & 0b1111111111111111111111111111111111111111L) | ((v19 & 0b1111111111111111L) << 40);
            output[outputOffset + 22] = ((v19 >>> 16) & 0b111111111111111111111111111111111111111111111111L) | ((v20 & 0b11111111L) << 48);
            output[outputOffset + 23] = (v20 >>> 8) & 0b11111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 24] = v21 & 0b11111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 25] = ((v21 >>> 56) & 0b11111111L) | ((v22 & 0b111111111111111111111111111111111111111111111111L) << 8);
            output[outputOffset + 26] = ((v22 >>> 48) & 0b1111111111111111L) | ((v23 & 0b1111111111111111111111111111111111111111L) << 16);
            output[outputOffset + 27] = ((v23 >>> 40) & 0b111111111111111111111111L) | ((v24 & 0b11111111111111111111111111111111L) << 24);
            output[outputOffset + 28] = ((v24 >>> 32) & 0b11111111111111111111111111111111L) | ((v25 & 0b111111111111111111111111L) << 32);
            output[outputOffset + 29] = ((v25 >>> 24) & 0b1111111111111111111111111111111111111111L) | ((v26 & 0b1111111111111111L) << 40);
            output[outputOffset + 30] = ((v26 >>> 16) & 0b111111111111111111111111111111111111111111111111L) | ((v27 & 0b11111111L) << 48);
            output[outputOffset + 31] = (v27 >>> 8) & 0b11111111111111111111111111111111111111111111111111111111L;
        }
    }

    private static final class Unpacker57
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            long v18 = input.readLong();
            long v19 = input.readLong();
            long v20 = input.readLong();
            long v21 = input.readLong();
            long v22 = input.readLong();
            long v23 = input.readLong();
            long v24 = input.readLong();
            long v25 = input.readLong();
            long v26 = input.readLong();
            long v27 = input.readLong();
            int v28 = input.readInt();
            output[outputOffset] = v0 & 0b111111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 57) & 0b1111111L) | ((v1 & 0b11111111111111111111111111111111111111111111111111L) << 7);
            output[outputOffset + 2] = ((v1 >>> 50) & 0b11111111111111L) | ((v2 & 0b1111111111111111111111111111111111111111111L) << 14);
            output[outputOffset + 3] = ((v2 >>> 43) & 0b111111111111111111111L) | ((v3 & 0b111111111111111111111111111111111111L) << 21);
            output[outputOffset + 4] = ((v3 >>> 36) & 0b1111111111111111111111111111L) | ((v4 & 0b11111111111111111111111111111L) << 28);
            output[outputOffset + 5] = ((v4 >>> 29) & 0b11111111111111111111111111111111111L) | ((v5 & 0b1111111111111111111111L) << 35);
            output[outputOffset + 6] = ((v5 >>> 22) & 0b111111111111111111111111111111111111111111L) | ((v6 & 0b111111111111111L) << 42);
            output[outputOffset + 7] = ((v6 >>> 15) & 0b1111111111111111111111111111111111111111111111111L) | ((v7 & 0b11111111L) << 49);
            output[outputOffset + 8] = ((v7 >>> 8) & 0b11111111111111111111111111111111111111111111111111111111L) | ((v8 & 0b1L) << 56);
            output[outputOffset + 9] = (v8 >>> 1) & 0b111111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 10] = ((v8 >>> 58) & 0b111111L) | ((v9 & 0b111111111111111111111111111111111111111111111111111L) << 6);
            output[outputOffset + 11] = ((v9 >>> 51) & 0b1111111111111L) | ((v10 & 0b11111111111111111111111111111111111111111111L) << 13);
            output[outputOffset + 12] = ((v10 >>> 44) & 0b11111111111111111111L) | ((v11 & 0b1111111111111111111111111111111111111L) << 20);
            output[outputOffset + 13] = ((v11 >>> 37) & 0b111111111111111111111111111L) | ((v12 & 0b111111111111111111111111111111L) << 27);
            output[outputOffset + 14] = ((v12 >>> 30) & 0b1111111111111111111111111111111111L) | ((v13 & 0b11111111111111111111111L) << 34);
            output[outputOffset + 15] = ((v13 >>> 23) & 0b11111111111111111111111111111111111111111L) | ((v14 & 0b1111111111111111L) << 41);
            output[outputOffset + 16] = ((v14 >>> 16) & 0b111111111111111111111111111111111111111111111111L) | ((v15 & 0b111111111L) << 48);
            output[outputOffset + 17] = ((v15 >>> 9) & 0b1111111111111111111111111111111111111111111111111111111L) | ((v16 & 0b11L) << 55);
            output[outputOffset + 18] = (v16 >>> 2) & 0b111111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 19] = ((v16 >>> 59) & 0b11111L) | ((v17 & 0b1111111111111111111111111111111111111111111111111111L) << 5);
            output[outputOffset + 20] = ((v17 >>> 52) & 0b111111111111L) | ((v18 & 0b111111111111111111111111111111111111111111111L) << 12);
            output[outputOffset + 21] = ((v18 >>> 45) & 0b1111111111111111111L) | ((v19 & 0b11111111111111111111111111111111111111L) << 19);
            output[outputOffset + 22] = ((v19 >>> 38) & 0b11111111111111111111111111L) | ((v20 & 0b1111111111111111111111111111111L) << 26);
            output[outputOffset + 23] = ((v20 >>> 31) & 0b111111111111111111111111111111111L) | ((v21 & 0b111111111111111111111111L) << 33);
            output[outputOffset + 24] = ((v21 >>> 24) & 0b1111111111111111111111111111111111111111L) | ((v22 & 0b11111111111111111L) << 40);
            output[outputOffset + 25] = ((v22 >>> 17) & 0b11111111111111111111111111111111111111111111111L) | ((v23 & 0b1111111111L) << 47);
            output[outputOffset + 26] = ((v23 >>> 10) & 0b111111111111111111111111111111111111111111111111111111L) | ((v24 & 0b111L) << 54);
            output[outputOffset + 27] = (v24 >>> 3) & 0b111111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 28] = ((v24 >>> 60) & 0b1111L) | ((v25 & 0b11111111111111111111111111111111111111111111111111111L) << 4);
            output[outputOffset + 29] = ((v25 >>> 53) & 0b11111111111L) | ((v26 & 0b1111111111111111111111111111111111111111111111L) << 11);
            output[outputOffset + 30] = ((v26 >>> 46) & 0b111111111111111111L) | ((v27 & 0b111111111111111111111111111111111111111L) << 18);
            output[outputOffset + 31] = ((v27 >>> 39) & 0b1111111111111111111111111L) | ((v28 & 0b11111111111111111111111111111111L) << 25);
        }
    }

    private static final class Unpacker58
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            long v18 = input.readLong();
            long v19 = input.readLong();
            long v20 = input.readLong();
            long v21 = input.readLong();
            long v22 = input.readLong();
            long v23 = input.readLong();
            long v24 = input.readLong();
            long v25 = input.readLong();
            long v26 = input.readLong();
            long v27 = input.readLong();
            long v28 = input.readLong();
            output[outputOffset] = v0 & 0b1111111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 58) & 0b111111L) | ((v1 & 0b1111111111111111111111111111111111111111111111111111L) << 6);
            output[outputOffset + 2] = ((v1 >>> 52) & 0b111111111111L) | ((v2 & 0b1111111111111111111111111111111111111111111111L) << 12);
            output[outputOffset + 3] = ((v2 >>> 46) & 0b111111111111111111L) | ((v3 & 0b1111111111111111111111111111111111111111L) << 18);
            output[outputOffset + 4] = ((v3 >>> 40) & 0b111111111111111111111111L) | ((v4 & 0b1111111111111111111111111111111111L) << 24);
            output[outputOffset + 5] = ((v4 >>> 34) & 0b111111111111111111111111111111L) | ((v5 & 0b1111111111111111111111111111L) << 30);
            output[outputOffset + 6] = ((v5 >>> 28) & 0b111111111111111111111111111111111111L) | ((v6 & 0b1111111111111111111111L) << 36);
            output[outputOffset + 7] = ((v6 >>> 22) & 0b111111111111111111111111111111111111111111L) | ((v7 & 0b1111111111111111L) << 42);
            output[outputOffset + 8] = ((v7 >>> 16) & 0b111111111111111111111111111111111111111111111111L) | ((v8 & 0b1111111111L) << 48);
            output[outputOffset + 9] = ((v8 >>> 10) & 0b111111111111111111111111111111111111111111111111111111L) | ((v9 & 0b1111L) << 54);
            output[outputOffset + 10] = (v9 >>> 4) & 0b1111111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 11] = ((v9 >>> 62) & 0b11L) | ((v10 & 0b11111111111111111111111111111111111111111111111111111111L) << 2);
            output[outputOffset + 12] = ((v10 >>> 56) & 0b11111111L) | ((v11 & 0b11111111111111111111111111111111111111111111111111L) << 8);
            output[outputOffset + 13] = ((v11 >>> 50) & 0b11111111111111L) | ((v12 & 0b11111111111111111111111111111111111111111111L) << 14);
            output[outputOffset + 14] = ((v12 >>> 44) & 0b11111111111111111111L) | ((v13 & 0b11111111111111111111111111111111111111L) << 20);
            output[outputOffset + 15] = ((v13 >>> 38) & 0b11111111111111111111111111L) | ((v14 & 0b11111111111111111111111111111111L) << 26);
            output[outputOffset + 16] = ((v14 >>> 32) & 0b11111111111111111111111111111111L) | ((v15 & 0b11111111111111111111111111L) << 32);
            output[outputOffset + 17] = ((v15 >>> 26) & 0b11111111111111111111111111111111111111L) | ((v16 & 0b11111111111111111111L) << 38);
            output[outputOffset + 18] = ((v16 >>> 20) & 0b11111111111111111111111111111111111111111111L) | ((v17 & 0b11111111111111L) << 44);
            output[outputOffset + 19] = ((v17 >>> 14) & 0b11111111111111111111111111111111111111111111111111L) | ((v18 & 0b11111111L) << 50);
            output[outputOffset + 20] = ((v18 >>> 8) & 0b11111111111111111111111111111111111111111111111111111111L) | ((v19 & 0b11L) << 56);
            output[outputOffset + 21] = (v19 >>> 2) & 0b1111111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 22] = ((v19 >>> 60) & 0b1111L) | ((v20 & 0b111111111111111111111111111111111111111111111111111111L) << 4);
            output[outputOffset + 23] = ((v20 >>> 54) & 0b1111111111L) | ((v21 & 0b111111111111111111111111111111111111111111111111L) << 10);
            output[outputOffset + 24] = ((v21 >>> 48) & 0b1111111111111111L) | ((v22 & 0b111111111111111111111111111111111111111111L) << 16);
            output[outputOffset + 25] = ((v22 >>> 42) & 0b1111111111111111111111L) | ((v23 & 0b111111111111111111111111111111111111L) << 22);
            output[outputOffset + 26] = ((v23 >>> 36) & 0b1111111111111111111111111111L) | ((v24 & 0b111111111111111111111111111111L) << 28);
            output[outputOffset + 27] = ((v24 >>> 30) & 0b1111111111111111111111111111111111L) | ((v25 & 0b111111111111111111111111L) << 34);
            output[outputOffset + 28] = ((v25 >>> 24) & 0b1111111111111111111111111111111111111111L) | ((v26 & 0b111111111111111111L) << 40);
            output[outputOffset + 29] = ((v26 >>> 18) & 0b1111111111111111111111111111111111111111111111L) | ((v27 & 0b111111111111L) << 46);
            output[outputOffset + 30] = ((v27 >>> 12) & 0b1111111111111111111111111111111111111111111111111111L) | ((v28 & 0b111111L) << 52);
            output[outputOffset + 31] = (v28 >>> 6) & 0b1111111111111111111111111111111111111111111111111111111111L;
        }
    }

    private static final class Unpacker59
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            long v18 = input.readLong();
            long v19 = input.readLong();
            long v20 = input.readLong();
            long v21 = input.readLong();
            long v22 = input.readLong();
            long v23 = input.readLong();
            long v24 = input.readLong();
            long v25 = input.readLong();
            long v26 = input.readLong();
            long v27 = input.readLong();
            long v28 = input.readLong();
            int v29 = input.readInt();
            output[outputOffset] = v0 & 0b11111111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 59) & 0b11111L) | ((v1 & 0b111111111111111111111111111111111111111111111111111111L) << 5);
            output[outputOffset + 2] = ((v1 >>> 54) & 0b1111111111L) | ((v2 & 0b1111111111111111111111111111111111111111111111111L) << 10);
            output[outputOffset + 3] = ((v2 >>> 49) & 0b111111111111111L) | ((v3 & 0b11111111111111111111111111111111111111111111L) << 15);
            output[outputOffset + 4] = ((v3 >>> 44) & 0b11111111111111111111L) | ((v4 & 0b111111111111111111111111111111111111111L) << 20);
            output[outputOffset + 5] = ((v4 >>> 39) & 0b1111111111111111111111111L) | ((v5 & 0b1111111111111111111111111111111111L) << 25);
            output[outputOffset + 6] = ((v5 >>> 34) & 0b111111111111111111111111111111L) | ((v6 & 0b11111111111111111111111111111L) << 30);
            output[outputOffset + 7] = ((v6 >>> 29) & 0b11111111111111111111111111111111111L) | ((v7 & 0b111111111111111111111111L) << 35);
            output[outputOffset + 8] = ((v7 >>> 24) & 0b1111111111111111111111111111111111111111L) | ((v8 & 0b1111111111111111111L) << 40);
            output[outputOffset + 9] = ((v8 >>> 19) & 0b111111111111111111111111111111111111111111111L) | ((v9 & 0b11111111111111L) << 45);
            output[outputOffset + 10] = ((v9 >>> 14) & 0b11111111111111111111111111111111111111111111111111L) | ((v10 & 0b111111111L) << 50);
            output[outputOffset + 11] = ((v10 >>> 9) & 0b1111111111111111111111111111111111111111111111111111111L) | ((v11 & 0b1111L) << 55);
            output[outputOffset + 12] = (v11 >>> 4) & 0b11111111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 13] = ((v11 >>> 63) & 0b1L) | ((v12 & 0b1111111111111111111111111111111111111111111111111111111111L) << 1);
            output[outputOffset + 14] = ((v12 >>> 58) & 0b111111L) | ((v13 & 0b11111111111111111111111111111111111111111111111111111L) << 6);
            output[outputOffset + 15] = ((v13 >>> 53) & 0b11111111111L) | ((v14 & 0b111111111111111111111111111111111111111111111111L) << 11);
            output[outputOffset + 16] = ((v14 >>> 48) & 0b1111111111111111L) | ((v15 & 0b1111111111111111111111111111111111111111111L) << 16);
            output[outputOffset + 17] = ((v15 >>> 43) & 0b111111111111111111111L) | ((v16 & 0b11111111111111111111111111111111111111L) << 21);
            output[outputOffset + 18] = ((v16 >>> 38) & 0b11111111111111111111111111L) | ((v17 & 0b111111111111111111111111111111111L) << 26);
            output[outputOffset + 19] = ((v17 >>> 33) & 0b1111111111111111111111111111111L) | ((v18 & 0b1111111111111111111111111111L) << 31);
            output[outputOffset + 20] = ((v18 >>> 28) & 0b111111111111111111111111111111111111L) | ((v19 & 0b11111111111111111111111L) << 36);
            output[outputOffset + 21] = ((v19 >>> 23) & 0b11111111111111111111111111111111111111111L) | ((v20 & 0b111111111111111111L) << 41);
            output[outputOffset + 22] = ((v20 >>> 18) & 0b1111111111111111111111111111111111111111111111L) | ((v21 & 0b1111111111111L) << 46);
            output[outputOffset + 23] = ((v21 >>> 13) & 0b111111111111111111111111111111111111111111111111111L) | ((v22 & 0b11111111L) << 51);
            output[outputOffset + 24] = ((v22 >>> 8) & 0b11111111111111111111111111111111111111111111111111111111L) | ((v23 & 0b111L) << 56);
            output[outputOffset + 25] = (v23 >>> 3) & 0b11111111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 26] = ((v23 >>> 62) & 0b11L) | ((v24 & 0b111111111111111111111111111111111111111111111111111111111L) << 2);
            output[outputOffset + 27] = ((v24 >>> 57) & 0b1111111L) | ((v25 & 0b1111111111111111111111111111111111111111111111111111L) << 7);
            output[outputOffset + 28] = ((v25 >>> 52) & 0b111111111111L) | ((v26 & 0b11111111111111111111111111111111111111111111111L) << 12);
            output[outputOffset + 29] = ((v26 >>> 47) & 0b11111111111111111L) | ((v27 & 0b111111111111111111111111111111111111111111L) << 17);
            output[outputOffset + 30] = ((v27 >>> 42) & 0b1111111111111111111111L) | ((v28 & 0b1111111111111111111111111111111111111L) << 22);
            output[outputOffset + 31] = ((v28 >>> 37) & 0b111111111111111111111111111L) | ((v29 & 0b11111111111111111111111111111111L) << 27);
        }
    }

    private static final class Unpacker60
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            long v18 = input.readLong();
            long v19 = input.readLong();
            long v20 = input.readLong();
            long v21 = input.readLong();
            long v22 = input.readLong();
            long v23 = input.readLong();
            long v24 = input.readLong();
            long v25 = input.readLong();
            long v26 = input.readLong();
            long v27 = input.readLong();
            long v28 = input.readLong();
            long v29 = input.readLong();
            output[outputOffset] = v0 & 0b111111111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 60) & 0b1111L) | ((v1 & 0b11111111111111111111111111111111111111111111111111111111L) << 4);
            output[outputOffset + 2] = ((v1 >>> 56) & 0b11111111L) | ((v2 & 0b1111111111111111111111111111111111111111111111111111L) << 8);
            output[outputOffset + 3] = ((v2 >>> 52) & 0b111111111111L) | ((v3 & 0b111111111111111111111111111111111111111111111111L) << 12);
            output[outputOffset + 4] = ((v3 >>> 48) & 0b1111111111111111L) | ((v4 & 0b11111111111111111111111111111111111111111111L) << 16);
            output[outputOffset + 5] = ((v4 >>> 44) & 0b11111111111111111111L) | ((v5 & 0b1111111111111111111111111111111111111111L) << 20);
            output[outputOffset + 6] = ((v5 >>> 40) & 0b111111111111111111111111L) | ((v6 & 0b111111111111111111111111111111111111L) << 24);
            output[outputOffset + 7] = ((v6 >>> 36) & 0b1111111111111111111111111111L) | ((v7 & 0b11111111111111111111111111111111L) << 28);
            output[outputOffset + 8] = ((v7 >>> 32) & 0b11111111111111111111111111111111L) | ((v8 & 0b1111111111111111111111111111L) << 32);
            output[outputOffset + 9] = ((v8 >>> 28) & 0b111111111111111111111111111111111111L) | ((v9 & 0b111111111111111111111111L) << 36);
            output[outputOffset + 10] = ((v9 >>> 24) & 0b1111111111111111111111111111111111111111L) | ((v10 & 0b11111111111111111111L) << 40);
            output[outputOffset + 11] = ((v10 >>> 20) & 0b11111111111111111111111111111111111111111111L) | ((v11 & 0b1111111111111111L) << 44);
            output[outputOffset + 12] = ((v11 >>> 16) & 0b111111111111111111111111111111111111111111111111L) | ((v12 & 0b111111111111L) << 48);
            output[outputOffset + 13] = ((v12 >>> 12) & 0b1111111111111111111111111111111111111111111111111111L) | ((v13 & 0b11111111L) << 52);
            output[outputOffset + 14] = ((v13 >>> 8) & 0b11111111111111111111111111111111111111111111111111111111L) | ((v14 & 0b1111L) << 56);
            output[outputOffset + 15] = (v14 >>> 4) & 0b111111111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 16] = v15 & 0b111111111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 17] = ((v15 >>> 60) & 0b1111L) | ((v16 & 0b11111111111111111111111111111111111111111111111111111111L) << 4);
            output[outputOffset + 18] = ((v16 >>> 56) & 0b11111111L) | ((v17 & 0b1111111111111111111111111111111111111111111111111111L) << 8);
            output[outputOffset + 19] = ((v17 >>> 52) & 0b111111111111L) | ((v18 & 0b111111111111111111111111111111111111111111111111L) << 12);
            output[outputOffset + 20] = ((v18 >>> 48) & 0b1111111111111111L) | ((v19 & 0b11111111111111111111111111111111111111111111L) << 16);
            output[outputOffset + 21] = ((v19 >>> 44) & 0b11111111111111111111L) | ((v20 & 0b1111111111111111111111111111111111111111L) << 20);
            output[outputOffset + 22] = ((v20 >>> 40) & 0b111111111111111111111111L) | ((v21 & 0b111111111111111111111111111111111111L) << 24);
            output[outputOffset + 23] = ((v21 >>> 36) & 0b1111111111111111111111111111L) | ((v22 & 0b11111111111111111111111111111111L) << 28);
            output[outputOffset + 24] = ((v22 >>> 32) & 0b11111111111111111111111111111111L) | ((v23 & 0b1111111111111111111111111111L) << 32);
            output[outputOffset + 25] = ((v23 >>> 28) & 0b111111111111111111111111111111111111L) | ((v24 & 0b111111111111111111111111L) << 36);
            output[outputOffset + 26] = ((v24 >>> 24) & 0b1111111111111111111111111111111111111111L) | ((v25 & 0b11111111111111111111L) << 40);
            output[outputOffset + 27] = ((v25 >>> 20) & 0b11111111111111111111111111111111111111111111L) | ((v26 & 0b1111111111111111L) << 44);
            output[outputOffset + 28] = ((v26 >>> 16) & 0b111111111111111111111111111111111111111111111111L) | ((v27 & 0b111111111111L) << 48);
            output[outputOffset + 29] = ((v27 >>> 12) & 0b1111111111111111111111111111111111111111111111111111L) | ((v28 & 0b11111111L) << 52);
            output[outputOffset + 30] = ((v28 >>> 8) & 0b11111111111111111111111111111111111111111111111111111111L) | ((v29 & 0b1111L) << 56);
            output[outputOffset + 31] = (v29 >>> 4) & 0b111111111111111111111111111111111111111111111111111111111111L;
        }
    }

    private static final class Unpacker61
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            long v18 = input.readLong();
            long v19 = input.readLong();
            long v20 = input.readLong();
            long v21 = input.readLong();
            long v22 = input.readLong();
            long v23 = input.readLong();
            long v24 = input.readLong();
            long v25 = input.readLong();
            long v26 = input.readLong();
            long v27 = input.readLong();
            long v28 = input.readLong();
            long v29 = input.readLong();
            int v30 = input.readInt();
            output[outputOffset] = v0 & 0b1111111111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 61) & 0b111L) | ((v1 & 0b1111111111111111111111111111111111111111111111111111111111L) << 3);
            output[outputOffset + 2] = ((v1 >>> 58) & 0b111111L) | ((v2 & 0b1111111111111111111111111111111111111111111111111111111L) << 6);
            output[outputOffset + 3] = ((v2 >>> 55) & 0b111111111L) | ((v3 & 0b1111111111111111111111111111111111111111111111111111L) << 9);
            output[outputOffset + 4] = ((v3 >>> 52) & 0b111111111111L) | ((v4 & 0b1111111111111111111111111111111111111111111111111L) << 12);
            output[outputOffset + 5] = ((v4 >>> 49) & 0b111111111111111L) | ((v5 & 0b1111111111111111111111111111111111111111111111L) << 15);
            output[outputOffset + 6] = ((v5 >>> 46) & 0b111111111111111111L) | ((v6 & 0b1111111111111111111111111111111111111111111L) << 18);
            output[outputOffset + 7] = ((v6 >>> 43) & 0b111111111111111111111L) | ((v7 & 0b1111111111111111111111111111111111111111L) << 21);
            output[outputOffset + 8] = ((v7 >>> 40) & 0b111111111111111111111111L) | ((v8 & 0b1111111111111111111111111111111111111L) << 24);
            output[outputOffset + 9] = ((v8 >>> 37) & 0b111111111111111111111111111L) | ((v9 & 0b1111111111111111111111111111111111L) << 27);
            output[outputOffset + 10] = ((v9 >>> 34) & 0b111111111111111111111111111111L) | ((v10 & 0b1111111111111111111111111111111L) << 30);
            output[outputOffset + 11] = ((v10 >>> 31) & 0b111111111111111111111111111111111L) | ((v11 & 0b1111111111111111111111111111L) << 33);
            output[outputOffset + 12] = ((v11 >>> 28) & 0b111111111111111111111111111111111111L) | ((v12 & 0b1111111111111111111111111L) << 36);
            output[outputOffset + 13] = ((v12 >>> 25) & 0b111111111111111111111111111111111111111L) | ((v13 & 0b1111111111111111111111L) << 39);
            output[outputOffset + 14] = ((v13 >>> 22) & 0b111111111111111111111111111111111111111111L) | ((v14 & 0b1111111111111111111L) << 42);
            output[outputOffset + 15] = ((v14 >>> 19) & 0b111111111111111111111111111111111111111111111L) | ((v15 & 0b1111111111111111L) << 45);
            output[outputOffset + 16] = ((v15 >>> 16) & 0b111111111111111111111111111111111111111111111111L) | ((v16 & 0b1111111111111L) << 48);
            output[outputOffset + 17] = ((v16 >>> 13) & 0b111111111111111111111111111111111111111111111111111L) | ((v17 & 0b1111111111L) << 51);
            output[outputOffset + 18] = ((v17 >>> 10) & 0b111111111111111111111111111111111111111111111111111111L) | ((v18 & 0b1111111L) << 54);
            output[outputOffset + 19] = ((v18 >>> 7) & 0b111111111111111111111111111111111111111111111111111111111L) | ((v19 & 0b1111L) << 57);
            output[outputOffset + 20] = ((v19 >>> 4) & 0b111111111111111111111111111111111111111111111111111111111111L) | ((v20 & 0b1L) << 60);
            output[outputOffset + 21] = (v20 >>> 1) & 0b1111111111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 22] = ((v20 >>> 62) & 0b11L) | ((v21 & 0b11111111111111111111111111111111111111111111111111111111111L) << 2);
            output[outputOffset + 23] = ((v21 >>> 59) & 0b11111L) | ((v22 & 0b11111111111111111111111111111111111111111111111111111111L) << 5);
            output[outputOffset + 24] = ((v22 >>> 56) & 0b11111111L) | ((v23 & 0b11111111111111111111111111111111111111111111111111111L) << 8);
            output[outputOffset + 25] = ((v23 >>> 53) & 0b11111111111L) | ((v24 & 0b11111111111111111111111111111111111111111111111111L) << 11);
            output[outputOffset + 26] = ((v24 >>> 50) & 0b11111111111111L) | ((v25 & 0b11111111111111111111111111111111111111111111111L) << 14);
            output[outputOffset + 27] = ((v25 >>> 47) & 0b11111111111111111L) | ((v26 & 0b11111111111111111111111111111111111111111111L) << 17);
            output[outputOffset + 28] = ((v26 >>> 44) & 0b11111111111111111111L) | ((v27 & 0b11111111111111111111111111111111111111111L) << 20);
            output[outputOffset + 29] = ((v27 >>> 41) & 0b11111111111111111111111L) | ((v28 & 0b11111111111111111111111111111111111111L) << 23);
            output[outputOffset + 30] = ((v28 >>> 38) & 0b11111111111111111111111111L) | ((v29 & 0b11111111111111111111111111111111111L) << 26);
            output[outputOffset + 31] = ((v29 >>> 35) & 0b11111111111111111111111111111L) | ((v30 & 0b11111111111111111111111111111111L) << 29);
        }
    }

    private static final class Unpacker62
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            long v18 = input.readLong();
            long v19 = input.readLong();
            long v20 = input.readLong();
            long v21 = input.readLong();
            long v22 = input.readLong();
            long v23 = input.readLong();
            long v24 = input.readLong();
            long v25 = input.readLong();
            long v26 = input.readLong();
            long v27 = input.readLong();
            long v28 = input.readLong();
            long v29 = input.readLong();
            long v30 = input.readLong();
            output[outputOffset] = v0 & 0b11111111111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 62) & 0b11L) | ((v1 & 0b111111111111111111111111111111111111111111111111111111111111L) << 2);
            output[outputOffset + 2] = ((v1 >>> 60) & 0b1111L) | ((v2 & 0b1111111111111111111111111111111111111111111111111111111111L) << 4);
            output[outputOffset + 3] = ((v2 >>> 58) & 0b111111L) | ((v3 & 0b11111111111111111111111111111111111111111111111111111111L) << 6);
            output[outputOffset + 4] = ((v3 >>> 56) & 0b11111111L) | ((v4 & 0b111111111111111111111111111111111111111111111111111111L) << 8);
            output[outputOffset + 5] = ((v4 >>> 54) & 0b1111111111L) | ((v5 & 0b1111111111111111111111111111111111111111111111111111L) << 10);
            output[outputOffset + 6] = ((v5 >>> 52) & 0b111111111111L) | ((v6 & 0b11111111111111111111111111111111111111111111111111L) << 12);
            output[outputOffset + 7] = ((v6 >>> 50) & 0b11111111111111L) | ((v7 & 0b111111111111111111111111111111111111111111111111L) << 14);
            output[outputOffset + 8] = ((v7 >>> 48) & 0b1111111111111111L) | ((v8 & 0b1111111111111111111111111111111111111111111111L) << 16);
            output[outputOffset + 9] = ((v8 >>> 46) & 0b111111111111111111L) | ((v9 & 0b11111111111111111111111111111111111111111111L) << 18);
            output[outputOffset + 10] = ((v9 >>> 44) & 0b11111111111111111111L) | ((v10 & 0b111111111111111111111111111111111111111111L) << 20);
            output[outputOffset + 11] = ((v10 >>> 42) & 0b1111111111111111111111L) | ((v11 & 0b1111111111111111111111111111111111111111L) << 22);
            output[outputOffset + 12] = ((v11 >>> 40) & 0b111111111111111111111111L) | ((v12 & 0b11111111111111111111111111111111111111L) << 24);
            output[outputOffset + 13] = ((v12 >>> 38) & 0b11111111111111111111111111L) | ((v13 & 0b111111111111111111111111111111111111L) << 26);
            output[outputOffset + 14] = ((v13 >>> 36) & 0b1111111111111111111111111111L) | ((v14 & 0b1111111111111111111111111111111111L) << 28);
            output[outputOffset + 15] = ((v14 >>> 34) & 0b111111111111111111111111111111L) | ((v15 & 0b11111111111111111111111111111111L) << 30);
            output[outputOffset + 16] = ((v15 >>> 32) & 0b11111111111111111111111111111111L) | ((v16 & 0b111111111111111111111111111111L) << 32);
            output[outputOffset + 17] = ((v16 >>> 30) & 0b1111111111111111111111111111111111L) | ((v17 & 0b1111111111111111111111111111L) << 34);
            output[outputOffset + 18] = ((v17 >>> 28) & 0b111111111111111111111111111111111111L) | ((v18 & 0b11111111111111111111111111L) << 36);
            output[outputOffset + 19] = ((v18 >>> 26) & 0b11111111111111111111111111111111111111L) | ((v19 & 0b111111111111111111111111L) << 38);
            output[outputOffset + 20] = ((v19 >>> 24) & 0b1111111111111111111111111111111111111111L) | ((v20 & 0b1111111111111111111111L) << 40);
            output[outputOffset + 21] = ((v20 >>> 22) & 0b111111111111111111111111111111111111111111L) | ((v21 & 0b11111111111111111111L) << 42);
            output[outputOffset + 22] = ((v21 >>> 20) & 0b11111111111111111111111111111111111111111111L) | ((v22 & 0b111111111111111111L) << 44);
            output[outputOffset + 23] = ((v22 >>> 18) & 0b1111111111111111111111111111111111111111111111L) | ((v23 & 0b1111111111111111L) << 46);
            output[outputOffset + 24] = ((v23 >>> 16) & 0b111111111111111111111111111111111111111111111111L) | ((v24 & 0b11111111111111L) << 48);
            output[outputOffset + 25] = ((v24 >>> 14) & 0b11111111111111111111111111111111111111111111111111L) | ((v25 & 0b111111111111L) << 50);
            output[outputOffset + 26] = ((v25 >>> 12) & 0b1111111111111111111111111111111111111111111111111111L) | ((v26 & 0b1111111111L) << 52);
            output[outputOffset + 27] = ((v26 >>> 10) & 0b111111111111111111111111111111111111111111111111111111L) | ((v27 & 0b11111111L) << 54);
            output[outputOffset + 28] = ((v27 >>> 8) & 0b11111111111111111111111111111111111111111111111111111111L) | ((v28 & 0b111111L) << 56);
            output[outputOffset + 29] = ((v28 >>> 6) & 0b1111111111111111111111111111111111111111111111111111111111L) | ((v29 & 0b1111L) << 58);
            output[outputOffset + 30] = ((v29 >>> 4) & 0b111111111111111111111111111111111111111111111111111111111111L) | ((v30 & 0b11L) << 60);
            output[outputOffset + 31] = (v30 >>> 2) & 0b11111111111111111111111111111111111111111111111111111111111111L;
        }
    }

    private static final class Unpacker63
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
        }

        private static void unpack32(long[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            long v7 = input.readLong();
            long v8 = input.readLong();
            long v9 = input.readLong();
            long v10 = input.readLong();
            long v11 = input.readLong();
            long v12 = input.readLong();
            long v13 = input.readLong();
            long v14 = input.readLong();
            long v15 = input.readLong();
            long v16 = input.readLong();
            long v17 = input.readLong();
            long v18 = input.readLong();
            long v19 = input.readLong();
            long v20 = input.readLong();
            long v21 = input.readLong();
            long v22 = input.readLong();
            long v23 = input.readLong();
            long v24 = input.readLong();
            long v25 = input.readLong();
            long v26 = input.readLong();
            long v27 = input.readLong();
            long v28 = input.readLong();
            long v29 = input.readLong();
            long v30 = input.readLong();
            int v31 = input.readInt();
            output[outputOffset] = v0 & 0b111111111111111111111111111111111111111111111111111111111111111L;
            output[outputOffset + 1] = ((v0 >>> 63) & 0b1L) | ((v1 & 0b11111111111111111111111111111111111111111111111111111111111111L) << 1);
            output[outputOffset + 2] = ((v1 >>> 62) & 0b11L) | ((v2 & 0b1111111111111111111111111111111111111111111111111111111111111L) << 2);
            output[outputOffset + 3] = ((v2 >>> 61) & 0b111L) | ((v3 & 0b111111111111111111111111111111111111111111111111111111111111L) << 3);
            output[outputOffset + 4] = ((v3 >>> 60) & 0b1111L) | ((v4 & 0b11111111111111111111111111111111111111111111111111111111111L) << 4);
            output[outputOffset + 5] = ((v4 >>> 59) & 0b11111L) | ((v5 & 0b1111111111111111111111111111111111111111111111111111111111L) << 5);
            output[outputOffset + 6] = ((v5 >>> 58) & 0b111111L) | ((v6 & 0b111111111111111111111111111111111111111111111111111111111L) << 6);
            output[outputOffset + 7] = ((v6 >>> 57) & 0b1111111L) | ((v7 & 0b11111111111111111111111111111111111111111111111111111111L) << 7);
            output[outputOffset + 8] = ((v7 >>> 56) & 0b11111111L) | ((v8 & 0b1111111111111111111111111111111111111111111111111111111L) << 8);
            output[outputOffset + 9] = ((v8 >>> 55) & 0b111111111L) | ((v9 & 0b111111111111111111111111111111111111111111111111111111L) << 9);
            output[outputOffset + 10] = ((v9 >>> 54) & 0b1111111111L) | ((v10 & 0b11111111111111111111111111111111111111111111111111111L) << 10);
            output[outputOffset + 11] = ((v10 >>> 53) & 0b11111111111L) | ((v11 & 0b1111111111111111111111111111111111111111111111111111L) << 11);
            output[outputOffset + 12] = ((v11 >>> 52) & 0b111111111111L) | ((v12 & 0b111111111111111111111111111111111111111111111111111L) << 12);
            output[outputOffset + 13] = ((v12 >>> 51) & 0b1111111111111L) | ((v13 & 0b11111111111111111111111111111111111111111111111111L) << 13);
            output[outputOffset + 14] = ((v13 >>> 50) & 0b11111111111111L) | ((v14 & 0b1111111111111111111111111111111111111111111111111L) << 14);
            output[outputOffset + 15] = ((v14 >>> 49) & 0b111111111111111L) | ((v15 & 0b111111111111111111111111111111111111111111111111L) << 15);
            output[outputOffset + 16] = ((v15 >>> 48) & 0b1111111111111111L) | ((v16 & 0b11111111111111111111111111111111111111111111111L) << 16);
            output[outputOffset + 17] = ((v16 >>> 47) & 0b11111111111111111L) | ((v17 & 0b1111111111111111111111111111111111111111111111L) << 17);
            output[outputOffset + 18] = ((v17 >>> 46) & 0b111111111111111111L) | ((v18 & 0b111111111111111111111111111111111111111111111L) << 18);
            output[outputOffset + 19] = ((v18 >>> 45) & 0b1111111111111111111L) | ((v19 & 0b11111111111111111111111111111111111111111111L) << 19);
            output[outputOffset + 20] = ((v19 >>> 44) & 0b11111111111111111111L) | ((v20 & 0b1111111111111111111111111111111111111111111L) << 20);
            output[outputOffset + 21] = ((v20 >>> 43) & 0b111111111111111111111L) | ((v21 & 0b111111111111111111111111111111111111111111L) << 21);
            output[outputOffset + 22] = ((v21 >>> 42) & 0b1111111111111111111111L) | ((v22 & 0b11111111111111111111111111111111111111111L) << 22);
            output[outputOffset + 23] = ((v22 >>> 41) & 0b11111111111111111111111L) | ((v23 & 0b1111111111111111111111111111111111111111L) << 23);
            output[outputOffset + 24] = ((v23 >>> 40) & 0b111111111111111111111111L) | ((v24 & 0b111111111111111111111111111111111111111L) << 24);
            output[outputOffset + 25] = ((v24 >>> 39) & 0b1111111111111111111111111L) | ((v25 & 0b11111111111111111111111111111111111111L) << 25);
            output[outputOffset + 26] = ((v25 >>> 38) & 0b11111111111111111111111111L) | ((v26 & 0b1111111111111111111111111111111111111L) << 26);
            output[outputOffset + 27] = ((v26 >>> 37) & 0b111111111111111111111111111L) | ((v27 & 0b111111111111111111111111111111111111L) << 27);
            output[outputOffset + 28] = ((v27 >>> 36) & 0b1111111111111111111111111111L) | ((v28 & 0b11111111111111111111111111111111111L) << 28);
            output[outputOffset + 29] = ((v28 >>> 35) & 0b11111111111111111111111111111L) | ((v29 & 0b1111111111111111111111111111111111L) << 29);
            output[outputOffset + 30] = ((v29 >>> 34) & 0b111111111111111111111111111111L) | ((v30 & 0b111111111111111111111111111111111L) << 30);
            output[outputOffset + 31] = ((v30 >>> 33) & 0b1111111111111111111111111111111L) | ((v31 & 0b11111111111111111111111111111111L) << 31);
        }
    }

    private static class Unpacker64
            implements LongBitUnpacker
    {
        @Override
        public void unpack(long[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            input.readLongs(output, outputOffset, length);
        }
    }
}
