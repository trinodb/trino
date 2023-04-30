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

public final class IntBitUnpackers
{
    private static final IntBitUnpacker[] UNPACKERS = {
            new Unpacker0(),
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
            new Unpacker32()};

    public static IntBitUnpacker getIntBitUnpacker(int bitWidth)
    {
        return UNPACKERS[bitWidth];
    }

    private IntBitUnpackers()
    {
    }

    private static final class Unpacker0
            implements IntBitUnpacker
    {
        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input,
                int length)
        {
            // Do nothing
        }
    }

    private static final class Unpacker1
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, byte[] input, int inputOffset)
        {
            byte v0 = input[inputOffset];
            output[outputOffset] = v0 & 1;
            output[outputOffset + 1] = (v0 >>> 1) & 1;
            output[outputOffset + 2] = (v0 >>> 2) & 1;
            output[outputOffset + 3] = (v0 >>> 3) & 1;
            output[outputOffset + 4] = (v0 >>> 4) & 1;
            output[outputOffset + 5] = (v0 >>> 5) & 1;
            output[outputOffset + 6] = (v0 >>> 6) & 1;
            output[outputOffset + 7] = (v0 >>> 7) & 1;
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            byte[] inputArr = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            while (length >= 8) {
                unpack8(output, outputOffset, inputArr, inputOffset + inputBytesRead);
                outputOffset += 8;
                length -= 8;
                inputBytesRead++;
            }
            input.skip(inputBytesRead);
        }
    }

    private static final class Unpacker2
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, byte[] input, int inputOffset)
        {
            byte v0 = input[inputOffset];
            byte v1 = input[inputOffset + 1];

            output[outputOffset] = v0 & 0b11;
            output[outputOffset + 1] = (v0 >>> 2) & 0b11;
            output[outputOffset + 2] = (v0 >>> 4) & 0b11;
            output[outputOffset + 3] = (v0 >>> 6) & 0b11;

            output[outputOffset + 4] = v1 & 0b11;
            output[outputOffset + 5] = (v1 >>> 2) & 0b11;
            output[outputOffset + 6] = (v1 >>> 4) & 0b11;
            output[outputOffset + 7] = (v1 >>> 6) & 0b11;
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            byte[] inputArr = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            while (length >= 8) {
                unpack8(output, outputOffset, inputArr, inputOffset + inputBytesRead);
                outputOffset += 8;
                length -= 8;
                inputBytesRead += 2;
            }
            input.skip(inputBytesRead);
        }
    }

    private static final class Unpacker3
            implements IntBitUnpacker
    {
        private static void unpack64(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            output[outputOffset] = (int) (v0 & 0b111L);
            output[outputOffset + 1] = (int) ((v0 >>> 3) & 0b111L);
            output[outputOffset + 2] = (int) ((v0 >>> 6) & 0b111L);
            output[outputOffset + 3] = (int) ((v0 >>> 9) & 0b111L);
            output[outputOffset + 4] = (int) ((v0 >>> 12) & 0b111L);
            output[outputOffset + 5] = (int) ((v0 >>> 15) & 0b111L);
            output[outputOffset + 6] = (int) ((v0 >>> 18) & 0b111L);
            output[outputOffset + 7] = (int) ((v0 >>> 21) & 0b111L);
            output[outputOffset + 8] = (int) ((v0 >>> 24) & 0b111L);
            output[outputOffset + 9] = (int) ((v0 >>> 27) & 0b111L);
            output[outputOffset + 10] = (int) ((v0 >>> 30) & 0b111L);
            output[outputOffset + 11] = (int) ((v0 >>> 33) & 0b111L);
            output[outputOffset + 12] = (int) ((v0 >>> 36) & 0b111L);
            output[outputOffset + 13] = (int) ((v0 >>> 39) & 0b111L);
            output[outputOffset + 14] = (int) ((v0 >>> 42) & 0b111L);
            output[outputOffset + 15] = (int) ((v0 >>> 45) & 0b111L);
            output[outputOffset + 16] = (int) ((v0 >>> 48) & 0b111L);
            output[outputOffset + 17] = (int) ((v0 >>> 51) & 0b111L);
            output[outputOffset + 18] = (int) ((v0 >>> 54) & 0b111L);
            output[outputOffset + 19] = (int) ((v0 >>> 57) & 0b111L);
            output[outputOffset + 20] = (int) ((v0 >>> 60) & 0b111L);
            output[outputOffset + 21] = (int) (((v0 >>> 63) & 0b1L) | ((v1 & 0b11L) << 1));
            output[outputOffset + 22] = (int) ((v1 >>> 2) & 0b111L);
            output[outputOffset + 23] = (int) ((v1 >>> 5) & 0b111L);
            output[outputOffset + 24] = (int) ((v1 >>> 8) & 0b111L);
            output[outputOffset + 25] = (int) ((v1 >>> 11) & 0b111L);
            output[outputOffset + 26] = (int) ((v1 >>> 14) & 0b111L);
            output[outputOffset + 27] = (int) ((v1 >>> 17) & 0b111L);
            output[outputOffset + 28] = (int) ((v1 >>> 20) & 0b111L);
            output[outputOffset + 29] = (int) ((v1 >>> 23) & 0b111L);
            output[outputOffset + 30] = (int) ((v1 >>> 26) & 0b111L);
            output[outputOffset + 31] = (int) ((v1 >>> 29) & 0b111L);
            output[outputOffset + 32] = (int) ((v1 >>> 32) & 0b111L);
            output[outputOffset + 33] = (int) ((v1 >>> 35) & 0b111L);
            output[outputOffset + 34] = (int) ((v1 >>> 38) & 0b111L);
            output[outputOffset + 35] = (int) ((v1 >>> 41) & 0b111L);
            output[outputOffset + 36] = (int) ((v1 >>> 44) & 0b111L);
            output[outputOffset + 37] = (int) ((v1 >>> 47) & 0b111L);
            output[outputOffset + 38] = (int) ((v1 >>> 50) & 0b111L);
            output[outputOffset + 39] = (int) ((v1 >>> 53) & 0b111L);
            output[outputOffset + 40] = (int) ((v1 >>> 56) & 0b111L);
            output[outputOffset + 41] = (int) ((v1 >>> 59) & 0b111L);
            output[outputOffset + 42] = (int) (((v1 >>> 62) & 0b11L) | ((v2 & 0b1L) << 2));
            output[outputOffset + 43] = (int) ((v2 >>> 1) & 0b111L);
            output[outputOffset + 44] = (int) ((v2 >>> 4) & 0b111L);
            output[outputOffset + 45] = (int) ((v2 >>> 7) & 0b111L);
            output[outputOffset + 46] = (int) ((v2 >>> 10) & 0b111L);
            output[outputOffset + 47] = (int) ((v2 >>> 13) & 0b111L);
            output[outputOffset + 48] = (int) ((v2 >>> 16) & 0b111L);
            output[outputOffset + 49] = (int) ((v2 >>> 19) & 0b111L);
            output[outputOffset + 50] = (int) ((v2 >>> 22) & 0b111L);
            output[outputOffset + 51] = (int) ((v2 >>> 25) & 0b111L);
            output[outputOffset + 52] = (int) ((v2 >>> 28) & 0b111L);
            output[outputOffset + 53] = (int) ((v2 >>> 31) & 0b111L);
            output[outputOffset + 54] = (int) ((v2 >>> 34) & 0b111L);
            output[outputOffset + 55] = (int) ((v2 >>> 37) & 0b111L);
            output[outputOffset + 56] = (int) ((v2 >>> 40) & 0b111L);
            output[outputOffset + 57] = (int) ((v2 >>> 43) & 0b111L);
            output[outputOffset + 58] = (int) ((v2 >>> 46) & 0b111L);
            output[outputOffset + 59] = (int) ((v2 >>> 49) & 0b111L);
            output[outputOffset + 60] = (int) ((v2 >>> 52) & 0b111L);
            output[outputOffset + 61] = (int) ((v2 >>> 55) & 0b111L);
            output[outputOffset + 62] = (int) ((v2 >>> 58) & 0b111L);
            output[outputOffset + 63] = (int) ((v2 >>> 61) & 0b111L);
        }

        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            short v0 = input.readShort();
            byte v1 = input.readByte();
            output[outputOffset] = (int) (v0 & 0b111L);
            output[outputOffset + 1] = (int) ((v0 >>> 3) & 0b111L);
            output[outputOffset + 2] = (int) ((v0 >>> 6) & 0b111L);
            output[outputOffset + 3] = (int) ((v0 >>> 9) & 0b111L);
            output[outputOffset + 4] = (int) ((v0 >>> 12) & 0b111L);
            output[outputOffset + 5] = (int) (((v0 >>> 15) & 0b1L) | ((v1 & 0b11L) << 1));
            output[outputOffset + 6] = (int) ((v1 >>> 2) & 0b111L);
            output[outputOffset + 7] = (int) ((v1 >>> 5) & 0b111L);
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 64) {
                unpack64(output, outputOffset, input);
                outputOffset += 64;
                length -= 64;
            }
            switch (length) {
                case 56:
                    unpack8(output, outputOffset, input);
                    outputOffset += 8;
                    // fall through
                case 48:
                    unpack8(output, outputOffset, input);
                    outputOffset += 8;
                    // fall through
                case 40:
                    unpack8(output, outputOffset, input);
                    outputOffset += 8;
                    // fall through
                case 32:
                    unpack8(output, outputOffset, input);
                    outputOffset += 8;
                    // fall through
                case 24:
                    unpack8(output, outputOffset, input);
                    outputOffset += 8;
                    // fall through
                case 16:
                    unpack8(output, outputOffset, input);
                    outputOffset += 8;
                    // fall through
                case 8:
                    unpack8(output, outputOffset, input);
            }
        }
    }

    private static final class Unpacker4
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, byte[] input, int inputOffset)
        {
            byte v0 = input[inputOffset];
            byte v1 = input[inputOffset + 1];
            byte v2 = input[inputOffset + 2];
            byte v3 = input[inputOffset + 3];

            output[outputOffset] = v0 & 0b1111;
            output[outputOffset + 1] = (v0 >>> 4) & 0b1111;
            output[outputOffset + 2] = v1 & 0b1111;
            output[outputOffset + 3] = (v1 >>> 4) & 0b1111;
            output[outputOffset + 4] = v2 & 0b1111;
            output[outputOffset + 5] = (v2 >>> 4) & 0b1111;
            output[outputOffset + 6] = v3 & 0b1111;
            output[outputOffset + 7] = (v3 >>> 4) & 0b1111;
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            byte[] inputArr = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            while (length >= 8) {
                unpack8(output, outputOffset, inputArr, inputOffset + inputBytesRead);
                outputOffset += 8;
                length -= 8;
                inputBytesRead += 4;
            }
            input.skip(inputBytesRead);
        }
    }

    private static final class Unpacker5
            implements IntBitUnpacker
    {
        private static void unpack64(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            output[outputOffset] = (int) (v0 & 0b11111L);
            output[outputOffset + 1] = (int) ((v0 >>> 5) & 0b11111L);
            output[outputOffset + 2] = (int) ((v0 >>> 10) & 0b11111L);
            output[outputOffset + 3] = (int) ((v0 >>> 15) & 0b11111L);
            output[outputOffset + 4] = (int) ((v0 >>> 20) & 0b11111L);
            output[outputOffset + 5] = (int) ((v0 >>> 25) & 0b11111L);
            output[outputOffset + 6] = (int) ((v0 >>> 30) & 0b11111L);
            output[outputOffset + 7] = (int) ((v0 >>> 35) & 0b11111L);
            output[outputOffset + 8] = (int) ((v0 >>> 40) & 0b11111L);
            output[outputOffset + 9] = (int) ((v0 >>> 45) & 0b11111L);
            output[outputOffset + 10] = (int) ((v0 >>> 50) & 0b11111L);
            output[outputOffset + 11] = (int) ((v0 >>> 55) & 0b11111L);
            output[outputOffset + 12] = (int) (((v0 >>> 60) & 0b1111L) | ((v1 & 0b1L) << 4));
            output[outputOffset + 13] = (int) ((v1 >>> 1) & 0b11111L);
            output[outputOffset + 14] = (int) ((v1 >>> 6) & 0b11111L);
            output[outputOffset + 15] = (int) ((v1 >>> 11) & 0b11111L);
            output[outputOffset + 16] = (int) ((v1 >>> 16) & 0b11111L);
            output[outputOffset + 17] = (int) ((v1 >>> 21) & 0b11111L);
            output[outputOffset + 18] = (int) ((v1 >>> 26) & 0b11111L);
            output[outputOffset + 19] = (int) ((v1 >>> 31) & 0b11111L);
            output[outputOffset + 20] = (int) ((v1 >>> 36) & 0b11111L);
            output[outputOffset + 21] = (int) ((v1 >>> 41) & 0b11111L);
            output[outputOffset + 22] = (int) ((v1 >>> 46) & 0b11111L);
            output[outputOffset + 23] = (int) ((v1 >>> 51) & 0b11111L);
            output[outputOffset + 24] = (int) ((v1 >>> 56) & 0b11111L);
            output[outputOffset + 25] = (int) (((v1 >>> 61) & 0b111L) | ((v2 & 0b11L) << 3));
            output[outputOffset + 26] = (int) ((v2 >>> 2) & 0b11111L);
            output[outputOffset + 27] = (int) ((v2 >>> 7) & 0b11111L);
            output[outputOffset + 28] = (int) ((v2 >>> 12) & 0b11111L);
            output[outputOffset + 29] = (int) ((v2 >>> 17) & 0b11111L);
            output[outputOffset + 30] = (int) ((v2 >>> 22) & 0b11111L);
            output[outputOffset + 31] = (int) ((v2 >>> 27) & 0b11111L);
            output[outputOffset + 32] = (int) ((v2 >>> 32) & 0b11111L);
            output[outputOffset + 33] = (int) ((v2 >>> 37) & 0b11111L);
            output[outputOffset + 34] = (int) ((v2 >>> 42) & 0b11111L);
            output[outputOffset + 35] = (int) ((v2 >>> 47) & 0b11111L);
            output[outputOffset + 36] = (int) ((v2 >>> 52) & 0b11111L);
            output[outputOffset + 37] = (int) ((v2 >>> 57) & 0b11111L);
            output[outputOffset + 38] = (int) (((v2 >>> 62) & 0b11L) | ((v3 & 0b111L) << 2));
            output[outputOffset + 39] = (int) ((v3 >>> 3) & 0b11111L);
            output[outputOffset + 40] = (int) ((v3 >>> 8) & 0b11111L);
            output[outputOffset + 41] = (int) ((v3 >>> 13) & 0b11111L);
            output[outputOffset + 42] = (int) ((v3 >>> 18) & 0b11111L);
            output[outputOffset + 43] = (int) ((v3 >>> 23) & 0b11111L);
            output[outputOffset + 44] = (int) ((v3 >>> 28) & 0b11111L);
            output[outputOffset + 45] = (int) ((v3 >>> 33) & 0b11111L);
            output[outputOffset + 46] = (int) ((v3 >>> 38) & 0b11111L);
            output[outputOffset + 47] = (int) ((v3 >>> 43) & 0b11111L);
            output[outputOffset + 48] = (int) ((v3 >>> 48) & 0b11111L);
            output[outputOffset + 49] = (int) ((v3 >>> 53) & 0b11111L);
            output[outputOffset + 50] = (int) ((v3 >>> 58) & 0b11111L);
            output[outputOffset + 51] = (int) (((v3 >>> 63) & 0b1L) | ((v4 & 0b1111L) << 1));
            output[outputOffset + 52] = (int) ((v4 >>> 4) & 0b11111L);
            output[outputOffset + 53] = (int) ((v4 >>> 9) & 0b11111L);
            output[outputOffset + 54] = (int) ((v4 >>> 14) & 0b11111L);
            output[outputOffset + 55] = (int) ((v4 >>> 19) & 0b11111L);
            output[outputOffset + 56] = (int) ((v4 >>> 24) & 0b11111L);
            output[outputOffset + 57] = (int) ((v4 >>> 29) & 0b11111L);
            output[outputOffset + 58] = (int) ((v4 >>> 34) & 0b11111L);
            output[outputOffset + 59] = (int) ((v4 >>> 39) & 0b11111L);
            output[outputOffset + 60] = (int) ((v4 >>> 44) & 0b11111L);
            output[outputOffset + 61] = (int) ((v4 >>> 49) & 0b11111L);
            output[outputOffset + 62] = (int) ((v4 >>> 54) & 0b11111L);
            output[outputOffset + 63] = (int) ((v4 >>> 59) & 0b11111L);
        }

        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            int v0 = input.readInt();
            byte v1 = input.readByte();
            output[outputOffset] = (int) (v0 & 0b11111L);
            output[outputOffset + 1] = (int) ((v0 >>> 5) & 0b11111L);
            output[outputOffset + 2] = (int) ((v0 >>> 10) & 0b11111L);
            output[outputOffset + 3] = (int) ((v0 >>> 15) & 0b11111L);
            output[outputOffset + 4] = (int) ((v0 >>> 20) & 0b11111L);
            output[outputOffset + 5] = (int) ((v0 >>> 25) & 0b11111L);
            output[outputOffset + 6] = (int) (((v0 >>> 30) & 0b11L) | ((v1 & 0b111L) << 2));
            output[outputOffset + 7] = (int) ((v1 >>> 3) & 0b11111L);
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 64) {
                unpack64(output, outputOffset, input);
                outputOffset += 64;
                length -= 64;
            }
            switch (length) {
                case 56:
                    unpack8(output, outputOffset, input);
                    outputOffset += 8;
                    // fall through
                case 48:
                    unpack8(output, outputOffset, input);
                    outputOffset += 8;
                    // fall through
                case 40:
                    unpack8(output, outputOffset, input);
                    outputOffset += 8;
                    // fall through
                case 32:
                    unpack8(output, outputOffset, input);
                    outputOffset += 8;
                    // fall through
                case 24:
                    unpack8(output, outputOffset, input);
                    outputOffset += 8;
                    // fall through
                case 16:
                    unpack8(output, outputOffset, input);
                    outputOffset += 8;
                    // fall through
                case 8:
                    unpack8(output, outputOffset, input);
            }
        }
    }

    private static final class Unpacker6
            implements IntBitUnpacker
    {
        private static void unpack32(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            output[outputOffset] = (int) (v0 & 0b111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 6) & 0b111111L);
            output[outputOffset + 2] = (int) ((v0 >>> 12) & 0b111111L);
            output[outputOffset + 3] = (int) ((v0 >>> 18) & 0b111111L);
            output[outputOffset + 4] = (int) ((v0 >>> 24) & 0b111111L);
            output[outputOffset + 5] = (int) ((v0 >>> 30) & 0b111111L);
            output[outputOffset + 6] = (int) ((v0 >>> 36) & 0b111111L);
            output[outputOffset + 7] = (int) ((v0 >>> 42) & 0b111111L);
            output[outputOffset + 8] = (int) ((v0 >>> 48) & 0b111111L);
            output[outputOffset + 9] = (int) ((v0 >>> 54) & 0b111111L);
            output[outputOffset + 10] = (int) (((v0 >>> 60) & 0b1111L) | ((v1 & 0b11L) << 4));
            output[outputOffset + 11] = (int) ((v1 >>> 2) & 0b111111L);
            output[outputOffset + 12] = (int) ((v1 >>> 8) & 0b111111L);
            output[outputOffset + 13] = (int) ((v1 >>> 14) & 0b111111L);
            output[outputOffset + 14] = (int) ((v1 >>> 20) & 0b111111L);
            output[outputOffset + 15] = (int) ((v1 >>> 26) & 0b111111L);
            output[outputOffset + 16] = (int) ((v1 >>> 32) & 0b111111L);
            output[outputOffset + 17] = (int) ((v1 >>> 38) & 0b111111L);
            output[outputOffset + 18] = (int) ((v1 >>> 44) & 0b111111L);
            output[outputOffset + 19] = (int) ((v1 >>> 50) & 0b111111L);
            output[outputOffset + 20] = (int) ((v1 >>> 56) & 0b111111L);
            output[outputOffset + 21] = (int) (((v1 >>> 62) & 0b11L) | ((v2 & 0b1111L) << 2));
            output[outputOffset + 22] = (int) ((v2 >>> 4) & 0b111111L);
            output[outputOffset + 23] = (int) ((v2 >>> 10) & 0b111111L);
            output[outputOffset + 24] = (int) ((v2 >>> 16) & 0b111111L);
            output[outputOffset + 25] = (int) ((v2 >>> 22) & 0b111111L);
            output[outputOffset + 26] = (int) ((v2 >>> 28) & 0b111111L);
            output[outputOffset + 27] = (int) ((v2 >>> 34) & 0b111111L);
            output[outputOffset + 28] = (int) ((v2 >>> 40) & 0b111111L);
            output[outputOffset + 29] = (int) ((v2 >>> 46) & 0b111111L);
            output[outputOffset + 30] = (int) ((v2 >>> 52) & 0b111111L);
            output[outputOffset + 31] = (int) ((v2 >>> 58) & 0b111111L);
        }

        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            int v0 = input.readInt();
            short v1 = input.readShort();
            output[outputOffset] = (int) (v0 & 0b111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 6) & 0b111111L);
            output[outputOffset + 2] = (int) ((v0 >>> 12) & 0b111111L);
            output[outputOffset + 3] = (int) ((v0 >>> 18) & 0b111111L);
            output[outputOffset + 4] = (int) ((v0 >>> 24) & 0b111111L);
            output[outputOffset + 5] = (int) (((v0 >>> 30) & 0b11L) | ((v1 & 0b1111L) << 2));
            output[outputOffset + 6] = (int) ((v1 >>> 4) & 0b111111L);
            output[outputOffset + 7] = (int) ((v1 >>> 10) & 0b111111L);
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 32) {
                unpack32(output, outputOffset, input);
                outputOffset += 32;
                length -= 32;
            }
            switch (length) {
                case 24:
                    unpack8(output, outputOffset, input);
                    outputOffset += 8;
                    // fall through
                case 16:
                    unpack8(output, outputOffset, input);
                    outputOffset += 8;
                    // fall through
                case 8:
                    unpack8(output, outputOffset, input);
            }
        }
    }

    private static final class Unpacker7
            implements IntBitUnpacker
    {
        private static void unpack64(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            long v3 = input.readLong();
            long v4 = input.readLong();
            long v5 = input.readLong();
            long v6 = input.readLong();
            output[outputOffset] = (int) (v0 & 0b1111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 7) & 0b1111111L);
            output[outputOffset + 2] = (int) ((v0 >>> 14) & 0b1111111L);
            output[outputOffset + 3] = (int) ((v0 >>> 21) & 0b1111111L);
            output[outputOffset + 4] = (int) ((v0 >>> 28) & 0b1111111L);
            output[outputOffset + 5] = (int) ((v0 >>> 35) & 0b1111111L);
            output[outputOffset + 6] = (int) ((v0 >>> 42) & 0b1111111L);
            output[outputOffset + 7] = (int) ((v0 >>> 49) & 0b1111111L);
            output[outputOffset + 8] = (int) ((v0 >>> 56) & 0b1111111L);
            output[outputOffset + 9] = (int) (((v0 >>> 63) & 0b1L) | ((v1 & 0b111111L) << 1));
            output[outputOffset + 10] = (int) ((v1 >>> 6) & 0b1111111L);
            output[outputOffset + 11] = (int) ((v1 >>> 13) & 0b1111111L);
            output[outputOffset + 12] = (int) ((v1 >>> 20) & 0b1111111L);
            output[outputOffset + 13] = (int) ((v1 >>> 27) & 0b1111111L);
            output[outputOffset + 14] = (int) ((v1 >>> 34) & 0b1111111L);
            output[outputOffset + 15] = (int) ((v1 >>> 41) & 0b1111111L);
            output[outputOffset + 16] = (int) ((v1 >>> 48) & 0b1111111L);
            output[outputOffset + 17] = (int) ((v1 >>> 55) & 0b1111111L);
            output[outputOffset + 18] = (int) (((v1 >>> 62) & 0b11L) | ((v2 & 0b11111L) << 2));
            output[outputOffset + 19] = (int) ((v2 >>> 5) & 0b1111111L);
            output[outputOffset + 20] = (int) ((v2 >>> 12) & 0b1111111L);
            output[outputOffset + 21] = (int) ((v2 >>> 19) & 0b1111111L);
            output[outputOffset + 22] = (int) ((v2 >>> 26) & 0b1111111L);
            output[outputOffset + 23] = (int) ((v2 >>> 33) & 0b1111111L);
            output[outputOffset + 24] = (int) ((v2 >>> 40) & 0b1111111L);
            output[outputOffset + 25] = (int) ((v2 >>> 47) & 0b1111111L);
            output[outputOffset + 26] = (int) ((v2 >>> 54) & 0b1111111L);
            output[outputOffset + 27] = (int) (((v2 >>> 61) & 0b111L) | ((v3 & 0b1111L) << 3));
            output[outputOffset + 28] = (int) ((v3 >>> 4) & 0b1111111L);
            output[outputOffset + 29] = (int) ((v3 >>> 11) & 0b1111111L);
            output[outputOffset + 30] = (int) ((v3 >>> 18) & 0b1111111L);
            output[outputOffset + 31] = (int) ((v3 >>> 25) & 0b1111111L);
            output[outputOffset + 32] = (int) ((v3 >>> 32) & 0b1111111L);
            output[outputOffset + 33] = (int) ((v3 >>> 39) & 0b1111111L);
            output[outputOffset + 34] = (int) ((v3 >>> 46) & 0b1111111L);
            output[outputOffset + 35] = (int) ((v3 >>> 53) & 0b1111111L);
            output[outputOffset + 36] = (int) (((v3 >>> 60) & 0b1111L) | ((v4 & 0b111L) << 4));
            output[outputOffset + 37] = (int) ((v4 >>> 3) & 0b1111111L);
            output[outputOffset + 38] = (int) ((v4 >>> 10) & 0b1111111L);
            output[outputOffset + 39] = (int) ((v4 >>> 17) & 0b1111111L);
            output[outputOffset + 40] = (int) ((v4 >>> 24) & 0b1111111L);
            output[outputOffset + 41] = (int) ((v4 >>> 31) & 0b1111111L);
            output[outputOffset + 42] = (int) ((v4 >>> 38) & 0b1111111L);
            output[outputOffset + 43] = (int) ((v4 >>> 45) & 0b1111111L);
            output[outputOffset + 44] = (int) ((v4 >>> 52) & 0b1111111L);
            output[outputOffset + 45] = (int) (((v4 >>> 59) & 0b11111L) | ((v5 & 0b11L) << 5));
            output[outputOffset + 46] = (int) ((v5 >>> 2) & 0b1111111L);
            output[outputOffset + 47] = (int) ((v5 >>> 9) & 0b1111111L);
            output[outputOffset + 48] = (int) ((v5 >>> 16) & 0b1111111L);
            output[outputOffset + 49] = (int) ((v5 >>> 23) & 0b1111111L);
            output[outputOffset + 50] = (int) ((v5 >>> 30) & 0b1111111L);
            output[outputOffset + 51] = (int) ((v5 >>> 37) & 0b1111111L);
            output[outputOffset + 52] = (int) ((v5 >>> 44) & 0b1111111L);
            output[outputOffset + 53] = (int) ((v5 >>> 51) & 0b1111111L);
            output[outputOffset + 54] = (int) (((v5 >>> 58) & 0b111111L) | ((v6 & 0b1L) << 6));
            output[outputOffset + 55] = (int) ((v6 >>> 1) & 0b1111111L);
            output[outputOffset + 56] = (int) ((v6 >>> 8) & 0b1111111L);
            output[outputOffset + 57] = (int) ((v6 >>> 15) & 0b1111111L);
            output[outputOffset + 58] = (int) ((v6 >>> 22) & 0b1111111L);
            output[outputOffset + 59] = (int) ((v6 >>> 29) & 0b1111111L);
            output[outputOffset + 60] = (int) ((v6 >>> 36) & 0b1111111L);
            output[outputOffset + 61] = (int) ((v6 >>> 43) & 0b1111111L);
            output[outputOffset + 62] = (int) ((v6 >>> 50) & 0b1111111L);
            output[outputOffset + 63] = (int) ((v6 >>> 57) & 0b1111111L);
        }

        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            int v0 = input.readInt();
            short v1 = input.readShort();
            byte v2 = input.readByte();
            output[outputOffset] = (int) (v0 & 0b1111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 7) & 0b1111111L);
            output[outputOffset + 2] = (int) ((v0 >>> 14) & 0b1111111L);
            output[outputOffset + 3] = (int) ((v0 >>> 21) & 0b1111111L);
            output[outputOffset + 4] = (int) (((v0 >>> 28) & 0b1111L) | ((v1 & 0b111L) << 4));
            output[outputOffset + 5] = (int) ((v1 >>> 3) & 0b1111111L);
            output[outputOffset + 6] = (int) (((v1 >>> 10) & 0b111111L) | ((v2 & 0b1L) << 6));
            output[outputOffset + 7] = (int) ((v2 >>> 1) & 0b1111111L);
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input,
                int length)
        {
            while (length >= 64) {
                unpack64(output, outputOffset, input);
                outputOffset += 64;
                length -= 64;
            }
            switch (length) {
                case 56:
                    unpack8(output, outputOffset, input);
                    outputOffset += 8;
                    // fall through
                case 48:
                    unpack8(output, outputOffset, input);
                    outputOffset += 8;
                    // fall through
                case 40:
                    unpack8(output, outputOffset, input);
                    outputOffset += 8;
                    // fall through
                case 32:
                    unpack8(output, outputOffset, input);
                    outputOffset += 8;
                    // fall through
                case 24:
                    unpack8(output, outputOffset, input);
                    outputOffset += 8;
                    // fall through
                case 16:
                    unpack8(output, outputOffset, input);
                    outputOffset += 8;
                    // fall through
                case 8:
                    unpack8(output, outputOffset, input);
            }
        }
    }

    private static final class Unpacker8
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, byte[] input, int inputOffset)
        {
            output[outputOffset] = input[inputOffset] & 0b11111111;
            output[outputOffset + 1] = input[inputOffset + 1] & 0b11111111;
            output[outputOffset + 2] = input[inputOffset + 2] & 0b11111111;
            output[outputOffset + 3] = input[inputOffset + 3] & 0b11111111;
            output[outputOffset + 4] = input[inputOffset + 4] & 0b11111111;
            output[outputOffset + 5] = input[inputOffset + 5] & 0b11111111;
            output[outputOffset + 6] = input[inputOffset + 6] & 0b11111111;
            output[outputOffset + 7] = input[inputOffset + 7] & 0b11111111;
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            while (length >= 8) {
                unpack8(output, outputOffset, inputArray, inputOffset + inputBytesRead);
                outputOffset += 8;
                length -= 8;
                inputBytesRead += 8;
            }
            input.skip(inputBytesRead);
        }
    }

    private static final class Unpacker9
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            byte v1 = input.readByte();
            output[outputOffset] = (int) (v0 & 0b111111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 9) & 0b111111111L);
            output[outputOffset + 2] = (int) ((v0 >>> 18) & 0b111111111L);
            output[outputOffset + 3] = (int) ((v0 >>> 27) & 0b111111111L);
            output[outputOffset + 4] = (int) ((v0 >>> 36) & 0b111111111L);
            output[outputOffset + 5] = (int) ((v0 >>> 45) & 0b111111111L);
            output[outputOffset + 6] = (int) ((v0 >>> 54) & 0b111111111L);
            output[outputOffset + 7] = (int) (((v0 >>> 63) & 0b1L) | ((v1 & 0b11111111L) << 1));
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 8) {
                unpack8(output, outputOffset, input);
                outputOffset += 8;
                length -= 8;
            }
        }
    }

    private static final class Unpacker10
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            short v1 = input.readShort();
            output[outputOffset] = (int) (v0 & 0b1111111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 10) & 0b1111111111L);
            output[outputOffset + 2] = (int) ((v0 >>> 20) & 0b1111111111L);
            output[outputOffset + 3] = (int) ((v0 >>> 30) & 0b1111111111L);
            output[outputOffset + 4] = (int) ((v0 >>> 40) & 0b1111111111L);
            output[outputOffset + 5] = (int) ((v0 >>> 50) & 0b1111111111L);
            output[outputOffset + 6] = (int) (((v0 >>> 60) & 0b1111L) | ((v1 & 0b111111L) << 4));
            output[outputOffset + 7] = (int) ((v1 >>> 6) & 0b1111111111L);
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 8) {
                unpack8(output, outputOffset, input);
                outputOffset += 8;
                length -= 8;
            }
        }
    }

    private static final class Unpacker11
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            short v1 = input.readShort();
            byte v2 = input.readByte();
            output[outputOffset] = (int) (v0 & 0b11111111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 11) & 0b11111111111L);
            output[outputOffset + 2] = (int) ((v0 >>> 22) & 0b11111111111L);
            output[outputOffset + 3] = (int) ((v0 >>> 33) & 0b11111111111L);
            output[outputOffset + 4] = (int) ((v0 >>> 44) & 0b11111111111L);
            output[outputOffset + 5] = (int) (((v0 >>> 55) & 0b111111111L) | ((v1 & 0b11L) << 9));
            output[outputOffset + 6] = (int) ((v1 >>> 2) & 0b11111111111L);
            output[outputOffset + 7] = (int) (((v1 >>> 13) & 0b111L) | ((v2 & 0b11111111L) << 3));
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 8) {
                unpack8(output, outputOffset, input);
                outputOffset += 8;
                length -= 8;
            }
        }
    }

    private static final class Unpacker12
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            int v1 = input.readInt();
            output[outputOffset] = (int) (v0 & 0b111111111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 12) & 0b111111111111L);
            output[outputOffset + 2] = (int) ((v0 >>> 24) & 0b111111111111L);
            output[outputOffset + 3] = (int) ((v0 >>> 36) & 0b111111111111L);
            output[outputOffset + 4] = (int) ((v0 >>> 48) & 0b111111111111L);
            output[outputOffset + 5] = (int) (((v0 >>> 60) & 0b1111L) | ((v1 & 0b11111111L) << 4));
            output[outputOffset + 6] = (int) ((v1 >>> 8) & 0b111111111111L);
            output[outputOffset + 7] = (int) ((v1 >>> 20) & 0b111111111111L);
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 8) {
                unpack8(output, outputOffset, input);
                outputOffset += 8;
                length -= 8;
            }
        }
    }

    private static final class Unpacker13
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            int v1 = input.readInt();
            byte v2 = input.readByte();
            output[outputOffset] = (int) (v0 & 0b1111111111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 13) & 0b1111111111111L);
            output[outputOffset + 2] = (int) ((v0 >>> 26) & 0b1111111111111L);
            output[outputOffset + 3] = (int) ((v0 >>> 39) & 0b1111111111111L);
            output[outputOffset + 4] = (int) (((v0 >>> 52) & 0b111111111111L) | ((v1 & 0b1L) << 12));
            output[outputOffset + 5] = (int) ((v1 >>> 1) & 0b1111111111111L);
            output[outputOffset + 6] = (int) ((v1 >>> 14) & 0b1111111111111L);
            output[outputOffset + 7] = (int) (((v1 >>> 27) & 0b11111L) | ((v2 & 0b11111111L) << 5));
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 8) {
                unpack8(output, outputOffset, input);
                outputOffset += 8;
                length -= 8;
            }
        }
    }

    private static final class Unpacker14
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            int v1 = input.readInt();
            short v2 = input.readShort();
            output[outputOffset] = (int) (v0 & 0b11111111111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 14) & 0b11111111111111L);
            output[outputOffset + 2] = (int) ((v0 >>> 28) & 0b11111111111111L);
            output[outputOffset + 3] = (int) ((v0 >>> 42) & 0b11111111111111L);
            output[outputOffset + 4] = (int) (((v0 >>> 56) & 0b11111111L) | ((v1 & 0b111111L) << 8));
            output[outputOffset + 5] = (int) ((v1 >>> 6) & 0b11111111111111L);
            output[outputOffset + 6] = (int) (((v1 >>> 20) & 0b111111111111L) | ((v2 & 0b11L) << 12));
            output[outputOffset + 7] = (int) ((v2 >>> 2) & 0b11111111111111L);
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 8) {
                unpack8(output, outputOffset, input);
                outputOffset += 8;
                length -= 8;
            }
        }
    }

    private static final class Unpacker15
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            int v1 = input.readInt();
            short v2 = input.readShort();
            byte v3 = input.readByte();
            output[outputOffset] = (int) (v0 & 0b111111111111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 15) & 0b111111111111111L);
            output[outputOffset + 2] = (int) ((v0 >>> 30) & 0b111111111111111L);
            output[outputOffset + 3] = (int) ((v0 >>> 45) & 0b111111111111111L);
            output[outputOffset + 4] = (int) (((v0 >>> 60) & 0b1111L) | ((v1 & 0b11111111111L) << 4));
            output[outputOffset + 5] = (int) ((v1 >>> 11) & 0b111111111111111L);
            output[outputOffset + 6] = (int) (((v1 >>> 26) & 0b111111L) | ((v2 & 0b111111111L) << 6));
            output[outputOffset + 7] = (int) (((v2 >>> 9) & 0b1111111L) | ((v3 & 0b11111111L) << 7));
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 8) {
                unpack8(output, outputOffset, input);
                outputOffset += 8;
                length -= 8;
            }
        }
    }

    private static final class Unpacker16
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            output[outputOffset] = (int) (v0 & 0b1111111111111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 16) & 0b1111111111111111L);
            output[outputOffset + 2] = (int) ((v0 >>> 32) & 0b1111111111111111L);
            output[outputOffset + 3] = (int) ((v0 >>> 48) & 0b1111111111111111L);
            output[outputOffset + 4] = (int) (v1 & 0b1111111111111111L);
            output[outputOffset + 5] = (int) ((v1 >>> 16) & 0b1111111111111111L);
            output[outputOffset + 6] = (int) ((v1 >>> 32) & 0b1111111111111111L);
            output[outputOffset + 7] = (int) ((v1 >>> 48) & 0b1111111111111111L);
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 8) {
                unpack8(output, outputOffset, input);
                outputOffset += 8;
                length -= 8;
            }
        }
    }

    private static final class Unpacker17
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            byte v2 = input.readByte();
            output[outputOffset] = (int) (v0 & 0b11111111111111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 17) & 0b11111111111111111L);
            output[outputOffset + 2] = (int) ((v0 >>> 34) & 0b11111111111111111L);
            output[outputOffset + 3] = (int) (((v0 >>> 51) & 0b1111111111111L) | ((v1 & 0b1111L) << 13));
            output[outputOffset + 4] = (int) ((v1 >>> 4) & 0b11111111111111111L);
            output[outputOffset + 5] = (int) ((v1 >>> 21) & 0b11111111111111111L);
            output[outputOffset + 6] = (int) ((v1 >>> 38) & 0b11111111111111111L);
            output[outputOffset + 7] = (int) (((v1 >>> 55) & 0b111111111L) | ((v2 & 0b11111111L) << 9));
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 8) {
                unpack8(output, outputOffset, input);
                outputOffset += 8;
                length -= 8;
            }
        }
    }

    private static final class Unpacker18
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            short v2 = input.readShort();
            output[outputOffset] = (int) (v0 & 0b111111111111111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 18) & 0b111111111111111111L);
            output[outputOffset + 2] = (int) ((v0 >>> 36) & 0b111111111111111111L);
            output[outputOffset + 3] = (int) (((v0 >>> 54) & 0b1111111111L) | ((v1 & 0b11111111L) << 10));
            output[outputOffset + 4] = (int) ((v1 >>> 8) & 0b111111111111111111L);
            output[outputOffset + 5] = (int) ((v1 >>> 26) & 0b111111111111111111L);
            output[outputOffset + 6] = (int) ((v1 >>> 44) & 0b111111111111111111L);
            output[outputOffset + 7] = (int) (((v1 >>> 62) & 0b11L) | ((v2 & 0b1111111111111111L) << 2));
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 8) {
                unpack8(output, outputOffset, input);
                outputOffset += 8;
                length -= 8;
            }
        }
    }

    private static final class Unpacker19
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            short v2 = input.readShort();
            byte v3 = input.readByte();
            output[outputOffset] = (int) (v0 & 0b1111111111111111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 19) & 0b1111111111111111111L);
            output[outputOffset + 2] = (int) ((v0 >>> 38) & 0b1111111111111111111L);
            output[outputOffset + 3] = (int) (((v0 >>> 57) & 0b1111111L) | ((v1 & 0b111111111111L) << 7));
            output[outputOffset + 4] = (int) ((v1 >>> 12) & 0b1111111111111111111L);
            output[outputOffset + 5] = (int) ((v1 >>> 31) & 0b1111111111111111111L);
            output[outputOffset + 6] = (int) (((v1 >>> 50) & 0b11111111111111L) | ((v2 & 0b11111L) << 14));
            output[outputOffset + 7] = (int) (((v2 >>> 5) & 0b11111111111L) | ((v3 & 0b11111111L) << 11));
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 8) {
                unpack8(output, outputOffset, input);
                outputOffset += 8;
                length -= 8;
            }
        }
    }

    private static final class Unpacker20
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            int v2 = input.readInt();
            output[outputOffset] = (int) (v0 & 0b11111111111111111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 20) & 0b11111111111111111111L);
            output[outputOffset + 2] = (int) ((v0 >>> 40) & 0b11111111111111111111L);
            output[outputOffset + 3] = (int) (((v0 >>> 60) & 0b1111L) | ((v1 & 0b1111111111111111L) << 4));
            output[outputOffset + 4] = (int) ((v1 >>> 16) & 0b11111111111111111111L);
            output[outputOffset + 5] = (int) ((v1 >>> 36) & 0b11111111111111111111L);
            output[outputOffset + 6] = (int) (((v1 >>> 56) & 0b11111111L) | ((v2 & 0b111111111111L) << 8));
            output[outputOffset + 7] = (int) ((v2 >>> 12) & 0b11111111111111111111L);
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 8) {
                unpack8(output, outputOffset, input);
                outputOffset += 8;
                length -= 8;
            }
        }
    }

    private static final class Unpacker21
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            int v2 = input.readInt();
            byte v3 = input.readByte();
            output[outputOffset] = (int) (v0 & 0b111111111111111111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 21) & 0b111111111111111111111L);
            output[outputOffset + 2] = (int) ((v0 >>> 42) & 0b111111111111111111111L);
            output[outputOffset + 3] = (int) (((v0 >>> 63) & 0b1L) | ((v1 & 0b11111111111111111111L) << 1));
            output[outputOffset + 4] = (int) ((v1 >>> 20) & 0b111111111111111111111L);
            output[outputOffset + 5] = (int) ((v1 >>> 41) & 0b111111111111111111111L);
            output[outputOffset + 6] = (int) (((v1 >>> 62) & 0b11L) | ((v2 & 0b1111111111111111111L) << 2));
            output[outputOffset + 7] = (int) (((v2 >>> 19) & 0b1111111111111L) | ((v3 & 0b11111111L) << 13));
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 8) {
                unpack8(output, outputOffset, input);
                outputOffset += 8;
                length -= 8;
            }
        }
    }

    private static final class Unpacker22
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            int v2 = input.readInt();
            short v3 = input.readShort();
            output[outputOffset] = (int) (v0 & 0b1111111111111111111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 22) & 0b1111111111111111111111L);
            output[outputOffset + 2] = (int) (((v0 >>> 44) & 0b11111111111111111111L) | ((v1 & 0b11L) << 20));
            output[outputOffset + 3] = (int) ((v1 >>> 2) & 0b1111111111111111111111L);
            output[outputOffset + 4] = (int) ((v1 >>> 24) & 0b1111111111111111111111L);
            output[outputOffset + 5] = (int) (((v1 >>> 46) & 0b111111111111111111L) | ((v2 & 0b1111L) << 18));
            output[outputOffset + 6] = (int) ((v2 >>> 4) & 0b1111111111111111111111L);
            output[outputOffset + 7] = (int) (((v2 >>> 26) & 0b111111L) | ((v3 & 0b1111111111111111L) << 6));
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 8) {
                unpack8(output, outputOffset, input);
                outputOffset += 8;
                length -= 8;
            }
        }
    }

    private static final class Unpacker23
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            int v2 = input.readInt();
            short v3 = input.readShort();
            byte v4 = input.readByte();
            output[outputOffset] = (int) (v0 & 0b11111111111111111111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 23) & 0b11111111111111111111111L);
            output[outputOffset + 2] = (int) (((v0 >>> 46) & 0b111111111111111111L) | ((v1 & 0b11111L) << 18));
            output[outputOffset + 3] = (int) ((v1 >>> 5) & 0b11111111111111111111111L);
            output[outputOffset + 4] = (int) ((v1 >>> 28) & 0b11111111111111111111111L);
            output[outputOffset + 5] = (int) (((v1 >>> 51) & 0b1111111111111L) | ((v2 & 0b1111111111L) << 13));
            output[outputOffset + 6] = (int) (((v2 >>> 10) & 0b1111111111111111111111L) | ((v3 & 0b1L) << 22));
            output[outputOffset + 7] = (int) (((v3 >>> 1) & 0b111111111111111L) | ((v4 & 0b11111111L) << 15));
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 8) {
                unpack8(output, outputOffset, input);
                outputOffset += 8;
                length -= 8;
            }
        }
    }

    private static final class Unpacker24
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            output[outputOffset] = (int) (v0 & 0b111111111111111111111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 24) & 0b111111111111111111111111L);
            output[outputOffset + 2] = (int) (((v0 >>> 48) & 0b1111111111111111L) | ((v1 & 0b11111111L) << 16));
            output[outputOffset + 3] = (int) ((v1 >>> 8) & 0b111111111111111111111111L);
            output[outputOffset + 4] = (int) ((v1 >>> 32) & 0b111111111111111111111111L);
            output[outputOffset + 5] = (int) (((v1 >>> 56) & 0b11111111L) | ((v2 & 0b1111111111111111L) << 8));
            output[outputOffset + 6] = (int) ((v2 >>> 16) & 0b111111111111111111111111L);
            output[outputOffset + 7] = (int) ((v2 >>> 40) & 0b111111111111111111111111L);
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 8) {
                unpack8(output, outputOffset, input);
                outputOffset += 8;
                length -= 8;
            }
        }
    }

    private static final class Unpacker25
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            byte v3 = input.readByte();
            output[outputOffset] = (int) (v0 & 0b1111111111111111111111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 25) & 0b1111111111111111111111111L);
            output[outputOffset + 2] = (int) (((v0 >>> 50) & 0b11111111111111L) | ((v1 & 0b11111111111L) << 14));
            output[outputOffset + 3] = (int) ((v1 >>> 11) & 0b1111111111111111111111111L);
            output[outputOffset + 4] = (int) ((v1 >>> 36) & 0b1111111111111111111111111L);
            output[outputOffset + 5] = (int) (((v1 >>> 61) & 0b111L) | ((v2 & 0b1111111111111111111111L) << 3));
            output[outputOffset + 6] = (int) ((v2 >>> 22) & 0b1111111111111111111111111L);
            output[outputOffset + 7] = (int) (((v2 >>> 47) & 0b11111111111111111L) | ((v3 & 0b11111111L) << 17));
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 8) {
                unpack8(output, outputOffset, input);
                outputOffset += 8;
                length -= 8;
            }
        }
    }

    private static final class Unpacker26
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            short v3 = input.readShort();
            output[outputOffset] = (int) (v0 & 0b11111111111111111111111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 26) & 0b11111111111111111111111111L);
            output[outputOffset + 2] = (int) (((v0 >>> 52) & 0b111111111111L) | ((v1 & 0b11111111111111L) << 12));
            output[outputOffset + 3] = (int) ((v1 >>> 14) & 0b11111111111111111111111111L);
            output[outputOffset + 4] = (int) (((v1 >>> 40) & 0b111111111111111111111111L) | ((v2 & 0b11L) << 24));
            output[outputOffset + 5] = (int) ((v2 >>> 2) & 0b11111111111111111111111111L);
            output[outputOffset + 6] = (int) ((v2 >>> 28) & 0b11111111111111111111111111L);
            output[outputOffset + 7] = (int) (((v2 >>> 54) & 0b1111111111L) | ((v3 & 0b1111111111111111L) << 10));
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 8) {
                unpack8(output, outputOffset, input);
                outputOffset += 8;
                length -= 8;
            }
        }
    }

    private static final class Unpacker27
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            short v3 = input.readShort();
            byte v4 = input.readByte();
            output[outputOffset] = (int) (v0 & 0b111111111111111111111111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 27) & 0b111111111111111111111111111L);
            output[outputOffset + 2] = (int) (((v0 >>> 54) & 0b1111111111L) | ((v1 & 0b11111111111111111L) << 10));
            output[outputOffset + 3] = (int) ((v1 >>> 17) & 0b111111111111111111111111111L);
            output[outputOffset + 4] = (int) (((v1 >>> 44) & 0b11111111111111111111L) | ((v2 & 0b1111111L) << 20));
            output[outputOffset + 5] = (int) ((v2 >>> 7) & 0b111111111111111111111111111L);
            output[outputOffset + 6] = (int) ((v2 >>> 34) & 0b111111111111111111111111111L);
            output[outputOffset + 7] = (int) (((v2 >>> 61) & 0b111L) | ((v3 & 0b1111111111111111L) << 3) | ((v4 & 0b11111111L) << 19));
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 8) {
                unpack8(output, outputOffset, input);
                outputOffset += 8;
                length -= 8;
            }
        }
    }

    private static final class Unpacker28
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            int v3 = input.readInt();
            output[outputOffset] = (int) (v0 & 0b1111111111111111111111111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 28) & 0b1111111111111111111111111111L);
            output[outputOffset + 2] = (int) (((v0 >>> 56) & 0b11111111L) | ((v1 & 0b11111111111111111111L) << 8));
            output[outputOffset + 3] = (int) ((v1 >>> 20) & 0b1111111111111111111111111111L);
            output[outputOffset + 4] = (int) (((v1 >>> 48) & 0b1111111111111111L) | ((v2 & 0b111111111111L) << 16));
            output[outputOffset + 5] = (int) ((v2 >>> 12) & 0b1111111111111111111111111111L);
            output[outputOffset + 6] = (int) (((v2 >>> 40) & 0b111111111111111111111111L) | ((v3 & 0b1111L) << 24));
            output[outputOffset + 7] = (int) ((v3 >>> 4) & 0b1111111111111111111111111111L);
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 8) {
                unpack8(output, outputOffset, input);
                outputOffset += 8;
                length -= 8;
            }
        }
    }

    private static final class Unpacker29
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            int v3 = input.readInt();
            byte v4 = input.readByte();
            output[outputOffset] = (int) (v0 & 0b11111111111111111111111111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 29) & 0b11111111111111111111111111111L);
            output[outputOffset + 2] = (int) (((v0 >>> 58) & 0b111111L) | ((v1 & 0b11111111111111111111111L) << 6));
            output[outputOffset + 3] = (int) ((v1 >>> 23) & 0b11111111111111111111111111111L);
            output[outputOffset + 4] = (int) (((v1 >>> 52) & 0b111111111111L) | ((v2 & 0b11111111111111111L) << 12));
            output[outputOffset + 5] = (int) ((v2 >>> 17) & 0b11111111111111111111111111111L);
            output[outputOffset + 6] = (int) (((v2 >>> 46) & 0b111111111111111111L) | ((v3 & 0b11111111111L) << 18));
            output[outputOffset + 7] = (int) (((v3 >>> 11) & 0b111111111111111111111L) | ((v4 & 0b11111111L) << 21));
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input,
                int length)
        {
            while (length >= 8) {
                unpack8(output, outputOffset, input);
                outputOffset += 8;
                length -= 8;
            }
        }
    }

    private static final class Unpacker30
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            int v3 = input.readInt();
            short v4 = input.readShort();
            output[outputOffset] = (int) (v0 & 0b111111111111111111111111111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 30) & 0b111111111111111111111111111111L);
            output[outputOffset + 2] = (int) (((v0 >>> 60) & 0b1111L) | ((v1 & 0b11111111111111111111111111L) << 4));
            output[outputOffset + 3] = (int) ((v1 >>> 26) & 0b111111111111111111111111111111L);
            output[outputOffset + 4] = (int) (((v1 >>> 56) & 0b11111111L) | ((v2 & 0b1111111111111111111111L) << 8));
            output[outputOffset + 5] = (int) ((v2 >>> 22) & 0b111111111111111111111111111111L);
            output[outputOffset + 6] = (int) (((v2 >>> 52) & 0b111111111111L) | ((v3 & 0b111111111111111111L) << 12));
            output[outputOffset + 7] = (int) (((v3 >>> 18) & 0b11111111111111L) | ((v4 & 0b1111111111111111L) << 14));
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input,
                int length)
        {
            while (length >= 8) {
                unpack8(output, outputOffset, input);
                outputOffset += 8;
                length -= 8;
            }
        }
    }

    private static final class Unpacker31
            implements IntBitUnpacker
    {
        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            long v0 = input.readLong();
            long v1 = input.readLong();
            long v2 = input.readLong();
            int v3 = input.readInt();
            short v4 = input.readShort();
            byte v5 = input.readByte();
            output[outputOffset] = (int) (v0 & 0b1111111111111111111111111111111L);
            output[outputOffset + 1] = (int) ((v0 >>> 31) & 0b1111111111111111111111111111111L);
            output[outputOffset + 2] = (int) (((v0 >>> 62) & 0b11L) | ((v1 & 0b11111111111111111111111111111L) << 2));
            output[outputOffset + 3] = (int) ((v1 >>> 29) & 0b1111111111111111111111111111111L);
            output[outputOffset + 4] = (int) (((v1 >>> 60) & 0b1111L) | ((v2 & 0b111111111111111111111111111L) << 4));
            output[outputOffset + 5] = (int) ((v2 >>> 27) & 0b1111111111111111111111111111111L);
            output[outputOffset + 6] = (int) (((v2 >>> 58) & 0b111111L) | ((v3 & 0b1111111111111111111111111L) << 6));
            output[outputOffset + 7] = (int) (((v3 >>> 25) & 0b1111111L) | ((v4 & 0b1111111111111111L) << 7) | ((v5 & 0b11111111L) << 23));
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            while (length >= 8) {
                unpack8(output, outputOffset, input);
                outputOffset += 8;
                length -= 8;
            }
        }
    }

    private static final class Unpacker32
            implements IntBitUnpacker
    {
        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            input.readBytes(Slices.wrappedIntArray(output, outputOffset, length), 0, length * Integer.BYTES);
        }
    }
}
