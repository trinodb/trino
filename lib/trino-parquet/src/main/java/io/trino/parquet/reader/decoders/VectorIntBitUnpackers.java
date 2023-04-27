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
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorShuffle;

import static io.trino.parquet.reader.decoders.IntBitUnpackers.getIntBitUnpacker;

public final class VectorIntBitUnpackers
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
            new Unpacker20()};

    public static IntBitUnpacker getVectorIntBitUnpacker(int bitWidth)
    {
        // This encoding is used for dictionary ids, repetition and definition levels in V1 encodings
        // and additionally for delta encoded integers in V2 encodings.
        // We vectorize encodings upto 20 bit width as that is more than enough for real world usage of V1 encodings.
        // The remaining bit widths can be vectorized when there is a need to do so.
        if (bitWidth > 20) {
            return getIntBitUnpacker(bitWidth);
        }
        return UNPACKERS[bitWidth];
    }

    private VectorIntBitUnpackers()
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
        private static final ByteVector MASK_1 = ByteVector.broadcast(ByteVector.SPECIES_64, 1);
        private static final ByteVector LSHR_BYTE_VECTOR = ByteVector.fromArray(ByteVector.SPECIES_64, new byte[] {0, 1, 2, 3, 4, 5, 6, 7}, 0);

        private static void unpack8(int[] output, int outputOffset, byte input)
        {
            ByteVector.broadcast(ByteVector.SPECIES_64, input)
                    .lanewise(VectorOperators.LSHR, LSHR_BYTE_VECTOR)
                    .and(MASK_1)
                    .castShape(IntVector.SPECIES_256, 0)
                    .reinterpretAsInts()
                    .intoArray(output, outputOffset);
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            while (length >= 8) {
                unpack8(output, outputOffset, inputArray[inputOffset + inputBytesRead]);
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
        private static final ByteVector MASK_2 = ByteVector.broadcast(ByteVector.SPECIES_64, (1 << 2) - 1);
        private static final ByteVector LSHR_BYTE_VECTOR = ByteVector.fromArray(ByteVector.SPECIES_64, new byte[] {0, 2, 4, 6, 0, 2, 4, 6}, 0);
        private static final VectorShuffle<Byte> SHUFFLE = VectorShuffle.fromArray(ByteVector.SPECIES_64, new int[] {0, 0, 0, 0, 1, 1, 1, 1}, 0);

        private static void unpack8(int[] output, int outputOffset, byte[] input, int inputOffset)
        {
            ByteVector.fromArray(ByteVector.SPECIES_64, input, inputOffset)
                    .rearrange(SHUFFLE)
                    .lanewise(VectorOperators.LSHR, LSHR_BYTE_VECTOR)
                    .and(MASK_2)
                    .castShape(IntVector.SPECIES_256, 0)
                    .reinterpretAsInts()
                    .intoArray(output, outputOffset);
        }

        private static void unpack8Scalar(int[] output, int outputOffset, byte[] input, int inputOffset)
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
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            while (length >= 32) {
                unpack8(output, outputOffset, inputArray, inputOffset + inputBytesRead);
                outputOffset += 8;
                length -= 8;
                inputBytesRead += 2;
            }

            switch (length) {
                case 24:
                    unpack8Scalar(output, outputOffset, inputArray, inputOffset + inputBytesRead);
                    outputOffset += 8;
                    inputBytesRead += 2;
                    // fall through
                case 16:
                    unpack8Scalar(output, outputOffset, inputArray, inputOffset + inputBytesRead);
                    outputOffset += 8;
                    inputBytesRead += 2;
                    // fall through
                case 8:
                    unpack8Scalar(output, outputOffset, inputArray, inputOffset + inputBytesRead);
                    inputBytesRead += 2;
            }
            input.skip(inputBytesRead);
        }
    }

    private static final class Unpacker3
            implements IntBitUnpacker
    {
        private static final ByteVector MASK_3 = ByteVector.broadcast(ByteVector.SPECIES_64, (1 << 3) - 1);
        private static final ByteVector MASK_BEFORE_LSHL = ByteVector.fromArray(
                ByteVector.SPECIES_64,
                new byte[] {0, 0, 0b1, 0, 0, 0b11, 0, 0},
                0);
        private static final ByteVector LSHR_BYTE_VECTOR = ByteVector.fromArray(
                ByteVector.SPECIES_64,
                new byte[] {0, 3, 6, 1, 4, 7, 2, 5},
                0);
        private static final ByteVector LSHL_BYTE_VECTOR = ByteVector.fromArray(
                ByteVector.SPECIES_64,
                new byte[] {0, 0, 2, 0, 0, 1, 0, 0},
                0);
        private static final VectorShuffle<Byte> SHUFFLE_BEFORE_LSHR = VectorShuffle.fromArray(
                ByteVector.SPECIES_64,
                new int[] {0, 0, 0, 1, 1, 1, 2, 2},
                0);
        private static final VectorShuffle<Byte> SHUFFLE_BEFORE_LSHL = VectorShuffle.fromArray(
                ByteVector.SPECIES_64,
                new int[] {0, 0, 1, 1, 1, 2, 2, 2},
                0);

        private static void unpack8(int[] output, int outputOffset, byte[] input, int inputOffset)
        {
            ByteVector byteVector = ByteVector.fromArray(ByteVector.SPECIES_64, input, inputOffset);

            ByteVector shiftRightResult = byteVector.rearrange(SHUFFLE_BEFORE_LSHR)
                    .lanewise(VectorOperators.LSHR, LSHR_BYTE_VECTOR)
                    .and(MASK_3);

            ByteVector shiftLeftResult = byteVector.rearrange(SHUFFLE_BEFORE_LSHL)
                    .and(MASK_BEFORE_LSHL)
                    .lanewise(VectorOperators.LSHL, LSHL_BYTE_VECTOR);

            shiftRightResult.or(shiftLeftResult)
                    .castShape(IntVector.SPECIES_256, 0)
                    .reinterpretAsInts()
                    .intoArray(output, outputOffset);
        }

        private static void unpack8(int[] output, int outputOffset, SimpleSliceInputStream input)
        {
            short v0 = input.readShort();
            byte v1 = input.readByte();
            output[outputOffset] = v0 & 0b111;
            output[outputOffset + 1] = (v0 >>> 3) & 0b111;
            output[outputOffset + 2] = (v0 >>> 6) & 0b111;
            output[outputOffset + 3] = (v0 >>> 9) & 0b111;
            output[outputOffset + 4] = (v0 >>> 12) & 0b111;
            output[outputOffset + 5] = ((v0 >>> 15) & 0b1) | ((v1 & 0b11) << 1);
            output[outputOffset + 6] = (v1 >>> 2) & 0b111;
            output[outputOffset + 7] = (v1 >>> 5) & 0b111;
        }

        @Override
        public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
        {
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            while (length >= 24) {
                unpack8(output, outputOffset, inputArray, inputOffset + inputBytesRead);
                outputOffset += 8;
                length -= 8;
                inputBytesRead += 3;
            }
            input.skip(inputBytesRead);

            switch (length) {
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
        private static final ByteVector MASK_4 = ByteVector.broadcast(ByteVector.SPECIES_64, (1 << 4) - 1);
        private static final ByteVector LSHR_BYTE_VECTOR = ByteVector.fromArray(ByteVector.SPECIES_64, new byte[] {0, 4, 0, 4, 0, 4, 0, 4}, 0);
        private static final VectorShuffle<Byte> SHUFFLE = VectorShuffle.fromArray(ByteVector.SPECIES_64, new int[] {0, 0, 1, 1, 2, 2, 3, 3}, 0);

        private static void unpack8(int[] output, int outputOffset, byte[] input, int inputOffset)
        {
            ByteVector.fromArray(ByteVector.SPECIES_64, input, inputOffset)
                    .rearrange(SHUFFLE)
                    .lanewise(VectorOperators.LSHR, LSHR_BYTE_VECTOR)
                    .and(MASK_4)
                    .castShape(IntVector.SPECIES_256, 0)
                    .reinterpretAsInts()
                    .intoArray(output, outputOffset);
        }

        private static void unpack8Scalar(int[] output, int outputOffset, byte[] input, int inputOffset)
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
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            while (length >= 16) {
                unpack8(output, outputOffset, inputArray, inputOffset + inputBytesRead);
                outputOffset += 8;
                length -= 8;
                inputBytesRead += 4;
            }

            if (length >= 8) {
                unpack8Scalar(output, outputOffset, inputArray, inputOffset + inputBytesRead);
                inputBytesRead += 4;
            }
            input.skip(inputBytesRead);
        }
    }

    private static final class Unpacker5
            implements IntBitUnpacker
    {
        private static final ByteVector MASK_5 = ByteVector.broadcast(ByteVector.SPECIES_64, (1 << 5) - 1);
        private static final ByteVector MASK_BEFORE_LSHL = ByteVector.fromArray(
                ByteVector.SPECIES_64,
                new byte[] {0, 0b11, 0, 0b1111, 0b1, 0, 0b111, 0},
                0);
        private static final ByteVector LSHR_BYTE_VECTOR = ByteVector.fromArray(
                ByteVector.SPECIES_64,
                new byte[] {0, 5, 2, 7, 4, 1, 6, 3},
                0);
        private static final ByteVector LSHL_BYTE_VECTOR = ByteVector.fromArray(
                ByteVector.SPECIES_64,
                new byte[] {0, 3, 0, 1, 4, 0, 2, 0},
                0);
        private static final VectorShuffle<Byte> SHUFFLE_BEFORE_LSHR = VectorShuffle.fromArray(
                ByteVector.SPECIES_64,
                new int[] {0, 0, 1, 1, 2, 3, 3, 4},
                0);
        private static final VectorShuffle<Byte> SHUFFLE_BEFORE_LSHL = VectorShuffle.fromArray(
                ByteVector.SPECIES_64,
                new int[] {0, 1, 1, 2, 3, 3, 4, 4},
                0);

        private static void unpack8(int[] output, int outputOffset, byte[] input, int inputOffset)
        {
            ByteVector byteVector = ByteVector.fromArray(ByteVector.SPECIES_64, input, inputOffset);

            ByteVector shiftRightResult = byteVector.rearrange(SHUFFLE_BEFORE_LSHR)
                    .lanewise(VectorOperators.LSHR, LSHR_BYTE_VECTOR)
                    .and(MASK_5);

            ByteVector shiftLeftResult = byteVector.rearrange(SHUFFLE_BEFORE_LSHL)
                    .and(MASK_BEFORE_LSHL)
                    .lanewise(VectorOperators.LSHL, LSHL_BYTE_VECTOR);

            shiftRightResult.or(shiftLeftResult)
                    .castShape(IntVector.SPECIES_256, 0)
                    .reinterpretAsInts()
                    .intoArray(output, outputOffset);
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
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            while (length >= 16) {
                unpack8(output, outputOffset, inputArray, inputOffset + inputBytesRead);
                outputOffset += 8;
                length -= 8;
                inputBytesRead += 5;
            }
            input.skip(inputBytesRead);

            if (length >= 8) {
                unpack8(output, outputOffset, input);
            }
        }
    }

    private static final class Unpacker6
            implements IntBitUnpacker
    {
        private static final ShortVector MASK_6 = ShortVector.broadcast(ShortVector.SPECIES_128, (1 << 6) - 1);
        private static final ShortVector MASK_BEFORE_LSHL = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 0, 0b11, 0, 0, 0b1111, 0, 0},
                0);
        private static final ShortVector LSHR_SHORT_VECTOR = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 6, 12, 2, 8, 14, 4, 10},
                0);
        private static final ShortVector LSHL_SHORT_VECTOR = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 0, 4, 0, 0, 2, 0, 0},
                0);
        private static final VectorShuffle<Short> SHUFFLE_BEFORE_LSHR = VectorShuffle.fromArray(
                ShortVector.SPECIES_128,
                new int[] {0, 0, 0, 1, 1, 1, 2, 2},
                0);
        private static final VectorShuffle<Short> SHUFFLE_BEFORE_LSHL = VectorShuffle.fromArray(
                ShortVector.SPECIES_128,
                new int[] {0, 0, 1, 1, 1, 2, 2, 2},
                0);

        private static void unpack8(int[] output, int outputOffset, byte[] input, int inputOffset)
        {
            ShortVector shortVector = ByteVector.fromArray(ByteVector.SPECIES_64, input, inputOffset)
                    .castShape(ByteVector.SPECIES_128, 0)
                    .reinterpretAsShorts();

            ShortVector shiftRightResult = shortVector.rearrange(SHUFFLE_BEFORE_LSHR)
                    .lanewise(VectorOperators.LSHR, LSHR_SHORT_VECTOR)
                    .and(MASK_6);

            ShortVector shiftLeftResult = shortVector.rearrange(SHUFFLE_BEFORE_LSHL)
                    .and(MASK_BEFORE_LSHL)
                    .lanewise(VectorOperators.LSHL, LSHL_SHORT_VECTOR);

            shiftRightResult.or(shiftLeftResult)
                    .castShape(IntVector.SPECIES_256, 0)
                    .reinterpretAsInts()
                    .intoArray(output, outputOffset);
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
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            while (length >= 16) {
                unpack8(output, outputOffset, inputArray, inputOffset + inputBytesRead);
                outputOffset += 8;
                length -= 8;
                inputBytesRead += 6;
            }
            input.skip(inputBytesRead);

            if (length >= 8) {
                unpack8(output, outputOffset, input);
            }
        }
    }

    private static final class Unpacker7
            implements IntBitUnpacker
    {
        private static final ShortVector MASK_7 = ShortVector.broadcast(ShortVector.SPECIES_128, (1 << 7) - 1);
        private static final ShortVector MASK_BEFORE_LSHL = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 0, 0b11111, 0, 0b111, 0, 0b1, 0},
                0);
        private static final ShortVector LSHR_SHORT_VECTOR = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 7, 14, 5, 12, 3, 10, 1},
                0);
        private static final ShortVector LSHL_SHORT_VECTOR = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 0, 2, 0, 4, 0, 6, 0},
                0);
        private static final VectorShuffle<Short> SHUFFLE_BEFORE_LSHR = VectorShuffle.fromArray(
                ShortVector.SPECIES_128,
                new int[] {0, 0, 0, 1, 1, 2, 2, 3},
                0);
        private static final VectorShuffle<Short> SHUFFLE_BEFORE_LSHL = VectorShuffle.fromArray(
                ShortVector.SPECIES_128,
                new int[] {0, 0, 1, 1, 2, 2, 3, 3},
                0);

        private static void unpack8(int[] output, int outputOffset, byte[] input, int inputOffset)
        {
            ShortVector shortVector = ByteVector.fromArray(ByteVector.SPECIES_64, input, inputOffset)
                    .castShape(ByteVector.SPECIES_128, 0)
                    .reinterpretAsShorts();

            ShortVector shiftRightResult = shortVector.rearrange(SHUFFLE_BEFORE_LSHR)
                    .lanewise(VectorOperators.LSHR, LSHR_SHORT_VECTOR)
                    .and(MASK_7);

            ShortVector shiftLeftResult = shortVector.rearrange(SHUFFLE_BEFORE_LSHL)
                    .and(MASK_BEFORE_LSHL)
                    .lanewise(VectorOperators.LSHL, LSHL_SHORT_VECTOR);

            shiftRightResult.or(shiftLeftResult)
                    .castShape(IntVector.SPECIES_256, 0)
                    .reinterpretAsInts()
                    .intoArray(output, outputOffset);
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
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            while (length >= 16) {
                unpack8(output, outputOffset, inputArray, inputOffset + inputBytesRead);
                outputOffset += 8;
                length -= 8;
                inputBytesRead += 7;
            }
            input.skip(inputBytesRead);

            if (length >= 8) {
                unpack8(output, outputOffset, input);
            }
        }
    }

    private static final class Unpacker8
            implements IntBitUnpacker
    {
        private static final IntVector MASK_8 = IntVector.broadcast(IntVector.SPECIES_256, (1 << 8) - 1);

        private static void unpack8(int[] output, int outputOffset, byte[] input, int inputOffset)
        {
            ByteVector.fromArray(ByteVector.SPECIES_64, input, inputOffset)
                    .castShape(IntVector.SPECIES_256, 0)
                    .reinterpretAsInts()
                    .and(MASK_8)
                    .intoArray(output, outputOffset);
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
        private static final ShortVector MASK_9 = ShortVector.broadcast(ShortVector.SPECIES_128, (1 << 9) - 1);
        private static final ShortVector MASK_BEFORE_LSHL = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 0b11, 0, 0b1111, 0, 0b111111, 0, 0b11111111},
                0);
        private static final ShortVector LSHR_SHORT_VECTOR = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 9, 2, 11, 4, 13, 6, 15},
                0);
        private static final ShortVector LSHL_SHORT_VECTOR = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 7, 0, 5, 0, 3, 0, 1},
                0);
        private static final VectorShuffle<Short> SHUFFLE_BEFORE_LSHR = VectorShuffle.fromArray(
                ShortVector.SPECIES_128,
                new int[] {0, 0, 1, 1, 2, 2, 3, 3},
                0);
        private static final VectorShuffle<Short> SHUFFLE_BEFORE_LSHL = VectorShuffle.fromArray(
                ShortVector.SPECIES_128,
                new int[] {0, 1, 1, 2, 2, 3, 3, 4},
                0);

        private static void unpack8(int[] output, int outputOffset, byte[] input, int inputOffset)
        {
            ShortVector shortVector = ByteVector.fromArray(ByteVector.SPECIES_128, input, inputOffset)
                    .reinterpretAsShorts();

            ShortVector shiftRightResult = shortVector.rearrange(SHUFFLE_BEFORE_LSHR)
                    .lanewise(VectorOperators.LSHR, LSHR_SHORT_VECTOR)
                    .and(MASK_9);

            ShortVector shiftLeftResult = shortVector.rearrange(SHUFFLE_BEFORE_LSHL)
                    .and(MASK_BEFORE_LSHL)
                    .lanewise(VectorOperators.LSHL, LSHL_SHORT_VECTOR);

            shiftRightResult.or(shiftLeftResult)
                    .castShape(IntVector.SPECIES_256, 0)
                    .reinterpretAsInts()
                    .intoArray(output, outputOffset);
        }

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
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            while (length >= 16) {
                unpack8(output, outputOffset, inputArray, inputOffset + inputBytesRead);
                outputOffset += 8;
                length -= 8;
                inputBytesRead += 9;
            }
            input.skip(inputBytesRead);

            if (length >= 8) {
                unpack8(output, outputOffset, input);
            }
        }
    }

    private static final class Unpacker10
            implements IntBitUnpacker
    {
        private static final ShortVector MASK_10 = ShortVector.broadcast(ShortVector.SPECIES_128, (1 << 10) - 1);
        private static final ShortVector MASK_BEFORE_LSHL = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 0b1111, 0, 0b11111111, 0b11, 0, 0b111111, 0},
                0);
        private static final ShortVector LSHR_SHORT_VECTOR = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 10, 4, 14, 8, 2, 12, 6},
                0);
        private static final ShortVector LSHL_SHORT_VECTOR = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 6, 0, 2, 8, 0, 4, 0},
                0);
        private static final VectorShuffle<Short> SHUFFLE_BEFORE_LSHR = VectorShuffle.fromArray(
                ShortVector.SPECIES_128,
                new int[] {0, 0, 1, 1, 2, 3, 3, 4},
                0);
        private static final VectorShuffle<Short> SHUFFLE_BEFORE_LSHL = VectorShuffle.fromArray(
                ShortVector.SPECIES_128,
                new int[] {0, 1, 1, 2, 3, 3, 4, 4},
                0);

        private static void unpack8(int[] output, int outputOffset, byte[] input, int inputOffset)
        {
            ShortVector shortVector = ByteVector.fromArray(ByteVector.SPECIES_128, input, inputOffset)
                    .reinterpretAsShorts();

            ShortVector shiftRightResult = shortVector.rearrange(SHUFFLE_BEFORE_LSHR)
                    .lanewise(VectorOperators.LSHR, LSHR_SHORT_VECTOR)
                    .and(MASK_10);

            ShortVector shiftLeftResult = shortVector.rearrange(SHUFFLE_BEFORE_LSHL)
                    .and(MASK_BEFORE_LSHL)
                    .lanewise(VectorOperators.LSHL, LSHL_SHORT_VECTOR);

            shiftRightResult.or(shiftLeftResult)
                    .castShape(IntVector.SPECIES_256, 0)
                    .reinterpretAsInts()
                    .intoArray(output, outputOffset);
        }

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
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            while (length >= 16) {
                unpack8(output, outputOffset, inputArray, inputOffset + inputBytesRead);
                outputOffset += 8;
                length -= 8;
                inputBytesRead += 10;
            }
            input.skip(inputBytesRead);

            if (length >= 8) {
                unpack8(output, outputOffset, input);
            }
        }
    }

    private static final class Unpacker11
            implements IntBitUnpacker
    {
        private static final ShortVector MASK_11 = ShortVector.broadcast(ShortVector.SPECIES_128, (1 << 11) - 1);
        private static final ShortVector MASK_BEFORE_LSHL = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 0b111111, 0b1, 0, 0b1111111, 0b11, 0, 0b11111111},
                0);
        private static final ShortVector LSHR_SHORT_VECTOR = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 11, 6, 1, 12, 7, 2, 13},
                0);
        private static final ShortVector LSHL_SHORT_VECTOR = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 5, 10, 0, 4, 9, 0, 3},
                0);
        private static final VectorShuffle<Short> SHUFFLE_BEFORE_LSHR = VectorShuffle.fromArray(
                ShortVector.SPECIES_128,
                new int[] {0, 0, 1, 2, 2, 3, 4, 4},
                0);
        private static final VectorShuffle<Short> SHUFFLE_BEFORE_LSHL = VectorShuffle.fromArray(
                ShortVector.SPECIES_128,
                new int[] {0, 1, 2, 2, 3, 4, 4, 5},
                0);

        private static void unpack8(int[] output, int outputOffset, byte[] input, int inputOffset)
        {
            ShortVector shortVector = ByteVector.fromArray(ByteVector.SPECIES_128, input, inputOffset)
                    .reinterpretAsShorts();

            ShortVector shiftRightResult = shortVector.rearrange(SHUFFLE_BEFORE_LSHR)
                    .lanewise(VectorOperators.LSHR, LSHR_SHORT_VECTOR)
                    .and(MASK_11);

            ShortVector shiftLeftResult = shortVector.rearrange(SHUFFLE_BEFORE_LSHL)
                    .and(MASK_BEFORE_LSHL)
                    .lanewise(VectorOperators.LSHL, LSHL_SHORT_VECTOR);

            shiftRightResult.or(shiftLeftResult)
                    .castShape(IntVector.SPECIES_256, 0)
                    .reinterpretAsInts()
                    .intoArray(output, outputOffset);
        }

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
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            while (length >= 16) {
                unpack8(output, outputOffset, inputArray, inputOffset + inputBytesRead);
                outputOffset += 8;
                length -= 8;
                inputBytesRead += 11;
            }
            input.skip(inputBytesRead);

            if (length >= 8) {
                unpack8(output, outputOffset, input);
            }
        }
    }

    private static final class Unpacker12
            implements IntBitUnpacker
    {
        private static final ShortVector MASK_12 = ShortVector.broadcast(ShortVector.SPECIES_128, (1 << 12) - 1);
        private static final ShortVector MASK_BEFORE_LSHL = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 0b11111111, 0b1111, 0, 0, 0b11111111, 0b1111, 0},
                0);
        private static final ShortVector LSHR_SHORT_VECTOR = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 12, 8, 4, 0, 12, 8, 4},
                0);
        private static final ShortVector LSHL_SHORT_VECTOR = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 4, 8, 0, 0, 4, 8, 0},
                0);
        private static final VectorShuffle<Short> SHUFFLE_BEFORE_LSHR = VectorShuffle.fromArray(
                ShortVector.SPECIES_128,
                new int[] {0, 0, 1, 2, 3, 3, 4, 5},
                0);
        private static final VectorShuffle<Short> SHUFFLE_BEFORE_LSHL = VectorShuffle.fromArray(
                ShortVector.SPECIES_128,
                new int[] {0, 1, 2, 2, 3, 4, 5, 5},
                0);

        private static void unpack8(int[] output, int outputOffset, byte[] input, int inputOffset)
        {
            ShortVector shortVector = ByteVector.fromArray(ByteVector.SPECIES_128, input, inputOffset)
                    .reinterpretAsShorts();

            ShortVector shiftRightResult = shortVector.rearrange(SHUFFLE_BEFORE_LSHR)
                    .lanewise(VectorOperators.LSHR, LSHR_SHORT_VECTOR)
                    .and(MASK_12);

            ShortVector shiftLeftResult = shortVector.rearrange(SHUFFLE_BEFORE_LSHL)
                    .and(MASK_BEFORE_LSHL)
                    .lanewise(VectorOperators.LSHL, LSHL_SHORT_VECTOR);

            shiftRightResult.or(shiftLeftResult)
                    .castShape(IntVector.SPECIES_256, 0)
                    .reinterpretAsInts()
                    .intoArray(output, outputOffset);
        }

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
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            while (length >= 16) {
                unpack8(output, outputOffset, inputArray, inputOffset + inputBytesRead);
                outputOffset += 8;
                length -= 8;
                inputBytesRead += 12;
            }
            input.skip(inputBytesRead);

            if (length >= 8) {
                unpack8(output, outputOffset, input);
            }
        }
    }

    private static final class Unpacker13
            implements IntBitUnpacker
    {
        private static final ShortVector MASK_13 = ShortVector.broadcast(ShortVector.SPECIES_128, (1 << 13) - 1);
        private static final ShortVector MASK_BEFORE_LSHL = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 0b1111111111, 0b1111111, 0b1111, 0b1, 0, 0b11111111111, 0b11111111},
                0);
        private static final ShortVector LSHR_SHORT_VECTOR = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 13, 10, 7, 4, 1, 14, 11},
                0);
        private static final ShortVector LSHL_SHORT_VECTOR = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 3, 6, 9, 12, 0, 2, 5},
                0);
        private static final VectorShuffle<Short> SHUFFLE_BEFORE_LSHR = VectorShuffle.fromArray(
                ShortVector.SPECIES_128,
                new int[] {0, 0, 1, 2, 3, 4, 4, 5},
                0);
        private static final VectorShuffle<Short> SHUFFLE_BEFORE_LSHL = VectorShuffle.fromArray(
                ShortVector.SPECIES_128,
                new int[] {0, 1, 2, 3, 4, 4, 5, 6},
                0);

        private static void unpack8(int[] output, int outputOffset, byte[] input, int inputOffset)
        {
            ShortVector shortVector = ByteVector.fromArray(ByteVector.SPECIES_128, input, inputOffset)
                    .reinterpretAsShorts();

            ShortVector shiftRightResult = shortVector.rearrange(SHUFFLE_BEFORE_LSHR)
                    .lanewise(VectorOperators.LSHR, LSHR_SHORT_VECTOR)
                    .and(MASK_13);

            ShortVector shiftLeftResult = shortVector.rearrange(SHUFFLE_BEFORE_LSHL)
                    .and(MASK_BEFORE_LSHL)
                    .lanewise(VectorOperators.LSHL, LSHL_SHORT_VECTOR);

            shiftRightResult.or(shiftLeftResult)
                    .castShape(IntVector.SPECIES_256, 0)
                    .reinterpretAsInts()
                    .intoArray(output, outputOffset);
        }

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
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            while (length >= 16) {
                unpack8(output, outputOffset, inputArray, inputOffset + inputBytesRead);
                outputOffset += 8;
                length -= 8;
                inputBytesRead += 13;
            }
            input.skip(inputBytesRead);

            if (length >= 8) {
                unpack8(output, outputOffset, input);
            }
        }
    }

    private static final class Unpacker14
            implements IntBitUnpacker
    {
        private static final ShortVector MASK_14 = ShortVector.broadcast(ShortVector.SPECIES_128, (1 << 14) - 1);
        private static final ShortVector MASK_BEFORE_LSHL = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 0b111111111111, 0b1111111111, 0b11111111, 0b111111, 0b1111, 0b11, 0},
                0);
        private static final ShortVector LSHR_SHORT_VECTOR = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 14, 12, 10, 8, 6, 4, 2},
                0);
        private static final ShortVector LSHL_SHORT_VECTOR = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 2, 4, 6, 8, 10, 12, 0},
                0);
        private static final VectorShuffle<Short> SHUFFLE_BEFORE_LSHR = VectorShuffle.fromArray(
                ShortVector.SPECIES_128,
                new int[] {0, 0, 1, 2, 3, 4, 5, 6},
                0);
        private static final VectorShuffle<Short> SHUFFLE_BEFORE_LSHL = VectorShuffle.fromArray(
                ShortVector.SPECIES_128,
                new int[] {0, 1, 2, 3, 4, 5, 6, 6},
                0);

        private static void unpack8(int[] output, int outputOffset, byte[] input, int inputOffset)
        {
            ShortVector shortVector = ByteVector.fromArray(ByteVector.SPECIES_128, input, inputOffset)
                    .reinterpretAsShorts();

            ShortVector shiftRightResult = shortVector.rearrange(SHUFFLE_BEFORE_LSHR)
                    .lanewise(VectorOperators.LSHR, LSHR_SHORT_VECTOR)
                    .and(MASK_14);

            ShortVector shiftLeftResult = shortVector.rearrange(SHUFFLE_BEFORE_LSHL)
                    .and(MASK_BEFORE_LSHL)
                    .lanewise(VectorOperators.LSHL, LSHL_SHORT_VECTOR);

            shiftRightResult.or(shiftLeftResult)
                    .castShape(IntVector.SPECIES_256, 0)
                    .reinterpretAsInts()
                    .intoArray(output, outputOffset);
        }

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
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            while (length >= 16) {
                unpack8(output, outputOffset, inputArray, inputOffset + inputBytesRead);
                outputOffset += 8;
                length -= 8;
                inputBytesRead += 14;
            }
            input.skip(inputBytesRead);

            if (length >= 8) {
                unpack8(output, outputOffset, input);
            }
        }
    }

    private static final class Unpacker15
            implements IntBitUnpacker
    {
        private static final ShortVector MASK_15 = ShortVector.broadcast(ShortVector.SPECIES_128, (1 << 15) - 1);
        private static final ShortVector MASK_BEFORE_LSHL = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 0b11111111111111, 0b1111111111111, 0b111111111111, 0b11111111111, 0b1111111111, 0b111111111, 0b11111111},
                0);
        private static final ShortVector LSHR_SHORT_VECTOR = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 15, 14, 13, 12, 11, 10, 9},
                0);
        private static final ShortVector LSHL_SHORT_VECTOR = ShortVector.fromArray(
                ShortVector.SPECIES_128,
                new short[] {0, 1, 2, 3, 4, 5, 6, 7},
                0);
        private static final VectorShuffle<Short> SHUFFLE_BEFORE_LSHR = VectorShuffle.fromArray(
                ShortVector.SPECIES_128,
                new int[] {0, 0, 1, 2, 3, 4, 5, 6},
                0);
        private static final VectorShuffle<Short> SHUFFLE_BEFORE_LSHL = VectorShuffle.fromArray(
                ShortVector.SPECIES_128,
                new int[] {0, 1, 2, 3, 4, 5, 6, 7},
                0);

        private static void unpack8(int[] output, int outputOffset, byte[] input, int inputOffset)
        {
            ShortVector shortVector = ByteVector.fromArray(ByteVector.SPECIES_128, input, inputOffset)
                    .reinterpretAsShorts();

            ShortVector shiftRightResult = shortVector.rearrange(SHUFFLE_BEFORE_LSHR)
                    .lanewise(VectorOperators.LSHR, LSHR_SHORT_VECTOR)
                    .and(MASK_15);

            ShortVector shiftLeftResult = shortVector.rearrange(SHUFFLE_BEFORE_LSHL)
                    .and(MASK_BEFORE_LSHL)
                    .lanewise(VectorOperators.LSHL, LSHL_SHORT_VECTOR);

            shiftRightResult.or(shiftLeftResult)
                    .castShape(IntVector.SPECIES_256, 0)
                    .reinterpretAsInts()
                    .intoArray(output, outputOffset);
        }

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
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            while (length >= 16) {
                unpack8(output, outputOffset, inputArray, inputOffset + inputBytesRead);
                outputOffset += 8;
                length -= 8;
                inputBytesRead += 15;
            }
            input.skip(inputBytesRead);

            if (length >= 8) {
                unpack8(output, outputOffset, input);
            }
        }
    }

    private static final class Unpacker16
            implements IntBitUnpacker
    {
        private static final IntVector MASK_16 = IntVector.broadcast(IntVector.SPECIES_256, (1 << 16) - 1);

        private static void unpack8(int[] output, int outputOffset, byte[] input, int inputOffset)
        {
            ByteVector.fromArray(ByteVector.SPECIES_128, input, inputOffset)
                    .reinterpretAsShorts()
                    .castShape(IntVector.SPECIES_256, 0)
                    .reinterpretAsInts()
                    .and(MASK_16)
                    .intoArray(output, outputOffset);
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
                inputBytesRead += 16;
            }
            input.skip(inputBytesRead);
        }
    }

    private static final class Unpacker17
            implements IntBitUnpacker
    {
        private static final IntVector MASK_17 = IntVector.broadcast(IntVector.SPECIES_256, (1 << 17) - 1);
        private static final IntVector MASK_BEFORE_LSHL = IntVector.fromArray(
                IntVector.SPECIES_256,
                new int[] {0, 0b11, 0, 0b1111, 0, 0b111111, 0, 0b11111111},
                0);
        private static final IntVector LSHR_INT_VECTOR = IntVector.fromArray(
                IntVector.SPECIES_256,
                new int[] {0, 17, 2, 19, 4, 21, 6, 23},
                0);
        private static final IntVector LSHL_INT_VECTOR = IntVector.fromArray(
                IntVector.SPECIES_256,
                new int[] {0, 15, 0, 13, 0, 11, 0, 9},
                0);
        private static final VectorShuffle<Integer> SHUFFLE_BEFORE_LSHR = VectorShuffle.fromArray(
                IntVector.SPECIES_256,
                new int[] {0, 0, 1, 1, 2, 2, 3, 3},
                0);
        private static final VectorShuffle<Integer> SHUFFLE_BEFORE_LSHL = VectorShuffle.fromArray(
                IntVector.SPECIES_256,
                new int[] {0, 1, 1, 2, 2, 3, 3, 4},
                0);

        private static void unpack8(int[] output, int outputOffset, byte[] input, int inputOffset)
        {
            IntVector intVector = ByteVector.fromArray(ByteVector.SPECIES_256, input, inputOffset)
                    .reinterpretAsInts();

            IntVector shiftRightResult = intVector.rearrange(SHUFFLE_BEFORE_LSHR)
                    .lanewise(VectorOperators.LSHR, LSHR_INT_VECTOR)
                    .and(MASK_17);

            IntVector shiftLeftResult = intVector.rearrange(SHUFFLE_BEFORE_LSHL)
                    .and(MASK_BEFORE_LSHL)
                    .lanewise(VectorOperators.LSHL, LSHL_INT_VECTOR);

            shiftRightResult.or(shiftLeftResult)
                    .castShape(IntVector.SPECIES_256, 0)
                    .reinterpretAsInts()
                    .intoArray(output, outputOffset);
        }

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
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            while (length >= 16) {
                unpack8(output, outputOffset, inputArray, inputOffset + inputBytesRead);
                outputOffset += 8;
                length -= 8;
                inputBytesRead += 17;
            }
            input.skip(inputBytesRead);

            if (length >= 8) {
                unpack8(output, outputOffset, input);
            }
        }
    }

    private static final class Unpacker18
            implements IntBitUnpacker
    {
        private static final IntVector MASK_18 = IntVector.broadcast(IntVector.SPECIES_256, (1 << 18) - 1);
        private static final IntVector MASK_BEFORE_LSHL = IntVector.fromArray(
                IntVector.SPECIES_256,
                new int[] {0, 0b1111, 0, 0b11111111, 0, 0b111111111111, 0, 0b1111111111111111},
                0);
        private static final IntVector LSHR_INT_VECTOR = IntVector.fromArray(
                IntVector.SPECIES_256,
                new int[] {0, 18, 4, 22, 8, 26, 12, 30},
                0);
        private static final IntVector LSHL_INT_VECTOR = IntVector.fromArray(
                IntVector.SPECIES_256,
                new int[] {0, 14, 0, 10, 0, 6, 0, 2},
                0);
        private static final VectorShuffle<Integer> SHUFFLE_BEFORE_LSHR = VectorShuffle.fromArray(
                IntVector.SPECIES_256,
                new int[] {0, 0, 1, 1, 2, 2, 3, 3},
                0);
        private static final VectorShuffle<Integer> SHUFFLE_BEFORE_LSHL = VectorShuffle.fromArray(
                IntVector.SPECIES_256,
                new int[] {0, 1, 1, 2, 2, 3, 3, 4},
                0);

        private static void unpack8(int[] output, int outputOffset, byte[] input, int inputOffset)
        {
            IntVector intVector = ByteVector.fromArray(ByteVector.SPECIES_256, input, inputOffset)
                    .reinterpretAsInts();

            IntVector shiftRightResult = intVector.rearrange(SHUFFLE_BEFORE_LSHR)
                    .lanewise(VectorOperators.LSHR, LSHR_INT_VECTOR)
                    .and(MASK_18);

            IntVector shiftLeftResult = intVector.rearrange(SHUFFLE_BEFORE_LSHL)
                    .and(MASK_BEFORE_LSHL)
                    .lanewise(VectorOperators.LSHL, LSHL_INT_VECTOR);

            shiftRightResult.or(shiftLeftResult)
                    .castShape(IntVector.SPECIES_256, 0)
                    .reinterpretAsInts()
                    .intoArray(output, outputOffset);
        }

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
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            while (length >= 16) {
                unpack8(output, outputOffset, inputArray, inputOffset + inputBytesRead);
                outputOffset += 8;
                length -= 8;
                inputBytesRead += 18;
            }
            input.skip(inputBytesRead);

            if (length >= 8) {
                unpack8(output, outputOffset, input);
            }
        }
    }

    private static final class Unpacker19
            implements IntBitUnpacker
    {
        private static final IntVector MASK_19 = IntVector.broadcast(IntVector.SPECIES_256, (1 << 19) - 1);
        private static final IntVector MASK_BEFORE_LSHL = IntVector.fromArray(
                IntVector.SPECIES_256,
                new int[] {0, 0b111111, 0, 0b111111111111, 0, 0b111111111111111111, 0b11111, 0},
                0);
        private static final IntVector LSHR_INT_VECTOR = IntVector.fromArray(
                IntVector.SPECIES_256,
                new int[] {0, 19, 6, 25, 12, 31, 18, 5},
                0);
        private static final IntVector LSHL_INT_VECTOR = IntVector.fromArray(
                IntVector.SPECIES_256,
                new int[] {0, 13, 0, 7, 0, 1, 14, 0},
                0);
        private static final VectorShuffle<Integer> SHUFFLE_BEFORE_LSHR = VectorShuffle.fromArray(
                IntVector.SPECIES_256,
                new int[] {0, 0, 1, 1, 2, 2, 3, 4},
                0);
        private static final VectorShuffle<Integer> SHUFFLE_BEFORE_LSHL = VectorShuffle.fromArray(
                IntVector.SPECIES_256,
                new int[] {0, 1, 1, 2, 2, 3, 4, 4},
                0);

        private static void unpack8(int[] output, int outputOffset, byte[] input, int inputOffset)
        {
            IntVector intVector = ByteVector.fromArray(ByteVector.SPECIES_256, input, inputOffset)
                    .reinterpretAsInts();

            IntVector shiftRightResult = intVector.rearrange(SHUFFLE_BEFORE_LSHR)
                    .lanewise(VectorOperators.LSHR, LSHR_INT_VECTOR)
                    .and(MASK_19);

            IntVector shiftLeftResult = intVector.rearrange(SHUFFLE_BEFORE_LSHL)
                    .and(MASK_BEFORE_LSHL)
                    .lanewise(VectorOperators.LSHL, LSHL_INT_VECTOR);

            shiftRightResult.or(shiftLeftResult)
                    .castShape(IntVector.SPECIES_256, 0)
                    .reinterpretAsInts()
                    .intoArray(output, outputOffset);
        }

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
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            while (length >= 16) {
                unpack8(output, outputOffset, inputArray, inputOffset + inputBytesRead);
                outputOffset += 8;
                length -= 8;
                inputBytesRead += 19;
            }
            input.skip(inputBytesRead);

            if (length >= 8) {
                unpack8(output, outputOffset, input);
            }
        }
    }

    private static final class Unpacker20
            implements IntBitUnpacker
    {
        private static final IntVector MASK_20 = IntVector.broadcast(IntVector.SPECIES_256, (1 << 20) - 1);
        private static final IntVector MASK_BEFORE_LSHL = IntVector.fromArray(
                IntVector.SPECIES_256,
                new int[] {0, 0b11111111, 0, 0b1111111111111111, 0b1111, 0, 0b111111111111, 0},
                0);
        private static final IntVector LSHR_INT_VECTOR = IntVector.fromArray(
                IntVector.SPECIES_256,
                new int[] {0, 20, 8, 28, 16, 4, 24, 12},
                0);
        private static final IntVector LSHL_INT_VECTOR = IntVector.fromArray(
                IntVector.SPECIES_256,
                new int[] {0, 12, 0, 4, 16, 0, 8, 0},
                0);
        private static final VectorShuffle<Integer> SHUFFLE_BEFORE_LSHR = VectorShuffle.fromArray(
                IntVector.SPECIES_256,
                new int[] {0, 0, 1, 1, 2, 3, 3, 4},
                0);
        private static final VectorShuffle<Integer> SHUFFLE_BEFORE_LSHL = VectorShuffle.fromArray(
                IntVector.SPECIES_256,
                new int[] {0, 1, 1, 2, 3, 3, 4, 4},
                0);

        private static void unpack8(int[] output, int outputOffset, byte[] input, int inputOffset)
        {
            IntVector intVector = ByteVector.fromArray(ByteVector.SPECIES_256, input, inputOffset)
                    .reinterpretAsInts();

            IntVector shiftRightResult = intVector.rearrange(SHUFFLE_BEFORE_LSHR)
                    .lanewise(VectorOperators.LSHR, LSHR_INT_VECTOR)
                    .and(MASK_20);

            IntVector shiftLeftResult = intVector.rearrange(SHUFFLE_BEFORE_LSHL)
                    .and(MASK_BEFORE_LSHL)
                    .lanewise(VectorOperators.LSHL, LSHL_INT_VECTOR);

            shiftRightResult.or(shiftLeftResult)
                    .castShape(IntVector.SPECIES_256, 0)
                    .reinterpretAsInts()
                    .intoArray(output, outputOffset);
        }

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
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            while (length >= 16) {
                unpack8(output, outputOffset, inputArray, inputOffset + inputBytesRead);
                outputOffset += 8;
                length -= 8;
                inputBytesRead += 20;
            }
            input.skip(inputBytesRead);

            if (length >= 8) {
                unpack8(output, outputOffset, input);
            }
        }
    }
}
