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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.reader.SimpleSliceInputStream;
import io.trino.parquet.reader.flat.BinaryBuffer;
import io.trino.spi.type.CharType;
import io.trino.spi.type.VarcharType;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.parquet.reader.decoders.DeltaBinaryPackedDecoders.DeltaBinaryPackedIntDecoder;
import static io.trino.spi.type.Chars.byteCountWithoutTrailingSpace;
import static io.trino.spi.type.Varchars.byteCount;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

/**
 * Implementation of decoding for the encoding described at
 * <a href="https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-length-byte-array-delta_length_byte_array--6">delta-length-byte-array</a>.
 * Data encoding here is identical to the one in Trino block. It the values are
 * not trimmed due to type bounds, it will push a single Slice object that will
 * effectively be used in Trino blocks without copying.
 * <p>
 * If the trimming occurs, data will be copied into a single byte array
 */
public class DeltaLengthByteArrayDecoders
{
    private DeltaLengthByteArrayDecoders() {}

    public static final class BoundedVarcharDeltaLengthDecoder
            extends AbstractDeltaLengthDecoder
    {
        private final int boundedLength;

        public BoundedVarcharDeltaLengthDecoder(VarcharType varcharType)
        {
            checkArgument(
                    !varcharType.isUnbounded(),
                    "Trino type %s is not a bounded varchar",
                    varcharType);
            this.boundedLength = varcharType.getBoundedLength();
        }

        @Override
        public void read(BinaryBuffer values, int offset, int length)
        {
            InputLengths lengths = getInputAndMaxLength(length);
            int maxLength = lengths.maxInputLength();
            int totalInputLength = lengths.totalInputLength();
            boolean truncate = maxLength > boundedLength;
            if (truncate) {
                readBounded(values, offset, length, totalInputLength);
            }
            else {
                readUnbounded(values, offset, length, totalInputLength);
            }
        }

        @Override
        protected int truncatedLength(Slice slice, int offset, int length)
        {
            return byteCount(slice, offset, length, boundedLength);
        }
    }

    public static final class CharDeltaLengthDecoder
            extends AbstractDeltaLengthDecoder
    {
        private final int maxLength;

        public CharDeltaLengthDecoder(CharType charType)
        {
            this.maxLength = charType.getLength();
        }

        @Override
        public void read(BinaryBuffer values, int offset, int length)
        {
            int totalInputLength = getInputLength(length);
            readBounded(values, offset, length, totalInputLength);
        }

        @Override
        protected int truncatedLength(Slice slice, int offset, int length)
        {
            return byteCountWithoutTrailingSpace(slice, offset, length, maxLength);
        }
    }

    public static final class BinaryDeltaLengthDecoder
            extends AbstractDeltaLengthDecoder
    {
        @Override
        protected int truncatedLength(Slice slice, int offset, int length)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void read(BinaryBuffer values, int offset, int length)
        {
            int totalInputLength = getInputLength(length);
            readUnbounded(values, offset, length, totalInputLength);
        }
    }

    private abstract static class AbstractDeltaLengthDecoder
            implements ValueDecoder<BinaryBuffer>
    {
        private int[] inputLengths;
        private int inputLengthsOffset;
        private SimpleSliceInputStream input;

        @Override
        public void init(SimpleSliceInputStream input)
        {
            this.input = requireNonNull(input, "input is null");
            this.inputLengths = readInputLengths(input);
        }

        @Override
        public void skip(int n)
        {
            int totalInputLength = getInputLength(n);
            input.skip(totalInputLength);
            inputLengthsOffset += n;
        }

        protected abstract int truncatedLength(Slice slice, int offset, int length);

        protected int getInputLength(int length)
        {
            int totalInputLength = 0;
            for (int i = 0; i < length; i++) {
                totalInputLength += inputLengths[inputLengthsOffset + i];
            }
            return totalInputLength;
        }

        protected InputLengths getInputAndMaxLength(int length)
        {
            int totalInputLength = 0;
            int maxLength = 0;
            for (int i = 0; i < length; i++) {
                int inputLength = inputLengths[inputLengthsOffset + i];
                totalInputLength += inputLength;
                maxLength = max(maxLength, inputLength);
            }
            return new InputLengths(totalInputLength, maxLength);
        }

        protected void readUnbounded(BinaryBuffer values, int offset, int length, int totalInputLength)
        {
            values.addChunk(input.readSlice(totalInputLength));
            int[] outputOffsets = values.getOffsets();

            // Some positions in offsets might have been skipped before this read,
            // adjust for this difference when copying offsets to the output
            int outputLength = 0;
            int baseOutputOffset = outputOffsets[offset];
            for (int i = 0; i < length; i++) {
                outputLength += inputLengths[inputLengthsOffset + i];
                outputOffsets[offset + i + 1] = baseOutputOffset + outputLength;
            }
            inputLengthsOffset += length;
        }

        protected void readBounded(BinaryBuffer values, int offset, int length, int totalInputLength)
        {
            // Use offset arrays to temporarily store output lengths
            int[] outputOffsets = values.getOffsets();
            Slice inputSlice = input.readSlice(totalInputLength);

            int currentInputOffset = 0;
            int totalOutputLength = 0;
            int baseOutputOffset = outputOffsets[offset];
            for (int i = 0; i < length; i++) {
                int currentLength = inputLengths[inputLengthsOffset + i];

                int currentOutputLength = truncatedLength(inputSlice, currentInputOffset, currentLength);
                currentInputOffset += currentLength;
                totalOutputLength += currentOutputLength;
                outputOffsets[offset + i + 1] = baseOutputOffset + totalOutputLength;
            }

            // No copying needed if there was no truncation
            if (totalOutputLength == totalInputLength) {
                values.addChunk(inputSlice);
            }
            else {
                values.addChunk(createOutputBuffer(outputOffsets, offset, length, inputSlice, totalOutputLength));
            }
            inputLengthsOffset += length;
        }

        /**
         * Constructs the output buffer out of input data and length array
         */
        private Slice createOutputBuffer(
                int[] outputOffsets,
                int offset,
                int length,
                Slice inputSlice,
                int totalOutputLength)
        {
            byte[] output = new byte[totalOutputLength];
            int outputOffset = 0;
            int outputLength = 0;
            int currentInputOffset = 0;
            int inputLength = 0;
            for (int i = 0; i < length; i++) {
                outputLength += outputOffsets[offset + i + 1] - outputOffsets[offset + i];
                inputLength += inputLengths[inputLengthsOffset + i];

                if (outputLength != inputLength) {
                    inputSlice.getBytes(currentInputOffset, output, outputOffset, outputLength);
                    currentInputOffset += inputLength;
                    inputLength = 0;
                    outputOffset += outputLength;
                    outputLength = 0;
                }
            }
            if (outputLength != 0) { // Write the remaining slice
                inputSlice.getBytes(currentInputOffset, output, outputOffset, outputLength);
            }
            return Slices.wrappedBuffer(output);
        }

        protected record InputLengths(int totalInputLength, int maxInputLength) {}
    }

    private static int[] readInputLengths(SimpleSliceInputStream input)
    {
        DeltaBinaryPackedIntDecoder decoder = new DeltaBinaryPackedIntDecoder();
        decoder.init(input);
        int valueCount = decoder.getValueCount();
        int[] lengths = new int[valueCount];
        decoder.read(lengths, 0, valueCount);
        return lengths;
    }
}
