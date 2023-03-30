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

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static io.trino.parquet.reader.decoders.DeltaBinaryPackedDecoders.DeltaBinaryPackedIntDecoder;
import static io.trino.spi.type.Chars.byteCountWithoutTrailingSpace;
import static io.trino.spi.type.Varchars.byteCount;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

/**
 * Implementation of decoder for the encoding described at
 * <a href="https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-strings-delta_byte_array--7">delta_byte_array</a>
 */
public class DeltaByteArrayDecoders
{
    private DeltaByteArrayDecoders() {}

    public static final class BoundedVarcharDeltaByteArrayDecoder
            extends AbstractDeltaByteArrayDecoder
    {
        private final int boundedLength;

        public BoundedVarcharDeltaByteArrayDecoder(VarcharType varcharType)
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

    public static final class CharDeltaByteArrayDecoder
            extends AbstractDeltaByteArrayDecoder
    {
        private final int maxLength;

        public CharDeltaByteArrayDecoder(CharType charType)
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

    public static final class BinaryDeltaByteArrayDecoder
            extends AbstractDeltaByteArrayDecoder
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

    private abstract static class AbstractDeltaByteArrayDecoder
            implements ValueDecoder<BinaryBuffer>
    {
        private int[] prefixLengths;
        private int[] suffixLengths;
        private int inputLengthsOffset;
        // At the end of skip/read for each batch of positions, this field is
        // populated with prefixLength bytes for the first position in the next read
        private byte[] firstPrefix = new byte[0];
        private SimpleSliceInputStream input;

        @Override
        public void init(SimpleSliceInputStream input)
        {
            this.input = requireNonNull(input, "input is null");
            this.prefixLengths = readDeltaEncodedLengths(input);
            this.suffixLengths = readDeltaEncodedLengths(input);
        }

        @Override
        public void skip(int n)
        {
            checkPositionIndexes(inputLengthsOffset, inputLengthsOffset + n, prefixLengths.length);
            if (n == 0) {
                return;
            }

            // If we've skipped to the end, there's no need to process anything
            if (inputLengthsOffset + n == prefixLengths.length) {
                inputLengthsOffset += n;
                return;
            }

            int totalSuffixesLength = getSuffixesLength(n);
            Slice inputSlice = input.asSlice();
            // Start from the suffix and go back position by position to fill the prefix for next read
            int bytesLeft = prefixLengths[inputLengthsOffset + n];
            byte[] newPrefix = new byte[bytesLeft];

            int current = n - 1;
            int inputOffset = totalSuffixesLength - suffixLengths[inputLengthsOffset + n - 1];
            while (bytesLeft > 0 && inputOffset >= 0) {
                int currentPrefixLength = prefixLengths[inputLengthsOffset + current];
                if (currentPrefixLength < bytesLeft) {
                    int toCopy = bytesLeft - currentPrefixLength;
                    inputSlice.getBytes(inputOffset, newPrefix, currentPrefixLength, toCopy);
                    bytesLeft -= toCopy;
                    if (bytesLeft == 0) {
                        break;
                    }
                }
                inputOffset -= suffixLengths[inputLengthsOffset + current - 1];
                current--;
            }
            System.arraycopy(firstPrefix, 0, newPrefix, 0, bytesLeft);
            firstPrefix = newPrefix;

            input.skip(totalSuffixesLength);
            inputLengthsOffset += n;
        }

        protected abstract int truncatedLength(Slice slice, int offset, int length);

        protected void readBounded(BinaryBuffer values, int offset, int length, int totalInputLength)
        {
            checkPositionIndexes(inputLengthsOffset, inputLengthsOffset + length, prefixLengths.length);
            int[] outputOffsets = values.getOffsets();
            byte[] dataBuffer = readUnbounded(outputOffsets, offset, length, totalInputLength);
            Slice inputSlice = Slices.wrappedBuffer(dataBuffer);
            inputLengthsOffset += length;

            // Try to find the first truncated position
            int i = 0;
            int inputOffset = 0;
            for (; i < length; i++) {
                int inputLength = outputOffsets[offset + i + 1] - outputOffsets[offset + i];
                int outputLength = truncatedLength(inputSlice, inputOffset, inputLength);
                if (inputLength != outputLength) {
                    break;
                }
                inputOffset += inputLength;
            }

            if (i == length) {
                // No trimming or truncating took place
                values.addChunk(inputSlice);
                return;
            }

            // Resume the iteration, this time shifting positions left according to trimming/truncation
            int outputOffset = inputOffset;
            int nextOffset = outputOffsets[offset + i];
            for (; i < length; i++) {
                int currentOffset = nextOffset;
                nextOffset = outputOffsets[offset + i + 1];
                int inputLength = nextOffset - currentOffset;
                int outputLength = truncatedLength(inputSlice, inputOffset, inputLength);
                System.arraycopy(dataBuffer, inputOffset, dataBuffer, outputOffset, outputLength);
                outputOffsets[offset + i + 1] = outputOffsets[offset + i] + outputLength;
                inputOffset += inputLength;
                outputOffset += outputLength;
            }

            values.addChunk(inputSlice.slice(0, outputOffset));
        }

        protected void readUnbounded(BinaryBuffer values, int offset, int length, int totalInputLength)
        {
            checkPositionIndexes(inputLengthsOffset, inputLengthsOffset + length, prefixLengths.length);
            int[] outputOffsets = values.getOffsets();
            Slice outputBuffer = Slices.wrappedBuffer(readUnbounded(outputOffsets, offset, length, totalInputLength));
            values.addChunk(outputBuffer);
            inputLengthsOffset += length;
        }

        protected int getSuffixesLength(int length)
        {
            int totalSuffixesLength = 0;
            for (int i = 0; i < length; i++) {
                totalSuffixesLength += suffixLengths[inputLengthsOffset + i];
            }
            return totalSuffixesLength;
        }

        protected int getInputLength(int length)
        {
            int totalInputLength = 0;
            for (int i = 0; i < length; i++) {
                totalInputLength += prefixLengths[inputLengthsOffset + i] + suffixLengths[inputLengthsOffset + i];
            }
            return totalInputLength;
        }

        protected InputLengths getInputAndMaxLength(int length)
        {
            int totalInputLength = 0;
            int maxLength = 0;
            for (int i = 0; i < length; i++) {
                int inputLength = prefixLengths[inputLengthsOffset + i] + suffixLengths[inputLengthsOffset + i];
                totalInputLength += inputLength;
                maxLength = max(maxLength, inputLength);
            }
            return new InputLengths(totalInputLength, maxLength);
        }

        protected record InputLengths(int totalInputLength, int maxInputLength) {}

        private byte[] readUnbounded(int[] outputOffsets, int offset, int length, int totalInputLength)
        {
            byte[] output = new byte[totalInputLength];
            Slice inputSlice = input.asSlice();
            // System#arraycopy performs better than Slice#getBytes, therefore we
            // process the input as a byte array rather than through SimpleSliceInputStream#readBytes
            byte[] inputBytes;
            int inputOffsetStart;
            if (inputSlice.length() != 0) {
                inputBytes = inputSlice.byteArray();
                inputOffsetStart = inputSlice.byteArrayOffset();
            }
            else {
                inputBytes = new byte[0];
                inputOffsetStart = 0;
            }
            int inputOffset = inputOffsetStart;

            // Read first position by copying prefix from previous read
            outputOffsets[offset + 1] = outputOffsets[offset] + prefixLengths[inputLengthsOffset] + suffixLengths[inputLengthsOffset];
            System.arraycopy(firstPrefix, 0, output, 0, prefixLengths[inputLengthsOffset]);
            int outputOffset = prefixLengths[inputLengthsOffset];
            int outputLength = suffixLengths[inputLengthsOffset];

            // Read remaining length - 1 positions
            for (int i = 1; i < length; i++) {
                int prefixLength = prefixLengths[inputLengthsOffset + i];
                int suffixLength = suffixLengths[inputLengthsOffset + i];
                outputOffsets[offset + i + 1] = outputOffsets[offset + i] + prefixLength + suffixLength;

                // prefixLength of 0 is a common case, batching arraycopy calls for continuous runs of 0s
                // performs better than copying position by position
                if (prefixLength > 0) {
                    // Copy all previous continuous suffixes
                    System.arraycopy(inputBytes, inputOffset, output, outputOffset, outputLength);
                    inputOffset += outputLength;
                    outputOffset += outputLength;
                    outputLength = 0;

                    // Copy the current prefix
                    int previousPositionLength = prefixLengths[inputLengthsOffset + i - 1] + suffixLengths[inputLengthsOffset + i - 1];
                    int previousOutputStart = outputOffset - previousPositionLength;
                    System.arraycopy(output, previousOutputStart, output, outputOffset, prefixLength);
                    outputOffset += prefixLength;
                }
                outputLength += suffixLength;
            }
            // Copy any remaining suffixes
            System.arraycopy(inputBytes, inputOffset, output, outputOffset, outputLength);
            inputOffset += outputLength;
            outputOffset += outputLength;
            input.skip(inputOffset - inputOffsetStart);

            if (inputLengthsOffset + length < prefixLengths.length) {
                // Prepare prefix for next read if end of input has not been reached
                int previousPositionLength = prefixLengths[inputLengthsOffset + length - 1] + suffixLengths[inputLengthsOffset + length - 1];
                int previousOutputStart = outputOffset - previousPositionLength;
                firstPrefix = Arrays.copyOfRange(output, previousOutputStart, previousOutputStart + prefixLengths[inputLengthsOffset + length]);
            }
            return output;
        }
    }

    private static int[] readDeltaEncodedLengths(SimpleSliceInputStream input)
    {
        DeltaBinaryPackedIntDecoder decoder = new DeltaBinaryPackedIntDecoder();
        decoder.init(input);
        int valueCount = decoder.getValueCount();
        int[] lengths = new int[valueCount];
        decoder.read(lengths, 0, valueCount);
        return lengths;
    }
}
