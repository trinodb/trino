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
import static io.trino.spi.type.Chars.byteCountWithoutTrailingSpace;
import static io.trino.spi.type.Varchars.byteCount;
import static java.lang.System.arraycopy;
import static java.util.Objects.requireNonNull;

/**
 * read methods in this class calculate offsets and lengths of positions and then
 * create a single byte array that is pushed to the output buffer
 */
public class PlainByteArrayDecoders
{
    private PlainByteArrayDecoders() {}

    public static final class BoundedVarcharPlainValueDecoder
            implements ValueDecoder<BinaryBuffer>
    {
        private final int boundedLength;

        private SimpleSliceInputStream input;

        public BoundedVarcharPlainValueDecoder(VarcharType varcharType)
        {
            checkArgument(
                    !varcharType.isUnbounded(),
                    "Trino type %s is not a bounded varchar",
                    varcharType);
            this.boundedLength = varcharType.getBoundedLength();
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            this.input = requireNonNull(input, "input is null");
        }

        @Override
        public void read(BinaryBuffer values, int offset, int length)
        {
            Slice inputSlice = input.asSlice();
            // Output offsets array is used as a temporary space for position lengths
            int[] offsets = values.getOffsets();
            int currentInputOffset = 0;
            int outputBufferSize = 0;

            for (int i = offset; i < offset + length; i++) {
                int positionLength = inputSlice.getInt(currentInputOffset);
                currentInputOffset += Integer.BYTES;
                int outputLength = byteCount(inputSlice, currentInputOffset, positionLength, boundedLength);
                offsets[i + 1] = outputLength;
                outputBufferSize += outputLength;
                currentInputOffset += positionLength;
            }

            createOutputBuffer(values, offset, length, inputSlice, outputBufferSize);
            input.skip(currentInputOffset);
        }

        @Override
        public void skip(int n)
        {
            skipPlainValues(input, n);
        }
    }

    public static final class CharPlainValueDecoder
            implements ValueDecoder<BinaryBuffer>
    {
        private final int maxLength;

        private SimpleSliceInputStream input;

        public CharPlainValueDecoder(CharType charType)
        {
            this.maxLength = charType.getLength();
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            this.input = requireNonNull(input, "input is null");
        }

        @Override
        public void read(BinaryBuffer values, int offset, int length)
        {
            Slice inputSlice = input.asSlice();
            // Output offsets array is used as a temporary space for position lengths
            int[] offsets = values.getOffsets();
            int currentInputOffset = 0;
            int outputBufferSize = 0;

            for (int i = offset; i < offset + length; i++) {
                int positionLength = inputSlice.getInt(currentInputOffset);
                currentInputOffset += Integer.BYTES;
                int outputLength = byteCountWithoutTrailingSpace(inputSlice, currentInputOffset, positionLength, maxLength);
                offsets[i + 1] = outputLength;
                outputBufferSize += outputLength;
                currentInputOffset += positionLength;
            }

            createOutputBuffer(values, offset, length, inputSlice, outputBufferSize);
            input.skip(currentInputOffset);
        }

        @Override
        public void skip(int n)
        {
            skipPlainValues(input, n);
        }
    }

    public static final class BinaryPlainValueDecoder
            implements ValueDecoder<BinaryBuffer>
    {
        private SimpleSliceInputStream input;

        @Override
        public void init(SimpleSliceInputStream input)
        {
            this.input = requireNonNull(input, "input is null");
        }

        @Override
        public void read(BinaryBuffer values, int offset, int length)
        {
            Slice inputSlice = input.asSlice();
            // Output offsets array is used as a temporary space for position lengths
            int[] offsets = values.getOffsets();
            int currentInputOffset = 0;
            int outputBufferSize = 0;

            for (int i = 0; i < length; i++) {
                int positionLength = inputSlice.getInt(currentInputOffset);
                offsets[offset + i + 1] = positionLength;
                outputBufferSize += positionLength;
                currentInputOffset += positionLength + Integer.BYTES;
            }

            createOutputBuffer(values, offset, length, inputSlice, outputBufferSize);
            input.skip(currentInputOffset);
        }

        @Override
        public void skip(int n)
        {
            skipPlainValues(input, n);
        }
    }

    private static void skipPlainValues(SimpleSliceInputStream input, int n)
    {
        for (int i = 0; i < n; i++) {
            int positionLength = input.readInt();
            input.skip(positionLength);
        }
    }

    /**
     * Create one big slice of data and add it to the output buffer since buffer size is known
     */
    private static void createOutputBuffer(BinaryBuffer values, int offset, int length, Slice inputSlice, int outputBufferSize)
    {
        int[] offsets = values.getOffsets();
        byte[] outputBuffer = new byte[outputBufferSize];
        int currentInputOffset = 0;
        int currentOutputOffset = 0;

        byte[] inputArray;
        int inputArrayOffset;
        if (length != 0) {
            inputArray = inputSlice.byteArray();
            inputArrayOffset = inputSlice.byteArrayOffset();
        }
        else {
            inputArray = new byte[0];
            inputArrayOffset = 0;
        }
        for (int i = 0; i < length; i++) {
            int inputPositionLength = inputSlice.getInt(currentInputOffset);
            int outputPositionLength = offsets[offset + i + 1];
            arraycopy(inputArray, inputArrayOffset + currentInputOffset + Integer.BYTES, outputBuffer, currentOutputOffset, outputPositionLength);
            offsets[offset + i + 1] = offsets[offset + i] + outputPositionLength;
            currentInputOffset += inputPositionLength + Integer.BYTES;
            currentOutputOffset += outputPositionLength;
        }
        values.addChunk(Slices.wrappedBuffer(outputBuffer));
    }
}
