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

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.parquet.ParquetReaderUtils.readFixedWidthInt;
import static io.trino.parquet.ParquetReaderUtils.readUleb128Int;
import static io.trino.parquet.reader.decoders.IntBitUnpackers.getIntBitUnpacker;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

/**
 * <a href="https://github.com/apache/parquet-format/blob/master/Encodings.md#run-length-encoding--bit-packing-hybrid-rle--3">
 * Run Length Encoding / Bit-Packing Hybrid (RLE)
 * </a>
 * This class is similar to {@link io.trino.parquet.reader.flat.NullsDecoder} but specialized for reading integers stored in bit width of 0 - 32.
 * It is used specifically for decoding dictionary ids currently.
 * It can be used for decoding definition and repetition levels of nested columns in future.
 */
public final class RleBitPackingHybridDecoder
        implements ValueDecoder<int[]>
{
    // Bit-packed values comes in batches of 8 according to specs
    private static final int EXTRACT_BATCH_SIZE = 8;

    private final int bitWidth;
    private final int byteWidth;
    private final IntBitUnpacker unpacker;

    private SimpleSliceInputStream input;

    // Encoding type if decoding stopped in the middle of the group
    private boolean isRle;
    // Values left to decode in the current group
    private int valuesLeftInGroup;
    // With RLE encoding - the current value
    private int rleValue;
    // With bit-packing - buffer of EXTRACT_BATCH_SIZE values currently read.
    private int[] valuesBuffer;
    // Number of values already read in the current buffer while reading bit-packed values
    private int alreadyReadInBuffer;

    public RleBitPackingHybridDecoder(int bitWidth)
    {
        checkArgument(bitWidth >= 0 && bitWidth <= 32, "bit width need to be between 0 and 32");
        this.bitWidth = bitWidth;
        this.byteWidth = byteWidth(bitWidth);
        this.unpacker = getIntBitUnpacker(bitWidth);
    }

    @Override
    public void init(SimpleSliceInputStream input)
    {
        this.input = requireNonNull(input, "input is null");
        this.valuesBuffer = new int[EXTRACT_BATCH_SIZE];
    }

    @Override
    public void read(int[] values, int offset, int length)
    {
        while (length > 0) {
            if (valuesLeftInGroup == 0) {
                readGroupHeader();
            }

            if (isRle) {
                int chunkSize = min(length, valuesLeftInGroup);
                Arrays.fill(values, offset, offset + chunkSize, rleValue);
                valuesLeftInGroup -= chunkSize;
                offset += chunkSize;
                length -= chunkSize;
            }
            else if (alreadyReadInBuffer != 0) { // bit-packed - read remaining bytes stored in the buffer
                int remainingValues = EXTRACT_BATCH_SIZE - alreadyReadInBuffer;
                int chunkSize = min(remainingValues, length);
                System.arraycopy(valuesBuffer, alreadyReadInBuffer, values, offset, chunkSize);
                valuesLeftInGroup -= chunkSize;
                alreadyReadInBuffer = (alreadyReadInBuffer + chunkSize) % EXTRACT_BATCH_SIZE;
                offset += chunkSize;
                length -= chunkSize;
            }
            else { // bit-packed
                // At this point we have only full batches to read and valuesLeftInGroup is a multiplication of 8
                int chunkSize = min(length, valuesLeftInGroup);
                int leftToRead = chunkSize % EXTRACT_BATCH_SIZE;
                int fullBatchesToRead = chunkSize - leftToRead;
                unpacker.unpack(values, offset, input, fullBatchesToRead);
                offset += fullBatchesToRead;
                if (leftToRead > 0) {
                    unpacker.unpack(valuesBuffer, 0, input, EXTRACT_BATCH_SIZE); // Unpack to temporary buffer
                    System.arraycopy(valuesBuffer, 0, values, offset, leftToRead);
                    offset += leftToRead;
                }
                alreadyReadInBuffer = leftToRead;
                valuesLeftInGroup -= chunkSize;
                length -= chunkSize;
            }
        }
    }

    @Override
    public void skip(int n)
    {
        while (n > 0) {
            if (valuesLeftInGroup == 0) {
                readGroupHeader();
            }

            if (isRle) {
                int chunkSize = min(n, valuesLeftInGroup);
                valuesLeftInGroup -= chunkSize;
                n -= chunkSize;
            }
            else if (alreadyReadInBuffer != 0) { // bit-packed - skip remaining bytes stored in the buffer
                int remainingValues = EXTRACT_BATCH_SIZE - alreadyReadInBuffer;
                int chunkSize = min(remainingValues, n);
                valuesLeftInGroup -= chunkSize;
                alreadyReadInBuffer = (alreadyReadInBuffer + chunkSize) % EXTRACT_BATCH_SIZE;
                n -= chunkSize;
            }
            else { // bit-packed
                int chunkSize = min(n, valuesLeftInGroup);
                int fullBatchesToRead = chunkSize / EXTRACT_BATCH_SIZE;
                input.skip(fullBatchesToRead * bitWidth * EXTRACT_BATCH_SIZE / Byte.SIZE);
                int leftToRead = chunkSize % EXTRACT_BATCH_SIZE;
                if (leftToRead > 0) {
                    unpacker.unpack(valuesBuffer, 0, input, EXTRACT_BATCH_SIZE);
                }
                alreadyReadInBuffer = leftToRead;
                valuesLeftInGroup -= chunkSize;
                n -= chunkSize;
            }
        }
    }

    private void readGroupHeader()
    {
        int header = readUleb128Int(input);
        isRle = (header & 1) == 0;
        valuesLeftInGroup = header >>> 1;
        if (isRle) {
            rleValue = readFixedWidthInt(input, byteWidth);
        }
        else {
            // Only full bytes are encoded
            valuesLeftInGroup *= 8;
        }
    }

    private static int byteWidth(int bitWidth)
    {
        // Equivalent of Math.ceil(bitWidth / Byte.SIZE) but without double arithmetics
        return (bitWidth + Byte.SIZE - 1) / Byte.SIZE;
    }
}
