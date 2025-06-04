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
package io.trino.parquet.reader.flat;

import io.airlift.slice.Slice;
import io.trino.parquet.reader.SimpleSliceInputStream;

import java.util.Arrays;

import static io.trino.parquet.ParquetReaderUtils.castToByteNegate;
import static io.trino.parquet.ParquetReaderUtils.readUleb128Int;
import static io.trino.parquet.reader.flat.BitPackingUtils.bitCount;
import static io.trino.parquet.reader.flat.BitPackingUtils.unpack;
import static io.trino.parquet.reader.flat.VectorBitPackingUtils.vectorUnpackAndInvert8;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

/**
 * The hybrid RLE/bit-packing encoding consists of multiple groups.
 * Each group is either encoded as RLE or bit-packed
 * <p>
 * For a primitive column, the definition level is always either 0 (null) or 1 (non-null).
 * Therefore, every value is decoded from a single bit and stored into a boolean array
 * which stores false for non-null and true for null.
 */
public class NullsDecoders
{
    private NullsDecoders() {}

    public static FlatDefinitionLevelDecoder createNullsDecoder(boolean vectorizedDecodingEnabled)
    {
        return vectorizedDecodingEnabled ? new VectorNullsDecoder() : new NullsDecoder();
    }

    private abstract static class AbstractNullsDecoder
            implements FlatDefinitionLevelDecoder
    {
        protected SimpleSliceInputStream input;
        // Encoding type if decoding stopped in the middle of the group
        protected boolean isRle;
        // Values left to decode in the current group
        protected int valuesLeftInGroup;
        // With RLE encoding - the current value
        protected boolean rleValue;
        // With bit-packing - the byte that has been partially read
        protected byte bitPackedValue;
        // Number of bits already read in the current byte while reading bit-packed values
        protected int bitPackedValueOffset;

        @Override
        public void init(Slice input)
        {
            this.input = new SimpleSliceInputStream(requireNonNull(input, "input is null"));
        }

        /**
         * Skip 'length' values and return the number of non-nulls encountered
         */
        @Override
        public int skip(int length)
        {
            int nonNullCount = 0;
            while (length > 0) {
                if (valuesLeftInGroup == 0) {
                    readGroupHeader();
                }

                if (isRle) {
                    int chunkSize = min(length, valuesLeftInGroup);
                    nonNullCount += castToByteNegate(rleValue) * chunkSize;
                    valuesLeftInGroup -= chunkSize;

                    length -= chunkSize;
                }
                else if (bitPackedValueOffset != 0) { // bit-packed - read remaining bits of current byte
                    int remainingBits = Byte.SIZE - bitPackedValueOffset;
                    int chunkSize = min(remainingBits, length);
                    int remainingPackedValue = (bitPackedValue & 0xff) >>> bitPackedValueOffset;
                    // In bitPackedValue 1's are nulls, so the number of non-nulls is
                    // chunkSize - bitCount(remainingBits up to chunkSize)
                    nonNullCount += chunkSize - bitCount((byte) (remainingPackedValue & ((1 << chunkSize) - 1)));
                    valuesLeftInGroup -= chunkSize;
                    bitPackedValueOffset = (bitPackedValueOffset + chunkSize) % Byte.SIZE;

                    length -= chunkSize;
                }
                else { // bit-packed
                    // At this point we have only full bytes to read and valuesLeft is a multiplication of 8
                    int chunkSize = min(length, valuesLeftInGroup);
                    int leftToRead = chunkSize;
                    // Parquet uses 1 for non-null value
                    while (leftToRead >= Long.SIZE) {
                        nonNullCount += Long.bitCount(input.readLong());
                        leftToRead -= Long.SIZE;
                    }
                    while (leftToRead >= Byte.SIZE) {
                        nonNullCount += bitCount(input.readByte());
                        leftToRead -= Byte.SIZE;
                    }
                    if (leftToRead > 0) {
                        byte packedValue = input.readByte();
                        nonNullCount += bitCount((byte) (packedValue & ((1 << leftToRead) - 1)));

                        // Inverting packedValue as readNext expects 1 for null
                        bitPackedValue = (byte) ~packedValue;
                        bitPackedValueOffset += leftToRead;
                    }
                    valuesLeftInGroup -= chunkSize;
                    length -= chunkSize;
                }
            }
            return nonNullCount;
        }

        protected void readGroupHeader()
        {
            int header = readUleb128Int(input);
            isRle = (header & 1) == 0;
            valuesLeftInGroup = header >>> 1;
            if (isRle) {
                // We need to negate the value as we convert the "does exist" to "is null", hence "== 0"
                rleValue = input.readByte() == 0;
            }
            else {
                // Only full bytes are encoded
                valuesLeftInGroup *= Byte.SIZE;
            }
        }
    }

    private static final class NullsDecoder
            extends AbstractNullsDecoder
    {
        /**
         * 'values' array needs to be empty, i.e. contain only false values.
         */
        @Override
        public int readNext(boolean[] values, int offset, int length)
        {
            int nonNullCount = 0;
            while (length > 0) {
                if (valuesLeftInGroup == 0) {
                    readGroupHeader();
                }

                if (isRle) {
                    int chunkSize = min(length, valuesLeftInGroup);
                    // The contract of the method requires values array to be empty (i.e. filled with false)
                    // so action is required only if the value is equal to true
                    if (rleValue) {
                        Arrays.fill(values, offset, offset + chunkSize, true);
                    }
                    nonNullCount += castToByteNegate(rleValue) * chunkSize;
                    valuesLeftInGroup -= chunkSize;

                    length -= chunkSize;
                    offset += chunkSize;
                }
                else if (bitPackedValueOffset != 0) { // bit-packed - read remaining bits of current byte
                    int remainingBits = Byte.SIZE - bitPackedValueOffset;
                    int chunkSize = min(remainingBits, length);
                    nonNullCount += unpack(values, offset, bitPackedValue, bitPackedValueOffset, bitPackedValueOffset + chunkSize);
                    valuesLeftInGroup -= chunkSize;
                    bitPackedValueOffset = (bitPackedValueOffset + chunkSize) % Byte.SIZE;

                    offset += chunkSize;
                    length -= chunkSize;
                }
                else { // bit-packed
                    // At this point we have only full bytes to read and valuesLeft is a multiplication of 8
                    int chunkSize = min(length, valuesLeftInGroup);
                    int leftToRead = chunkSize;
                    // All values read from input are inverted as Trino uses 1 for null but Parquet uses 1 for non-null value
                    while (leftToRead >= Byte.SIZE) {
                        nonNullCount += unpack(values, offset, (byte) ~input.readByte());
                        offset += Byte.SIZE;
                        leftToRead -= Byte.SIZE;
                    }
                    if (leftToRead > 0) {
                        bitPackedValue = (byte) ~input.readByte();
                        nonNullCount += unpack(values, offset, bitPackedValue, 0, leftToRead);
                        bitPackedValueOffset += leftToRead;
                        offset += leftToRead;
                    }
                    valuesLeftInGroup -= chunkSize;
                    length -= chunkSize;
                }
            }
            return nonNullCount;
        }
    }

    private static final class VectorNullsDecoder
            extends AbstractNullsDecoder
    {
        /**
         * 'values' array needs to be empty, i.e. contain only false values.
         */
        @Override
        public int readNext(boolean[] values, int offset, int length)
        {
            int nonNullCount = 0;
            while (length > 0) {
                if (valuesLeftInGroup == 0) {
                    readGroupHeader();
                }

                if (isRle) {
                    int chunkSize = min(length, valuesLeftInGroup);
                    // The contract of the method requires values array to be empty (i.e. filled with false)
                    // so action is required only if the value is equal to true
                    if (rleValue) {
                        Arrays.fill(values, offset, offset + chunkSize, true);
                    }
                    nonNullCount += castToByteNegate(rleValue) * chunkSize;
                    valuesLeftInGroup -= chunkSize;

                    length -= chunkSize;
                    offset += chunkSize;
                }
                else if (bitPackedValueOffset != 0) { // bit-packed - read remaining bits of current byte
                    int remainingBits = Byte.SIZE - bitPackedValueOffset;
                    int chunkSize = min(remainingBits, length);
                    nonNullCount += unpack(values, offset, bitPackedValue, bitPackedValueOffset, bitPackedValueOffset + chunkSize);
                    valuesLeftInGroup -= chunkSize;
                    bitPackedValueOffset = (bitPackedValueOffset + chunkSize) % Byte.SIZE;

                    offset += chunkSize;
                    length -= chunkSize;
                }
                else { // bit-packed
                    // At this point we have only full bytes to read and valuesLeft is a multiplication of 8
                    int chunkSize = min(length, valuesLeftInGroup);
                    int leftToRead = chunkSize;
                    // All values read from input are inverted as Trino uses 1 for null but Parquet uses 1 for non-null value
                    byte[] inputArray = input.getByteArray();
                    int inputOffset = input.getByteArrayOffset();
                    int inputBytesRead = 0;
                    while (leftToRead >= Byte.SIZE) {
                        nonNullCount += vectorUnpackAndInvert8(values, offset, inputArray[inputOffset + inputBytesRead]);
                        offset += Byte.SIZE;
                        leftToRead -= Byte.SIZE;
                        inputBytesRead++;
                    }
                    input.skip(inputBytesRead);

                    if (leftToRead > 0) {
                        bitPackedValue = (byte) ~input.readByte();
                        nonNullCount += unpack(values, offset, bitPackedValue, 0, leftToRead);
                        bitPackedValueOffset += leftToRead;
                        offset += leftToRead;
                    }
                    valuesLeftInGroup -= chunkSize;
                    length -= chunkSize;
                }
            }
            return nonNullCount;
        }
    }
}
