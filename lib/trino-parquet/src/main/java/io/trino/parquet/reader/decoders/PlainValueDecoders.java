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
import io.trino.parquet.reader.flat.BinaryBuffer;
import io.trino.plugin.base.type.DecodedTimestamp;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import org.apache.parquet.column.ColumnDescriptor;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.parquet.ParquetReaderUtils.toByteExact;
import static io.trino.parquet.ParquetReaderUtils.toShortExact;
import static io.trino.parquet.ParquetTimestampUtils.decodeInt96Timestamp;
import static io.trino.parquet.ParquetTypeUtils.checkBytesFitInShortDecimal;
import static io.trino.parquet.ParquetTypeUtils.getShortDecimalValue;
import static io.trino.spi.block.Fixed12Block.encodeFixed12;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;

public final class PlainValueDecoders
{
    private PlainValueDecoders() {}

    public static final class LongPlainValueDecoder
            implements ValueDecoder<long[]>
    {
        private SimpleSliceInputStream input;

        @Override
        public void init(SimpleSliceInputStream input)
        {
            this.input = requireNonNull(input, "input is null");
        }

        @Override
        public void read(long[] values, int offset, int length)
        {
            input.readLongs(values, offset, length);
        }

        @Override
        public void skip(int n)
        {
            input.skip(n * Long.BYTES);
        }
    }

    public static final class IntPlainValueDecoder
            implements ValueDecoder<int[]>
    {
        private SimpleSliceInputStream input;

        @Override
        public void init(SimpleSliceInputStream input)
        {
            this.input = requireNonNull(input, "input is null");
        }

        @Override
        public void read(int[] values, int offset, int length)
        {
            input.readInts(values, offset, length);
        }

        @Override
        public void skip(int n)
        {
            input.skip(n * Integer.BYTES);
        }
    }

    public static final class IntToShortPlainValueDecoder
            implements ValueDecoder<short[]>
    {
        private SimpleSliceInputStream input;

        @Override
        public void init(SimpleSliceInputStream input)
        {
            this.input = requireNonNull(input, "input is null");
        }

        @Override
        public void read(short[] values, int offset, int length)
        {
            for (int i = offset; i < offset + length; i++) {
                values[i] = toShortExact(input.readIntUnchecked());
            }
        }

        @Override
        public void skip(int n)
        {
            input.skip(n * Integer.BYTES);
        }
    }

    public static final class IntToBytePlainValueDecoder
            implements ValueDecoder<byte[]>
    {
        private SimpleSliceInputStream input;

        @Override
        public void init(SimpleSliceInputStream input)
        {
            this.input = requireNonNull(input, "input is null");
        }

        @Override
        public void read(byte[] values, int offset, int length)
        {
            for (int i = offset; i < offset + length; i++) {
                values[i] = toByteExact(input.readIntUnchecked());
            }
        }

        @Override
        public void skip(int n)
        {
            input.skip(n * Integer.BYTES);
        }
    }

    public static final class ShortDecimalFixedLengthByteArrayDecoder
            implements ValueDecoder<long[]>
    {
        private final int typeLength;
        private final ColumnDescriptor descriptor;
        private final ShortDecimalFixedWidthByteArrayBatchDecoder decimalValueDecoder;

        private SimpleSliceInputStream input;

        public ShortDecimalFixedLengthByteArrayDecoder(ColumnDescriptor descriptor)
        {
            DecimalLogicalTypeAnnotation decimalAnnotation = (DecimalLogicalTypeAnnotation) descriptor.getPrimitiveType().getLogicalTypeAnnotation();
            checkArgument(
                    decimalAnnotation.getPrecision() <= Decimals.MAX_SHORT_PRECISION,
                    "Decimal type %s is not a short decimal",
                    decimalAnnotation);
            this.typeLength = descriptor.getPrimitiveType().getTypeLength();
            checkArgument(typeLength > 0 && typeLength <= 16, "Expected column %s to have type length in range (1-16)", descriptor);
            this.descriptor = descriptor;
            this.decimalValueDecoder = new ShortDecimalFixedWidthByteArrayBatchDecoder(Math.min(typeLength, Long.BYTES));
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            this.input = requireNonNull(input, "input is null");
        }

        @Override
        public void read(long[] values, int offset, int length)
        {
            if (typeLength <= Long.BYTES) {
                decimalValueDecoder.getShortDecimalValues(input, values, offset, length);
                return;
            }
            int extraBytesLength = typeLength - Long.BYTES;
            byte[] inputBytes = input.getByteArray();
            int inputBytesOffset = input.getByteArrayOffset();
            for (int i = offset; i < offset + length; i++) {
                checkBytesFitInShortDecimal(inputBytes, inputBytesOffset, extraBytesLength, descriptor);
                values[i] = getShortDecimalValue(inputBytes, inputBytesOffset + extraBytesLength, Long.BYTES);
                inputBytesOffset += typeLength;
            }
            input.skip(length * typeLength);
        }

        @Override
        public void skip(int n)
        {
            input.skip(n * typeLength);
        }
    }

    public static final class LongDecimalPlainValueDecoder
            implements ValueDecoder<long[]>
    {
        private final int typeLength;
        private final byte[] inputBytes;

        private SimpleSliceInputStream input;

        public LongDecimalPlainValueDecoder(int typeLength)
        {
            checkArgument(typeLength > 0 && typeLength <= 16, "typeLength %s should be in range (1-16) for a long decimal", typeLength);
            this.typeLength = typeLength;
            this.inputBytes = new byte[typeLength];
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            this.input = requireNonNull(input, "input is null");
        }

        @Override
        public void read(long[] values, int offset, int length)
        {
            int endOffset = (offset + length) * 2;
            for (int currentOutputOffset = offset * 2; currentOutputOffset < endOffset; currentOutputOffset += 2) {
                input.readBytes(Slices.wrappedBuffer(inputBytes), 0, typeLength);
                Int128 value = Int128.fromBigEndian(inputBytes);
                values[currentOutputOffset] = value.getHigh();
                values[currentOutputOffset + 1] = value.getLow();
            }
        }

        @Override
        public void skip(int n)
        {
            input.skip(n * typeLength);
        }
    }

    public static final class UuidPlainValueDecoder
            implements ValueDecoder<long[]>
    {
        private static final int UUID_SIZE = 16;

        private SimpleSliceInputStream input;

        @Override
        public void init(SimpleSliceInputStream input)
        {
            this.input = requireNonNull(input, "input is null");
        }

        @Override
        public void read(long[] values, int offset, int length)
        {
            int endOffset = (offset + length) * 2;
            for (int currentOutputOffset = offset * 2; currentOutputOffset < endOffset; currentOutputOffset += 2) {
                values[currentOutputOffset] = input.readLong();
                values[currentOutputOffset + 1] = input.readLong();
            }
        }

        @Override
        public void skip(int n)
        {
            input.skip(n * UUID_SIZE);
        }
    }

    public static final class Int96TimestampPlainValueDecoder
            implements ValueDecoder<int[]>
    {
        private static final int LENGTH = SIZE_OF_LONG + SIZE_OF_INT;

        private SimpleSliceInputStream input;

        @Override
        public void init(SimpleSliceInputStream input)
        {
            this.input = requireNonNull(input, "input is null");
        }

        @Override
        public void read(int[] values, int offset, int length)
        {
            for (int i = offset; i < offset + length; i++) {
                DecodedTimestamp timestamp = decodeInt96Timestamp(input.readLongUnchecked(), input.readIntUnchecked());
                encodeFixed12(timestamp.epochSeconds(), timestamp.nanosOfSecond(), values, i);
            }
        }

        @Override
        public void skip(int n)
        {
            input.skip(n * LENGTH);
        }
    }

    public static final class FixedLengthPlainValueDecoder
            implements ValueDecoder<BinaryBuffer>
    {
        private final int typeLength;

        private SimpleSliceInputStream input;

        public FixedLengthPlainValueDecoder(int typeLength)
        {
            this.typeLength = typeLength;
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            this.input = requireNonNull(input, "input is null");
        }

        @Override
        public void read(BinaryBuffer values, int offset, int length)
        {
            values.addChunk(input.readSlice(typeLength * length));
            int[] outputOffsets = values.getOffsets();

            int inputLength = outputOffsets[offset] + typeLength;
            for (int i = offset; i < offset + length; i++) {
                outputOffsets[i + 1] = inputLength;
                inputLength += typeLength;
            }
        }

        @Override
        public void skip(int n)
        {
            input.skip(n * typeLength);
        }
    }
}
