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

import java.lang.invoke.VarHandle;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.parquet.ParquetReaderUtils.propagateSignBit;
import static java.lang.invoke.MethodHandles.byteArrayViewVarHandle;
import static java.nio.ByteOrder.BIG_ENDIAN;

public class ShortDecimalFixedWidthByteArrayBatchDecoder
{
    private static final ShortDecimalDecoder[] VALUE_DECODERS = new ShortDecimalDecoder[] {
            new BigEndianReader1(),
            new BigEndianReader2(),
            new BigEndianReader3(),
            new BigEndianReader4(),
            new BigEndianReader5(),
            new BigEndianReader6(),
            new BigEndianReader7(),
            new BigEndianReader8()
    };
    private static final VarHandle LONG_HANDLE_BIG_ENDIAN = byteArrayViewVarHandle(long[].class, BIG_ENDIAN);
    private static final VarHandle INT_HANDLE_BIG_ENDIAN = byteArrayViewVarHandle(int[].class, BIG_ENDIAN);

    public interface ShortDecimalDecoder
    {
        void decode(SimpleSliceInputStream input, long[] values, int offset, int length);
    }

    private final ShortDecimalDecoder decoder;

    public ShortDecimalFixedWidthByteArrayBatchDecoder(int length)
    {
        checkArgument(
                length > 0 && length <= 8,
                "Short decimal length %s must be in range 1-8",
                length);
        decoder = VALUE_DECODERS[length - 1];
        // Unscaled number is encoded as two's complement using big-endian byte order
        // (the most significant byte is the zeroth element)
    }

    public void getShortDecimalValues(SimpleSliceInputStream input, long[] values, int offset, int length)
    {
        decoder.decode(input, values, offset, length);
    }

    public static final class BigEndianReader8
            implements ShortDecimalDecoder
    {
        @Override
        public void decode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            for (int i = offset; i < offset + length; i++) {
                values[i] = (long) LONG_HANDLE_BIG_ENDIAN.get(inputArray, inputOffset);
                inputOffset += Long.BYTES;
            }
            input.skip(length * Long.BYTES);
        }
    }

    private static final class BigEndianReader7
            implements ShortDecimalDecoder
    {
        @Override
        public void decode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            if (length == 0) {
                return;
            }
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            int endOffset = offset + length;
            for (int i = offset; i < endOffset - 1; i++) {
                // We read redundant bytes and then ignore them. Sign bit is propagated by `>>` operator
                values[i] = (long) LONG_HANDLE_BIG_ENDIAN.get(inputArray, inputOffset + inputBytesRead) >> 8;
                inputBytesRead += 7;
            }
            // Decode the last one "normally" as it would read data out of bounds
            values[endOffset - 1] = decode(input, inputBytesRead);
            input.skip(inputBytesRead + 7);
        }

        private long decode(SimpleSliceInputStream input, int index)
        {
            long value = (input.getByteUnchecked(index + 6) & 0xFFL)
                    | (input.getByteUnchecked(index + 5) & 0xFFL) << 8
                    | (input.getByteUnchecked(index + 4) & 0xFFL) << 16
                    | (Integer.reverseBytes(input.getIntUnchecked(index)) & 0xFFFFFFFFL) << 24;
            return propagateSignBit(value, 8);
        }
    }

    private static final class BigEndianReader6
            implements ShortDecimalDecoder
    {
        @Override
        public void decode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            if (length == 0) {
                return;
            }
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            int endOffset = offset + length;
            for (int i = offset; i < endOffset - 1; i++) {
                // We read redundant bytes and then ignore them. Sign bit is propagated by `>>` operator
                values[i] = (long) LONG_HANDLE_BIG_ENDIAN.get(inputArray, inputOffset + inputBytesRead) >> 16;
                inputBytesRead += 6;
            }
            // Decode the last one "normally" as it would read data out of bounds
            values[endOffset - 1] = decode(input, inputBytesRead);
            input.skip(inputBytesRead + 6);
        }

        private long decode(SimpleSliceInputStream input, int index)
        {
            long value = (input.getByteUnchecked(index + 5) & 0xFFL)
                    | (input.getByteUnchecked(index + 4) & 0xFFL) << 8
                    | (Integer.reverseBytes(input.getIntUnchecked(index)) & 0xFFFFFFFFL) << 16;
            return propagateSignBit(value, 16);
        }
    }

    private static final class BigEndianReader5
            implements ShortDecimalDecoder
    {
        @Override
        public void decode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            if (length == 0) {
                return;
            }
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            int endOffset = offset + length;
            for (int i = offset; i < endOffset - 1; i++) {
                // We read redundant bytes and then ignore them. Sign bit is propagated by `>>` operator
                values[i] = (long) LONG_HANDLE_BIG_ENDIAN.get(inputArray, inputOffset + inputBytesRead) >> 24;
                inputBytesRead += 5;
            }
            // Decode the last one "normally" as it would read data out of bounds
            values[endOffset - 1] = decode(input, inputBytesRead);
            input.skip(inputBytesRead + 5);
        }

        private long decode(SimpleSliceInputStream input, int index)
        {
            long value = (input.getByteUnchecked(index + 4) & 0xFFL)
                    | (Integer.reverseBytes(input.getIntUnchecked(index)) & 0xFFFFFFFFL) << 8;
            return propagateSignBit(value, 24);
        }
    }

    private static final class BigEndianReader4
            implements ShortDecimalDecoder
    {
        @Override
        public void decode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            for (int i = offset; i < offset + length; i++) {
                values[i] = (int) INT_HANDLE_BIG_ENDIAN.get(inputArray, inputOffset);
                inputOffset += Integer.BYTES;
            }
            input.skip(length * Integer.BYTES);
        }
    }

    private static final class BigEndianReader3
            implements ShortDecimalDecoder
    {
        @Override
        public void decode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            while (length > 2) {
                // We read redundant bytes and then ignore them. Sign bit is propagated by `>>` operator
                long value = (long) LONG_HANDLE_BIG_ENDIAN.get(inputArray, inputOffset + inputBytesRead);
                inputBytesRead += 6;

                values[offset] = value >> 40;
                values[offset + 1] = value << 24 >> 40;

                offset += 2;
                length -= 2;
            }
            // Decode the last values "normally" as it would read data out of bounds
            while (length > 0) {
                values[offset++] = decode(input, inputBytesRead);
                length--;
                inputBytesRead += 3;
            }
            input.skip(inputBytesRead);
        }

        private long decode(SimpleSliceInputStream input, int index)
        {
            long value = (input.getByteUnchecked(index + 2) & 0xFFL)
                    | (input.getByteUnchecked(index + 1) & 0xFFL) << 8
                    | (input.getByteUnchecked(index) & 0xFFL) << 16;
            return propagateSignBit(value, 40);
        }
    }

    private static final class BigEndianReader2
            implements ShortDecimalDecoder
    {
        @Override
        public void decode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            byte[] inputArray = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            while (length > 3) {
                // Reverse all bytes at once
                long value = (long) LONG_HANDLE_BIG_ENDIAN.get(inputArray, inputOffset + inputBytesRead);
                inputBytesRead += Long.BYTES;

                // We first shift the byte as left as possible. Then, when shifting back right,
                // the sign bit will get propagated
                values[offset] = value >> 48;
                values[offset + 1] = value << 16 >> 48;
                values[offset + 2] = value << 32 >> 48;
                values[offset + 3] = value << 48 >> 48;

                offset += 4;
                length -= 4;
            }
            input.skip(inputBytesRead);

            while (length > 0) {
                // Implicit cast will propagate the sign bit correctly, as it is performed after the byte reversal.
                values[offset++] = Short.reverseBytes(input.readShort());
                length--;
            }
        }
    }

    private static final class BigEndianReader1
            implements ShortDecimalDecoder
    {
        @Override
        public void decode(SimpleSliceInputStream input, long[] values, int offset, int length)
        {
            byte[] inputArr = input.getByteArray();
            int inputOffset = input.getByteArrayOffset();
            int inputBytesRead = 0;
            int outputOffset = offset;
            while (length > 0) {
                // Implicit cast will propagate the sign bit correctly
                values[outputOffset++] = inputArr[inputOffset + inputBytesRead];
                inputBytesRead++;
                length--;
            }
            input.skip(inputBytesRead);
        }
    }
}
