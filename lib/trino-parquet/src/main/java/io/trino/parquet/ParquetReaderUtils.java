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
package io.trino.parquet;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.slice.Slice;
import io.trino.parquet.reader.SimpleSliceInputStream;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

import java.util.Set;

import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static org.apache.parquet.column.Encoding.BIT_PACKED;
import static org.apache.parquet.column.Encoding.PLAIN_DICTIONARY;
import static org.apache.parquet.column.Encoding.RLE;

public final class ParquetReaderUtils
{
    private ParquetReaderUtils() {}

    public static ByteBufferInputStream toInputStream(Slice slice)
    {
        return ByteBufferInputStream.wrap(slice.toByteBuffer());
    }

    public static ByteBufferInputStream toInputStream(DictionaryPage page)
    {
        return toInputStream(page.getSlice());
    }

    /**
     * Reads an integer formatted in ULEB128 variable-width format described in
     * <a href="https://en.wikipedia.org/wiki/LEB128">...</a>
     */
    public static int readUleb128Int(SimpleSliceInputStream input)
    {
        byte[] inputBytes = input.getByteArray();
        int offset = input.getByteArrayOffset();
        // Manual loop unrolling shows improvements in BenchmarkReadUleb128Int
        int inputByte = inputBytes[offset];
        int value = inputByte & 0x7F;
        if ((inputByte & 0x80) == 0) {
            input.skip(1);
            return value;
        }
        inputByte = inputBytes[offset + 1];
        value |= (inputByte & 0x7F) << 7;
        if ((inputByte & 0x80) == 0) {
            input.skip(2);
            return value;
        }
        inputByte = inputBytes[offset + 2];
        value |= (inputByte & 0x7F) << 14;
        if ((inputByte & 0x80) == 0) {
            input.skip(3);
            return value;
        }
        inputByte = inputBytes[offset + 3];
        value |= (inputByte & 0x7F) << 21;
        if ((inputByte & 0x80) == 0) {
            input.skip(4);
            return value;
        }
        inputByte = inputBytes[offset + 4];
        verify((inputByte & 0x80) == 0, "ULEB128 variable-width integer should not be longer than 5 bytes");
        input.skip(5);
        return value | inputByte << 28;
    }

    public static long readUleb128Long(SimpleSliceInputStream input)
    {
        byte[] inputBytes = input.getByteArray();
        int offset = input.getByteArrayOffset();
        // Manual loop unrolling shows improvements in BenchmarkReadUleb128Long
        long inputByte = inputBytes[offset];
        long value = inputByte & 0x7F;
        if ((inputByte & 0x80) == 0) {
            input.skip(1);
            return value;
        }
        inputByte = inputBytes[offset + 1];
        value |= (inputByte & 0x7F) << 7;
        if ((inputByte & 0x80) == 0) {
            input.skip(2);
            return value;
        }
        inputByte = inputBytes[offset + 2];
        value |= (inputByte & 0x7F) << 14;
        if ((inputByte & 0x80) == 0) {
            input.skip(3);
            return value;
        }
        inputByte = inputBytes[offset + 3];
        value |= (inputByte & 0x7F) << 21;
        if ((inputByte & 0x80) == 0) {
            input.skip(4);
            return value;
        }
        inputByte = inputBytes[offset + 4];
        value |= (inputByte & 0x7F) << 28;
        if ((inputByte & 0x80) == 0) {
            input.skip(5);
            return value;
        }
        inputByte = inputBytes[offset + 5];
        value |= (inputByte & 0x7F) << 35;
        if ((inputByte & 0x80) == 0) {
            input.skip(6);
            return value;
        }
        inputByte = inputBytes[offset + 6];
        value |= (inputByte & 0x7F) << 42;
        if ((inputByte & 0x80) == 0) {
            input.skip(7);
            return value;
        }
        inputByte = inputBytes[offset + 7];
        value |= (inputByte & 0x7F) << 49;
        if ((inputByte & 0x80) == 0) {
            input.skip(8);
            return value;
        }
        inputByte = inputBytes[offset + 8];
        value |= (inputByte & 0x7F) << 56;
        if ((inputByte & 0x80) == 0) {
            input.skip(9);
            return value;
        }
        inputByte = inputBytes[offset + 9];
        verify((inputByte & 0x80) == 0, "ULEB128 variable-width long should not be longer than 10 bytes");
        input.skip(10);
        return value | inputByte << 63;
    }

    public static int readFixedWidthInt(SimpleSliceInputStream input, int bytesWidth)
    {
        return switch (bytesWidth) {
            case 0 -> 0;
            case 1 -> input.readByte() & 0xFF;
            case 2 -> input.readShort() & 0xFFFF;
            case 3 -> {
                int value = input.readShort() & 0xFFFF;
                yield ((input.readByte() & 0xFF) << 16) | value;
            }
            case 4 -> input.readInt();
            default -> throw new IllegalArgumentException(format("Encountered bytesWidth (%d) that requires more than 4 bytes", bytesWidth));
        };
    }

    /**
     * For storing signed values (not the deltas themselves) in DELTA_BINARY_PACKED encoding, zigzag encoding
     * (<a href="https://developers.google.com/protocol-buffers/docs/encoding#signed-integers">...</a>)
     * is used to map negative values to positive ones and then apply ULEB128 on the result.
     */
    public static long zigzagDecode(long value)
    {
        return (value >>> 1) ^ -(value & 1);
    }

    /**
     * Returns the result of arguments division rounded up.
     * <p>
     * Works only for positive numbers.
     * The sum of dividend and divisor cannot exceed Integer.MAX_VALUE
     */
    public static int ceilDiv(int dividend, int divisor)
    {
        return (dividend + divisor - 1) / divisor;
    }

    /**
     * Propagate the sign bit in values that are shorter than 8 bytes.
     * <p>
     * When the value of less than 8 bytes in put into a long variable, the padding bytes on the
     * left side of the number should be all zeros for a positive number or all ones for negatives.
     * This method does this padding using signed bit shift operator without branches.
     *
     * @param value Value to trim
     * @param bitsToPad Number of bits to pad
     * @return Value with correct padding
     */
    public static long propagateSignBit(long value, int bitsToPad)
    {
        return value << bitsToPad >> bitsToPad;
    }

    /**
     * Method simulates a cast from boolean to byte value. Despite using
     * a ternary (?) operator, the just-in-time compiler usually figures out
     * that this is a cast and turns that into a no-op.
     * <p>
     * Method may be used to avoid branches that may be CPU costly due to
     * branch misprediction.
     * The following code:
     * <pre>
     *      boolean[] flags = ...
     *      int sum = 0;
     *      for (int i = 0; i &lt; length; i++){
     *          if (flags[i])
     *              sum++;
     *      }
     * </pre>
     * will perform better when rewritten to
     * <pre>
     *      boolean[] flags = ...
     *      int sum = 0;
     *      for (int i = 0; i &lt; length; i++){
     *          sum += castToByte(flags[i]);
     *      }
     * </pre>
     */
    public static byte castToByte(boolean value)
    {
        return (byte) (value ? 1 : 0);
    }

    /**
     * Works the same as {@link io.trino.parquet.ParquetReaderUtils#castToByte(boolean)} and negates the boolean value
     */
    public static byte castToByteNegate(boolean value)
    {
        return (byte) (value ? 0 : 1);
    }

    public static short toShortExact(long value)
    {
        if ((short) value != value) {
            throw new ArithmeticException("short overflow");
        }
        return (short) value;
    }

    public static short toShortExact(int value)
    {
        if ((short) value != value) {
            throw new ArithmeticException(format("Value %d exceeds short range", value));
        }
        return (short) value;
    }

    public static byte toByteExact(long value)
    {
        if ((byte) value != value) {
            throw new ArithmeticException("byte overflow");
        }
        return (byte) value;
    }

    public static byte toByteExact(int value)
    {
        if ((byte) value != value) {
            throw new ArithmeticException(format("Value %d exceeds byte range", value));
        }
        return (byte) value;
    }

    @SuppressWarnings("deprecation")
    public static boolean isOnlyDictionaryEncodingPages(ColumnChunkMetaData columnMetaData)
    {
        // Files written with newer versions of Parquet libraries (e.g. parquet-mr 1.9.0) will have EncodingStats available
        // Otherwise, fallback to v1 logic
        EncodingStats stats = columnMetaData.getEncodingStats();
        if (stats != null) {
            return stats.hasDictionaryPages() && !stats.hasNonDictionaryEncodedPages();
        }

        Set<Encoding> encodings = columnMetaData.getEncodings();
        if (encodings.contains(PLAIN_DICTIONARY)) {
            // PLAIN_DICTIONARY was present, which means at least one page was
            // dictionary-encoded and 1.0 encodings are used
            // The only other allowed encodings are RLE and BIT_PACKED which are used for repetition or definition levels
            return Sets.difference(encodings, ImmutableSet.of(PLAIN_DICTIONARY, RLE, BIT_PACKED)).isEmpty();
        }

        return false;
    }
}
