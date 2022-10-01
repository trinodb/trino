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

import io.airlift.slice.Slice;
import io.trino.parquet.reader.SimpleSliceInputStream;
import org.apache.parquet.bytes.ByteBufferInputStream;

import static com.google.common.base.Verify.verify;
import static java.lang.String.format;

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

    public static short toShortExact(int value)
    {
        if ((short) value != value) {
            throw new ArithmeticException(format("Value %d exceeds short range", value));
        }
        return (short) value;
    }

    public static byte toByteExact(int value)
    {
        if ((byte) value != value) {
            throw new ArithmeticException(format("Value %d exceeds byte range", value));
        }
        return (byte) value;
    }
}
