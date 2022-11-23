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
package io.trino.parquet.reader;

import io.trino.spi.type.Decimals;

import java.util.Random;
import java.util.function.IntFunction;

import static com.google.common.base.Preconditions.checkArgument;

public final class TestData
{
    private TestData() {}

    public static IntFunction<long[]> unscaledRandomShortDecimalSupplier(int bitWidth, int precision)
    {
        long min = (-1 * Decimals.longTenToNth(precision)) + 1;
        long max = Decimals.longTenToNth(precision) - 1;
        return size -> {
            Random random = new Random(1);
            long[] result = new long[size];
            for (int i = 0; i < size; i++) {
                result[i] = Math.max(
                        Math.min(randomLong(random, bitWidth), max),
                        min);
            }
            return result;
        };
    }

    public static byte[] longToBytes(long value, int length)
    {
        byte[] result = new byte[length];
        for (int i = length - 1; i >= 0; i--) {
            result[i] = (byte) (value & 0xFF);
            value >>= Byte.SIZE;
        }
        return result;
    }

    private static long randomLong(Random r, int bitWidth)
    {
        checkArgument(bitWidth <= 64 && bitWidth > 0, "bit width must be in range 1 - 64 inclusive");
        if (bitWidth == 64) {
            return r.nextLong();
        }
        return propagateSignBit(r.nextLong(), 64 - bitWidth);
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
    private static long propagateSignBit(long value, int bitsToPad)
    {
        return value << bitsToPad >> bitsToPad;
    }
}
