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

import com.google.common.primitives.Bytes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.ParquetReaderUtils;
import io.trino.spi.type.Decimals;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Objects;
import java.util.Random;
import java.util.function.IntFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.parquet.ParquetTypeUtils.paddingBigInteger;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class TestData
{
    private TestData() {}

    // Based on org.apache.parquet.schema.Types.BasePrimitiveBuilder.maxPrecision to determine the max decimal precision supported by INT32/INT64
    public static int maxPrecision(int numBytes)
    {
        return toIntExact(
                // convert double to long
                Math.round(
                        // number of base-10 digits
                        Math.floor(Math.log10(
                                Math.pow(2, 8 * numBytes - 1) - 1))));  // max value stored in numBytes
    }

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

    public static boolean[] generateMixedData(Random r, int size, int maxGroupSize)
    {
        BooleanArrayList mixedList = new BooleanArrayList(size);
        while (mixedList.size() < size) {
            boolean isGroup = r.nextBoolean();
            int groupSize = r.nextInt(maxGroupSize);
            if (isGroup) {
                boolean value = r.nextBoolean();
                for (int i = 0; i < groupSize; i++) {
                    mixedList.add(value);
                }
            }
            else {
                for (int i = 0; i < groupSize; i++) {
                    mixedList.add(r.nextBoolean());
                }
            }
        }
        boolean[] result = new boolean[size];
        mixedList.getElements(0, result, 0, size);
        return result;
    }

    public static int[] generateMixedData(Random r, int size, int maxGroupSize, int bitWidth)
    {
        IntList mixedList = new IntArrayList();
        while (mixedList.size() < size) {
            boolean isGroup = r.nextBoolean();
            int groupSize = r.nextInt(maxGroupSize);
            if (isGroup) {
                int value = randomInt(r, bitWidth);
                for (int i = 0; i < groupSize; i++) {
                    mixedList.add(value);
                }
            }
            else {
                for (int i = 0; i < groupSize; i++) {
                    mixedList.add(randomInt(r, bitWidth));
                }
            }
        }
        int[] result = new int[size];
        mixedList.getElements(0, result, 0, size);
        return result;
    }

    public static Slice randomBigInteger(Random r)
    {
        BigInteger bigInteger = new BigInteger(126, r);
        byte[] result = paddingBigInteger(bigInteger, 2 * SIZE_OF_LONG);
        Bytes.reverse(result);
        return Slices.wrappedBuffer(result);
    }

    public static int randomInt(Random r, int bitWidth)
    {
        checkArgument(bitWidth <= 32 && bitWidth > 0, "bit width must be in range 1 - 32 inclusive");
        if (bitWidth == 32) {
            return r.nextInt();
        }
        return propagateSignBit(r.nextInt(), 32 - bitWidth);
    }

    public static int randomUnsignedInt(Random r, int bitWidth)
    {
        checkArgument(bitWidth <= 32 && bitWidth >= 0, "bit width must be in range 0 - 32 inclusive");
        if (bitWidth == 32) {
            return r.nextInt();
        }
        else if (bitWidth == 31) {
            return r.nextInt() & ((1 << 31) - 1);
        }
        return r.nextInt(1 << bitWidth);
    }

    public static long randomLong(Random r, int bitWidth)
    {
        checkArgument(bitWidth <= 64 && bitWidth > 0, "bit width must be in range 1 - 64 inclusive");
        if (bitWidth == 64) {
            return r.nextLong();
        }
        return ParquetReaderUtils.propagateSignBit(r.nextLong(), 64 - bitWidth);
    }

    public static byte[][] randomBinaryData(int size, int minLength, int maxLength)
    {
        Random random = new Random(Objects.hash(size, minLength, maxLength));
        byte[][] data = new byte[size][];
        for (int i = 0; i < size; i++) {
            int length = random.nextInt(maxLength - minLength + 1) + minLength;
            byte[] value = new byte[length];
            random.nextBytes(value);
            data[i] = value;
        }

        return data;
    }

    public static byte[][] randomUtf8(int size, int length)
    {
        Random random = new Random(Objects.hash(size, length));
        byte[][] data = new byte[size][];
        for (int i = 0; i < size; i++) {
            StringBuilder builder = new StringBuilder();
            for (int j = 0; j < length; j++) {
                builder.append((char) random.nextInt(1 << 16));
            }
            data[i] = Arrays.copyOf(builder.toString().getBytes(UTF_8), length);
        }
        return data;
    }

    public static byte[][] randomAsciiData(int size, int minLength, int maxLength)
    {
        Random random = new Random(Objects.hash(size, minLength, maxLength));
        byte[][] data = new byte[size][];
        for (int i = 0; i < size; i++) {
            int length = random.nextInt(maxLength - minLength + 1) + minLength;
            byte[] value = new byte[length];
            for (int j = 0; j < length; j++) {
                value[j] = (byte) random.nextInt(128);
            }
            data[i] = value;
        }

        return data;
    }

    private static int propagateSignBit(int value, int bitsToPad)
    {
        return value << bitsToPad >> bitsToPad;
    }
}
