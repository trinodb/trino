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

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.parquet.ParquetReaderUtils.toByteExact;
import static io.trino.parquet.ParquetReaderUtils.toShortExact;
import static io.trino.parquet.reader.decoders.ByteBitUnpackers.getByteBitUnpacker;
import static io.trino.parquet.reader.decoders.IntBitUnpackers.getIntBitUnpacker;
import static io.trino.parquet.reader.decoders.LongBitUnpackers.getLongBitUnpacker;
import static io.trino.parquet.reader.decoders.ShortBitUnpackers.getShortBitUnpacker;

public final class DeltaPackingUtils
{
    private static final int SHORTS_IN_LONG = Long.BYTES / Short.BYTES;

    private DeltaPackingUtils() {}

    public static void unpackDelta(byte[] output, int outputOffset, int length, SimpleSliceInputStream input, long minDelta, byte bitWidth)
    {
        if (bitWidth == 0) {
            unpackEmpty(output, outputOffset, length, toByteExact(minDelta));
        }
        else {
            unpackByte(output, outputOffset, input, length, bitWidth, minDelta);
        }
    }

    public static void unpackDelta(short[] output, int outputOffset, int length, SimpleSliceInputStream input, long minDelta, byte bitWidth)
    {
        if (bitWidth == 0) {
            unpackEmpty(output, outputOffset, length, toShortExact(minDelta));
        }
        else {
            unpackShort(output, outputOffset, input, length, bitWidth, minDelta);
        }
    }

    public static void unpackDelta(int[] output, int outputOffset, int length, SimpleSliceInputStream input, long minDelta, byte bitWidth)
    {
        if (bitWidth == 0) {
            unpackEmpty(output, outputOffset, length, (int) minDelta);
        }
        else {
            unpackInt(output, outputOffset, input, length, bitWidth, (int) minDelta);
        }
    }

    public static void unpackDelta(long[] output, int outputOffset, int length, SimpleSliceInputStream input, long minDelta, byte bitWidth)
    {
        if (bitWidth == 0) {
            unpackEmpty(output, outputOffset, length, minDelta);
        }
        else {
            unpackLong(output, outputOffset, input, length, bitWidth, minDelta);
        }
    }

    /**
     * Fills output array with values that differ by a constant delta.
     * With delta = 0 all values are the same and equal to the last written one
     */
    private static void unpackEmpty(byte[] output, int outputOffset, int length, byte delta)
    {
        if (delta == 0) { // Common case
            fillArray8(output, outputOffset, length / 8, output[outputOffset - 1]);
        }
        else {
            fillArray8(output, outputOffset, length / 8, delta);
            for (int i = outputOffset; i < outputOffset + length; i += 32) {
                output[i] += output[i - 1];
                output[i + 1] += output[i];
                output[i + 2] += output[i + 1];
                output[i + 3] += output[i + 2];
                output[i + 4] += output[i + 3];
                output[i + 5] += output[i + 4];
                output[i + 6] += output[i + 5];
                output[i + 7] += output[i + 6];
                output[i + 8] += output[i + 7];
                output[i + 9] += output[i + 8];
                output[i + 10] += output[i + 9];
                output[i + 11] += output[i + 10];
                output[i + 12] += output[i + 11];
                output[i + 13] += output[i + 12];
                output[i + 14] += output[i + 13];
                output[i + 15] += output[i + 14];
                output[i + 16] += output[i + 15];
                output[i + 17] += output[i + 16];
                output[i + 18] += output[i + 17];
                output[i + 19] += output[i + 18];
                output[i + 20] += output[i + 19];
                output[i + 21] += output[i + 20];
                output[i + 22] += output[i + 21];
                output[i + 23] += output[i + 22];
                output[i + 24] += output[i + 23];
                output[i + 25] += output[i + 24];
                output[i + 26] += output[i + 25];
                output[i + 27] += output[i + 26];
                output[i + 28] += output[i + 27];
                output[i + 29] += output[i + 28];
                output[i + 30] += output[i + 29];
                output[i + 31] += output[i + 30];
            }
        }
    }

    /**
     * Fills output array with values that differ by a constant delta.
     * With delta = 0 all values are the same and equal to the last written one
     */
    private static void unpackEmpty(short[] output, int outputOffset, int length, short delta)
    {
        if (delta == 0) { // Common case
            fillArray4(output, outputOffset, length / 4, output[outputOffset - 1]);
        }
        else {
            fillArray4(output, outputOffset, length / 4, delta);
            for (int i = outputOffset; i < outputOffset + length; i += 32) {
                output[i] += output[i - 1];
                output[i + 1] += output[i];
                output[i + 2] += output[i + 1];
                output[i + 3] += output[i + 2];
                output[i + 4] += output[i + 3];
                output[i + 5] += output[i + 4];
                output[i + 6] += output[i + 5];
                output[i + 7] += output[i + 6];
                output[i + 8] += output[i + 7];
                output[i + 9] += output[i + 8];
                output[i + 10] += output[i + 9];
                output[i + 11] += output[i + 10];
                output[i + 12] += output[i + 11];
                output[i + 13] += output[i + 12];
                output[i + 14] += output[i + 13];
                output[i + 15] += output[i + 14];
                output[i + 16] += output[i + 15];
                output[i + 17] += output[i + 16];
                output[i + 18] += output[i + 17];
                output[i + 19] += output[i + 18];
                output[i + 20] += output[i + 19];
                output[i + 21] += output[i + 20];
                output[i + 22] += output[i + 21];
                output[i + 23] += output[i + 22];
                output[i + 24] += output[i + 23];
                output[i + 25] += output[i + 24];
                output[i + 26] += output[i + 25];
                output[i + 27] += output[i + 26];
                output[i + 28] += output[i + 27];
                output[i + 29] += output[i + 28];
                output[i + 30] += output[i + 29];
                output[i + 31] += output[i + 30];
            }
        }
    }

    /**
     * Fills output array with values that differ by a constant delta.
     * With delta = 0 all values are the same and equal to the last written one
     */
    private static void unpackEmpty(int[] output, int outputOffset, int length, int delta)
    {
        if (delta == 0) { // Common case
            Arrays.fill(output, outputOffset, outputOffset + length, output[outputOffset - 1]);
        }
        else {
            Arrays.fill(output, outputOffset, outputOffset + length, delta);
            for (int i = outputOffset; i < outputOffset + length; i += 32) {
                output[i] += output[i - 1];
                output[i + 1] += output[i];
                output[i + 2] += output[i + 1];
                output[i + 3] += output[i + 2];
                output[i + 4] += output[i + 3];
                output[i + 5] += output[i + 4];
                output[i + 6] += output[i + 5];
                output[i + 7] += output[i + 6];
                output[i + 8] += output[i + 7];
                output[i + 9] += output[i + 8];
                output[i + 10] += output[i + 9];
                output[i + 11] += output[i + 10];
                output[i + 12] += output[i + 11];
                output[i + 13] += output[i + 12];
                output[i + 14] += output[i + 13];
                output[i + 15] += output[i + 14];
                output[i + 16] += output[i + 15];
                output[i + 17] += output[i + 16];
                output[i + 18] += output[i + 17];
                output[i + 19] += output[i + 18];
                output[i + 20] += output[i + 19];
                output[i + 21] += output[i + 20];
                output[i + 22] += output[i + 21];
                output[i + 23] += output[i + 22];
                output[i + 24] += output[i + 23];
                output[i + 25] += output[i + 24];
                output[i + 26] += output[i + 25];
                output[i + 27] += output[i + 26];
                output[i + 28] += output[i + 27];
                output[i + 29] += output[i + 28];
                output[i + 30] += output[i + 29];
                output[i + 31] += output[i + 30];
            }
        }
    }

    /**
     * Fills output array with values that differ by a constant delta.
     * With delta = 0 all values are the same and equal to the last written one
     */
    private static void unpackEmpty(long[] output, int outputOffset, int length, long delta)
    {
        if (delta == 0) { // Common case
            Arrays.fill(output, outputOffset, outputOffset + length, output[outputOffset - 1]);
        }
        else {
            Arrays.fill(output, outputOffset, outputOffset + length, delta);
            for (int i = outputOffset; i < outputOffset + length; i += 32) {
                output[i] += output[i - 1];
                output[i + 1] += output[i];
                output[i + 2] += output[i + 1];
                output[i + 3] += output[i + 2];
                output[i + 4] += output[i + 3];
                output[i + 5] += output[i + 4];
                output[i + 6] += output[i + 5];
                output[i + 7] += output[i + 6];
                output[i + 8] += output[i + 7];
                output[i + 9] += output[i + 8];
                output[i + 10] += output[i + 9];
                output[i + 11] += output[i + 10];
                output[i + 12] += output[i + 11];
                output[i + 13] += output[i + 12];
                output[i + 14] += output[i + 13];
                output[i + 15] += output[i + 14];
                output[i + 16] += output[i + 15];
                output[i + 17] += output[i + 16];
                output[i + 18] += output[i + 17];
                output[i + 19] += output[i + 18];
                output[i + 20] += output[i + 19];
                output[i + 21] += output[i + 20];
                output[i + 22] += output[i + 21];
                output[i + 23] += output[i + 22];
                output[i + 24] += output[i + 23];
                output[i + 25] += output[i + 24];
                output[i + 26] += output[i + 25];
                output[i + 27] += output[i + 26];
                output[i + 28] += output[i + 27];
                output[i + 29] += output[i + 28];
                output[i + 30] += output[i + 29];
                output[i + 31] += output[i + 30];
            }
        }
    }

    private static void unpackByte(byte[] output, int outputOffset, SimpleSliceInputStream input, int length, byte bitWidth, long minDelta)
    {
        getByteBitUnpacker(bitWidth).unpack(output, outputOffset, input, length);
        inPlacePrefixSum(output, outputOffset, length, (short) minDelta);
    }

    private static void unpackShort(short[] output, int outputOffset, SimpleSliceInputStream input, int length, byte bitWidth, long minDelta)
    {
        getShortBitUnpacker(bitWidth).unpack(output, outputOffset, input, length);
        inPlacePrefixSum(output, outputOffset, length, (int) minDelta);
    }

    private static void unpackInt(int[] output, int outputOffset, SimpleSliceInputStream input, int length, byte bitWidth, int minDelta)
    {
        getIntBitUnpacker(bitWidth).unpack(output, outputOffset, input, length);
        inPlacePrefixSum(output, outputOffset, length, minDelta);
    }

    private static void unpackLong(long[] output, int outputOffset, SimpleSliceInputStream input, int length, byte bitWidth, long minDelta)
    {
        getLongBitUnpacker(bitWidth).unpack(output, outputOffset, input, length);
        inPlacePrefixSum(output, outputOffset, length, minDelta);
    }

    /**
     * Fill byte array with a value. Fills 8 values at a time
     *
     * @param length Number of LONG values to write i.e. number of bytes / 8
     */
    private static void fillArray8(byte[] output, int outputOffset, int length, byte baseValue)
    {
        int lengthInBytes = length * Long.BYTES;
        Slices.wrappedBuffer(output, outputOffset, lengthInBytes)
                .fill(baseValue);
    }

    /**
     * Fill short array with a value. Fills 4 values at a time
     *
     * @param length Number of LONG values to write i.e. number of shorts / 4
     */
    private static void fillArray4(short[] output, int outputOffset, int length, short baseValue)
    {
        Slice buffer = Slices.wrappedShortArray(output, outputOffset, length * SHORTS_IN_LONG);
        checkArgument(output.length - outputOffset >= length * SHORTS_IN_LONG, "Trying to write values out of array bounds");
        long value = fillLong(baseValue);
        for (int i = 0; i < length * Long.BYTES; i += Long.BYTES) {
            buffer.setLong(i, value);
        }
    }

    /**
     * @return long value made out of the argument concatenated 4 times
     */
    private static long fillLong(short baseValue)
    {
        long value = ((long) (baseValue & 0xFFFF) << 16) | (baseValue & 0xFFFF);
        value = (value << 32) | value;
        return value;
    }

    private static void inPlacePrefixSum(byte[] output, int outputOffset, int length, short minDelta)
    {
        for (int i = outputOffset; i < outputOffset + length; i += 32) {
            output[i] += output[i - 1] + minDelta;
            output[i + 1] += output[i] + minDelta;
            output[i + 2] += output[i + 1] + minDelta;
            output[i + 3] += output[i + 2] + minDelta;
            output[i + 4] += output[i + 3] + minDelta;
            output[i + 5] += output[i + 4] + minDelta;
            output[i + 6] += output[i + 5] + minDelta;
            output[i + 7] += output[i + 6] + minDelta;
            output[i + 8] += output[i + 7] + minDelta;
            output[i + 9] += output[i + 8] + minDelta;
            output[i + 10] += output[i + 9] + minDelta;
            output[i + 11] += output[i + 10] + minDelta;
            output[i + 12] += output[i + 11] + minDelta;
            output[i + 13] += output[i + 12] + minDelta;
            output[i + 14] += output[i + 13] + minDelta;
            output[i + 15] += output[i + 14] + minDelta;
            output[i + 16] += output[i + 15] + minDelta;
            output[i + 17] += output[i + 16] + minDelta;
            output[i + 18] += output[i + 17] + minDelta;
            output[i + 19] += output[i + 18] + minDelta;
            output[i + 20] += output[i + 19] + minDelta;
            output[i + 21] += output[i + 20] + minDelta;
            output[i + 22] += output[i + 21] + minDelta;
            output[i + 23] += output[i + 22] + minDelta;
            output[i + 24] += output[i + 23] + minDelta;
            output[i + 25] += output[i + 24] + minDelta;
            output[i + 26] += output[i + 25] + minDelta;
            output[i + 27] += output[i + 26] + minDelta;
            output[i + 28] += output[i + 27] + minDelta;
            output[i + 29] += output[i + 28] + minDelta;
            output[i + 30] += output[i + 29] + minDelta;
            output[i + 31] += output[i + 30] + minDelta;
        }
    }

    private static void inPlacePrefixSum(short[] output, int outputOffset, int length, int minDelta)
    {
        for (int i = outputOffset; i < outputOffset + length; i += 32) {
            output[i] += output[i - 1] + minDelta;
            output[i + 1] += output[i] + minDelta;
            output[i + 2] += output[i + 1] + minDelta;
            output[i + 3] += output[i + 2] + minDelta;
            output[i + 4] += output[i + 3] + minDelta;
            output[i + 5] += output[i + 4] + minDelta;
            output[i + 6] += output[i + 5] + minDelta;
            output[i + 7] += output[i + 6] + minDelta;
            output[i + 8] += output[i + 7] + minDelta;
            output[i + 9] += output[i + 8] + minDelta;
            output[i + 10] += output[i + 9] + minDelta;
            output[i + 11] += output[i + 10] + minDelta;
            output[i + 12] += output[i + 11] + minDelta;
            output[i + 13] += output[i + 12] + minDelta;
            output[i + 14] += output[i + 13] + minDelta;
            output[i + 15] += output[i + 14] + minDelta;
            output[i + 16] += output[i + 15] + minDelta;
            output[i + 17] += output[i + 16] + minDelta;
            output[i + 18] += output[i + 17] + minDelta;
            output[i + 19] += output[i + 18] + minDelta;
            output[i + 20] += output[i + 19] + minDelta;
            output[i + 21] += output[i + 20] + minDelta;
            output[i + 22] += output[i + 21] + minDelta;
            output[i + 23] += output[i + 22] + minDelta;
            output[i + 24] += output[i + 23] + minDelta;
            output[i + 25] += output[i + 24] + minDelta;
            output[i + 26] += output[i + 25] + minDelta;
            output[i + 27] += output[i + 26] + minDelta;
            output[i + 28] += output[i + 27] + minDelta;
            output[i + 29] += output[i + 28] + minDelta;
            output[i + 30] += output[i + 29] + minDelta;
            output[i + 31] += output[i + 30] + minDelta;
        }
    }

    private static void inPlacePrefixSum(int[] output, int outputOffset, int length, int minDelta)
    {
        for (int i = outputOffset; i < outputOffset + length; i += 32) {
            output[i] += output[i - 1] + minDelta;
            output[i + 1] += output[i] + minDelta;
            output[i + 2] += output[i + 1] + minDelta;
            output[i + 3] += output[i + 2] + minDelta;
            output[i + 4] += output[i + 3] + minDelta;
            output[i + 5] += output[i + 4] + minDelta;
            output[i + 6] += output[i + 5] + minDelta;
            output[i + 7] += output[i + 6] + minDelta;
            output[i + 8] += output[i + 7] + minDelta;
            output[i + 9] += output[i + 8] + minDelta;
            output[i + 10] += output[i + 9] + minDelta;
            output[i + 11] += output[i + 10] + minDelta;
            output[i + 12] += output[i + 11] + minDelta;
            output[i + 13] += output[i + 12] + minDelta;
            output[i + 14] += output[i + 13] + minDelta;
            output[i + 15] += output[i + 14] + minDelta;
            output[i + 16] += output[i + 15] + minDelta;
            output[i + 17] += output[i + 16] + minDelta;
            output[i + 18] += output[i + 17] + minDelta;
            output[i + 19] += output[i + 18] + minDelta;
            output[i + 20] += output[i + 19] + minDelta;
            output[i + 21] += output[i + 20] + minDelta;
            output[i + 22] += output[i + 21] + minDelta;
            output[i + 23] += output[i + 22] + minDelta;
            output[i + 24] += output[i + 23] + minDelta;
            output[i + 25] += output[i + 24] + minDelta;
            output[i + 26] += output[i + 25] + minDelta;
            output[i + 27] += output[i + 26] + minDelta;
            output[i + 28] += output[i + 27] + minDelta;
            output[i + 29] += output[i + 28] + minDelta;
            output[i + 30] += output[i + 29] + minDelta;
            output[i + 31] += output[i + 30] + minDelta;
        }
    }

    private static void inPlacePrefixSum(long[] output, int outputOffset, int length, long minDelta)
    {
        for (int i = outputOffset; i < outputOffset + length; i += 32) {
            output[i] += output[i - 1] + minDelta;
            output[i + 1] += output[i] + minDelta;
            output[i + 2] += output[i + 1] + minDelta;
            output[i + 3] += output[i + 2] + minDelta;
            output[i + 4] += output[i + 3] + minDelta;
            output[i + 5] += output[i + 4] + minDelta;
            output[i + 6] += output[i + 5] + minDelta;
            output[i + 7] += output[i + 6] + minDelta;
            output[i + 8] += output[i + 7] + minDelta;
            output[i + 9] += output[i + 8] + minDelta;
            output[i + 10] += output[i + 9] + minDelta;
            output[i + 11] += output[i + 10] + minDelta;
            output[i + 12] += output[i + 11] + minDelta;
            output[i + 13] += output[i + 12] + minDelta;
            output[i + 14] += output[i + 13] + minDelta;
            output[i + 15] += output[i + 14] + minDelta;
            output[i + 16] += output[i + 15] + minDelta;
            output[i + 17] += output[i + 16] + minDelta;
            output[i + 18] += output[i + 17] + minDelta;
            output[i + 19] += output[i + 18] + minDelta;
            output[i + 20] += output[i + 19] + minDelta;
            output[i + 21] += output[i + 20] + minDelta;
            output[i + 22] += output[i + 21] + minDelta;
            output[i + 23] += output[i + 22] + minDelta;
            output[i + 24] += output[i + 23] + minDelta;
            output[i + 25] += output[i + 24] + minDelta;
            output[i + 26] += output[i + 25] + minDelta;
            output[i + 27] += output[i + 26] + minDelta;
            output[i + 28] += output[i + 27] + minDelta;
            output[i + 29] += output[i + 28] + minDelta;
            output[i + 30] += output[i + 29] + minDelta;
            output[i + 31] += output[i + 30] + minDelta;
        }
    }
}
