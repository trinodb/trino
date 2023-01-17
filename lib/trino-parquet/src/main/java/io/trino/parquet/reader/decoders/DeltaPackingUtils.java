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

import static io.trino.parquet.reader.decoders.IntBitUnpackers.getIntBitUnpacker;
import static io.trino.parquet.reader.decoders.LongBitUnpackers.getLongBitUnpacker;

public final class DeltaPackingUtils
{
    private DeltaPackingUtils() {}

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
