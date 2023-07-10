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

import static io.trino.parquet.ParquetReaderUtils.castToByteNegate;

public class BitPackingUtils
{
    private BitPackingUtils() {}

    /**
     * @return number of bits equal to 0 (non-nulls)
     */
    public static int unpack(boolean[] values, int offset, byte packedByte, int startBit, int endBit)
    {
        int nonNullCount = 0;
        for (int i = 0; i < endBit - startBit; i++) {
            // We need to negate the value as we convert the "does exist" to "is null", hence '== 0' instead of '== 1'
            boolean value = (((packedByte >>> (startBit + i)) & 1) == 1);
            nonNullCount += castToByteNegate(value);
            values[offset + i] = value;
        }

        return nonNullCount;
    }

    /**
     * @return number of bits equal to 0 (non-nulls)
     */
    public static int unpack(boolean[] values, int offset, byte packedByte)
    {
        values[offset] = (packedByte & 1) == 1;
        values[offset + 1] = ((packedByte >>> 1) & 1) == 1;
        values[offset + 2] = ((packedByte >>> 2) & 1) == 1;
        values[offset + 3] = ((packedByte >>> 3) & 1) == 1;
        values[offset + 4] = ((packedByte >>> 4) & 1) == 1;
        values[offset + 5] = ((packedByte >>> 5) & 1) == 1;
        values[offset + 6] = ((packedByte >>> 6) & 1) == 1;
        values[offset + 7] = ((packedByte >>> 7) & 1) == 1;

        return Byte.SIZE - bitCount(packedByte);
    }

    /**
     * @return number of bits equal to 0 (non-nulls)
     */
    public static int unpack(boolean[] values, int offset, long packedValue)
    {
        values[offset] = (packedValue & 1) == 1;
        values[offset + 1] = ((packedValue >>> 1) & 1) == 1;
        values[offset + 2] = ((packedValue >>> 2) & 1) == 1;
        values[offset + 3] = ((packedValue >>> 3) & 1) == 1;
        values[offset + 4] = ((packedValue >>> 4) & 1) == 1;
        values[offset + 5] = ((packedValue >>> 5) & 1) == 1;
        values[offset + 6] = ((packedValue >>> 6) & 1) == 1;
        values[offset + 7] = ((packedValue >>> 7) & 1) == 1;
        values[offset + 8] = ((packedValue >>> 8) & 1) == 1;
        values[offset + 9] = ((packedValue >>> 9) & 1) == 1;

        values[offset + 10] = ((packedValue >>> 10) & 1) == 1;
        values[offset + 11] = ((packedValue >>> 11) & 1) == 1;
        values[offset + 12] = ((packedValue >>> 12) & 1) == 1;
        values[offset + 13] = ((packedValue >>> 13) & 1) == 1;
        values[offset + 14] = ((packedValue >>> 14) & 1) == 1;
        values[offset + 15] = ((packedValue >>> 15) & 1) == 1;
        values[offset + 16] = ((packedValue >>> 16) & 1) == 1;
        values[offset + 17] = ((packedValue >>> 17) & 1) == 1;
        values[offset + 18] = ((packedValue >>> 18) & 1) == 1;
        values[offset + 19] = ((packedValue >>> 19) & 1) == 1;

        values[offset + 20] = ((packedValue >>> 20) & 1) == 1;
        values[offset + 21] = ((packedValue >>> 21) & 1) == 1;
        values[offset + 22] = ((packedValue >>> 22) & 1) == 1;
        values[offset + 23] = ((packedValue >>> 23) & 1) == 1;
        values[offset + 24] = ((packedValue >>> 24) & 1) == 1;
        values[offset + 25] = ((packedValue >>> 25) & 1) == 1;
        values[offset + 26] = ((packedValue >>> 26) & 1) == 1;
        values[offset + 27] = ((packedValue >>> 27) & 1) == 1;
        values[offset + 28] = ((packedValue >>> 28) & 1) == 1;
        values[offset + 29] = ((packedValue >>> 29) & 1) == 1;

        values[offset + 30] = ((packedValue >>> 30) & 1) == 1;
        values[offset + 31] = ((packedValue >>> 31) & 1) == 1;
        values[offset + 32] = ((packedValue >>> 32) & 1) == 1;
        values[offset + 33] = ((packedValue >>> 33) & 1) == 1;
        values[offset + 34] = ((packedValue >>> 34) & 1) == 1;
        values[offset + 35] = ((packedValue >>> 35) & 1) == 1;
        values[offset + 36] = ((packedValue >>> 36) & 1) == 1;
        values[offset + 37] = ((packedValue >>> 37) & 1) == 1;
        values[offset + 38] = ((packedValue >>> 38) & 1) == 1;
        values[offset + 39] = ((packedValue >>> 39) & 1) == 1;

        values[offset + 40] = ((packedValue >>> 40) & 1) == 1;
        values[offset + 41] = ((packedValue >>> 41) & 1) == 1;
        values[offset + 42] = ((packedValue >>> 42) & 1) == 1;
        values[offset + 43] = ((packedValue >>> 43) & 1) == 1;
        values[offset + 44] = ((packedValue >>> 44) & 1) == 1;
        values[offset + 45] = ((packedValue >>> 45) & 1) == 1;
        values[offset + 46] = ((packedValue >>> 46) & 1) == 1;
        values[offset + 47] = ((packedValue >>> 47) & 1) == 1;
        values[offset + 48] = ((packedValue >>> 48) & 1) == 1;
        values[offset + 49] = ((packedValue >>> 49) & 1) == 1;

        values[offset + 50] = ((packedValue >>> 50) & 1) == 1;
        values[offset + 51] = ((packedValue >>> 51) & 1) == 1;
        values[offset + 52] = ((packedValue >>> 52) & 1) == 1;
        values[offset + 53] = ((packedValue >>> 53) & 1) == 1;
        values[offset + 54] = ((packedValue >>> 54) & 1) == 1;
        values[offset + 55] = ((packedValue >>> 55) & 1) == 1;
        values[offset + 56] = ((packedValue >>> 56) & 1) == 1;
        values[offset + 57] = ((packedValue >>> 57) & 1) == 1;
        values[offset + 58] = ((packedValue >>> 58) & 1) == 1;
        values[offset + 59] = ((packedValue >>> 59) & 1) == 1;

        values[offset + 60] = ((packedValue >>> 60) & 1) == 1;
        values[offset + 61] = ((packedValue >>> 61) & 1) == 1;
        values[offset + 62] = ((packedValue >>> 62) & 1) == 1;
        values[offset + 63] = ((packedValue >>> 63) & 1) == 1;

        return Long.SIZE - Long.bitCount(packedValue);
    }

    public static void unpack(byte[] values, int offset, byte packedByte, int startBit, int endBit)
    {
        for (int i = 0; i < endBit - startBit; i++) {
            values[offset + i] = (byte) ((packedByte >>> (startBit + i)) & 1);
        }
    }

    public static void unpack8FromByte(byte[] values, int offset, byte packedByte)
    {
        values[offset] = (byte) (packedByte & 1);
        values[offset + 1] = (byte) ((packedByte >>> 1) & 1);
        values[offset + 2] = (byte) ((packedByte >>> 2) & 1);
        values[offset + 3] = (byte) ((packedByte >>> 3) & 1);
        values[offset + 4] = (byte) ((packedByte >>> 4) & 1);
        values[offset + 5] = (byte) ((packedByte >>> 5) & 1);
        values[offset + 6] = (byte) ((packedByte >>> 6) & 1);
        values[offset + 7] = (byte) ((packedByte >>> 7) & 1);
    }

    public static void unpack64FromLong(byte[] values, int offset, long packedValue)
    {
        values[offset] = (byte) (packedValue & 1);
        values[offset + 1] = (byte) ((packedValue >>> 1) & 1);
        values[offset + 2] = (byte) ((packedValue >>> 2) & 1);
        values[offset + 3] = (byte) ((packedValue >>> 3) & 1);
        values[offset + 4] = (byte) ((packedValue >>> 4) & 1);
        values[offset + 5] = (byte) ((packedValue >>> 5) & 1);
        values[offset + 6] = (byte) ((packedValue >>> 6) & 1);
        values[offset + 7] = (byte) ((packedValue >>> 7) & 1);
        values[offset + 8] = (byte) ((packedValue >>> 8) & 1);
        values[offset + 9] = (byte) ((packedValue >>> 9) & 1);

        values[offset + 10] = (byte) ((packedValue >>> 10) & 1);
        values[offset + 11] = (byte) ((packedValue >>> 11) & 1);
        values[offset + 12] = (byte) ((packedValue >>> 12) & 1);
        values[offset + 13] = (byte) ((packedValue >>> 13) & 1);
        values[offset + 14] = (byte) ((packedValue >>> 14) & 1);
        values[offset + 15] = (byte) ((packedValue >>> 15) & 1);
        values[offset + 16] = (byte) ((packedValue >>> 16) & 1);
        values[offset + 17] = (byte) ((packedValue >>> 17) & 1);
        values[offset + 18] = (byte) ((packedValue >>> 18) & 1);
        values[offset + 19] = (byte) ((packedValue >>> 19) & 1);

        values[offset + 20] = (byte) ((packedValue >>> 20) & 1);
        values[offset + 21] = (byte) ((packedValue >>> 21) & 1);
        values[offset + 22] = (byte) ((packedValue >>> 22) & 1);
        values[offset + 23] = (byte) ((packedValue >>> 23) & 1);
        values[offset + 24] = (byte) ((packedValue >>> 24) & 1);
        values[offset + 25] = (byte) ((packedValue >>> 25) & 1);
        values[offset + 26] = (byte) ((packedValue >>> 26) & 1);
        values[offset + 27] = (byte) ((packedValue >>> 27) & 1);
        values[offset + 28] = (byte) ((packedValue >>> 28) & 1);
        values[offset + 29] = (byte) ((packedValue >>> 29) & 1);

        values[offset + 30] = (byte) ((packedValue >>> 30) & 1);
        values[offset + 31] = (byte) ((packedValue >>> 31) & 1);
        values[offset + 32] = (byte) ((packedValue >>> 32) & 1);
        values[offset + 33] = (byte) ((packedValue >>> 33) & 1);
        values[offset + 34] = (byte) ((packedValue >>> 34) & 1);
        values[offset + 35] = (byte) ((packedValue >>> 35) & 1);
        values[offset + 36] = (byte) ((packedValue >>> 36) & 1);
        values[offset + 37] = (byte) ((packedValue >>> 37) & 1);
        values[offset + 38] = (byte) ((packedValue >>> 38) & 1);
        values[offset + 39] = (byte) ((packedValue >>> 39) & 1);

        values[offset + 40] = (byte) ((packedValue >>> 40) & 1);
        values[offset + 41] = (byte) ((packedValue >>> 41) & 1);
        values[offset + 42] = (byte) ((packedValue >>> 42) & 1);
        values[offset + 43] = (byte) ((packedValue >>> 43) & 1);
        values[offset + 44] = (byte) ((packedValue >>> 44) & 1);
        values[offset + 45] = (byte) ((packedValue >>> 45) & 1);
        values[offset + 46] = (byte) ((packedValue >>> 46) & 1);
        values[offset + 47] = (byte) ((packedValue >>> 47) & 1);
        values[offset + 48] = (byte) ((packedValue >>> 48) & 1);
        values[offset + 49] = (byte) ((packedValue >>> 49) & 1);

        values[offset + 50] = (byte) ((packedValue >>> 50) & 1);
        values[offset + 51] = (byte) ((packedValue >>> 51) & 1);
        values[offset + 52] = (byte) ((packedValue >>> 52) & 1);
        values[offset + 53] = (byte) ((packedValue >>> 53) & 1);
        values[offset + 54] = (byte) ((packedValue >>> 54) & 1);
        values[offset + 55] = (byte) ((packedValue >>> 55) & 1);
        values[offset + 56] = (byte) ((packedValue >>> 56) & 1);
        values[offset + 57] = (byte) ((packedValue >>> 57) & 1);
        values[offset + 58] = (byte) ((packedValue >>> 58) & 1);
        values[offset + 59] = (byte) ((packedValue >>> 59) & 1);

        values[offset + 60] = (byte) ((packedValue >>> 60) & 1);
        values[offset + 61] = (byte) ((packedValue >>> 61) & 1);
        values[offset + 62] = (byte) ((packedValue >>> 62) & 1);
        values[offset + 63] = (byte) ((packedValue >>> 63) & 1);
    }

    public static int bitCount(byte value)
    {
        return Integer.bitCount(value & 0xFF);
    }
}
