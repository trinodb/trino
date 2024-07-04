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

public class BitPackingUtils
{
    private BitPackingUtils() {}

    /**
     * Sets the value to 1 if the bit is not set, 0 otherwise.
     *
     * @return number of bits equal to 0 (non-nulls)
     */
    public static int unpackUnset(byte[] values, int offset, byte packedByte, int startBit, int endBit)
    {
        int nonNullCount = 0;
        for (int i = 0; i < endBit - startBit; i++) {
            // We need to negate the value as we convert the "does exist" to "is null", hence '== 0' instead of '== 1'
            byte value = (byte) ((packedByte >>> (startBit + i)) & 1);
            nonNullCount += (value == 1 ? 0 : 1);
            values[offset + i] = value;
        }

        return nonNullCount;
    }

    /**
     * Sets the value to 1 if the bit is not set, 0 otherwise.
     *
     * @return number of bits equal to 0 (non-nulls)
     */
    public static int unpackUnset(byte[] values, int offset, byte packedByte)
    {
        unpack8FromByte(values, offset, packedByte);
        return Byte.SIZE - bitCount(packedByte);
    }

    /**
     * Sets the value to 1 if the bit is set, 0 otherwise.
     */
    public static void unpackSet(byte[] values, int offset, byte packedByte, int startBit, int endBit)
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

    public static int bitCount(byte value)
    {
        return Integer.bitCount(value & 0xFF);
    }
}
