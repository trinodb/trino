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

import static io.trino.spi.block.Bitmap.orPackedBits;
import static io.trino.spi.block.Bitmap.set;

public class BitPackingUtils
{
    private BitPackingUtils() {}

    /**
     * @return number of set bits (non-nulls)
     */
    public static int unpack(long[] values, int offset, byte packedByte, int startBit, int endBit)
    {
        int nonNullCount = 0;
        for (int i = 0; i < endBit - startBit; i++) {
            if (((packedByte >>> (startBit + i)) & 1) != 0) {
                set(values, 0, offset + i);
                nonNullCount++;
            }
        }

        return nonNullCount;
    }

    /**
     * @return number of set bits (non-nulls)
     */
    public static int unpack(long[] values, int offset, byte packedByte)
    {
        orPackedBits(values, 0, offset, packedByte & 0xFF, Byte.SIZE);

        return bitCount(packedByte);
    }

    /**
     * @return number of set bits (non-nulls)
     */
    public static int unpack(long[] values, int offset, long packedBits)
    {
        orPackedBits(values, 0, offset, packedBits, Long.SIZE);
        return Long.bitCount(packedBits);
    }

    public static int bitCount(byte value)
    {
        return Integer.bitCount(value & 0xFF);
    }
}
