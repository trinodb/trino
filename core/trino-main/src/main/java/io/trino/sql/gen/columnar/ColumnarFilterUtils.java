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
package io.trino.sql.gen.columnar;

import static io.trino.spi.block.Bitmap.getBits;
import static io.trino.spi.block.Bitmap.isSet;
import static java.lang.Math.min;

final class ColumnarFilterUtils
{
    private ColumnarFilterUtils() {}

    static boolean isValid(long[] validity, int rawBitOffset, int position)
    {
        return isSet(validity, rawBitOffset, position);
    }

    static int filterSetBitsRange(long[] validity, int rawBitOffset, int offset, int size, int[] outputPositions)
    {
        int outputCount = 0;
        int end = offset + size;
        int position = offset;
        while (position < end) {
            int bitsInWord = min(Long.SIZE, end - position);
            long bits = getBits(validity, rawBitOffset, position, bitsInWord);
            while (bits != 0) {
                int bit = Long.numberOfTrailingZeros(bits);
                outputPositions[outputCount++] = position + bit;
                bits &= bits - 1;
            }
            position += bitsInWord;
        }
        return outputCount;
    }

    static int filterUnsetBitsRange(long[] validity, int rawBitOffset, int offset, int size, int[] outputPositions)
    {
        int outputCount = 0;
        int end = offset + size;
        int position = offset;
        while (position < end) {
            int bitsInWord = min(Long.SIZE, end - position);
            long bits = ~getBits(validity, rawBitOffset, position, bitsInWord);
            while (bits != 0) {
                int bit = Long.numberOfTrailingZeros(bits);
                if (bit >= bitsInWord) {
                    break;
                }
                outputPositions[outputCount++] = position + bit;
                bits &= bits - 1;
            }
            position += bitsInWord;
        }
        return outputCount;
    }
}
