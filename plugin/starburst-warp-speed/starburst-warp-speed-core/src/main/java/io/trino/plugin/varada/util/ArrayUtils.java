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
package io.trino.plugin.varada.util;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import java.nio.ByteBuffer;
import java.util.Collection;

public class ArrayUtils
{
    private static final Logger logger = Logger.get(ArrayUtils.class);
    private static final int[] EMPTY_INT_ARRAY = {};

    private ArrayUtils() {}

    public static int[] convertToIntArray(Collection<? extends Number> col)
    {
        final int size = col.size();
        if (size == 0) {
            return EMPTY_INT_ARRAY;
        }

        int[] result = new int[size];
        int i = 0;
        for (Number value : col) {
            result[i++] = value.intValue();
        }
        return result;
    }

    /**
     * Returns a copy of the input {@code array}, set to length {@code len}.
     * If {@code array} is shorter than {@code len}, the returned array will be padded with {@code padChar}
     *
     * @param array source array to copy
     * @param len length of returned copy
     * @param padChar pad character to use if source array is too short
     * @return copy of original array that is set to desired length
     */
    public static byte[] copyArray(byte[] array, int len, byte padChar)
    {
        final byte[] res = new byte[len];
        if (array.length > len) {
            System.arraycopy(array, 0, res, 0, len);
        }
        else {
            System.arraycopy(array, 0, res, 0, array.length);
            // pad until required length
            for (int i = array.length; i < res.length; ++i) {
                res[i] = padChar;
            }
        }

        return res;
    }

    /**
     * replaces consecutive occurrences of a value at the end of a byte array with another value
     *
     * @param value - array to replace in
     * @param valToReplace - value to replace
     * @param valToPut - value to put instead
     */
    public static boolean replaceSuffix(Slice value, byte valToReplace, byte valToPut)
    {
        if (value.length() == 0) {
            return false;
        }

        ByteBuffer byteBuffer = value.toByteBuffer();
        int startPos = byteBuffer.position();
        boolean shouldTrim = false;
        if (byteBuffer.get(value.length() - 1) == valToReplace) {
            logger.debug("going to trim slice=%s, byteBuffer=%s", value.toStringUtf8(), byteBuffer);
            shouldTrim = true;
        }
        for (int ix = value.length() - 1 + startPos; ix >= startPos && byteBuffer.get(ix) == valToReplace; --ix) {
            byteBuffer.put(ix, valToPut);
        }
        if (shouldTrim) {
            logger.debug("trim slice to slice=%s", value.toStringUtf8());
            return true;
        }
        return false;
    }

    public static void intFill(int[] array, byte value)
    {
        int len = array.length;

        if (len > 0) {
            array[0] = value;
        }

        //Value of i will be [1, 2, 4, 8, 16, 32, ..., len]
        for (int i = 1; i < len; i += i) {
            System.arraycopy(array, 0, array, i, Math.min((len - i), i));
        }
    }

    /*
     * initialize a smaller piece of the array and use the System.arraycopy
     * call to fill in the rest of the array in an expanding binary fashion
     * //https://stackoverflow.com/questions/9128737/fastest-way-to-set-all-values-of-an-array
     */
    public static void longfill(long[] array, byte value)
    {
        int len = array.length;

        if (len > 0) {
            array[0] = value;
        }

        //Value of i will be [1, 2, 4, 8, 16, 32, ..., len]
        for (int i = 1; i < len; i += i) {
            System.arraycopy(array, 0, array, i, Math.min((len - i), i));
        }
    }
}
