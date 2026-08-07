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
package io.trino.plugin.base.util;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

/**
 * Renders integers as their ASCII bytes.
 * <p>
 * {@link Long#toString} produces a {@link String}, so rendering a value into a slice through it
 * writes the digits into a {@code String} only to encode them back out to UTF-8 immediately.
 */
public final class NumberFormatter
{
    private static final long[] POWERS_OF_TEN = {
            1L, 10L, 100L, 1000L, 10_000L, 100_000L, 1_000_000L, 10_000_000L, 100_000_000L,
            1_000_000_000L, 10_000_000_000L, 100_000_000_000L, 1_000_000_000_000L,
            10_000_000_000_000L, 100_000_000_000_000L, 1_000_000_000_000_000L,
            10_000_000_000_000_000L, 100_000_000_000_000_000L, 1_000_000_000_000_000_000L,
    };

    private NumberFormatter() {}

    /**
     * Returns the number of ASCII bytes {@link #formatLong} writes, which is also the number of
     * characters {@code Long.toString(value)} would return, as the rendering is all ASCII.
     */
    public static int decimalLength(long value)
    {
        if (value == Long.MIN_VALUE) {
            // its magnitude has no positive counterpart
            return 20;
        }

        int sign = 0;
        if (value < 0) {
            sign = 1;
            value = -value;
        }

        for (int digits = 1; digits < POWERS_OF_TEN.length; digits++) {
            if (value < POWERS_OF_TEN[digits]) {
                return sign + digits;
            }
        }
        return sign + 19;
    }

    public static Slice formatLong(long value)
    {
        byte[] bytes = new byte[decimalLength(value)];
        int index = bytes.length;

        // accumulate on the negative side, which is the only one that can hold Long.MIN_VALUE
        boolean negative = value < 0;
        long magnitude = negative ? value : -value;
        do {
            bytes[--index] = (byte) ('0' - (magnitude % 10));
            magnitude /= 10;
        }
        while (magnitude != 0);

        if (negative) {
            bytes[--index] = '-';
        }
        return Slices.wrappedBuffer(bytes);
    }
}
