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
package io.trino.hive.formats;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Searches a byte array eight bytes at a time.
 * <p>
 * {@link io.airlift.slice.Slice#indexOfByte} implements the same search, but always starts from the
 * beginning of the slice, so scanning successive fields of a line would require allocating a slice
 * view per field.
 */
public final class ByteSearch
{
    // little endian, so that the lowest matching byte is the one with the fewest trailing zeros
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, LITTLE_ENDIAN);

    private static final long LOW_BITS = 0x01_01_01_01_01_01_01_01L;
    private static final long HIGH_BITS = 0x80_80_80_80_80_80_80_80L;

    private ByteSearch() {}

    /**
     * Returns the index of the first byte equal to {@code value} in {@code [offset, end)}, or -1.
     */
    public static int indexOfByte(byte[] buffer, int offset, int end, byte value)
    {
        long pattern = repeat(value);
        while (offset + Long.BYTES <= end) {
            long matches = match((long) LONG_HANDLE.get(buffer, offset), pattern);
            if (matches != 0) {
                return offset + (Long.numberOfTrailingZeros(matches) >>> 3);
            }
            offset += Long.BYTES;
        }

        while (offset < end) {
            if (buffer[offset] == value) {
                return offset;
            }
            offset++;
        }
        return -1;
    }

    /**
     * Returns the index of the first byte equal to either {@code first} or {@code second} in
     * {@code [offset, end)}, or -1.
     */
    public static int indexOfByte(byte[] buffer, int offset, int end, byte first, byte second)
    {
        long firstPattern = repeat(first);
        long secondPattern = repeat(second);
        while (offset + Long.BYTES <= end) {
            long word = (long) LONG_HANDLE.get(buffer, offset);
            long matches = match(word, firstPattern) | match(word, secondPattern);
            if (matches != 0) {
                return offset + (Long.numberOfTrailingZeros(matches) >>> 3);
            }
            offset += Long.BYTES;
        }

        while (offset < end) {
            byte currentByte = buffer[offset];
            if (currentByte == first || currentByte == second) {
                return offset;
            }
            offset++;
        }
        return -1;
    }

    private static long repeat(byte value)
    {
        return (value & 0xFFL) * LOW_BITS;
    }

    /**
     * Sets the high bit of every byte in {@code word} that equals the byte broadcast in
     * {@code repeatedValue}, and clears it everywhere else (Hacker's Delight 6-1).
     */
    private static long match(long word, long repeatedValue)
    {
        long comparison = word ^ repeatedValue;
        return (comparison - LOW_BITS) & ~comparison & HIGH_BITS;
    }
}
