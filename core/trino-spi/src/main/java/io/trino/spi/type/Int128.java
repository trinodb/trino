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
package io.trino.spi.type;

import org.openjdk.jol.info.ClassLayout;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.math.BigInteger;
import java.nio.ByteOrder;

public class Int128
        implements Comparable<Int128>
{
    private static final VarHandle BIG_ENDIAN_LONG_VIEW = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    public static final int SIZE = 2 * Long.BYTES;
    public static final int INSTANCE_SIZE = ClassLayout.parseClass(Int128.class).instanceSize();

    public static final Int128 MAX_VALUE = Int128.valueOf(0x7FFF_FFFF_FFFF_FFFFL, 0xFFFF_FFFF_FFFF_FFFFL);
    public static final Int128 MIN_VALUE = Int128.valueOf(0x8000_0000_0000_0000L, 0x0000_0000_0000_0000L);
    public static final Int128 ZERO = Int128.valueOf(0, 0);

    private final long high;
    private final long low;

    private Int128(long high, long low)
    {
        this.high = high;
        this.low = low;
    }

    /**
     * Decode an Int128 from the two's complement big-endian representation.
     *
     * @param bytes the two's complement big-endian encoding of the number. It must contain at least 1 byte.
     *              It may contain more than 16 bytes if the leading bytes are not significant (either zeros or -1)
     * @throws ArithmeticException if the bytes represent a number outside of the range [-2^127, 2^127 - 1]
     */
    public static Int128 fromBigEndian(byte[] bytes)
    {
        if (bytes.length >= 16) {
            int offset = bytes.length - Long.BYTES;
            long low = (long) BIG_ENDIAN_LONG_VIEW.get(bytes, offset);

            offset -= Long.BYTES;
            long high = (long) BIG_ENDIAN_LONG_VIEW.get(bytes, offset);

            for (int i = 0; i < offset; i++) {
                if (bytes[i] != (high >> 63)) {
                    throw new ArithmeticException("Overflow");
                }
            }

            return Int128.valueOf(high, low);
        }
        else if (bytes.length > 8) {
            // read the last 8 bytes into low
            int offset = bytes.length - Long.BYTES;
            long low = (long) BIG_ENDIAN_LONG_VIEW.get(bytes, offset);

            // At this point, we're guaranteed to have between 9 and 15 bytes available.
            // Read 8 bytes into high, starting at offset 0. There will be some over-read
            // of bytes belonging to low, so adjust by shifting them out
            long high = (long) BIG_ENDIAN_LONG_VIEW.get(bytes, 0);
            offset -= Long.BYTES;
            high >>= (-offset * Byte.SIZE);

            return Int128.valueOf(high, low);
        }
        else if (bytes.length == 8) {
            long low = (long) BIG_ENDIAN_LONG_VIEW.get(bytes, 0);
            long high = (low >> 63);

            return Int128.valueOf(high, low);
        }
        else {
            long high = (bytes[0] >> 7);
            long low = high;
            for (int i = 0; i < bytes.length; i++) {
                low = (low << 8) | (bytes[i] & 0xFF);
            }

            return Int128.valueOf(high, low);
        }
    }

    public static Int128 valueOf(long[] value)
    {
        if (value.length != 2) {
            throw new IllegalArgumentException("Expected long[2]");
        }

        long high = value[0];
        long low = value[1];
        return valueOf(high, low);
    }

    public static Int128 valueOf(long high, long low)
    {
        return new Int128(high, low);
    }

    public static Int128 valueOf(String value)
    {
        return Int128.valueOf(new BigInteger(value));
    }

    public static Int128 valueOf(BigInteger value)
    {
        long low = value.longValue();
        long high;
        try {
            high = value.shiftRight(64).longValueExact();
        }
        catch (ArithmeticException e) {
            throw new ArithmeticException("BigInteger out of Int128 range");
        }

        return new Int128(high, low);
    }

    public static Int128 valueOf(long value)
    {
        return new Int128(value >> 63, value);
    }

    public long getHigh()
    {
        return high;
    }

    public long getLow()
    {
        return low;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Int128 that = (Int128) o;
        return high == that.high && low == that.low;
    }

    @Override
    public int hashCode()
    {
        // FNV-1a style hash
        long hash = 0x9E3779B185EBCA87L;
        hash = (hash ^ high) * 0xC2B2AE3D27D4EB4FL;
        hash = (hash ^ low) * 0xC2B2AE3D27D4EB4FL;
        return Long.hashCode(hash);
    }

    @Override
    public int compareTo(Int128 other)
    {
        return compare(high, low, other.high, other.low);
    }

    @Override
    public String toString()
    {
        return toBigInteger().toString();
    }

    public BigInteger toBigInteger()
    {
        return new BigInteger(toBigEndianBytes());
    }

    public byte[] toBigEndianBytes()
    {
        byte[] bytes = new byte[16];
        toBigEndianBytes(bytes, 0);
        return bytes;
    }

    public void toBigEndianBytes(byte[] bytes, int offset)
    {
        BIG_ENDIAN_LONG_VIEW.set(bytes, offset, high);
        BIG_ENDIAN_LONG_VIEW.set(bytes, offset + Long.BYTES, low);
    }

    public long toLong()
    {
        return low;
    }

    public long toLongExact()
    {
        if (high != (low >> 63)) {
            throw new ArithmeticException("Overflow");
        }

        return low;
    }

    public long[] toLongArray()
    {
        return new long[] {high, low};
    }

    public static int compare(long leftHigh, long leftLow, long rightHigh, long rightLow)
    {
        int comparison = Long.compare(leftHigh, rightHigh);
        if (comparison == 0) {
            comparison = Long.compareUnsigned(leftLow, rightLow);
        }

        return comparison;
    }

    public boolean isZero()
    {
        return high == 0 && low == 0;
    }

    public boolean isNegative()
    {
        return high < 0;
    }
}
