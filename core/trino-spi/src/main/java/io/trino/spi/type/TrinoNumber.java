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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HexFormat;

import static io.trino.spi.type.DecimalType.checkArgument;
import static java.math.RoundingMode.HALF_UP;

/**
 * Stack representation for {@link NumberType}. See there for binary format specification.
 */
public class TrinoNumber
{
    static final int SCALE_BASE = 1 << 14;
    private static final int MIN_SCALE = -SCALE_BASE;
    private static final int MAX_SCALE = (SCALE_BASE - 1);

    private static short header(boolean negated, int scale)
    {
        checkArgument(MIN_SCALE <= scale && scale <= MAX_SCALE, "Scale out of range: %s", scale);
        short header = (short) ((scale + SCALE_BASE) & 0x7fff);
        if (negated) {
            header = (short) (header | 0x8000);
        }
        return header;
    }

    private static short scaleFromHeader(short header)
    {
        return (short) ((header & 0x7fff) - SCALE_BASE);
    }

    private static boolean negatedFromHeader(short header)
    {
        return (header & 0x8000) != 0;
    }

    private final Slice bytes;

    public TrinoNumber(Slice bytes)
    {
        switch (bytes.length()) {
            case 0, 1 -> throw new IllegalArgumentException("Invalid bytes length for TrinoNumber: " + bytes.length());
            case 2 -> {
                // zero
                // TODO support ±infinity and NaN
                short header = bytes.getShort(0);
                checkArgument(!negatedFromHeader(header) && scaleFromHeader(header) == 0, "Invalid header for TrinoNumber with unscaled zero value: %s", header);
            }
            default -> {
                if (bytes.getByte(2) == 0) {
                    throw new IllegalArgumentException("TrinoNumber has leading zero byte in unscaled value: " +
                            HexFormat.of().withDelimiter(" ").formatHex(bytes.byteArray(), bytes.byteArrayOffset(), bytes.length()));
                }
            }
        }
        this.bytes = bytes;
    }

    public BigDecimal toBigDecimal()
    {
        // little-endian
        short header = bytes.getShort(0);
        int unscaledBytesLength = bytes.length() - 2;

        boolean negated = negatedFromHeader(header);
        int scale = scaleFromHeader(header);
        int signum = unscaledBytesLength == 0 ? 0 : (negated ? -1 : 1);
        // big-endian
        BigInteger unscaled = new BigInteger(signum, bytes.byteArray(), bytes.byteArrayOffset() + 2, unscaledBytesLength);
        return new BigDecimal(unscaled, scale);
    }

    public static TrinoNumber from(BigDecimal bigDecimal)
    {
        return from(bigDecimal, NumberType.MAX_DECIMAL_PRECISION);
    }

    // visible for testing
    static TrinoNumber from(BigDecimal bigDecimal, int maxDecimalPrecision)
    {
        BigDecimal normalized = bigDecimal.stripTrailingZeros();
        if (normalized.precision() > maxDecimalPrecision) {
            normalized = normalized.setScale(
                    normalized.scale() - (normalized.precision() - maxDecimalPrecision),
                    HALF_UP);
        }
        BigInteger unscaledValue = normalized.unscaledValue();
        Slice slice;
        boolean negated = unscaledValue.signum() < 0;
        if (negated) {
            unscaledValue = unscaledValue.negate();
        }
        byte[] unscaledBytes = unscaledValue.toByteArray();
        int unscaledBytesOffset = 0;
        int unscaledBytesLength = unscaledBytes.length;
        if (unscaledBytes[0] == 0) {
            // BigInteger.toByteArray outputs 2-complement big-endian bytes, so even though the value is known to be positive,
            // there still needs to be one bit reserved for the sign, which sometimes results in a leading zero byte.
            unscaledBytesOffset++;
            unscaledBytesLength--;
        }
        slice = Slices.allocate(2 + unscaledBytesLength);
        slice.setShort(0, header(negated, normalized.scale()));
        slice.setBytes(2, unscaledBytes, unscaledBytesOffset, unscaledBytesLength);
        return new TrinoNumber(slice);
    }

    Slice bytes()
    {
        return bytes;
    }
}
