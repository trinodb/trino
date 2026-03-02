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

import com.google.errorprone.annotations.CheckReturnValue;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.type.Verify.VerifyException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Comparator;
import java.util.HexFormat;

import static io.trino.spi.type.DecimalType.checkArgument;
import static io.trino.spi.type.Verify.verify;
import static java.math.RoundingMode.HALF_UP;
import static java.util.Objects.requireNonNull;

/**
 * Stack representation for {@link NumberType}. See there for binary format specification.
 */
public class TrinoNumber
{
    public static final int HEADER_LENGTH = 2;
    static final int SCALE_BASE = 1 << 14;
    // visible for testing
    static final int MIN_SCALE = -SCALE_BASE;
    // visible for testing
    static final int MAX_SCALE = (SCALE_BASE - 1);

    public static final int MARKER_SCALE_NAN = 1;
    public static final int MARKER_SCALE_INFINITY = 2;

    private static final byte[] EMPTY_BYTES = new byte[0];

    // visible for testing
    static short header(boolean negated, int scale)
    {
        checkArgument(MIN_SCALE <= scale && scale <= MAX_SCALE, "Scale out of range: %s", scale);
        short header = (short) ((scale + SCALE_BASE) & 0x7fff);
        if (negated) {
            header = (short) (header | 0x8000);
        }
        return header;
    }

    // visible for testing
    static short scaleFromHeader(short header)
    {
        return (short) ((header & 0x7fff) - SCALE_BASE);
    }

    // visible for testing
    static boolean negatedFromHeader(short header)
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
                short header = bytes.getShort(0);
                short scale = scaleFromHeader(header);
                switch (scale) {
                    case 0 -> checkArgument(!negatedFromHeader(header), "Invalid negated header for TrinoNumber with unscaled 0 value and scale 0: %s", header);
                    case MARKER_SCALE_NAN -> checkArgument(!negatedFromHeader(header), "Invalid negated header for TrinoNumber with unscaled 0 value and NaN marker: %s", header);
                    case MARKER_SCALE_INFINITY -> {
                        // Ok
                    }
                    default -> throw new IllegalArgumentException("Invalid header with scale %s for TrinoNumber with unscaled 0 value: %s".formatted(scale, header));
                }
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

    boolean isNaN()
    {
        return bytes().length() == HEADER_LENGTH &&
                scaleFromHeader(bytes.getShort(0)) == MARKER_SCALE_NAN;
    }

    Slice bytes()
    {
        return bytes;
    }

    @CheckReturnValue
    public AsBigDecimal toBigDecimal()
    {
        // little-endian
        short header = bytes.getShort(0);
        int unscaledBytesLength = bytes.length() - HEADER_LENGTH;

        boolean negated = negatedFromHeader(header);
        int scale = scaleFromHeader(header);
        if (unscaledBytesLength == 0) {
            return switch (scale) {
                case 0 -> {
                    verify(!negated, "Invalid negated header with zero byte unscaled value: header=%s, scale=%s", header, scale);
                    yield new BigDecimalValue(BigDecimal.ZERO);
                }
                case MARKER_SCALE_NAN -> {
                    verify(!negated, "Invalid negated header with zero byte unscaled value: header=%s, scale=%s", header, scale);
                    yield new NotANumber();
                }
                case MARKER_SCALE_INFINITY -> new Infinity(negated);
                default -> throw new VerifyException("Invalid negated header with zero byte unscaled value: header=%s, scale=%s".formatted(header, scale));
            };
        }
        int signum = negated ? -1 : 1;
        // big-endian
        BigInteger unscaled = new BigInteger(signum, bytes.byteArray(), bytes.byteArrayOffset() + HEADER_LENGTH, unscaledBytesLength);
        return new BigDecimalValue(new BigDecimal(unscaled, scale));
    }

    @CheckReturnValue
    public static TrinoNumber from(BigDecimal bigDecimal)
    {
        return from(new BigDecimalValue(bigDecimal));
    }

    @CheckReturnValue
    public static TrinoNumber from(AsBigDecimal bigDecimal)
    {
        return from(bigDecimal, NumberType.MAX_DECIMAL_PRECISION);
    }

    // visible for testing
    @CheckReturnValue
    static TrinoNumber from(AsBigDecimal asBigDecimal, int maxDecimalPrecision)
    {
        return switch (asBigDecimal) {
            case BigDecimalValue(BigDecimal bigDecimal) -> {
                BigDecimal normalized = bigDecimal.stripTrailingZeros();
                if (normalized.precision() > maxDecimalPrecision) {
                    normalized = normalized.setScale(
                            normalized.scale() - (normalized.precision() - maxDecimalPrecision),
                            HALF_UP);
                }
                BigInteger unscaledValue = normalized.unscaledValue();
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
                yield numberFromParts(header(negated, normalized.scale()), unscaledBytes, unscaledBytesOffset, unscaledBytesLength);
            }
            case NotANumber _ -> numberFromParts(header(false, MARKER_SCALE_NAN), EMPTY_BYTES, 0, 0);
            case Infinity(boolean negative) -> numberFromParts(header(negative, MARKER_SCALE_INFINITY), EMPTY_BYTES, 0, 0);
        };
    }

    @CheckReturnValue
    private static TrinoNumber numberFromParts(short header, byte[] unscaledBytes, int unscaledBytesOffset, int unscaledBytesLength)
    {
        Slice slice = Slices.allocate(2 + unscaledBytesLength);
        // little-endian
        slice.setShort(0, header);
        slice.setBytes(2, unscaledBytes, unscaledBytesOffset, unscaledBytesLength);
        return new TrinoNumber(slice);
    }

    public sealed interface AsBigDecimal
            permits NotANumber, Infinity, BigDecimalValue
    {
        /**
         * Comparator for AsBigDecimal values that treats NaN as greater than any other value, including positive infinity.
         * <p>
         * The comparison is in the following order:
         * <ol>
         *     <li>negative infinity</li>
         *     <li>finite numbers, ordered consistently with {@link BigDecimal} order</li>
         *     <li>positive infinity</li>
         *     <li>NaN</li>
         * </ol>
         */
        Comparator<AsBigDecimal> COMPARE_NAN_LAST = (left, right) -> {
            switch (left) {
                case NotANumber() -> {
                    // NaN goes last
                    return right instanceof NotANumber ? 0 : 1;
                }
                case Infinity(boolean negative) -> {
                    if (right instanceof NotANumber) {
                        // NaN goes last
                        return -1;
                    }
                    if (negative) {
                        return right.equals(left) ? 0 : -1;
                    }
                    return right.equals(left) ? 0 : 1;
                }
                case BigDecimalValue(BigDecimal value) -> {
                    switch (right) {
                        case NotANumber() -> {
                            // NaN goes last
                            return -1;
                        }
                        case Infinity(boolean otherNegative) -> {
                            return otherNegative ? 1 : -1;
                        }
                        case BigDecimalValue(BigDecimal otherValue) -> {
                            return value.compareTo(otherValue);
                        }
                    }
                }
            }
        };
    }

    public record Infinity(boolean negative)
            implements AsBigDecimal
    {
        @Override
        public String toString()
        {
            return negative ? "-Infinity" : "+Infinity";
        }
    }

    public record NotANumber()
            implements AsBigDecimal
    {
        @Override
        public String toString()
        {
            return "NaN";
        }
    }

    public record BigDecimalValue(BigDecimal value)
            implements AsBigDecimal
    {
        public BigDecimalValue
        {
            requireNonNull(value, "value is null");
        }

        @Override
        public String toString()
        {
            return value.toString();
        }
    }
}
