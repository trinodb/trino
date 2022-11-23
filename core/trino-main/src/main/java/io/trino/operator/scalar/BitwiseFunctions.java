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
package io.trino.operator.scalar;

import io.trino.spi.TrinoException;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public final class BitwiseFunctions
{
    private static final long TINYINT_MASK = 0b1111_1111L;
    private static final long TINYINT_SIGNED_BIT = 0b1000_0000L;
    private static final long SMALLINT_MASK = 0b1111_1111_1111_1111L;
    private static final long SMALLINT_SIGNED_BIT = 0b1000_0000_0000_0000L;
    private static final long INTEGER_MASK = 0x00_00_00_00_ff_ff_ff_ffL;
    private static final long INTEGER_SIGNED_BIT = 0x00_00_00_00_00_80_00_00_00L;

    private BitwiseFunctions() {}

    @Description("Count number of set bits in 2's complement representation")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long bitCount(@SqlType(StandardTypes.BIGINT) long num, @SqlType(StandardTypes.BIGINT) long bits)
    {
        if (bits == 64) {
            return Long.bitCount(num);
        }
        if (bits <= 1 || bits > 64) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Bits specified in bit_count must be between 2 and 64, got " + bits);
        }
        long lowBitsMask = (1L << (bits - 1)) - 1; // set the least (bits - 1) bits
        if (num > lowBitsMask || num < ~lowBitsMask) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Number must be representable with the bits specified. " + num + " cannot be represented with " + bits + " bits");
        }
        long mask = (1L << bits) - 1;
        return Long.bitCount(num & mask);
    }

    @Description("Bitwise NOT in 2's complement arithmetic")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long bitwiseNot(@SqlType(StandardTypes.BIGINT) long num)
    {
        return ~num;
    }

    @Description("Bitwise AND in 2's complement arithmetic")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long bitwiseAnd(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        return left & right;
    }

    @Description("Bitwise OR in 2's complement arithmetic")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long bitwiseOr(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        return left | right;
    }

    @Description("Bitwise XOR in 2's complement arithmetic")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long bitwiseXor(@SqlType(StandardTypes.BIGINT) long left, @SqlType(StandardTypes.BIGINT) long right)
    {
        return left ^ right;
    }

    @Description("bitwise left shift")
    @ScalarFunction("bitwise_left_shift")
    @SqlType(StandardTypes.TINYINT)
    public static long bitwiseLeftShiftTinyint(@SqlType(StandardTypes.TINYINT) long value, @SqlType(StandardTypes.INTEGER) long shift)
    {
        if (shift >= 64) {
            return 0L;
        }
        long shifted = (value << shift);
        return preserveSign(shifted, TINYINT_MASK, TINYINT_SIGNED_BIT);
    }

    @Description("bitwise left shift")
    @ScalarFunction("bitwise_left_shift")
    @SqlType(StandardTypes.SMALLINT)
    public static long bitwiseLeftShiftSmallint(@SqlType(StandardTypes.SMALLINT) long value, @SqlType(StandardTypes.INTEGER) long shift)
    {
        if (shift >= 64) {
            return 0L;
        }
        long shifted = (value << shift);
        return preserveSign(shifted, SMALLINT_MASK, SMALLINT_SIGNED_BIT);
    }

    @Description("bitwise left shift")
    @ScalarFunction("bitwise_left_shift")
    @SqlType(StandardTypes.INTEGER)
    public static long bitwiseLeftShiftInteger(@SqlType(StandardTypes.INTEGER) long value, @SqlType(StandardTypes.INTEGER) long shift)
    {
        if (shift >= 64) {
            return 0L;
        }
        long shifted = (value << shift);
        return preserveSign(shifted, INTEGER_MASK, INTEGER_SIGNED_BIT);
    }

    @Description("bitwise left shift")
    @ScalarFunction("bitwise_left_shift")
    @SqlType(StandardTypes.BIGINT)
    public static long bitwiseLeftShiftBigint(@SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.INTEGER) long shift)
    {
        if (shift >= 64) {
            return 0L;
        }
        return value << shift;
    }

    private static long preserveSign(long shiftedValue, long mask, long signedBit)
    {
        if ((shiftedValue & signedBit) != 0) {
            // Preserve the sign in 2's complement format
            return shiftedValue | ~mask;
        }

        return shiftedValue & mask;
    }

    @Description("bitwise logical right shift")
    @ScalarFunction("bitwise_right_shift")
    @SqlType(StandardTypes.TINYINT)
    public static long bitwiseRightShiftTinyint(@SqlType(StandardTypes.TINYINT) long value, @SqlType(StandardTypes.INTEGER) long shift)
    {
        if (shift >= 64) {
            return 0L;
        }
        if (shift == 0) {
            return value;
        }
        return (value & TINYINT_MASK) >>> shift;
    }

    @Description("bitwise logical right shift")
    @ScalarFunction("bitwise_right_shift")
    @SqlType(StandardTypes.SMALLINT)
    public static long bitwiseRightShiftSmallint(@SqlType(StandardTypes.SMALLINT) long value, @SqlType(StandardTypes.INTEGER) long shift)
    {
        if (shift >= 64) {
            return 0L;
        }
        if (shift == 0) {
            return value;
        }
        return (value & SMALLINT_MASK) >>> shift;
    }

    @Description("bitwise logical right shift")
    @ScalarFunction("bitwise_right_shift")
    @SqlType(StandardTypes.INTEGER)
    public static long bitwiseRightShiftInteger(@SqlType(StandardTypes.INTEGER) long value, @SqlType(StandardTypes.INTEGER) long shift)
    {
        if (shift >= 64) {
            return 0L;
        }
        if (shift == 0) {
            return value;
        }
        return (value & INTEGER_MASK) >>> shift;
    }

    @Description("bitwise logical right shift")
    @ScalarFunction("bitwise_right_shift")
    @SqlType(StandardTypes.BIGINT)
    public static long bitwiseRightShiftBigint(@SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.INTEGER) long shift)
    {
        if (shift >= 64) {
            return 0L;
        }
        return value >>> shift;
    }

    @Description("bitwise arithmetic right shift")
    @ScalarFunction("bitwise_right_shift_arithmetic")
    @SqlType(StandardTypes.TINYINT)
    public static long bitwiseRightShiftArithmeticTinyint(@SqlType(StandardTypes.TINYINT) long value, @SqlType(StandardTypes.INTEGER) long shift)
    {
        if (shift >= 64) {
            if (value >= 0) {
                return 0L;
            }
            return -1L;
        }
        return preserveSign(value, TINYINT_MASK, TINYINT_SIGNED_BIT) >> shift;
    }

    @Description("bitwise arithmetic right shift")
    @ScalarFunction("bitwise_right_shift_arithmetic")
    @SqlType(StandardTypes.SMALLINT)
    public static long bitwiseRightShiftArithmeticSmallint(@SqlType(StandardTypes.SMALLINT) long value, @SqlType(StandardTypes.INTEGER) long shift)
    {
        if (shift >= 64) {
            if (value >= 0) {
                return 0L;
            }
            return -1L;
        }
        return preserveSign(value, SMALLINT_MASK, SMALLINT_SIGNED_BIT) >> shift;
    }

    @Description("bitwise arithmetic right shift")
    @ScalarFunction("bitwise_right_shift_arithmetic")
    @SqlType(StandardTypes.INTEGER)
    public static long bitwiseRightShiftArithmeticInteger(@SqlType(StandardTypes.INTEGER) long value, @SqlType(StandardTypes.INTEGER) long shift)
    {
        if (shift >= 64) {
            if (value >= 0) {
                return 0L;
            }
            return -1L;
        }
        return preserveSign(value, INTEGER_MASK, INTEGER_SIGNED_BIT) >> shift;
    }

    @Description("bitwise arithmetic right shift")
    @ScalarFunction("bitwise_right_shift_arithmetic")
    @SqlType(StandardTypes.BIGINT)
    public static long bitwiseRightShiftArithmeticBigint(@SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.INTEGER) long shift)
    {
        if (shift >= 64) {
            if (value >= 0) {
                return 0L;
            }
            return -1L;
        }
        return value >> shift;
    }
}
