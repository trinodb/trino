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
package io.trino.type;

import io.trino.spi.TrinoException;

import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.Decimals.longTenToNth;
import static io.trino.type.IntervalDayTimeType.MAX_SHORT_FRACTIONAL_PRECISION;
import static java.lang.Math.addExact;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.subtractExact;

/// Checked arithmetic on interval microseconds, including fractional carries and rounding.
public final class IntervalArithmetic
{
    private IntervalArithmetic() {}

    public static long addMicros(long left, long right)
    {
        return addMicros(left, right, false);
    }

    /// Includes the fractional carry in the range check. With a carry, `left + right + 1` is
    /// `left - ~right`, so an overflowing intermediate sum cannot reject a representable result.
    public static long addMicros(long left, long right, boolean carry)
    {
        try {
            return carry ? subtractExact(left, ~right) : addExact(left, right);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "interval day to second addition overflow: %s + %s%s".formatted(left, right, carry ? " + 1" : ""), e);
        }
    }

    public static long subtractMicros(long left, long right)
    {
        return subtractMicros(left, right, false);
    }

    /// Includes the fractional borrow in the range check, using `left - right - 1 == left + ~right`.
    public static long subtractMicros(long left, long right, boolean borrow)
    {
        try {
            return borrow ? addExact(left, ~right) : subtractExact(left, right);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "interval day to second subtraction overflow: %s - %s%s".formatted(left, right, borrow ? " - 1" : ""), e);
        }
    }

    /// Rounds to 0–6 fractional-second digits, with ties toward positive infinity. Divide before
    /// rounding so the rounding offset cannot overflow; only the final rounded value must fit.
    public static long roundMicros(long micros, int fractionalPrecision)
    {
        long factor = longTenToNth(MAX_SHORT_FRACTIONAL_PRECISION - fractionalPrecision);
        long quotient = micros / factor;
        long remainder = micros % factor;
        if (factor > 1) {
            if (remainder >= factor / 2) {
                quotient++;
            }
            else if (remainder < -factor / 2) {
                quotient--;
            }
        }
        try {
            return multiplyExact(quotient, factor);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Value out of range for an interval after rounding", e);
        }
    }
}
