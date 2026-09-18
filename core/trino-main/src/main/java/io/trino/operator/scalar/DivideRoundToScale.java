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
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Int128Math;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.Decimals.overflows;

/// Divides a decimal by an integer divisor and rounds the quotient (HALF_UP) to the dividend's scale.
/// This is unlike the built-in decimal `/` operator, which widens the result scale to at least 6.
///
/// This can be used when exact original scale is needed and `/ + CAST`'s double rounding is
/// unacceptable. In particular, this is designed so that:
/// ```
/// avg(x) = divide_round_to_scale(sum(x), count(x))
/// ```
public final class DivideRoundToScale
{
    public static final String NAME = "$divide_round_to_scale";

    private DivideRoundToScale() {}

    @ScalarFunction(value = NAME, hidden = true)
    @Description("Divides a decimal by an integer divisor, rounding HALF_UP to the dividend's scale")
    @LiteralParameters({"p", "s"})
    @SqlType("decimal(p, s)")
    public static Int128 divideRoundToScale(
            @SqlType("decimal(p, s)") Int128 dividend,
            @SqlType(StandardTypes.BIGINT) long divisor)
    {
        if (divisor < 0) {
            throw new TrinoException(NOT_SUPPORTED, "Negative divisor is not supported");
        }
        if (divisor == 0) {
            throw new TrinoException(DIVISION_BY_ZERO, "Division by zero");
        }
        Int128 result = Int128Math.divideRoundUp(dividend.getHigh(), dividend.getLow(), 0, 0, divisor, 0);
        if (overflows(result)) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow");
        }
        return result;
    }
}
