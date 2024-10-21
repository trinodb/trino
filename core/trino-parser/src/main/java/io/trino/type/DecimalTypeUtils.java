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

import java.math.BigInteger;

public final class DecimalTypeUtils
{
    private DecimalTypeUtils()
    {
    }

    public static BigInteger normalizeDecimalDividePrecision(BigInteger precision, BigInteger scale)
    {
        return BigInteger.valueOf(normalizeDecimalDividePrecisionScale(precision.intValueExact(), scale.intValueExact()).precision());
    }

    public static BigInteger normalizeDecimalDivideScale(BigInteger precision, BigInteger scale)
    {
        return BigInteger.valueOf(normalizeDecimalDividePrecisionScale(precision.intValueExact(), scale.intValueExact()).scale());
    }

    /**
     * Accepts uncapped precision and scale for DECIMAL divide operation and returns precision and scale calculated according to MS SQL Server rules
     * described in https://learn.microsoft.com/en-us/sql/t-sql/data-types/precision-scale-and-length-transact-sql?view=sql-server-ver16#remarks.
     */
    private static PrecisionAndScale normalizeDecimalDividePrecisionScale(int precision, int scale)
    {
        int integral = precision - scale;
        if (integral <= 32) {
            // If integral part is <= 32, prioritize integral part over fractional
            int resultScale = Math.min(scale, 38 - integral);
            int resultPrecision = integral + resultScale;
            return new PrecisionAndScale(resultPrecision, resultScale);
        }
        else {
            // If integral part is > 32, prioritize fractional part over integral at the cost of possible overflow for the scale up to 6.
            int resultScale = Math.min(scale, 6);
            int resultPrecision = Math.min(38, integral + resultScale);
            return new PrecisionAndScale(resultPrecision, resultScale);
        }
    }

    private record PrecisionAndScale(long precision, long scale) {}
}
