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

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.math.RoundingMode;
import java.text.DecimalFormat;

import static io.airlift.slice.Slices.utf8Slice;

public final class FormatNumberFunction
{
    private final DecimalFormat format3Number;
    private final DecimalFormat format2Number;
    private final DecimalFormat format1Number;

    public FormatNumberFunction()
    {
        format3Number = new DecimalFormat("#.##");
        format3Number.setRoundingMode(RoundingMode.HALF_UP);
        format2Number = new DecimalFormat("#.#");
        format2Number.setRoundingMode(RoundingMode.HALF_UP);
        format1Number = new DecimalFormat("#");
        format1Number.setRoundingMode(RoundingMode.HALF_UP);
    }

    @ScalarFunction
    @Description("Formats large number using a unit symbol")
    @SqlType(StandardTypes.VARCHAR)
    public Slice formatNumber(@SqlType(StandardTypes.BIGINT) long value)
    {
        return utf8Slice(format(value));
    }

    @ScalarFunction
    @Description("Formats large number using a unit symbol")
    @SqlType(StandardTypes.VARCHAR)
    public Slice formatNumber(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return utf8Slice(format((long) value));
    }

    private String format(long count)
    {
        double fractional = count;
        String unit = "";
        if (fractional >= 1000 || fractional <= -1000) {
            fractional /= 1000;
            unit = "K";
        }
        if (fractional >= 1000 || fractional <= -1000) {
            fractional /= 1000;
            unit = "M";
        }
        if (fractional >= 1000 || fractional <= -1000) {
            fractional /= 1000;
            unit = "B";
        }
        if (fractional >= 1000 || fractional <= -1000) {
            fractional /= 1000;
            unit = "T";
        }
        if (fractional >= 1000 || fractional <= -1000) {
            fractional /= 1000;
            unit = "Q";
        }

        return getFormat(fractional).format(fractional) + unit;
    }

    private DecimalFormat getFormat(double value)
    {
        if (value < 10) {
            // show up to two decimals to get 3 significant digits
            return format3Number;
        }
        if (value < 100) {
            // show up to one decimal to get 3 significant digits
            return format2Number;
        }
        // show no decimals -- we have enough digits in the integer part
        return format1Number;
    }
}
