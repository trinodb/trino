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
package io.trino.plugin.varada.type.cast;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.type.DecimalConversions;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;

import java.text.DecimalFormat;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.varada.type.cast.DateTimeUtils.printDate;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.US_ASCII;

public final class ToVarchar
{
    private static final ThreadLocal<DecimalFormat> DOUBLE_FORMAT = ThreadLocal.withInitial(() -> new DecimalFormat("0.0###################E0"));
    private static final ThreadLocal<DecimalFormat> REAL_FORMAT = ThreadLocal.withInitial(() -> new DecimalFormat("0.0#####E0"));
    private static final Slice TRUE = Slices.copiedBuffer("true", US_ASCII);
    private static final Slice FALSE = Slices.copiedBuffer("false", US_ASCII);

    private ToVarchar() {}

    public static Slice fromInteger(long x, long value)
    {
        // todo optimize me
        String stringValue = String.valueOf(value);
        // String is all-ASCII, so String.length() here returns actual code points count
        if (stringValue.length() <= x) {
            return utf8Slice(stringValue);
        }

        throw new TrinoException(INVALID_CAST_ARGUMENT, format("Value %s cannot be represented as varchar(%s)", value, x));
    }

    public static Slice fromBigint(long x, long value)
    {
        // todo optimize me
        String stringValue = String.valueOf(value);
        // String is all-ASCII, so String.length() here returns actual code points count
        if (stringValue.length() <= x) {
            return utf8Slice(stringValue);
        }

        throw new TrinoException(INVALID_CAST_ARGUMENT, format("Value %s cannot be represented as varchar(%s)", value, x));
    }

    public static Slice fromReal(long x, long value)
    {
        float floatValue = intBitsToFloat((int) value);
        String stringValue;

        // handle positive and negative 0
        if (floatValue == 0.0f) {
            if (1.0f / floatValue > 0) {
                stringValue = "0E0";
            }
            else {
                stringValue = "-0E0";
            }
        }
        else if (Float.isInfinite(floatValue)) {
            if (floatValue > 0) {
                stringValue = "Infinity";
            }
            else {
                stringValue = "-Infinity";
            }
        }
        else {
            stringValue = REAL_FORMAT.get().format(Double.parseDouble(Float.toString(floatValue)));
        }

        // String is all-ASCII, so String.length() here returns actual code points count
        if (stringValue.length() <= x) {
            return utf8Slice(stringValue);
        }

        throw new TrinoException(INVALID_CAST_ARGUMENT, format("Value %s (%s) cannot be represented as varchar(%s)", floatValue, stringValue, x));
    }

    public static Slice fromDouble(long x, double value)
    {
        String stringValue;

        // handle positive and negative 0
        if (value == 0e0) {
            if (1e0 / value > 0) {
                stringValue = "0E0";
            }
            else {
                stringValue = "-0E0";
            }
        }
        else if (Double.isInfinite(value)) {
            if (value > 0) {
                stringValue = "Infinity";
            }
            else {
                stringValue = "-Infinity";
            }
        }
        else {
            stringValue = DOUBLE_FORMAT.get().format(value);
        }

        // String is all-ASCII, so String.length() here returns actual code points count
        if (stringValue.length() <= x) {
            return utf8Slice(stringValue);
        }

        throw new TrinoException(INVALID_CAST_ARGUMENT, format("Value %s (%s) cannot be represented as varchar(%s)", value, stringValue, x));
    }

    public static Slice fromBoolean(boolean value)
    {
        return value ? TRUE : FALSE;
    }

    public static Slice fromShortDecimal(long decimal, long scale, long varcharLength)
    {
        String stringValue = Decimals.toString(decimal, DecimalConversions.intScale(scale));
        // String is all-ASCII, so String.length() here returns actual code points count
        if (stringValue.length() <= varcharLength) {
            return utf8Slice(stringValue);
        }

        throw new TrinoException(INVALID_CAST_ARGUMENT, format("Value %s cannot be represented as varchar(%s)", stringValue, varcharLength));
    }

    public static Slice fromLongDecimal(Int128 decimal, long scale, long varcharLength)
    {
        String stringValue = Decimals.toString(decimal, DecimalConversions.intScale(scale));
        // String is all-ASCII, so String.length() here returns actual code points count
        if (stringValue.length() <= varcharLength) {
            return utf8Slice(stringValue);
        }

        throw new TrinoException(INVALID_CAST_ARGUMENT, format("Value %s cannot be represented as varchar(%s)", stringValue, varcharLength));
    }

    public static Slice fromDate(long x, long value)
    {
        String stringValue = printDate(toIntExact(value));
        // String is all-ASCII, so String.length() here returns actual code points count
        if (stringValue.length() <= x) {
            return utf8Slice(stringValue);
        }

        throw new TrinoException(INVALID_CAST_ARGUMENT, format("Value %s cannot be represented as varchar(%s)", stringValue, x));
    }

    public static Slice fromTinyint(long x, long value)
    {
        // todo optimize me
        String stringValue = String.valueOf(value);
        // String is all-ASCII, so String.length() here returns actual code points count
        if (stringValue.length() <= x) {
            return utf8Slice(stringValue);
        }

        throw new TrinoException(INVALID_CAST_ARGUMENT, format("Value %s cannot be represented as varchar(%s)", value, x));
    }

    public static Slice fromSmallint(long x, long value)
    {
        // todo optimize me
        String stringValue = String.valueOf(value);
        // String is all-ASCII, so String.length() here returns actual code points count
        if (stringValue.length() <= x) {
            return utf8Slice(stringValue);
        }

        throw new TrinoException(INVALID_CAST_ARGUMENT, format("Value %s cannot be represented as varchar(%s)", value, x));
    }
}
