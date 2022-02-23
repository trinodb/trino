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
import io.trino.spi.TrinoException;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.StandardTypes;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.text.DecimalFormat;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static java.lang.Character.isDigit;
import static java.lang.String.format;

public final class DataSizeFunctions
{
    private final DecimalFormat format3Number;
    private final DecimalFormat format2Number;
    private final DecimalFormat format1Number;

    public DataSizeFunctions()
    {
        format3Number = new DecimalFormat("#.##");
        format3Number.setRoundingMode(RoundingMode.HALF_UP);
        format2Number = new DecimalFormat("#.#");
        format2Number.setRoundingMode(RoundingMode.HALF_UP);
        format1Number = new DecimalFormat("#");
        format1Number.setRoundingMode(RoundingMode.HALF_UP);
    }

    @Description("Converts data size string to bytes")
    @ScalarFunction(value = "parse_data_size", alias = "parse_presto_data_size")
    @LiteralParameters("x")
    @SqlType("decimal(38,0)")
    public static Int128 parsePrestoDataSize(@SqlType("varchar(x)") Slice input)
    {
        String dataSize = input.toStringUtf8();
        int valueLength = getValueLength(dataSize);
        BigDecimal value = parseValue(dataSize.substring(0, valueLength), dataSize);
        Unit unit = Unit.parse(dataSize.substring(valueLength), dataSize);
        BigInteger bytes = value.multiply(unit.getFactor()).toBigInteger();
        try {
            return Decimals.valueOf(bytes);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Value out of range: '%s' ('%sB')", dataSize, bytes));
        }
    }

    @Description("Converts decimal data size string to bytes")
    @ScalarFunction("parse_data_size_decimal")
    @LiteralParameters("x")
    @SqlType("decimal(38,0)")
    public static Int128 parseDataSizeDecimal(@SqlType("varchar(x)") Slice input)
    {
        String dataSize = input.toStringUtf8();
        int valueLength = getValueLength(dataSize);
        BigDecimal value = parseValue(dataSize.substring(0, valueLength), dataSize);
        Unit unit = Unit.parseDecimal(dataSize.substring(valueLength), dataSize);
        BigInteger bytes = value.multiply(unit.getFactor()).toBigInteger();
        try {
            return Decimals.valueOf(bytes);
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Value out of range: '%s' ('%sB')", dataSize, bytes));
        }
    }

    private static int getValueLength(String dataSize)
    {
        int valueLength = 0;
        for (int i = 0; i < dataSize.length(); i++) {
            char c = dataSize.charAt(i);
            if (isDigit(c) || c == '.') {
                valueLength++;
            }
            else {
                break;
            }
        }

        if (valueLength == 0) {
            throw invalidDataSize(dataSize);
        }
        return valueLength;
    }

    private static BigDecimal parseValue(String value, String dataSize)
    {
        try {
            return new BigDecimal(value);
        }
        catch (NumberFormatException e) {
            throw invalidDataSize(dataSize);
        }
    }

    private static TrinoException invalidDataSize(String dataSize)
    {
        return new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Invalid data size: '%s'", dataSize));
    }

    private enum Unit
    {
        BYTE(0, 1024),
        KILOBYTE(1, 1000),
        MEGABYTE(2, 1000),
        GIGABYTE(3, 1000),
        TERABYTE(4, 1000),
        PETABYTE(5, 1000),
        EXABYTE(6, 1000),
        ZETTABYTE(7, 1000),
        YOTTABYTE(8, 1000),

        KIBIBYTE(1, 1024),
        MEBIBYTE(2, 1024),
        GIBIBYTE(3, 1024),
        TEBIBYTE(4, 1024),
        PEBIBYTE(5, 1024),
        EXBIBYTE(6, 1024),
        ZEBIBYTE(7, 1024),
        YOBIBYTE(8, 1024);

        private final BigDecimal factor;

        Unit(int factor, int base)
        {
            if (base == 1024) {
                if (factor <= 6) {
                    this.factor = new BigDecimal(1L << factor * 10);
                }
                else {
                    this.factor = new BigDecimal(1L << 60)
                            .multiply(new BigDecimal(1L << (factor % 6) * 10));
                }
                return;
            }
            if (base == 1000) {
                if (factor <= 6) {
                    this.factor = new BigDecimal("1" + "0".repeat(factor * 3));
                }
                else {
                    this.factor = new BigDecimal("1" + "0".repeat(6 * 3))
                            .multiply(new BigDecimal("1" + "0".repeat((factor % 6) * 3)));
                }
                return;
            }
            throw new IllegalArgumentException("Base of Unit must be either 1024 or 1000");
        }

        public BigDecimal getFactor()
        {
            return factor;
        }

        public static Unit parse(String unitString, String dataSize)
        {
            switch (unitString) {
                case "B":
                    return BYTE;
                case "kB":
                    return KIBIBYTE;
                case "MB":
                    return MEBIBYTE;
                case "GB":
                    return GIBIBYTE;
                case "TB":
                    return TEBIBYTE;
                case "PB":
                    return PEBIBYTE;
                case "EB":
                    return EXBIBYTE;
                case "ZB":
                    return ZEBIBYTE;
                case "YB":
                    return YOBIBYTE;
                default:
                    throw invalidDataSize(dataSize);
            }
        }

        public static Unit parseDecimal(String unitString, String dataSize)
        {
            switch (unitString) {
                case "B":
                    return BYTE;
                case "kB":
                    return KILOBYTE;
                case "MB":
                    return MEGABYTE;
                case "GB":
                    return GIGABYTE;
                case "TB":
                    return TERABYTE;
                case "PB":
                    return PETABYTE;
                case "EB":
                    return EXABYTE;
                case "ZB":
                    return ZETTABYTE;
                case "YB":
                    return YOTTABYTE;
                default:
                    throw invalidDataSize(dataSize);
            }
        }
    }

    @Description("Formats data size as bytes using a unit symbol")
    @ScalarFunction("format_data_size")
    @SqlType(StandardTypes.VARCHAR)
    public Slice formatDataSize(@SqlType(StandardTypes.BIGINT) long value)
    {
        return utf8Slice(formatLong(value, 1024));
    }

    @Description("Formats data size as bytes using a unit symbol")
    @ScalarFunction("format_data_size")
    @SqlType(StandardTypes.VARCHAR)
    public Slice formatDataSize(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return utf8Slice(formatLong(value, 1024));
    }

    @Description("Formats data size as bytes using a unit symbol and a decimal base")
    @ScalarFunction("format_data_size_decimal")
    @SqlType(StandardTypes.VARCHAR)
    public Slice formatDataSizeDecimal(@SqlType(StandardTypes.BIGINT) long value)
    {
        return utf8Slice(formatLong(value, 1000));
    }

    @Description("Formats data size as bytes using a unit symbol and a decimal base")
    @ScalarFunction("format_data_size_decimal")
    @SqlType(StandardTypes.VARCHAR)
    public Slice formatDataSizeDecimal(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return utf8Slice(formatLong(value, 1000));
    }

    private String formatLong(double value, long base)
    {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return Double.toString(value);
        }
        String unit = "B";
        if (value >= base || value <= -base) {
            value /= base;
            unit = "kB";
        }
        if (value >= base || value <= -base) {
            value /= base;
            unit = "MB";
        }
        if (value >= base || value <= -base) {
            value /= base;
            unit = "GB";
        }
        if (value >= base || value <= -base) {
            value /= base;
            unit = "TB";
        }
        if (value >= base || value <= -base) {
            value /= base;
            unit = "PB";
        }
        if (value >= base || value <= -base) {
            value /= base;
            unit = "EB";
        }
        if (value >= base || value <= -base) {
            value /= base;
            unit = "ZB";
        }
        if (value >= base || value <= -base) {
            value /= base;
            unit = "YB";
        }

        return getFormat(value).format(value) + unit;
    }

    private DecimalFormat getFormat(double value)
    {
        if (Math.abs(value) < 10) {
            // show up to two decimals to get 3 significant digits
            return format3Number;
        }
        if (Math.abs(value) < 100) {
            // show up to one decimal to get 3 significant digits
            return format2Number;
        }
        // show no decimals -- we have enough digits in the integer part
        return format1Number;
    }
}
