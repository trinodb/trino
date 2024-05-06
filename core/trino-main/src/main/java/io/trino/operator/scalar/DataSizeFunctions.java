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

import com.google.common.collect.ImmutableList;
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
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static java.lang.Character.isDigit;
import static java.lang.String.format;

public final class DataSizeFunctions
{
    private final static List<String> dataUnits = ImmutableList.of("B", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB");
    private final static DecimalFormat format3Number = new DecimalFormat("#.##");
    private final static DecimalFormat format2Number = new DecimalFormat("#.#");
    private final static DecimalFormat format1Number = new DecimalFormat("#");

    private DataSizeFunctions() {
        format3Number.setRoundingMode(RoundingMode.HALF_UP);
        format2Number.setRoundingMode(RoundingMode.HALF_UP);
        format1Number.setRoundingMode(RoundingMode.HALF_UP);
    }

    @Description("Converts data size string to bytes")
    @ScalarFunction(value = "parse_data_size", alias = "parse_presto_data_size")
    @LiteralParameters("x")
    @SqlType("decimal(38,0)")
    public static Int128 parseDataSize(@SqlType("varchar(x)") Slice input)
    {
        String dataSize = input.toStringUtf8();

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

        BigDecimal value = parseValue(dataSize.substring(0, valueLength), dataSize);
        Unit unit = Unit.parse(dataSize.substring(valueLength), dataSize);
        BigInteger bytes = value.multiply(unit.getFactor()).toBigInteger();
        try {
            return Decimals.valueOf(bytes);
        }
        catch (TrinoException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Value out of range: '%s' ('%sB')", dataSize, bytes));
        }
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
        BYTE(BigDecimal.ONE),
        KILOBYTE(new BigDecimal(1L << 10)),
        MEGABYTE(new BigDecimal(1L << 20)),
        GIGABYTE(new BigDecimal(1L << 30)),
        TERABYTE(new BigDecimal(1L << 40)),
        PETABYTE(new BigDecimal(1L << 50)),
        EXABYTE(new BigDecimal(1L << 60)),
        ZETTABYTE(new BigDecimal(1L << 60).multiply(new BigDecimal(1L << 10))),
        YOTTABYTE(new BigDecimal(1L << 60).multiply(new BigDecimal(1L << 20)));

        private final BigDecimal factor;

        Unit(BigDecimal factor)
        {
            this.factor = factor;
        }

        public BigDecimal getFactor()
        {
            return factor;
        }

        public static Unit parse(String unitString, String dataSize)
        {
            return switch (unitString) {
                case "B" -> BYTE;
                case "kB" -> KILOBYTE;
                case "MB" -> MEGABYTE;
                case "GB" -> GIGABYTE;
                case "TB" -> TERABYTE;
                case "PB" -> PETABYTE;
                case "EB" -> EXABYTE;
                case "ZB" -> ZETTABYTE;
                case "YB" -> YOTTABYTE;
                default -> throw invalidDataSize(dataSize);
            };
        }
    }

    @Description("Formats bytes to data size string")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice formatDataBytes(@SqlType("decimal(38,0)") Int128 bytes)
    {
        if (bytes.isNegative()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Invalid data bytes number: %s", bytes));
        }
        double fractional = bytes.toBigInteger().doubleValue();
        for (String unit : dataUnits) {
           if (fractional < 1024) {
               return utf8Slice(getFormat(fractional).format(fractional) + unit);
           }
           fractional /= 1024;
        }
        return utf8Slice(getFormat(fractional).format(fractional) + dataUnits.getLast());
    }

    private static DecimalFormat getFormat(double value)
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
