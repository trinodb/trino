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
package io.trino.testing.datatype;

import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.function.Function;

import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;

public class DataType<T>
{
    private final String insertType;
    private final Type trinoResultType;
    private final Function<T, String> toLiteral;
    private final Function<T, String> toTrinoLiteral;
    private final Function<T, ?> toTrinoQueryResult;

    public static DataType<Boolean> booleanDataType()
    {
        return dataType("boolean", BooleanType.BOOLEAN);
    }

    public static DataType<Integer> integerDataType()
    {
        return dataType("integer", IntegerType.INTEGER);
    }

    public static DataType<Float> realDataType()
    {
        return dataType("real", RealType.REAL,
                value -> {
                    if (Float.isFinite(value)) {
                        return value.toString();
                    }
                    if (Float.isNaN(value)) {
                        return "nan()";
                    }
                    return format("%sinfinity()", value > 0 ? "+" : "-");
                });
    }

    public static DataType<Double> doubleDataType()
    {
        return dataType("double", DoubleType.DOUBLE,
                value -> {
                    if (Double.isFinite(value)) {
                        return value.toString();
                    }
                    if (Double.isNaN(value)) {
                        return "nan()";
                    }
                    return format("%sinfinity()", value > 0 ? "+" : "-");
                });
    }

    public static DataType<BigDecimal> decimalDataType(int precision, int scale)
    {
        String databaseType = format("decimal(%s, %s)", precision, scale);
        return dataType(
                databaseType,
                createDecimalType(precision, scale),
                bigDecimal -> format("CAST('%s' AS %s)", bigDecimal, databaseType),
                bigDecimal -> bigDecimal.setScale(scale, UNNECESSARY));
    }

    public static DataType<LocalDate> dateDataType()
    {
        return dataType(
                "date",
                DATE,
                DateTimeFormatter.ofPattern("'DATE '''uuuu-MM-dd''")::format);
    }

    public static DataType<LocalTime> timeDataType(int precision)
    {
        DateTimeFormatterBuilder format = new DateTimeFormatterBuilder()
                .appendPattern("'TIME '''")
                .appendPattern("HH:mm:ss");
        if (precision != 0) {
            format.appendFraction(NANO_OF_SECOND, precision, precision, true);
        }
        format.appendPattern("''");

        return dataType(
                format("time(%s)", precision),
                createTimeType(precision),
                format.toFormatter()::format);
    }

    /**
     * @deprecated Use {@link #timestampDataType(int)} instead.
     */
    @Deprecated
    public static DataType<LocalDateTime> timestampDataType()
    {
        return dataType(
                "timestamp",
                TIMESTAMP_MILLIS,
                DateTimeFormatter.ofPattern("'TIMESTAMP '''uuuu-MM-dd HH:mm:ss.SSS''")::format);
    }

    public static DataType<LocalDateTime> timestampDataType(int precision)
    {
        DateTimeFormatterBuilder format = new DateTimeFormatterBuilder()
                .appendPattern("'TIMESTAMP '''")
                .appendPattern("uuuu-MM-dd HH:mm:ss");
        if (precision != 0) {
            format.appendFraction(NANO_OF_SECOND, precision, precision, true);
        }
        format.appendPattern("''");

        return dataType(
                format("timestamp(%s)", precision),
                createTimestampType(precision),
                format.toFormatter()::format);
    }

    public static DataType<ZonedDateTime> timestampWithTimeZoneDataType(int precision)
    {
        DateTimeFormatterBuilder format = new DateTimeFormatterBuilder()
                .appendPattern("'TIMESTAMP '''")
                .appendPattern("uuuu-MM-dd HH:mm:ss");
        if (precision != 0) {
            format.appendFraction(NANO_OF_SECOND, precision, precision, true);
        }
        format
                .appendPattern(" VV")
                .appendPattern("''");

        return dataType(
                format("timestamp(%s) with time zone", precision),
                createTimestampWithTimeZoneType(precision),
                format.toFormatter()::format);
    }

    private static <T> DataType<T> dataType(String insertType, Type trinoResultType)
    {
        return new DataType<>(insertType, trinoResultType, Object::toString, Object::toString, Function.identity());
    }

    public static <T> DataType<T> dataType(String insertType, Type trinoResultType, Function<T, String> toLiteral)
    {
        return new DataType<>(insertType, trinoResultType, toLiteral, toLiteral, Function.identity());
    }

    /**
     * @deprecated {@code toTrinoQueryResult} concept is deprecated. Use {@link SqlDataTypeTest} instead.
     */
    @Deprecated
    public static <T> DataType<T> dataType(String insertType, Type trinoResultType, Function<T, String> toLiteral, Function<T, ?> toTrinoQueryResult)
    {
        return new DataType<>(insertType, trinoResultType, toLiteral, toLiteral, toTrinoQueryResult);
    }

    /**
     * @deprecated {@code toTrinoQueryResult} concept is deprecated. Use {@link SqlDataTypeTest} instead.
     */
    @Deprecated
    public static <T> DataType<T> dataType(String insertType, Type trinoResultType, Function<T, String> toLiteral, Function<T, String> toTrinoLiteral, Function<T, ?> toTrinoQueryResult)
    {
        return new DataType<>(insertType, trinoResultType, toLiteral, toTrinoLiteral, toTrinoQueryResult);
    }

    private DataType(String insertType, Type trinoResultType, Function<T, String> toLiteral, Function<T, String> toTrinoLiteral, Function<T, ?> toTrinoQueryResult)
    {
        this.insertType = insertType;
        this.trinoResultType = trinoResultType;
        this.toLiteral = toLiteral;
        this.toTrinoLiteral = toTrinoLiteral;
        this.toTrinoQueryResult = toTrinoQueryResult;
    }

    public String toLiteral(T inputValue)
    {
        if (inputValue == null) {
            return "NULL";
        }
        return toLiteral.apply(inputValue);
    }

    public String toTrinoLiteral(T inputValue)
    {
        if (inputValue == null) {
            return "NULL";
        }
        return toTrinoLiteral.apply(inputValue);
    }

    public Object toTrinoQueryResult(T inputValue)
    {
        if (inputValue == null) {
            return null;
        }
        return toTrinoQueryResult.apply(inputValue);
    }

    public String getInsertType()
    {
        return insertType;
    }

    public Type getTrinoResultType()
    {
        return trinoResultType;
    }
}
