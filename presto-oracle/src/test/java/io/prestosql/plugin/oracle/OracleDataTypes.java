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
package io.prestosql.plugin.oracle;

import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.datatype.DataType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;

public final class OracleDataTypes
{
    private OracleDataTypes() {}

    /* Fixed-point numeric types */

    public static DataType<BigDecimal> unspecifiedNumberDataType(int expectedScaleInPresto)
    {
        return numberDataType(38, expectedScaleInPresto, "number");
    }

    public static DataType<BigDecimal> numberDataType(int precision)
    {
        return numberDataType(precision, 0, format("number(%d)", precision));
    }

    /**
     * Create a number type using the same transformation as
     * OracleClient.toPrestoType to handle negative scale.
     */
    public static DataType<BigDecimal> numberDataType(int precision, int scale)
    {
        return numberDataType(precision, scale, format("number(%d, %d)", precision, scale));
    }

    private static DataType<BigDecimal> numberDataType(int precision, int scale, String oracleInsertType)
    {
        int prestoPrecision = precision + max(-scale, 0);
        int prestoScale = max(scale, 0);
        return dataType(
                oracleInsertType,
                createDecimalType(prestoPrecision, prestoScale),
                BigDecimal::toString,
                // Round to Oracle's scale if necessary, then return to the scale Presto will use.
                i -> i.setScale(scale, RoundingMode.HALF_UP).setScale(prestoScale));
    }

    public static DataType<BigDecimal> oracleDecimalDataType(int precision, int scale)
    {
        String databaseType = format("decimal(%s, %s)", precision, scale);
        return dataType(
                databaseType,
                createDecimalType(precision, scale),
                bigDecimal -> format("CAST(TO_NUMBER('%s', '%s') AS %s)", bigDecimal.toPlainString(), toNumberFormatMask(bigDecimal), databaseType),
                bigDecimal -> bigDecimal.setScale(scale, UNNECESSARY));
    }

    private static String toNumberFormatMask(BigDecimal bigDecimal)
    {
        return bigDecimal.toPlainString()
                .replace("-", "")
                .replaceAll("[0-9]", "9");
    }

    public static DataType<Long> integerDataType(String name, int precision)
    {
        return dataType(name, createDecimalType(precision), Object::toString, BigDecimal::valueOf);
    }

    public static DataType<Boolean> booleanDataType()
    {
        return dataType("boolean", createDecimalType(1), Object::toString, value -> value ? BigDecimal.ONE : BigDecimal.ZERO);
    }

    /* Floating point numeric types */

    public static DataType<Double> binaryDoubleDataType()
    {
        return dataType("binary_double", DoubleType.DOUBLE,
                value -> {
                    if (Double.isFinite(value)) {
                        return value.toString();
                    }
                    if (Double.isNaN(value)) {
                        return "binary_double_nan";
                    }
                    return format("%sbinary_double_infinity", value > 0 ? "+" : "-");
                });
    }

    public static DataType<Float> binaryFloatDataType()
    {
        return dataType("binary_float", RealType.REAL,
                value -> {
                    if (Float.isFinite(value)) {
                        return value.toString();
                    }
                    if (Float.isNaN(value)) {
                        return "binary_float_nan";
                    }
                    return format("%sbinary_float_infinity", value > 0 ? "+" : "-");
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

    public static DataType<Double> oracleFloatDataType()
    {
        return oracleFloatDataType(Optional.empty());
    }

    public static DataType<Double> oracleFloatDataType(int precision)
    {
        return oracleFloatDataType(Optional.of(precision));
    }

    public static DataType<Double> oracleFloatDataType(Optional<Integer> precision)
    {
        String insertType = "float" + (precision.map(value -> format("(%s)", value)).orElse(""));

        return dataType(insertType, DoubleType.DOUBLE,
                value -> {
                    // we don't support infinity since is has no representation in Oracle's FLOAT type
                    checkArgument(Double.isFinite(value), "Invalid value: %s", value);
                    return value.toString();
                });
    }

    /* Datetime types */

    public static DataType<LocalDate> dateDataType()
    {
        return dataType("DATE", TimestampType.TIMESTAMP,
                DateTimeFormatter.ofPattern("'DATE '''yyyy-MM-dd''")::format,
                LocalDate::atStartOfDay);
    }

    public static DataType<ZonedDateTime> prestoTimestampWithTimeZoneDataType()
    {
        return dataType(
                "timestamp with time zone",
                TIMESTAMP_WITH_TIME_ZONE,
                DateTimeFormatter.ofPattern("'TIMESTAMP '''yyyy-MM-dd HH:mm:ss.SSS VV''")::format,
                OracleDataTypes::normalizeForOracleStorage);
    }

    @SuppressWarnings("MisusedWeekYear")
    public static DataType<ZonedDateTime> oracleTimestamp3TimeZoneDataType()
    {
        return dataType(
                "TIMESTAMP(3) WITH TIME ZONE",
                TIMESTAMP_WITH_TIME_ZONE,
                zonedDateTime -> {
                    String zoneId = zonedDateTime.getZone().getId();
                    if (zoneId.equals("Z")) {
                        zoneId = "UTC";
                    }
                    return format(
                            "from_tz(TIMESTAMP '%s', '%s')",
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS").format(zonedDateTime.toLocalDateTime()),
                            zoneId);
                },
                OracleDataTypes::normalizeForOracleStorage);
    }

    private static ZonedDateTime normalizeForOracleStorage(ZonedDateTime zonedDateTime)
    {
        String zoneId = zonedDateTime.getZone().getId();
        if (zoneId.equals("Z")) {
            // Oracle conflates UTC-equivalent zones to UTC.
            return zonedDateTime.withZoneSameInstant(ZoneId.of("UTC"));
        }
        return zonedDateTime;
    }

    /* Utility */

    private static <T> DataType<T> dataType(
            String insertType,
            Type prestoResultType,
            Function<T, String> toLiteral)
    {
        return dataType(insertType, prestoResultType, toLiteral, Function.identity());
    }

    private static <T> DataType<T> dataType(
            String insertType,
            Type prestoResultType,
            Function<T, String> toLiteral,
            Function<T, ?> toPrestoQueryResult)
    {
        return DataType.dataType(insertType, prestoResultType, toLiteral, toPrestoQueryResult);
    }
}
