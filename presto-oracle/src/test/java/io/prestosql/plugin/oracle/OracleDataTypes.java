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
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.testing.datatype.DataType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.IntFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.BaseEncoding.base16;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.testing.datatype.DataType.stringDataType;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;

public final class OracleDataTypes
{
    private OracleDataTypes() {}

    // Oracle data type limits
    public static final int MAX_CHAR_ON_READ = 2000;
    public static final int MAX_CHAR_ON_WRITE = 500;

    public static final int MAX_VARCHAR2_ON_READ = 4000;
    public static final int MAX_VARCHAR2_ON_WRITE = 1000;

    public static final int MAX_NCHAR = 1000;
    public static final int MAX_NVARCHAR2 = 2000;

    public enum CharacterSemantics
    {
        BYTE,
        CHAR,
    }

    /* Binary types */

    public static DataType<byte[]> blobDataType()
    {
        return dataType("blob", VarbinaryType.VARBINARY,
                // hextoraw('') is NULL, but you can create an empty BLOB directly
                bytes -> bytes.length == 0
                        ? "empty_blob()"
                        : format("hextoraw('%s')", base16().encode(bytes)));
    }

    public static DataType<byte[]> rawDataType(int size)
    {
        return dataType(format("raw(%d)", size), VarbinaryType.VARBINARY,
                bytes -> format("hextoraw('%s')", base16().encode(bytes)));
    }

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
        return dataType("DATE", TimestampType.TIMESTAMP_MILLIS,
                DateTimeFormatter.ofPattern("'DATE '''yyyy-MM-dd''")::format,
                LocalDate::atStartOfDay);
    }

    public static DataType<ZonedDateTime> prestoTimestampWithTimeZoneDataType()
    {
        return dataType(
                "timestamp with time zone",
                TIMESTAMP_TZ_MILLIS,
                DateTimeFormatter.ofPattern("'TIMESTAMP '''yyyy-MM-dd HH:mm:ss.SSS VV''")::format,
                OracleDataTypes::normalizeForOracleStorage);
    }

    @SuppressWarnings("MisusedWeekYear")
    public static DataType<ZonedDateTime> oracleTimestamp3TimeZoneDataType()
    {
        return dataType(
                "TIMESTAMP(3) WITH TIME ZONE",
                TIMESTAMP_TZ_MILLIS,
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

    /* Character LOBs */

    public static DataType<String> clobDataType()
    {
        return dataType("clob", createUnboundedVarcharType(),
                s -> s.isEmpty() ? "empty_clob()" : format("'%s'", s.replace("'", "''")));
    }

    public static DataType<String> nclobDataType()
    {
        return dataType("nclob", createUnboundedVarcharType(),
                s -> s.isEmpty() ? "empty_clob()" : format("'%s'", s.replace("'", "''")));
    }

    public static DataType<String> tooLargeVarcharDataType()
    {
        return stringDataType(format("varchar(%d)", MAX_VARCHAR2_ON_WRITE + 1),
                createUnboundedVarcharType());
    }

    public static DataType<String> tooLargeCharDataType()
    {
        return stringDataType(format("char(%d)", MAX_CHAR_ON_WRITE + 1),
                createUnboundedVarcharType());
        // Creating an NCLOB column with a too-large CHAR does not include
        // trailing spaces, so these values do not need to be padded.
    }

    /* Character types */

    public static DataType<String> varchar2DataType(int length, CharacterSemantics semantics)
    {
        return stringDataType(format("varchar2(%d %s)", length, semantics), createVarcharType(length));
    }

    public static DataType<String> nvarchar2DataType(int length)
    {
        return stringDataType(format("nvarchar2(%d)", length), createVarcharType(length));
    }

    public static DataType<String> charDataType(int length, CharacterSemantics semantics)
    {
        return DataType.charDataType(format("char(%d %s)", length, semantics), length);
    }

    public static DataType<String> ncharDataType(int length)
    {
        return DataType.charDataType(format("nchar(%d)", length), length);
    }

    // Easier access to character types without specified length

    public static IntFunction<DataType<String>> varchar2DataType(CharacterSemantics semantics)
    {
        return length -> varchar2DataType(length, semantics);
    }

    public static IntFunction<DataType<String>> nvarchar2DataType()
    {
        return OracleDataTypes::nvarchar2DataType;
    }

    public static IntFunction<DataType<String>> charDataType(CharacterSemantics semantics)
    {
        return length -> charDataType(length, semantics);
    }

    public static IntFunction<DataType<String>> ncharDataType()
    {
        return OracleDataTypes::ncharDataType;
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
