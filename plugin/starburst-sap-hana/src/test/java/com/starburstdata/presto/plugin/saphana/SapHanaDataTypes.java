/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.saphana;

import io.prestosql.spi.type.Type;
import io.prestosql.testing.datatype.DataType;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static com.google.common.base.Verify.verify;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TimeType.createTimeType;
import static io.prestosql.spi.type.TimestampType.createTimestampType;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.testing.datatype.DataType.charDataType;
import static io.prestosql.testing.datatype.DataType.dataType;
import static io.prestosql.testing.datatype.DataType.doubleDataType;
import static io.prestosql.testing.datatype.DataType.realDataType;
import static io.prestosql.testing.datatype.DataType.stringDataType;
import static io.prestosql.testing.datatype.DataType.timeDataType;
import static io.prestosql.testing.datatype.DataType.timestampDataType;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.function.Function.identity;

public final class SapHanaDataTypes
{
    private SapHanaDataTypes() {}

    /**
     * The FLOAT(<n>) data type specifies a 32-bit or 64-bit real number, where <n> specifies the number of significant bits and can range between 1 and 53.
     * <p>
     * If you use the FLOAT(<n>) data type, and <n> is smaller than 25, then the 32-bit REAL data type is used instead. If <n> is greater than or equal to 25, or if <n> is not declared, then the 64-bit DOUBLE data type is used.
     */
    public static DataType<Float> sapHanaShortFloatDataType(int precision)
    {
        verify(precision < 25);
        return dataType(
                format("float(%s)", precision),
                REAL,
                realDataType()::toLiteral,
                identity());
    }

    /**
     * The FLOAT(<n>) data type specifies a 32-bit or 64-bit real number, where <n> specifies the number of significant bits and can range between 1 and 53.
     * <p>
     * If you use the FLOAT(<n>) data type, and <n> is smaller than 25, then the 32-bit REAL data type is used instead. If <n> is greater than or equal to 25, or if <n> is not declared, then the 64-bit DOUBLE data type is used.
     */
    public static DataType<Double> sapHanaLongFloatDataType(int precision)
    {
        verify(precision >= 25);
        return dataType(
                format("float(%s)", precision),
                DOUBLE,
                doubleDataType()::toLiteral,
                identity());
    }

    public static DataType<BigDecimal> sapHanaSmalldecimalDataType()
    {
        return dataType(
                "smalldecimal",
                DOUBLE,
                bigDecimal -> format("CAST('%s' AS SMALLDECIMAL)", bigDecimal),
                BigDecimal::doubleValue); // The precision [..] can vary within the range 1-16
    }

    public static DataType<BigDecimal> sapHanaDecimalDataType()
    {
        // If precision and scale are not specified, then DECIMAL becomes a floating-point decimal number.
        // In this case, precision and scale can vary within the range of 1 to 34 for precision and -6,111 to 6,176 for scale, depending on the stored value.

        return dataType(
                "decimal",
                DOUBLE,
                bigDecimal -> format("CAST('%s' AS DECIMAL)", bigDecimal),
                BigDecimal::doubleValue);
    }

    public static DataType<String> sapHanaNcharDataType(int length)
    {
        return charDataType(format("nchar(%s)", length), length);
    }

    public static DataType<String> prestoVarcharForSapHanaDataType(int length)
    {
        Type prestoTypeAfterRoundTrip = length <= 5000
                ? createVarcharType(length)
                : createUnboundedVarcharType();
        return stringDataType(format("varchar(%s)", length), prestoTypeAfterRoundTrip);
    }

    public static DataType<String> sapHanaNvarcharDataType(int length)
    {
        return stringDataType(format("nvarchar(%s)", length), createVarcharType(length));
    }

    public static DataType<String> sapHanaAlphanumDataType()
    {
        return stringDataType("alphanum", createVarcharType(1));
    }

    public static DataType<String> sapHanaAlphanumDataType(int length)
    {
        return dataType(
                format("alphanum(%s)", length),
                createVarcharType(length),
                DataType::formatStringLiteral,
                value -> {
                    // In the case of a purely numeric value, this means that the value can be considered as an alpha value with leading zeros.
                    if (value.matches("\\d+")) {
                        return "0".repeat(length - value.length()) + value;
                    }
                    return value;
                });
    }

    public static DataType<String> sapHanaShorttextDataType(int length)
    {
        return stringDataType(format("shorttext(%s)", length), createVarcharType(length));
    }

    public static DataType<String> sapHanaTextDataType()
    {
        return stringDataType("text", createUnboundedVarcharType());
    }

    public static DataType<String> sapHanaBintextDataType()
    {
        return stringDataType("bintext", createUnboundedVarcharType());
    }

    public static DataType<String> sapHanaClobDataType()
    {
        return stringDataType("clob", createUnboundedVarcharType());
    }

    public static DataType<String> sapHanaNclobDataType()
    {
        return stringDataType("nclob", createUnboundedVarcharType());
    }

    public static DataType<byte[]> sapHanaBlobDataType()
    {
        return dataType("blob", VARBINARY, DataType::binaryLiteral, identity());
    }

    public static DataType<byte[]> sapHanaVarbinaryDataType(int length)
    {
        return dataType(format("varbinary(%s)", length), VARBINARY, DataType::binaryLiteral, identity());
    }

    public static DataType<LocalDateTime> sapHanaSeconddateDataType()
    {
        return dataType(
                "seconddate",
                createTimestampType(0),
                timestampDataType(0)::toLiteral,
                identity());
    }

    public static DataType<LocalTime> sapHanaTimeDataType()
    {
        return dataType(
                "time",
                createTimeType(0),
                timeDataType(9)::toLiteral,
                localTime -> localTime.withNano(0));
    }

    public static DataType<LocalTime> prestoTimeForSapHanaDataType(int precision)
    {
        return dataType(
                format("time(%s)", precision),
                createTimeType(0),
                // TODO remove min here, and update TestSapHanaTypeMapping#testPrestoTime not to exceed supported precision, leaving high precision test cases to TestSapHanaTypeMapping#testTimeCoercion
                timeDataType(min(precision, 9))::toLiteral,
                localTime -> {
                    if (localTime.getNano() >= 500_000_000) {
                        return localTime.withNano(0).plusSeconds(1);
                    }
                    return localTime.withNano(0);
                });
    }

    public static DataType<LocalDateTime> sapHanaTimestampDataType()
    {
        return dataType(
                "timestamp",
                createTimestampType(7),
                timestampDataType(7)::toLiteral,
                identity());
    }
}
