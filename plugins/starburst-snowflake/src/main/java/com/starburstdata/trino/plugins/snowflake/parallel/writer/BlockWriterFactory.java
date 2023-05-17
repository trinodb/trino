/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.parallel.writer;

import io.airlift.slice.Slice;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import net.snowflake.client.core.arrow.ArrowVectorConverter;

import java.sql.Types;

import static com.google.common.base.Preconditions.checkArgument;
import static com.starburstdata.trino.plugins.snowflake.jdbc.SnowflakeClient.SNOWFLAKE_MAX_TIMESTAMP_PRECISION;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Math.max;
import static java.lang.String.format;

public final class BlockWriterFactory
{
    private static final String MAX_TIMESTAMP_PRECISION_ERROR_MESSAGE = "The max timestamp precision in Snowflake is 9";

    private BlockWriterFactory()
    {
        // static utility class
    }

    public static BlockWriter createWriter(JdbcColumnHandle columnHandle, ArrowVectorConverter converter, int rowCount)
    {
        JdbcTypeHandle typeHandle = columnHandle.getJdbcTypeHandle();
        Type type = columnHandle.getColumnType();
        String typeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        if (type == BOOLEAN && typeHandle.getJdbcType() == Types.BOOLEAN) {
            return new BooleanValueWriter(converter, rowCount, type);
        }
        else if (type == TINYINT && typeHandle.getJdbcType() == Types.TINYINT) {
            return new LongValueWriter(converter, rowCount, type);
        }
        else if (type == SMALLINT && typeHandle.getJdbcType() == Types.SMALLINT) {
            return new SmallIntValueWriter(converter, rowCount, type);
        }
        else if (type == INTEGER && typeHandle.getJdbcType() == Types.INTEGER) {
            return new IntegerValueWriter(converter, rowCount, type);
        }
        else if (type == BIGINT && typeHandle.getJdbcType() == Types.BIGINT) {
            return new LongValueWriter(converter, rowCount, type);
        }
        else if (type == REAL && typeHandle.getJdbcType() == Types.REAL) {
            return new RealValueWriter(converter, rowCount, type);
        }
        else if (type == DoubleType.DOUBLE && (typeHandle.getJdbcType() == Types.DOUBLE || typeHandle.getJdbcType() == Types.FLOAT)) {
            return new DoubleValueWriter(converter, rowCount, type);
        }
        else if (typeName.equals("NUMBER")) {
            int decimalDigits = typeHandle.getRequiredDecimalDigits();
            int precision = typeHandle.getRequiredColumnSize() + max(-decimalDigits, 0);
            DecimalType decimalType = createDecimalType(precision, max(decimalDigits, 0));
            if (decimalType.isShort()) {
                return new ShortDecimalValueWriter(converter, rowCount, decimalType, type);
            }
            else {
                return new LongDecimalValueWriter(converter, rowCount, decimalDigits, type);
            }
        }
        else if (type == VarcharType.VARCHAR && typeName.equals("VARIANT")) {
            return new VariantValueWriter(converter, rowCount, type);
        }
        else if (type == VarcharType.VARCHAR && (typeName.equals("OBJECT") || typeName.equals("ARRAY"))) {
            return new VarcharValueWriter(converter, rowCount, type);
        }
        else if (type == DateType.DATE && typeHandle.getJdbcType() == Types.DATE) {
            return new LongValueWriter(converter, rowCount, type);
        }
        else if (typeHandle.getJdbcType() == Types.TIME) {
            return new TimeValueWriter(converter, rowCount, type);
        }
        else if (typeHandle.getJdbcType() == Types.TIMESTAMP_WITH_TIMEZONE || typeName.equals("TIMESTAMPLTZ")) {
            int precision = typeHandle.getRequiredDecimalDigits();
            checkArgument(precision <= SNOWFLAKE_MAX_TIMESTAMP_PRECISION, MAX_TIMESTAMP_PRECISION_ERROR_MESSAGE);
            if (precision <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION && type.getJavaType() == long.class) {
                return new ShortTimestampWithTimeZoneValueWriter(converter, rowCount, type);
            }
            else {
                return new LongTimestampWithTimeZoneValueWriter(converter, rowCount, type);
            }
        }
        else if (typeHandle.getJdbcType() == Types.TIMESTAMP) {
            int precision = typeHandle.getRequiredDecimalDigits();
            checkArgument(precision <= SNOWFLAKE_MAX_TIMESTAMP_PRECISION, MAX_TIMESTAMP_PRECISION_ERROR_MESSAGE);
            if (precision <= TimestampType.MAX_SHORT_PRECISION && type.getJavaType() == long.class) {
                return new ShortTimestampValueWriter(converter, rowCount, type, precision);
            }
            else {
                return new TimestampValueWriter(converter, rowCount, type, precision);
            }
        }
        else if (typeHandle.getJdbcType() == Types.CHAR) {
            int requiredColumnSize = typeHandle.getRequiredColumnSize();
            if (requiredColumnSize > CharType.MAX_LENGTH) {
                return new VarcharValueWriter(converter, rowCount, type);
            }
            else {
                return new CharValueWriter(converter, rowCount, type);
            }
        }
        else if (type.getJavaType() == Slice.class && typeHandle.getJdbcType() == Types.VARCHAR) {
            return new VarcharValueWriter(converter, rowCount, type);
        }
        else if (type == VARBINARY && typeHandle.getJdbcType() == Types.BINARY) {
            return new VarbinaryValueWriter(converter, rowCount, type);
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", type.getJavaType().getSimpleName(), columnHandle));
        }
    }
}
