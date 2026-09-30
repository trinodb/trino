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
package io.trino.plugin.ydb;

import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Locale;

import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateReadFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalReadFunction;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static java.time.ZoneOffset.UTC;
import static tech.ydb.jdbc.YdbConst.SQL_KIND_DECIMAL;
import static tech.ydb.jdbc.YdbConst.SQL_KIND_PRIMITIVE;

final class YdbColumnMappings
{
    // https://github.com/ydb-platform/ydb-jdbc-driver/blob/v2.4.1/jdbc/src/main/java/tech/ydb/jdbc/common/YdbTypes.java#L64-L77
    private static final int YDB_DATE_SQL_TYPE = SQL_KIND_PRIMITIVE + 16;
    private static final int YDB_DATETIME_SQL_TYPE = SQL_KIND_PRIMITIVE + 17;
    private static final int YDB_TIMESTAMP_SQL_TYPE = SQL_KIND_PRIMITIVE + 18;
    static final int YDB_DATE32_SQL_TYPE = SQL_KIND_PRIMITIVE + 25;
    private static final int YDB_DATETIME64_SQL_TYPE = SQL_KIND_PRIMITIVE + 26;
    static final int YDB_TIMESTAMP64_SQL_TYPE = SQL_KIND_PRIMITIVE + 27;

    private YdbColumnMappings() {}

    static ColumnMapping dateColumnMapping(JdbcTypeHandle typeHandle)
    {
        String typeName = typeHandle.jdbcTypeName().orElse("");
        LongWriteFunction writeFunction = switch (typeName.toLowerCase(Locale.ROOT)) {
            case "date" -> dateWriteFunction(YDB_DATE_SQL_TYPE);
            case "date32" -> dateWriteFunction(YDB_DATE32_SQL_TYPE);
            default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported YDB date type: " + typeName);
        };
        return ColumnMapping.longMapping(
                DATE,
                dateReadFunctionUsingLocalDate(),
                writeFunction,
                DISABLE_PUSHDOWN);
    }

    static ColumnMapping timestampColumnMapping(JdbcTypeHandle typeHandle)
    {
        String typeName = typeHandle.jdbcTypeName().orElse("");
        LongWriteFunction writeFunction = switch (typeName.toLowerCase(Locale.ROOT)) {
            case "datetime" -> datetimeWriteFunction(YDB_DATETIME_SQL_TYPE);
            case "datetime64" -> datetimeWriteFunction(YDB_DATETIME64_SQL_TYPE);
            case "timestamp" -> timestampWriteFunction(YDB_TIMESTAMP_SQL_TYPE);
            case "timestamp64" -> timestampWriteFunction(YDB_TIMESTAMP64_SQL_TYPE);
            default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported YDB timestamp type: " + typeName);
        };
        return ColumnMapping.longMapping(
                TIMESTAMP_MICROS,
                (resultSet, index) -> {
                    Instant instant = typeName.equalsIgnoreCase("Datetime") || typeName.equalsIgnoreCase("Datetime64")
                            ? resultSet.getObject(index, LocalDateTime.class).toInstant(UTC)
                            : resultSet.getObject(index, Instant.class);
                    return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000), instant.getNano() / 1_000);
                },
                writeFunction,
                typeName.equalsIgnoreCase("Timestamp64") ? FULL_PUSHDOWN : DISABLE_PUSHDOWN);
    }

    static ColumnMapping unsignedColumnMapping(Type type, int typeOffset)
    {
        int sqlType = SQL_KIND_PRIMITIVE + typeOffset;
        long maximum = switch (typeOffset) {
            case 2 -> 255;
            case 4 -> 65535;
            case 6 -> 0xFFFF_FFFFL;
            default -> throw new IllegalArgumentException("Unsupported unsigned type offset: " + typeOffset);
        };
        return ColumnMapping.longMapping(
                type,
                ResultSet::getLong,
                LongWriteFunction.of(sqlType, (statement, index, value) -> {
                    if (value < 0 || value > maximum) {
                        throw new SQLDataException("Value is outside the YDB unsigned column range");
                    }
                    statement.setObject(index, value, sqlType);
                }),
                DISABLE_PUSHDOWN);
    }

    static ColumnMapping unsignedBigintColumnMapping()
    {
        var type = createDecimalType(20, 0);
        int sqlType = SQL_KIND_PRIMITIVE + 8;
        ObjectWriteFunction writer = objectWriteFunction(sqlType, (statement, index, value) -> {
            if (value.toBigInteger().signum() < 0 || value.toBigInteger().bitLength() > 64) {
                throw new SQLDataException("Value is outside the YDB Uint64 range");
            }
            // The vendor setter consumes the unsigned integer's bit representation.
            statement.setObject(index, value.toBigInteger().longValue(), sqlType);
        });
        return ColumnMapping.objectMapping(type, longDecimalReadFunction(type), writer, DISABLE_PUSHDOWN);
    }

    static ColumnMapping decimalColumnMapping(DecimalType type)
    {
        int sqlType = SQL_KIND_DECIMAL + (type.getPrecision() << 6) + type.getScale();
        if (type.isShort()) {
            return ColumnMapping.longMapping(
                    type,
                    shortDecimalReadFunction(type),
                    LongWriteFunction.of(sqlType, (statement, index, value) ->
                            statement.setObject(index, BigDecimal.valueOf(value, type.getScale()), sqlType)),
                    DISABLE_PUSHDOWN);
        }
        return ColumnMapping.objectMapping(
                type,
                longDecimalReadFunction(type),
                objectWriteFunction(sqlType, (statement, index, value) ->
                        statement.setObject(index, new BigDecimal(value.toBigInteger(), type.getScale()), sqlType)),
                DISABLE_PUSHDOWN);
    }

    private static ObjectWriteFunction objectWriteFunction(int sqlType, ObjectWriteFunction.ObjectWriteFunctionImplementation<Int128> implementation)
    {
        return new ObjectWriteFunction()
        {
            @Override
            public Class<?> getJavaType()
            {
                return Int128.class;
            }

            @Override
            public void set(PreparedStatement statement, int index, Object value)
                    throws SQLException
            {
                implementation.set(statement, index, (Int128) value);
            }

            @Override
            public void setNull(PreparedStatement statement, int index)
                    throws SQLException
            {
                statement.setNull(index, sqlType);
            }
        };
    }

    static LongWriteFunction dateWriteFunction(int sqlType)
    {
        return LongWriteFunction.of(
                sqlType,
                (statement, index, value) -> {
                    if (sqlType == YDB_DATE_SQL_TYPE && (value < 0 || value > 0xFFFFL)) {
                        throw new SQLDataException("Value is outside the YDB Date storage range");
                    }
                    statement.setObject(index, value, sqlType);
                });
    }

    private static LongWriteFunction datetimeWriteFunction(int sqlType)
    {
        return LongWriteFunction.of(
                sqlType,
                (statement, index, value) -> {
                    if (value % 1_000_000 != 0) {
                        throw new SQLDataException("YDB Datetime columns cannot store fractional seconds");
                    }
                    long seconds = fromTrinoTimestamp(value).toEpochSecond(UTC);
                    if (sqlType == YDB_DATETIME_SQL_TYPE && (seconds < 0 || seconds > 0xFFFF_FFFFL)) {
                        throw new SQLDataException("Value is outside the YDB Datetime storage range");
                    }
                    statement.setObject(index, seconds, sqlType);
                });
    }

    static LongWriteFunction timestampWriteFunction(int sqlType)
    {
        return LongWriteFunction.of(
                sqlType,
                (statement, index, value) -> {
                    if (sqlType == YDB_TIMESTAMP_SQL_TYPE && value < 0) {
                        throw new SQLDataException("Value is outside the YDB Timestamp storage range");
                    }
                    statement.setObject(index, fromTrinoTimestamp(value).toInstant(UTC), sqlType);
                });
    }
}
