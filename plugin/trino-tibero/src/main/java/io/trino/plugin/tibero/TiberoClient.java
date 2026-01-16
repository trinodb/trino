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
package io.trino.plugin.tibero;

import com.google.inject.Inject;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromLongTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.toLongTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.toTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.String.format;

public class TiberoClient
        extends BaseJdbcClient
{
    private static final int MAX_TIBERO_TIMESTAMP_PRECISION = 9;

    private static final DateTimeFormatter TIMESTAMP_SECONDS_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");

    private static final DateTimeFormatter TIMESTAMP_NANO_OPTIONAL_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("uuuu-MM-dd HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .toFormatter();

    @Inject
    public TiberoClient(
            BaseJdbcConfig config,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier remoteQueryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, remoteQueryModifier, true);
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        String jdbcTypeName = typeHandle.jdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        if (jdbcTypeName.equalsIgnoreCase("date")) {
            return Optional.of(ColumnMapping.longMapping(
                    TIMESTAMP_SECONDS,
                    tiberoTimestampReadFunction(TIMESTAMP_SECONDS),
                    trinoTimestampToTiberoDateWriteFunction(),
                    FULL_PUSHDOWN));
        }
        switch (typeHandle.jdbcType()) {
            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());

            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.REAL:
                return Optional.of(realColumnMapping());

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.CHAR:
                return Optional.of(charColumnMapping(typeHandle.requiredColumnSize()));

            case Types.NUMERIC:
            case Types.DECIMAL:
                int precision = typeHandle.columnSize().orElse(38);
                int scale = typeHandle.decimalDigits().orElse(0);
                DecimalType decimalType = DecimalType.createDecimalType(precision, scale);
                return Optional.of(decimalColumnMapping(decimalType));

            case Types.TIMESTAMP:
                int timestampPrecision = typeHandle.requiredDecimalDigits();
                return Optional.of(tiberoTimestampColumnMapping(createTimestampType(timestampPrecision)));

            case Types.CLOB:
            case Types.NCLOB:
                return Optional.of(ColumnMapping.sliceMapping(
                        createUnboundedVarcharType(),
                        (resultSet, columnIndex) -> utf8Slice(resultSet.getString(columnIndex)),
                        varcharWriteFunction(),
                        DISABLE_PUSHDOWN));

            case Types.VARBINARY: // Tibero's RAW(n)
            case Types.BLOB:
                return Optional.of(ColumnMapping.sliceMapping(
                        VARBINARY,
                        (resultSet, columnIndex) -> wrappedBuffer(resultSet.getBytes(columnIndex)),
                        varbinaryWriteFunction(),
                        DISABLE_PUSHDOWN));

            case Types.VARCHAR:
            case Types.NVARCHAR:
                return Optional.of(ColumnMapping.sliceMapping(
                        createVarcharType(typeHandle.requiredColumnSize()),
                        (varcharResultSet, varcharColumnIndex) -> utf8Slice(varcharResultSet.getString(varcharColumnIndex)),
                        varcharWriteFunction(),
                        FULL_PUSHDOWN));
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }

        return Optional.empty();
    }

    private static ColumnMapping tiberoTimestampColumnMapping(TimestampType timestampType)
    {
        if (timestampType.isShort()) {
            return ColumnMapping.longMapping(
                    timestampType,
                    tiberoTimestampReadFunction(timestampType),
                    tiberoTimestampWriteFunction(timestampType),
                    FULL_PUSHDOWN);
        }
        return ColumnMapping.objectMapping(
                timestampType,
                tiberoLongTimestampReadFunction(timestampType),
                tiberoLongTimestampWriteFunction(timestampType),
                FULL_PUSHDOWN);
    }

    private static ObjectReadFunction tiberoLongTimestampReadFunction(TimestampType timestampType)
    {
        verifyLongTimestampPrecision(timestampType);
        return ObjectReadFunction.of(
                LongTimestamp.class,
                (resultSet, columnIndex) -> {
                    LocalDateTime timestamp = resultSet.getObject(columnIndex, LocalDateTime.class);
                    // Adjust years when the value is B.C. dates because Oracle returns +1 year unless converting to string in their server side
                    if (timestamp.getYear() <= 0) {
                        timestamp = timestamp.minusYears(1);
                    }
                    return toLongTrinoTimestamp(timestampType, timestamp);
                });
    }

    private static ObjectWriteFunction tiberoLongTimestampWriteFunction(TimestampType timestampType)
    {
        int precision = timestampType.getPrecision();
        verifyLongTimestampPrecision(timestampType);

        return new ObjectWriteFunction() {
            @Override
            public Class<?> getJavaType()
            {
                return LongTimestamp.class;
            }

            @Override
            public void set(PreparedStatement statement, int index, Object value)
                    throws SQLException
            {
                LocalDateTime timestamp = fromLongTrinoTimestamp((LongTimestamp) value, precision);
                statement.setString(index, TIMESTAMP_NANO_OPTIONAL_FORMATTER.format(timestamp));
            }

            @Override
            public String getBindExpression()
            {
                return getTiberoBindExpression(precision);
            }

            @Override
            public void setNull(PreparedStatement statement, int index)
                    throws SQLException
            {
                statement.setNull(index, Types.VARCHAR);
            }
        };
    }

    private static void verifyLongTimestampPrecision(TimestampType timestampType)
    {
        int precision = timestampType.getPrecision();
        checkArgument(precision > MAX_SHORT_PRECISION && precision <= MAX_TIBERO_TIMESTAMP_PRECISION,
                "Precision is out of range: %s", precision);
    }

    private static LongWriteFunction trinoTimestampToTiberoDateWriteFunction()
    {
        return new LongWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                // Oracle's DATE stores year, month, day, hour, minute, seconds, but not second fraction
                return "TO_DATE(?, 'SYYYY-MM-DD HH24:MI:SS')";
            }

            @Override
            public void set(PreparedStatement statement, int index, long value)
                    throws SQLException
            {
                long epochSecond = floorDiv(value, MICROSECONDS_PER_SECOND);
                int microsOfSecond = floorMod(value, MICROSECONDS_PER_SECOND);
                verify(microsOfSecond == 0, "Micros of second must be zero: '%s'", value);
                LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(epochSecond, 0, ZoneOffset.UTC);
                statement.setString(index, TIMESTAMP_SECONDS_FORMATTER.format(localDateTime));
            }

            @Override
            public void setNull(PreparedStatement statement, int index)
                    throws SQLException
            {
                statement.setNull(index, Types.VARCHAR);
            }
        };
    }

    private static LongReadFunction tiberoTimestampReadFunction(TimestampType timestampType)
    {
        return (resultSet, columnIndex) -> {
            java.sql.Timestamp ts = resultSet.getTimestamp(columnIndex);

            // LocalDateTime timestamp = resultSet.getObject(columnIndex, LocalDateTime.class);
            LocalDateTime timestamp = ts.toLocalDateTime();

            // Adjust years when the value is B.C. dates because Oracle returns +1 year unless converting to string in their server side
            if (timestamp.getYear() <= 0) {
                timestamp = timestamp.minusYears(1);
            }
            return toTrinoTimestamp(timestampType, timestamp);
        };
    }

    private static LongWriteFunction tiberoTimestampWriteFunction(TimestampType timestampType)
    {
        return new LongWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                return getTiberoBindExpression(timestampType.getPrecision());
            }

            @Override
            public void set(PreparedStatement statement, int index, long epochMicros)
                    throws SQLException
            {
                LocalDateTime timestamp = fromTrinoTimestamp(epochMicros);
                statement.setString(index, TIMESTAMP_NANO_OPTIONAL_FORMATTER.format(timestamp));
            }

            @Override
            public void setNull(PreparedStatement statement, int index)
                    throws SQLException
            {
                statement.setNull(index, Types.VARCHAR);
            }
        };
    }

    private static String getTiberoBindExpression(int precision)
    {
        if (precision == 0) {
            return "TO_TIMESTAMP(?, 'SYYYY-MM-DD HH24:MI:SS')";
        }
        if (precision <= 2) {
            return "TO_TIMESTAMP(?, 'SYYYY-MM-DD HH24:MI:SS.FF')";
        }

        return format("TO_TIMESTAMP(?, 'SYYYY-MM-DD HH24:MI:SS.FF%d')", precision);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == SMALLINT) {
            return WriteMapping.longMapping("smallint", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("integer", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }

        if (type == REAL) {
            return WriteMapping.longMapping("real", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double precision", doubleWriteFunction());
        }

        if (type instanceof CharType charType) {
            return WriteMapping.sliceMapping("char(" + charType.getLength() + ")", charWriteFunction());
        }

        if (type instanceof VarcharType varcharType) {
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "varchar";
            }
            else {
                dataType = "varchar(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    private static ColumnMapping charColumnMapping(int charLength)
    {
        if (charLength > CharType.MAX_LENGTH) {
            return varcharColumnMapping(charLength);
        }
        CharType charType = createCharType(charLength);
        return ColumnMapping.sliceMapping(
                charType,
                charReadFunction(charType),
                charWriteFunction(),
                DISABLE_PUSHDOWN);
    }

    private static ColumnMapping varcharColumnMapping(int varcharLength)
    {
        VarcharType varcharType = varcharLength <= VarcharType.MAX_LENGTH
                ? createVarcharType(varcharLength)
                : createUnboundedVarcharType();
        return ColumnMapping.sliceMapping(
                varcharType,
                varcharReadFunction(varcharType),
                varcharWriteFunction(),
                DISABLE_PUSHDOWN);
    }
}
