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
package io.trino.plugin.exasol;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultCharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromLongTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.toLongTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.toTrinoTimestamp;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.ENGLISH;

public class ExasolClient
        extends BaseJdbcClient
{
    private static final int EXASOL_TIMESTAMP_WITH_TIMEZONE = 124;

    private static final Set<String> INTERNAL_SCHEMAS = ImmutableSet.<String>builder()
            .add("EXA_STATISTICS")
            .add("SYS")
            .build();

    private static final int MAX_EXASOL_TIMESTAMP_PRECISION = 9;

    private static final DateTimeFormatter TIMESTAMP_NANO_OPTIONAL_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("uuuu-MM-dd HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .toFormatter();

    @Inject
    public ExasolClient(
            BaseJdbcConfig config,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, false);
    }

    @Override
    protected boolean filterSchema(String schemaName)
    {
        if (INTERNAL_SCHEMAS.contains(schemaName.toUpperCase(ENGLISH))) {
            return false;
        }
        return super.filterSchema(schemaName);
    }

    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, MODIFYING_ROWS_MESSAGE);
    }

    @Override
    public void truncateTable(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support truncating tables");
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    @Override
    public void commitCreateTable(ConnectorSession session, JdbcOutputTableHandle handle, Set<Long> pageSinkIds)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column, ColumnPosition position)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns");
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming columns");
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping columns");
    }

    @Override
    public void dropTable(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping tables");
    }

    @Override
    public void rollbackCreateTable(ConnectorSession session, JdbcOutputTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle, List<WriteFunction> columnWriters)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support inserts");
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating schemas");
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas");
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
    }

    @Override
    protected void renameTable(ConnectorSession session, Connection connection, String catalogName, String remoteSchemaName, String remoteTableName, String newRemoteSchemaName, String newRemoteTableName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables");
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        // Deactivated because test 'testJoinPushdown()' requires write access which is not implemented for Exasol
        return false;
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        // Deactivated because test 'testCaseSensitiveAggregationPushdown()' requires write access which is not implemented for Exasol
        return Optional.empty();
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        switch (typeHandle.jdbcType()) {
            case Types.BOOLEAN:
                return Optional.of(booleanColumnMapping());
            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());
            case Types.INTEGER:
                return Optional.of(integerColumnMapping());
            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());
            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());
            case Types.DECIMAL:
                int decimalDigits = typeHandle.requiredDecimalDigits();
                int columnSize = typeHandle.requiredColumnSize();
                return Optional.of(decimalColumnMapping(createDecimalType(columnSize, decimalDigits)));
            case Types.CHAR:
                return Optional.of(defaultCharColumnMapping(typeHandle.requiredColumnSize(), true));
            case Types.VARCHAR:
                // String data is sorted by its binary representation.
                // https://docs.exasol.com/db/latest/sql/select.htm#UsageNotes
                return Optional.of(defaultVarcharColumnMapping(typeHandle.requiredColumnSize(), true));
            case Types.DATE:
                return Optional.of(dateColumnMapping());
            case Types.TIMESTAMP:
                return Optional.of(timestampColumnMapping(typeHandle));
            case EXASOL_TIMESTAMP_WITH_TIMEZONE:
                return Optional.of(timestampWithTimeZoneColumnMapping(typeHandle));
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.empty();
    }

    private static ColumnMapping timestampColumnMapping(JdbcTypeHandle typeHandle)
    {
        int timestampPrecision = typeHandle.requiredDecimalDigits();
        TimestampType timestampType = createTimestampType(timestampPrecision);
        if (timestampType.isShort()) {
            return ColumnMapping.longMapping(
                    timestampType,
                    longTimestampReadFunction(timestampType),
                    longTimestampWriteFunction(timestampType),
                    FULL_PUSHDOWN);
        }
        return ColumnMapping.objectMapping(
                timestampType,
                objectTimestampReadFunction(timestampType),
                objectTimestampWriteFunction(timestampType),
                FULL_PUSHDOWN);
    }

    private static LongReadFunction longTimestampReadFunction(TimestampType timestampType)
    {
        return (resultSet, columnIndex) -> {
            Timestamp timestamp = resultSet.getObject(columnIndex, Timestamp.class);
            return toTrinoTimestamp(timestampType, timestamp.toLocalDateTime());
        };
    }

    private static ObjectReadFunction objectTimestampReadFunction(TimestampType timestampType)
    {
        verifyObjectTimestampPrecision(timestampType);
        return ObjectReadFunction.of(
                LongTimestamp.class,
                (resultSet, columnIndex) -> {
                    Timestamp timestamp = resultSet.getObject(columnIndex, Timestamp.class);
                    return toLongTrinoTimestamp(timestampType, timestamp.toLocalDateTime());
                });
    }

    private static void verifyObjectTimestampPrecision(TimestampType timestampType)
    {
        int precision = timestampType.getPrecision();
        checkArgument(precision > TimestampType.MAX_SHORT_PRECISION && precision <= MAX_EXASOL_TIMESTAMP_PRECISION,
                "Precision is out of range: %s", precision);
    }

    private static ObjectWriteFunction objectTimestampWriteFunction(TimestampType timestampType)
    {
        int precision = timestampType.getPrecision();
        verifyObjectTimestampPrecision(timestampType);

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
                return getTimestampBindExpression(precision);
            }

            @Override
            public void setNull(PreparedStatement statement, int index)
                    throws SQLException
            {
                statement.setNull(index, Types.VARCHAR);
            }
        };
    }

    private static LongWriteFunction longTimestampWriteFunction(TimestampType timestampType)
    {
        return new LongWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                return getTimestampBindExpression(timestampType.getPrecision());
            }

            @Override
            public void set(PreparedStatement statement, int index, long epochMicros)
                    throws SQLException
            {
                LocalDateTime localDateTime = fromTrinoTimestamp(epochMicros);
                Timestamp timestampValue = Timestamp.valueOf(localDateTime);
                statement.setObject(index, timestampValue);
            }

            @Override
            public void setNull(PreparedStatement statement, int index)
                    throws SQLException
            {
                statement.setNull(index, Types.VARCHAR);
            }
        };
    }

    private static ColumnMapping timestampWithTimeZoneColumnMapping(JdbcTypeHandle typeHandle)
    {
        int timestampPrecision = typeHandle.requiredDecimalDigits();
        TimestampWithTimeZoneType timestampWithTimeZoneType = createTimestampWithTimeZoneType(timestampPrecision);

        if (timestampWithTimeZoneType.isShort()) {
            return ColumnMapping.longMapping(
                    timestampWithTimeZoneType,
                    longTimestampWithTimeZoneReadFunction(),
                    longTimestampWithTimeZoneWriteFunction(timestampWithTimeZoneType),
                    FULL_PUSHDOWN);
        }
        return ColumnMapping.objectMapping(
                timestampWithTimeZoneType,
                objectTimestampWithTimeZoneReadFunction(timestampWithTimeZoneType),
                objectTimestampWithTimeZoneWriteFunction(timestampWithTimeZoneType),
                FULL_PUSHDOWN);
    }

    private static LongReadFunction longTimestampWithTimeZoneReadFunction()
    {
        return (resultSet, columnIndex) -> {
            Timestamp timestamp = resultSet.getObject(columnIndex, Timestamp.class);
            return packDateTimeWithZone(timestamp.getTime(), UTC_KEY);
        };
    }

    private static ObjectReadFunction objectTimestampWithTimeZoneReadFunction(
            TimestampWithTimeZoneType timestampType)
    {
        verifyObjectTimestampWithTimeZonePrecision(timestampType);
        return ObjectReadFunction.of(
                LongTimestampWithTimeZone.class,
                (resultSet, columnIndex) -> {
                    Timestamp timestamp = resultSet.getObject(columnIndex, Timestamp.class);
                    if (timestamp == null) {
                        return null;
                    }

                    long millisUtc = timestamp.getTime();
                    long nanosUtc = millisUtc * 1_000_000L + timestamp.getNanos() % 1_000_000;
                    int picosOfMilli = (int) ((nanosUtc - millisUtc * 1_000_000) * 1_000);

                    return LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                            millisUtc,
                            picosOfMilli,
                            UTC_KEY);
                });
    }

    private static void verifyObjectTimestampWithTimeZonePrecision(TimestampWithTimeZoneType timestampType)
    {
        int precision = timestampType.getPrecision();
        checkArgument(precision > TimestampWithTimeZoneType.MAX_SHORT_PRECISION && precision <= MAX_EXASOL_TIMESTAMP_PRECISION,
                "Precision is out of range: %s", precision);
    }

    private static ObjectWriteFunction objectTimestampWithTimeZoneWriteFunction(TimestampWithTimeZoneType timestampType)
    {
        int precision = timestampType.getPrecision();
        verifyObjectTimestampWithTimeZonePrecision(timestampType);

        return new ObjectWriteFunction() {
            @Override
            public Class<?> getJavaType()
            {
                return LongTimestampWithTimeZone.class;
            }

            @Override
            public void set(PreparedStatement statement, int index, Object value)
                    throws SQLException
            {
                if (value == null) {
                    statement.setNull(index, Types.VARCHAR);
                    return;
                }

                LongTimestampWithTimeZone timestampValue = (LongTimestampWithTimeZone) value;

                long epochMillis = timestampValue.getEpochMillis();
                int picosOfMilli = timestampValue.getPicosOfMilli();
                int nanos = picosOfMilli / PICOSECONDS_PER_NANOSECOND;

                Instant instant = Instant.ofEpochMilli(epochMillis).plusNanos(nanos);
                ZonedDateTime zonedDateTime = instant.atZone(UTC);

                String formatted = TIMESTAMP_NANO_OPTIONAL_FORMATTER.format(zonedDateTime);
                statement.setString(index, formatted);
            }

            @Override
            public String getBindExpression()
            {
                return getTimestampBindExpression(precision);
            }

            @Override
            public void setNull(PreparedStatement statement, int index)
                    throws SQLException
            {
                statement.setNull(index, Types.VARCHAR);
            }
        };
    }

    private static LongWriteFunction longTimestampWithTimeZoneWriteFunction(TimestampWithTimeZoneType timestampType)
    {
        return new LongWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                return getTimestampBindExpression(timestampType.getPrecision());
            }

            @Override
            public void set(PreparedStatement statement, int index, long epochMicros)
                    throws SQLException
            {
                Instant instant = Instant.ofEpochMilli(unpackMillisUtc(epochMicros));
                LocalDateTime localDateTime = instant.atZone(ZoneOffset.UTC).toLocalDateTime();
                statement.setString(index, TIMESTAMP_NANO_OPTIONAL_FORMATTER.format(localDateTime));
            }

            @Override
            public void setNull(PreparedStatement statement, int index)
                    throws SQLException
            {
                statement.setNull(index, Types.VARCHAR);
            }
        };
    }

    private static String getTimestampBindExpression(int precision)
    {
        if (precision <= 0) {
            return "TO_TIMESTAMP(?, 'YYYY-MM-DD HH24:MI:SS')";
        }
        else {
            return format("TO_TIMESTAMP(?, 'YYYY-MM-DD HH24:MI:SS.FF%d')", precision);
        }
    }

    private static ColumnMapping dateColumnMapping()
    {
        // Exasol driver does not support LocalDate
        return ColumnMapping.longMapping(
                DATE,
                dateReadFunctionUsingSqlDate(),
                dateWriteFunctionUsingSqlDate());
    }

    private static LongReadFunction dateReadFunctionUsingSqlDate()
    {
        return (resultSet, columnIndex) -> {
            Date date = resultSet.getDate(columnIndex);
            return date.toLocalDate().toEpochDay();
        };
    }

    private static LongWriteFunction dateWriteFunctionUsingSqlDate()
    {
        return LongWriteFunction.of(Types.DATE, (statement, index, value) -> {
            LocalDate localDate = LocalDate.ofEpochDay(value);
            statement.setDate(index, Date.valueOf(localDate));
        });
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support writing");
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with data");
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support inserts");
    }
}
