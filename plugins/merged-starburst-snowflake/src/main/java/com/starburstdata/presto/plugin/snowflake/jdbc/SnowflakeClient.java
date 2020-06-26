/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starburstdata.presto.plugin.jdbc.stats.JdbcStatisticsConfig;
import com.starburstdata.presto.plugin.jdbc.stats.TableStatisticsClient;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcOutputTableHandle;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.LongWriteFunction;
import io.prestosql.plugin.jdbc.SliceReadFunction;
import io.prestosql.plugin.jdbc.SliceWriteFunction;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.Estimate;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.Chars;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.jdbcTypeToPrestoType;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.math.RoundingMode.UNNECESSARY;
import static java.sql.Types.BINARY;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;
import static java.time.ZoneOffset.UTC;

public class SnowflakeClient
        extends BaseJdbcClient
{
    public static final String IDENTIFIER_QUOTE = "\"";

    private static final LocalDate EPOCH_DAY = LocalDate.ofEpochDay(0);
    private static final DateTimeFormatter SNOWFLAKE_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX");
    private static final int SNOWFLAKE_MAX_LIST_EXPRESSIONS = 1000;
    private static final UnaryOperator<Domain> SIMPLIFY_UNSUPPORTED_PUSHDOWN = domain -> {
        if (domain.getValues().getRanges().getRangeCount() <= SNOWFLAKE_MAX_LIST_EXPRESSIONS) {
            return domain;
        }
        return domain.simplify();
    };
    private static final Map<Type, WriteMapping> WRITE_MAPPINGS = ImmutableMap.<Type, WriteMapping>builder()
            .put(BIGINT, WriteMapping.longMapping("number(19)", bigintWriteFunction()))
            .put(INTEGER, WriteMapping.longMapping("number(10)", integerWriteFunction()))
            .put(SMALLINT, WriteMapping.longMapping("number(5)", smallintWriteFunction()))
            .put(TINYINT, WriteMapping.longMapping("number(3)", tinyintWriteFunction()))
            .build();
    private static final UtcTimeZoneCalendar UTC_TZ_PASSING_CALENDAR = UtcTimeZoneCalendar.getUtcTimeZoneCalendarInstance();

    private final TableStatisticsClient tableStatisticsClient;
    private final boolean distributedConnector;

    public SnowflakeClient(BaseJdbcConfig config, JdbcStatisticsConfig statisticsConfig, ConnectionFactory connectionFactory, boolean distributedConnector)
    {
        super(config, IDENTIFIER_QUOTE, connectionFactory);
        this.tableStatisticsClient = new TableStatisticsClient(this::readTableStatistics, statisticsConfig);
        this.distributedConnector = distributedConnector;
    }

    @Override
    public Connection getConnection(JdbcIdentity identity, JdbcSplit split)
            throws SQLException
    {
        return connectionFactory.openConnection(identity);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, TupleDomain<ColumnHandle> tupleDomain)
    {
        return tableStatisticsClient.getTableStatistics(session, handle, tupleDomain);
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of(SnowflakeClient::applyLimit);
    }

    public static String applyLimit(String sql, Long limit)
    {
        return sql + " LIMIT " + limit;
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String typeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new PrestoException(JDBC_ERROR, "Type name is missing: " + typeHandle));
        int columnSize = typeHandle.getColumnSize();

        if (typeName.equals("NUMBER")) {
            int decimalDigits = typeHandle.getDecimalDigits();
            int precision = columnSize + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
            if (precision > Decimals.MAX_PRECISION) {
                return Optional.empty();
            }
            return Optional.of(withSimplifiedPushdownConverter(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0)), UNNECESSARY)));
        }

        if (typeName.equals("VARIANT")) {
            return Optional.of(ColumnMapping.sliceMapping(createUnboundedVarcharType(), variantReadFunction(), varcharWriteFunction(), SIMPLIFY_UNSUPPORTED_PUSHDOWN));
        }

        if (typeName.equals("OBJECT") || typeName.equals("ARRAY")) {
            // TODO: better support for OBJECT (Presto MAP/ROW) and ARRAY (Presto ARRAY)
            return Optional.of(withSimplifiedPushdownConverter(varcharColumnMapping(createUnboundedVarcharType())));
        }

        if (typeHandle.getJdbcType() == Types.TIME) {
            return Optional.of(withSimplifiedPushdownConverter(timeColumnMapping(session)));
        }

        if (typeHandle.getJdbcType() == Types.TIMESTAMP) {
            if (typeName.equals("TIMESTAMPTZ") || typeName.equals("TIMESTAMPLTZ")) {
                return Optional.of(timestampWithTimezoneColumnMapping());
            }
            return Optional.of(timestampColumnMapping(session));
        }

        if (typeHandle.getJdbcType() == VARCHAR && distributedConnector) {
            return Optional.of(withSimplifiedPushdownConverter(varcharColumnMapping(createVarcharType(min(columnSize, HiveVarchar.MAX_VARCHAR_LENGTH)))));
        }

        if (typeHandle.getJdbcType() == VARBINARY || typeHandle.getJdbcType() == BINARY || typeHandle.getJdbcType() == LONGVARBINARY) {
            return Optional.of(varbinaryColumnMapping());
        }

        return jdbcTypeToPrestoType(session, typeHandle).map(this::withSimplifiedPushdownConverter);
    }

    private ColumnMapping withSimplifiedPushdownConverter(ColumnMapping mapping)
    {
        verify(mapping.getPushdownConverter() != ColumnMapping.DISABLE_PUSHDOWN);
        return new ColumnMapping(
                mapping.getType(),
                mapping.getReadFunction(),
                mapping.getWriteFunction(),
                SIMPLIFY_UNSUPPORTED_PUSHDOWN);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type instanceof CharType) {
            // Snowflake CHAR is an alias for VARCHAR so we need to pad value with spaces
            return WriteMapping.sliceMapping("char(" + ((CharType) type).getLength() + ")", charWriteFunction((CharType) type));
        }

        if (type instanceof TimeType) {
            return WriteMapping.longMapping("time", timeWriteFunction(session));
        }

        if (type instanceof TimestampType) {
            return WriteMapping.longMapping("timestamp_ntz", timestampWriteFunction(session));
        }

        if (type instanceof TimestampWithTimeZoneType) {
            return WriteMapping.longMapping("timestamp_tz", timestampWithTimezoneWriteFunction());
        }

        WriteMapping writeMapping = WRITE_MAPPINGS.get(type);
        if (writeMapping != null) {
            return writeMapping;
        }

        return super.toWriteMapping(session, type);
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        checkColumnsForInvalidCharacters(tableMetadata.getColumns());
        return super.beginCreateTable(session, tableMetadata);
    }

    @Override
    public JdbcOutputTableHandle createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, String tableName)
            throws SQLException
    {
        checkColumnsForInvalidCharacters(tableMetadata.getColumns());
        return super.createTable(session, tableMetadata, tableName);
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        checkColumnsForInvalidCharacters(ImmutableList.of(column));
        super.addColumn(session, handle, column);
    }

    public static void checkColumnsForInvalidCharacters(List<ColumnMetadata> columns)
    {
        columns.forEach(columnMetadata -> {
            if (columnMetadata.getName().contains("\"")) {
                throw new PrestoException(NOT_SUPPORTED, "Snowflake columns cannot contain quotes: " + columnMetadata.getName());
            }
        });
    }

    private static SliceReadFunction variantReadFunction()
    {
        return (resultSet, columnIndex) -> utf8Slice(resultSet.getString(columnIndex).replaceAll("^\"|\"$", ""));
    }

    private static SliceWriteFunction charWriteFunction(CharType charType)
    {
        return (statement, index, value) -> statement.setString(index, Chars.padSpaces(value, charType).toStringUtf8());
    }

    private static ColumnMapping timestampWithTimezoneColumnMapping()
    {
        return ColumnMapping.longMapping(
                TIMESTAMP_WITH_TIME_ZONE,
                (resultSet, columnIndex) -> {
                    ZonedDateTime timestamp = SNOWFLAKE_DATE_TIME_FORMATTER.parse(resultSet.getString(columnIndex), ZonedDateTime::from);
                    return packDateTimeWithZone(
                            timestamp.toInstant().toEpochMilli(),
                            timestamp.getZone().getId());
                },
                timestampWithTimezoneWriteFunction(),
                SIMPLIFY_UNSUPPORTED_PUSHDOWN);
    }

    private static LongWriteFunction timestampWithTimezoneWriteFunction()
    {
        return (statement, index, encodedTimeWithZone) -> {
            Instant time = Instant.ofEpochMilli(unpackMillisUtc(encodedTimeWithZone));
            ZoneId zone = ZoneId.of(unpackZoneKey(encodedTimeWithZone).getId());
            statement.setString(index, SNOWFLAKE_DATE_TIME_FORMATTER.format(time.atZone(zone)));
        };
    }

    private static ColumnMapping timestampColumnMapping(ConnectorSession session)
    {
        if (session.isLegacyTimestamp()) {
            ZoneId sessionZone = ZoneId.of(session.getTimeZoneKey().getId());
            return ColumnMapping.longMapping(
                    TIMESTAMP,
                    (resultSet, columnIndex) -> toPrestoLegacyTimestamp(toLocalDateTime(resultSet, columnIndex), sessionZone),
                    timestampWriteFunction(session));
        }

        return ColumnMapping.longMapping(
                TIMESTAMP,
                (resultSet, columnIndex) -> toPrestoTimestamp(toLocalDateTime(resultSet, columnIndex)),
                timestampWriteFunction(session));
    }

    private static LongWriteFunction timestampWriteFunction(ConnectorSession session)
    {
        if (session.isLegacyTimestamp()) {
            ZoneId sessionZone = ZoneId.of(session.getTimeZoneKey().getId());
            return (statement, index, value) -> statement.setString(index, fromPrestoLegacyTimestamp(value, sessionZone).toString());
        }
        return (statement, index, value) -> statement.setString(index, fromPrestoTimestamp(value).toString());
    }

    private static ColumnMapping timeColumnMapping(ConnectorSession session)
    {
        if (session.isLegacyTimestamp()) {
            ZoneId sessionZone = ZoneId.of(session.getTimeZoneKey().getId());
            return ColumnMapping.longMapping(
                    TIME,
                    (resultSet, columnIndex) -> toPrestoLegacyTime(toLocalTime(resultSet, columnIndex), sessionZone),
                    timeWriteFunction(session));
        }

        return ColumnMapping.longMapping(
                TIME,
                (resultSet, columnIndex) -> toPrestoTime(toLocalTime(resultSet, columnIndex)),
                timeWriteFunction(session));
    }

    private static LongWriteFunction timeWriteFunction(ConnectorSession session)
    {
        if (session.isLegacyTimestamp()) {
            ZoneId sessionZone = ZoneId.of(session.getTimeZoneKey().getId());
            return (statement, index, value) -> statement.setString(index, fromPrestoLegacyTime(value, sessionZone).toString());
        }
        return (statement, index, value) -> statement.setString(index, fromPrestoTime(value).toString());
    }

    /**
     * @deprecated applicable in legacy timestamp semantics only
     */
    @Deprecated
    private static long toPrestoLegacyTimestamp(LocalDateTime localDateTime, ZoneId sessionZone)
    {
        return localDateTime.atZone(sessionZone).toInstant().toEpochMilli();
    }

    private static long toPrestoTimestamp(LocalDateTime localDateTime)
    {
        return localDateTime.atZone(UTC).toInstant().toEpochMilli();
    }

    /**
     * @deprecated applicable in legacy timestamp semantics only
     */
    @Deprecated
    private static LocalDateTime fromPrestoLegacyTimestamp(long value, ZoneId sessionZone)
    {
        return Instant.ofEpochMilli(value).atZone(sessionZone).toLocalDateTime();
    }

    private static LocalDateTime fromPrestoTimestamp(long value)
    {
        return Instant.ofEpochMilli(value).atZone(UTC).toLocalDateTime();
    }

    private static LocalDateTime toLocalDateTime(ResultSet resultSet, int columnIndex)
            throws SQLException
    {
        Timestamp ts = resultSet.getTimestamp(columnIndex, UTC_TZ_PASSING_CALENDAR);
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(ts.getTime()), UTC);
    }

    /**
     * @deprecated applicable in legacy timestamp semantics only
     */
    @Deprecated
    private static long toPrestoLegacyTime(LocalTime localTime, ZoneId sessionZone)
    {
        return localTime.atDate(EPOCH_DAY).atZone(sessionZone).toInstant().toEpochMilli();
    }

    private static long toPrestoTime(LocalTime localTime)
    {
        return localTime.atDate(EPOCH_DAY).atZone(UTC).toInstant().toEpochMilli();
    }

    /**
     * @deprecated applicable in legacy timestamp semantics only
     */
    @Deprecated
    private static LocalTime fromPrestoLegacyTime(long value, ZoneId sessionZone)
    {
        return Instant.ofEpochMilli(value).atZone(sessionZone).toLocalTime();
    }

    private static LocalTime fromPrestoTime(long value)
    {
        return Instant.ofEpochMilli(value).atZone(UTC).toLocalTime();
    }

    private static LocalTime toLocalTime(ResultSet resultSet, int columnIndex)
            throws SQLException
    {
        return LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(resultSet.getTime(columnIndex).getTime()));
    }

    private Optional<TableStatistics> readTableStatistics(ConnectorSession session, JdbcTableHandle table)
            throws SQLException
    {
        if (table.getGroupingSets().isPresent()) {
            // TODO retrieve statistics for base table and derive statistics for the aggregation
            return Optional.empty();
        }

        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session));
                Handle handle = Jdbi.open(connection)) {
            Long rowCount = handle.createQuery("" +
                    "SELECT (" + // Verify we do not get ignore second row result, should there be any
                    "  SELECT ROW_COUNT " +
                    "  FROM information_schema.tables " +
                    "  WHERE table_catalog = :table_catalog " +
                    "  AND table_schema = :table_schema " +
                    "  AND table_name = :table_name " +
                    ")")
                    .bind("table_catalog", table.getCatalogName())
                    .bind("table_schema", table.getSchemaName())
                    .bind("table_name", table.getTableName())
                    .mapTo(Long.class)
                    .findOnly();

            if (rowCount == null) {
                return Optional.empty();
            }

            TableStatistics.Builder tableStatistics = TableStatistics.builder();
            tableStatistics.setRowCount(Estimate.of(rowCount));
            return Optional.of(tableStatistics.build());
        }
    }
}
