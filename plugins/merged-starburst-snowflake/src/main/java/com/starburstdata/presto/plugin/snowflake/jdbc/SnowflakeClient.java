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

import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.BaseJdbcStatisticsConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.LongWriteFunction;
import io.prestosql.plugin.jdbc.SliceReadFunction;
import io.prestosql.plugin.jdbc.SliceWriteFunction;
import io.prestosql.plugin.jdbc.StatsCollecting;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.predicate.Domain;
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

import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.jdbcTypeToPrestoType;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.prestosql.plugin.jdbc.WriteNullFunction.DEFAULT_WRITE_NULL_FUNCTION;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.sql.Types.VARCHAR;
import static java.time.ZoneOffset.UTC;

public class SnowflakeClient
        extends BaseJdbcClient
{
    public static final String IDENTIFIER_QUOTE = "\"";

    private static final LocalDate EPOCH_DAY = LocalDate.ofEpochDay(0);
    private static final java.time.format.DateTimeFormatter SNOWFLAKE_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX");
    private static final int SNOWFLAKE_MAX_LIST_EXPRESSIONS = 1000;

    private static final UnaryOperator<Domain> DISABLE_UNSUPPORTED_PUSHDOWN = domain -> {
        if (domain.getValues().getRanges().getRangeCount() <= SNOWFLAKE_MAX_LIST_EXPRESSIONS) {
            return domain;
        }
        return Domain.all(domain.getType());
    };

    private final boolean distributedConnector;

    SnowflakeClient(BaseJdbcConfig config, BaseJdbcStatisticsConfig statisticsConfig, @StatsCollecting ConnectionFactory connectionFactory, boolean distributedConnector)
    {
        super(config, statisticsConfig, IDENTIFIER_QUOTE, connectionFactory);
        this.distributedConnector = distributedConnector;
    }

    @Override
    public Connection getConnection(JdbcIdentity identity, JdbcSplit split)
            throws SQLException
    {
        return connectionFactory.openConnection(identity);
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
    public boolean isLimitGuaranteed()
    {
        return true;
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        return toPrestoType(session, typeHandle).map(mapping -> new ColumnMapping(
                mapping.getType(),
                mapping.getReadFunction(),
                mapping.getWriteFunction(),
                DEFAULT_WRITE_NULL_FUNCTION,
                DISABLE_UNSUPPORTED_PUSHDOWN));
    }

    private Optional<ColumnMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        String typeName = typeHandle.getJdbcTypeName().get();
        int columnSize = typeHandle.getColumnSize();

        // TODO: Snowflake uses -5 (BIGINT) type for NUMBER data type: https://github.com/snowflakedb/snowflake-jdbc/issues/154
        if (typeName.equals("NUMBER")) {
            int decimalDigits = typeHandle.getDecimalDigits();
            int precision = columnSize + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
            if (precision > Decimals.MAX_PRECISION) {
                return Optional.empty();
            }
            return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0))));
        }

        if (typeName.equals("VARIANT")) {
            return Optional.of(ColumnMapping.sliceMapping(createUnboundedVarcharType(), variantReadFunction(), varcharWriteFunction()));
        }

        if (typeName.equals("OBJECT") || typeName.equals("ARRAY")) {
            // TODO: better support for OBJECT (Presto MAP/ROW) and ARRAY (Presto ARRAY)
            return Optional.of(varcharColumnMapping(createUnboundedVarcharType()));
        }

        if (typeHandle.getJdbcType() == Types.TIME) {
            return Optional.of(timeColumnMapping(session));
        }

        if (typeHandle.getJdbcType() == Types.TIMESTAMP) {
            if (typeName.equals("TIMESTAMPTZ") || typeName.equals("TIMESTAMPLTZ")) {
                return Optional.of(timestampWithTimezoneColumnMapping());
            }
            return Optional.of(timestampColumnMapping(session));
        }

        if (typeHandle.getJdbcType() == VARCHAR && distributedConnector) {
            return Optional.of(varcharColumnMapping(createVarcharType(min(columnSize, HiveVarchar.MAX_VARCHAR_LENGTH))));
        }

        return jdbcTypeToPrestoType(session, typeHandle);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type instanceof CharType) {
            // Snowflake CHAR is an alias for VARCHAR so we need to pad value with spaces
            return WriteMapping.sliceMapping("char(" + ((CharType) type).getLength() + ")", charWriteFunction((CharType) type));
        }

        if (type instanceof TimeType) {
            return WriteMapping.longMapping("time", timeWriteFunction(session), DEFAULT_WRITE_NULL_FUNCTION);
        }

        if (type instanceof TimestampType) {
            return WriteMapping.longMapping("timestamp_ntz", timestampWriteFunction(session), DEFAULT_WRITE_NULL_FUNCTION);
        }

        if (type instanceof TimestampWithTimeZoneType) {
            return WriteMapping.longMapping("timestamp_tz", timestampWithTimezoneWriteFunction(), DEFAULT_WRITE_NULL_FUNCTION);
        }

        return super.toWriteMapping(session, type);
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
                timestampWithTimezoneWriteFunction());
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
        long millisSinceEpoch = resultSet.getBigDecimal(columnIndex)
                .scaleByPowerOfTen(3)
                // Presto doesn't support nanosecond precision
                .setScale(0, RoundingMode.HALF_UP)
                .longValueExact();
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(millisSinceEpoch), UTC);
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
        long nanoOfDay = resultSet.getBigDecimal(columnIndex)
                .scaleByPowerOfTen(9)
                .longValueExact();
        return LocalTime.ofNanoOfDay(nanoOfDay);
    }

    @Override
    protected Optional<TableStatistics> readTableStatistics(ConnectorSession session, JdbcTableHandle table)
            throws SQLException
    {
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
