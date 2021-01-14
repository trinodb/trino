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
import com.google.common.collect.ImmutableSet;
import com.starburstdata.presto.plugin.jdbc.redirection.TableScanRedirection;
import com.starburstdata.presto.plugin.jdbc.stats.JdbcStatisticsConfig;
import com.starburstdata.presto.plugin.jdbc.stats.TableStatisticsClient;
import com.starburstdata.presto.plugin.toolkit.UtcTimeZoneCalendar;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.SliceReadFunction;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.expression.AggregateFunctionRewriter;
import io.trino.plugin.jdbc.expression.ImplementAvgDecimal;
import io.trino.plugin.jdbc.expression.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.expression.ImplementCount;
import io.trino.plugin.jdbc.expression.ImplementCountAll;
import io.trino.plugin.jdbc.expression.ImplementMinMax;
import io.trino.plugin.jdbc.expression.ImplementSum;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Chars;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromPrestoTime;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromPrestoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.jdbcTypeToPrestoType;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.toPrestoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.math.RoundingMode.UNNECESSARY;
import static java.sql.Types.BINARY;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

public class SnowflakeClient
        extends BaseJdbcClient
{
    public static final String IDENTIFIER_QUOTE = "\"";

    private static final DateTimeFormatter SNOWFLAKE_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("y-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX");
    public static final int SNOWFLAKE_MAX_LIST_EXPRESSIONS = 1000;

    private static final Map<Type, WriteMapping> WRITE_MAPPINGS = ImmutableMap.<Type, WriteMapping>builder()
            .put(BIGINT, WriteMapping.longMapping("number(19)", bigintWriteFunction()))
            .put(INTEGER, WriteMapping.longMapping("number(10)", integerWriteFunction()))
            .put(SMALLINT, WriteMapping.longMapping("number(5)", smallintWriteFunction()))
            .put(TINYINT, WriteMapping.longMapping("number(3)", tinyintWriteFunction()))
            .build();
    private static final UtcTimeZoneCalendar UTC_TZ_PASSING_CALENDAR = UtcTimeZoneCalendar.getUtcTimeZoneCalendarInstance();

    private final AggregateFunctionRewriter aggregateFunctionRewriter;
    private final TableStatisticsClient tableStatisticsClient;
    private final TableScanRedirection tableScanRedirection;
    private final boolean distributedConnector;

    public SnowflakeClient(
            BaseJdbcConfig config,
            JdbcStatisticsConfig statisticsConfig,
            TableScanRedirection tableScanRedirection,
            ConnectionFactory connectionFactory,
            boolean distributedConnector)
    {
        super(config, IDENTIFIER_QUOTE, connectionFactory);
        this.tableStatisticsClient = new TableStatisticsClient(this::readTableStatistics, statisticsConfig);
        this.tableScanRedirection = requireNonNull(tableScanRedirection, "tableScanRedirection is null");
        this.distributedConnector = distributedConnector;
        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter(
                this::quoted,
                ImmutableSet.of(
                        new ImplementCountAll(bigintTypeHandle),
                        new ImplementCount(bigintTypeHandle),
                        new ImplementMinMax(),
                        new ImplementSum(SnowflakeClient::decimalTypeHandle),
                        new ImplementAvgDecimal(),
                        new ImplementAvgFloatingPoint()));
    }

    private static Optional<JdbcTypeHandle> decimalTypeHandle(DecimalType decimalType)
    {
        return Optional.of(
                new JdbcTypeHandle(
                        Types.NUMERIC,
                        Optional.of("NUMBER"),
                        Optional.of(decimalType.getPrecision()),
                        Optional.of(decimalType.getScale()),
                        Optional.empty(),
                        Optional.empty()));
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcSplit split)
            throws SQLException
    {
        return connectionFactory.openConnection(session);
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> getTableScanRedirection(ConnectorSession session, JdbcTableHandle handle)
    {
        return tableScanRedirection.getTableScanRedirection(session, handle, this);
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
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String typeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        if (typeName.equals("NUMBER")) {
            int decimalDigits = typeHandle.getDecimalDigits().orElseThrow(() -> new IllegalStateException("decimal digits not present"));
            int precision = typeHandle.getRequiredColumnSize() + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
            if (precision > Decimals.MAX_PRECISION) {
                return Optional.empty();
            }
            return Optional.of(updatePushdownCotroller(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0)), UNNECESSARY)));
        }

        if (typeName.equals("VARIANT")) {
            return Optional.of(ColumnMapping.sliceMapping(createUnboundedVarcharType(), variantReadFunction(), varcharWriteFunction(), FULL_PUSHDOWN));
        }

        if (typeName.equals("OBJECT") || typeName.equals("ARRAY")) {
            // TODO: better support for OBJECT (Presto MAP/ROW) and ARRAY (Presto ARRAY)
            return Optional.of(updatePushdownCotroller(varcharColumnMapping(createUnboundedVarcharType())));
        }

        if (typeHandle.getJdbcType() == Types.TIME) {
            return Optional.of(updatePushdownCotroller(timeColumnMapping()));
        }

        if (typeHandle.getJdbcType() == Types.TIMESTAMP) {
            if (typeName.equals("TIMESTAMPTZ") || typeName.equals("TIMESTAMPLTZ")) {
                return Optional.of(timestampWithTimezoneColumnMapping());
            }
            return Optional.of(timestampColumnMapping());
        }

        if (typeHandle.getJdbcType() == VARCHAR && distributedConnector) {
            // TODO test varchar values exceeding HiveVarchar.MAX_VARCHAR_LENGTH (65535)
            return Optional.of(updatePushdownCotroller(varcharColumnMapping(createVarcharType(min(typeHandle.getRequiredColumnSize(), HiveVarchar.MAX_VARCHAR_LENGTH)))));
        }

        if (typeHandle.getJdbcType() == VARBINARY || typeHandle.getJdbcType() == BINARY || typeHandle.getJdbcType() == LONGVARBINARY) {
            return Optional.of(varbinaryColumnMapping());
        }

        return jdbcTypeToPrestoType(typeHandle).map(this::updatePushdownCotroller);
    }

    private ColumnMapping updatePushdownCotroller(ColumnMapping mapping)
    {
        verify(mapping.getPredicatePushdownController() != DISABLE_PUSHDOWN);
        return new ColumnMapping(
                mapping.getType(),
                mapping.getReadFunction(),
                mapping.getWriteFunction(),
                FULL_PUSHDOWN);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type instanceof CharType) {
            // Snowflake CHAR is an alias for VARCHAR so we need to pad value with spaces
            return WriteMapping.sliceMapping("char(" + ((CharType) type).getLength() + ")", charWriteFunction((CharType) type));
        }

        if (type instanceof TimeType) {
            return WriteMapping.longMapping("time", timeWriteFunction());
        }

        if (type instanceof TimestampType) {
            return WriteMapping.longMapping("timestamp_ntz", timestampWriteFunction());
        }

        if (type instanceof TimestampWithTimeZoneType) {
            return WriteMapping.longMapping("timestamp_tz", timestampWithTimezoneWriteFunction());
        }

        WriteMapping writeMapping = WRITE_MAPPINGS.get(type);
        if (writeMapping != null) {
            return writeMapping;
        }

        // TODO provide explicit write type mappings
        return legacyToWriteMapping(session, type);
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
                throw new TrinoException(NOT_SUPPORTED, "Snowflake columns cannot contain quotes: " + columnMetadata.getName());
            }
        });
    }

    @SuppressWarnings("UnnecessaryLambda")
    private static SliceReadFunction variantReadFunction()
    {
        return (resultSet, columnIndex) -> utf8Slice(resultSet.getString(columnIndex).replaceAll("^\"|\"$", ""));
    }

    @SuppressWarnings("UnnecessaryLambda")
    private static SliceWriteFunction charWriteFunction(CharType charType)
    {
        return (statement, index, value) -> statement.setString(index, Chars.padSpaces(value, charType).toStringUtf8());
    }

    private static ColumnMapping timestampWithTimezoneColumnMapping()
    {
        return ColumnMapping.longMapping(
                TIMESTAMP_TZ_MILLIS,
                (resultSet, columnIndex) -> {
                    ZonedDateTime timestamp = SNOWFLAKE_DATE_TIME_FORMATTER.parse(resultSet.getString(columnIndex), ZonedDateTime::from);
                    return packDateTimeWithZone(
                            timestamp.toInstant().toEpochMilli(),
                            timestamp.getZone().getId());
                },
                timestampWithTimezoneWriteFunction(),
                FULL_PUSHDOWN);
    }

    private static LongWriteFunction timestampWithTimezoneWriteFunction()
    {
        return (statement, index, encodedTimeWithZone) -> {
            Instant time = Instant.ofEpochMilli(unpackMillisUtc(encodedTimeWithZone));
            ZoneId zone = ZoneId.of(unpackZoneKey(encodedTimeWithZone).getId());
            statement.setString(index, SNOWFLAKE_DATE_TIME_FORMATTER.format(time.atZone(zone)));
        };
    }

    private static ColumnMapping timestampColumnMapping()
    {
        return ColumnMapping.longMapping(
                TIMESTAMP_MILLIS,
                (resultSet, columnIndex) -> toPrestoTimestamp(TIMESTAMP_MILLIS, toLocalDateTime(resultSet, columnIndex)),
                timestampWriteFunction());
    }

    private static LongWriteFunction timestampWriteFunction()
    {
        return (statement, index, value) -> statement.setString(index, fromPrestoTimestamp(value).toString());
    }

    private static ColumnMapping timeColumnMapping()
    {
        return ColumnMapping.longMapping(
                TIME,
                (resultSet, columnIndex) -> toPrestoTime(resultSet.getTime(columnIndex)),
                timeWriteFunction());
    }

    private static LongWriteFunction timeWriteFunction()
    {
        return (statement, index, value) -> statement.setString(index, fromPrestoTime(value).toString());
    }

    private static LocalDateTime toLocalDateTime(ResultSet resultSet, int columnIndex)
            throws SQLException
    {
        Timestamp ts = resultSet.getTimestamp(columnIndex, UTC_TZ_PASSING_CALENDAR);
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(ts.getTime()), UTC);
    }

    private static long toPrestoTime(Time sqlTime)
    {
        return PICOSECONDS_PER_MILLISECOND * sqlTime.getTime();
    }

    private Optional<TableStatistics> readTableStatistics(ConnectorSession session, JdbcTableHandle table)
            throws SQLException
    {
        if (table.getGroupingSets().isPresent()) {
            // TODO retrieve statistics for base table and derive statistics for the aggregation
            return Optional.empty();
        }

        try (Connection connection = connectionFactory.openConnection(session);
                Handle handle = Jdbi.open(connection)) {
            Long rowCount = handle.createQuery("" +
                    "SELECT (" + // Verify we do not ignore second result row, should there be any
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
