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
import com.starburstdata.presto.plugin.toolkit.UtcTimeZoneCalendar;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.SliceReadFunction;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgDecimal;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCorr;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementCountDistinct;
import io.trino.plugin.jdbc.aggregation.ImplementCovariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementCovarianceSamp;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementRegrIntercept;
import io.trino.plugin.jdbc.aggregation.ImplementRegrSlope;
import io.trino.plugin.jdbc.aggregation.ImplementStddevPop;
import io.trino.plugin.jdbc.aggregation.ImplementStddevSamp;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.aggregation.ImplementVariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementVarianceSamp;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
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
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Chars;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.RealType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.getRootCause;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_NON_TRANSIENT_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultCharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromLongTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromTrinoTime;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.toTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
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
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;
import static java.sql.Types.BINARY;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CHAR;
import static java.sql.Types.DATE;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.REAL;
import static java.sql.Types.VARCHAR;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class SnowflakeClient
        extends BaseJdbcClient
{
    public static final String IDENTIFIER_QUOTE = "\"";

    private static final DateTimeFormatter SNOWFLAKE_DATE_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd");
    private static final DateTimeFormatter SNOWFLAKE_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("y-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX");
    private static final DateTimeFormatter SNOWFLAKE_TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("y-MM-dd'T'HH:mm:ss.SSSSSSSSS");
    private static final int SNOWFLAKE_MAX_TIMESTAMP_PRECISION = 9;
    public static final int SNOWFLAKE_MAX_LIST_EXPRESSIONS = 1000;

    private static final Map<Type, WriteMapping> WRITE_MAPPINGS = ImmutableMap.<Type, WriteMapping>builder()
            .put(BooleanType.BOOLEAN, WriteMapping.booleanMapping("boolean", booleanWriteFunction()))
            .put(BIGINT, WriteMapping.longMapping("number(19)", bigintWriteFunction()))
            .put(INTEGER, WriteMapping.longMapping("number(10)", integerWriteFunction()))
            .put(SMALLINT, WriteMapping.longMapping("number(5)", smallintWriteFunction()))
            .put(TINYINT, WriteMapping.longMapping("number(3)", tinyintWriteFunction()))
            .put(DoubleType.DOUBLE, WriteMapping.doubleMapping("double precision", doubleWriteFunction()))
            .put(RealType.REAL, WriteMapping.longMapping("real", realWriteFunction()))
            .put(VARBINARY, WriteMapping.sliceMapping("varbinary", varbinaryWriteFunction()))
            .put(DateType.DATE, WriteMapping.longMapping("date", dateWriteFunctionUsingString()))
            .build();
    private static final UtcTimeZoneCalendar UTC_TZ_PASSING_CALENDAR = UtcTimeZoneCalendar.getUtcTimeZoneCalendarInstance();
    private static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone(ZoneId.of("UTC"));

    private final AggregateFunctionRewriter<JdbcExpression> aggregateFunctionRewriter;
    private final boolean statisticsEnabled;
    private final TableScanRedirection tableScanRedirection;
    private final boolean distributedConnector;

    public SnowflakeClient(
            BaseJdbcConfig config,
            JdbcStatisticsConfig statisticsConfig,
            TableScanRedirection tableScanRedirection,
            ConnectionFactory connectionFactory,
            boolean distributedConnector,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping)
    {
        super(config, IDENTIFIER_QUOTE, connectionFactory, queryBuilder, identifierMapping);
        this.statisticsEnabled = requireNonNull(statisticsConfig, "statisticsConfig is null").isEnabled();
        this.tableScanRedirection = requireNonNull(tableScanRedirection, "tableScanRedirection is null");
        this.distributedConnector = distributedConnector;
        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                this::quoted,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementCountDistinct(bigintTypeHandle, true))
                        .add(new ImplementMinMax(true))
                        .add(new ImplementSum(SnowflakeClient::decimalTypeHandle))
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementStddevSamp())
                        .add(new ImplementStddevPop())
                        .add(new ImplementVarianceSamp())
                        .add(new ImplementVariancePop())
                        .add(new ImplementCovarianceSamp())
                        .add(new ImplementCovariancePop())
                        .add(new ImplementCorr())
                        .add(new ImplementRegrIntercept())
                        .add(new ImplementRegrSlope())
                        .build());
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
    public PreparedQuery applyQueryTransformations(JdbcTableHandle tableHandle, PreparedQuery query)
    {
        return super.applyQueryTransformations(tableHandle, query);
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> getTableScanRedirection(ConnectorSession session, JdbcTableHandle handle)
    {
        return tableScanRedirection.getTableScanRedirection(session, handle, this);
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.of(TopNFunction.sqlStandard(this::quoted));
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        // The data returned conforms to TopN requirements, but can be returned out of order
        return !distributedConnector;
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String typeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        if (typeHandle.getJdbcType() == BOOLEAN) {
            return Optional.of(booleanColumnMapping());
        }

        if (typeHandle.getJdbcType() == Types.TINYINT) {
            return Optional.of(tinyintColumnMapping());
        }

        if (typeHandle.getJdbcType() == Types.SMALLINT) {
            return Optional.of(smallintColumnMapping());
        }

        if (typeHandle.getJdbcType() == Types.INTEGER) {
            return Optional.of(integerColumnMapping());
        }

        if (typeHandle.getJdbcType() == Types.BIGINT) {
            return Optional.of(bigintColumnMapping());
        }

        if (typeHandle.getJdbcType() == REAL) {
            return Optional.of(realColumnMapping());
        }

        if (typeHandle.getJdbcType() == FLOAT || typeHandle.getJdbcType() == DOUBLE) {
            return Optional.of(doubleColumnMapping());
        }

        if (typeName.equals("NUMBER")) {
            int decimalDigits = typeHandle.getRequiredDecimalDigits();
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
            return Optional.of(ColumnMapping.sliceMapping(createUnboundedVarcharType(), varcharReadFunction(createUnboundedVarcharType()), varcharWriteFunction(), DISABLE_PUSHDOWN));
        }

        if (typeHandle.getJdbcType() == DATE) {
            return Optional.of(ColumnMapping.longMapping(
                    DateType.DATE,
                    ResultSet::getLong,
                    dateWriteFunctionUsingString()));
        }

        if (typeHandle.getJdbcType() == Types.TIME) {
            return Optional.of(updatePushdownCotroller(timeColumnMapping()));
        }

        if (typeHandle.getJdbcType() == Types.TIMESTAMP) {
            if (typeName.equals("TIMESTAMPTZ") || typeName.equals("TIMESTAMPLTZ")) {
                return Optional.of(timestampWithTimezoneColumnMapping(getSupportedTimestampPrecision(typeHandle.getRequiredDecimalDigits())));
            }
            return Optional.of(timestampColumnMapping(getSupportedTimestampPrecision(typeHandle.getRequiredDecimalDigits())));
        }

        if (typeHandle.getJdbcType() == CHAR) {
            return Optional.of(defaultCharColumnMapping(typeHandle.getRequiredColumnSize(), true));
        }

        if (typeHandle.getJdbcType() == VARCHAR) {
            // The max varchar size is different between distributed connector and JDBC connector
            if (distributedConnector) {
                return Optional.of(varcharColumnMapping(createVarcharType(min(typeHandle.getRequiredColumnSize(), HiveVarchar.MAX_VARCHAR_LENGTH)), true));
            }
            return Optional.of(defaultVarcharColumnMapping(typeHandle.getRequiredColumnSize(), true));
        }

        if (typeHandle.getJdbcType() == BINARY) {
            return Optional.of(varbinaryColumnMapping());
        }

        return Optional.empty();
    }

    private int getSupportedTimestampPrecision(int precision)
    {
        // Coerce the timestamp precision to 3 in case of distributed mode because the footer of Parquet files generated by Snowflake always have TIMESTAMP_MILLIS for timestamp type
        // https://community.snowflake.com/s/article/Nano-second-precision-lost-after-Parquet-file-Unload
        return distributedConnector ? 3 : precision;
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
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }

        if (type instanceof CharType) {
            // Snowflake CHAR is an alias for VARCHAR so we need to pad value with spaces
            return WriteMapping.sliceMapping("char(" + ((CharType) type).getLength() + ")", charWriteFunction((CharType) type));
        }

        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "varchar";
            }
            else {
                dataType = "varchar(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }

        if (type instanceof TimeType) {
            return WriteMapping.longMapping("time", timeWriteFunction());
        }

        if (type instanceof TimestampType) {
            TimestampType timestampType = (TimestampType) type;
            checkArgument(timestampType.getPrecision() <= SNOWFLAKE_MAX_TIMESTAMP_PRECISION, "The max timestamp precision in Snowflake is 9");
            if (timestampType.isShort()) {
                return WriteMapping.longMapping(format("timestamp_ntz(%d)", timestampType.getPrecision()), timestampWriteFunction());
            }
            return WriteMapping.objectMapping(format("timestamp_ntz(%d)", timestampType.getPrecision()), longTimestampWriteFunction(timestampType.getPrecision()));
        }

        if (type instanceof TimestampWithTimeZoneType) {
            TimestampWithTimeZoneType timestampWithTimeZoneType = (TimestampWithTimeZoneType) type;
            checkArgument(timestampWithTimeZoneType.getPrecision() <= SNOWFLAKE_MAX_TIMESTAMP_PRECISION, "The max timestamp precision in Snowflake is 9");
            if (timestampWithTimeZoneType.isShort()) {
                return WriteMapping.longMapping(format("timestamp_tz(%d)", timestampWithTimeZoneType.getPrecision()), timestampWithTimezoneWriteFunction());
            }
            return WriteMapping.objectMapping(format("timestamp_tz(%d)", timestampWithTimeZoneType.getPrecision()), longTimestampWithTimezoneWriteFunction());
        }

        WriteMapping writeMapping = WRITE_MAPPINGS.get(type);
        if (writeMapping != null) {
            return writeMapping;
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
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

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        return true;
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

    private static LongWriteFunction dateWriteFunctionUsingString()
    {
        return (statement, index, day) -> statement.setString(index, SNOWFLAKE_DATE_FORMATTER.format(LocalDate.ofEpochDay(day)));
    }

    private static ColumnMapping timestampWithTimezoneColumnMapping(int precision)
    {
        checkArgument(precision <= SNOWFLAKE_MAX_TIMESTAMP_PRECISION, "The max timestamp precision in Snowflake is 9");

        if (precision <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
            return ColumnMapping.longMapping(
                    createTimestampWithTimeZoneType(precision),
                    (resultSet, columnIndex) -> {
                        ZonedDateTime timestamp = SNOWFLAKE_DATE_TIME_FORMATTER.parse(resultSet.getString(columnIndex), ZonedDateTime::from);
                        return packDateTimeWithZone(
                                timestamp.toInstant().toEpochMilli(),
                                timestamp.getZone().getId());
                    },
                    timestampWithTimezoneWriteFunction(),
                    FULL_PUSHDOWN);
        }

        return ColumnMapping.objectMapping(
                createTimestampWithTimeZoneType(precision),
                longTimestampWithTimezoneReadFunction(),
                longTimestampWithTimezoneWriteFunction());
    }

    private static LongWriteFunction timestampWithTimezoneWriteFunction()
    {
        return (statement, index, encodedTimeWithZone) -> {
            Instant time = Instant.ofEpochMilli(unpackMillisUtc(encodedTimeWithZone));
            ZoneId zone = ZoneId.of(unpackZoneKey(encodedTimeWithZone).getId());
            statement.setString(index, SNOWFLAKE_DATE_TIME_FORMATTER.format(time.atZone(zone)));
        };
    }

    private static ColumnMapping timestampColumnMapping(int precision)
    {
        checkArgument(precision <= SNOWFLAKE_MAX_TIMESTAMP_PRECISION, "The max timestamp precision in Snowflake is 9");

        if (precision <= TimestampType.MAX_SHORT_PRECISION) {
            return ColumnMapping.longMapping(
                    createTimestampType(precision),
                    (resultSet, columnIndex) -> toTrinoTimestamp(createTimestampType(precision), toLocalDateTime(resultSet, columnIndex)),
                    timestampWriteFunction());
        }

        return ColumnMapping.objectMapping(
                createTimestampType(precision),
                longTimestampReadFunction(),
                longTimestampWriteFunction(precision));
    }

    private static ObjectReadFunction longTimestampReadFunction()
    {
        return ObjectReadFunction.of(LongTimestamp.class, (resultSet, columnIndex) -> {
            Timestamp timestamp = resultSet.getTimestamp(columnIndex, newUtcCalendar());

            long epochMillis = timestamp.getTime();
            int nanosOfSecond = timestamp.getNanos();
            int nanosOfMilli = nanosOfSecond % NANOSECONDS_PER_MILLISECOND;

            long epochMicros = epochMillis * MICROSECONDS_PER_MILLISECOND + nanosOfMilli / NANOSECONDS_PER_MICROSECOND;
            int picosOfMicro = nanosOfMilli % NANOSECONDS_PER_MICROSECOND * PICOSECONDS_PER_NANOSECOND;

            return new LongTimestamp(epochMicros, picosOfMicro);
        });
    }

    // Note: allocating a new Calendar per row may turn out to be too expensive.
    private static Calendar newUtcCalendar()
    {
        Calendar calendar = new GregorianCalendar(UTC_TIME_ZONE, ENGLISH);
        calendar.setTime(new Date(0));
        return calendar;
    }

    private static ObjectWriteFunction longTimestampWriteFunction(int precision)
    {
        return ObjectWriteFunction.of(
                LongTimestamp.class,
                (statement, index, value) -> statement.setString(index, SNOWFLAKE_TIMESTAMP_FORMATTER.format(fromLongTrinoTimestamp(value, precision))));
    }

    private static ObjectReadFunction longTimestampWithTimezoneReadFunction()
    {
        return ObjectReadFunction.of(
                LongTimestampWithTimeZone.class,
                (resultSet, columnIndex) -> {
                    ZonedDateTime timestamp = SNOWFLAKE_DATE_TIME_FORMATTER.parse(resultSet.getString(columnIndex), ZonedDateTime::from);
                    return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(
                            timestamp.toEpochSecond(),
                            (long) timestamp.getNano() * PICOSECONDS_PER_NANOSECOND,
                            TimeZoneKey.getTimeZoneKey(timestamp.getZone().getId()));
                });
    }

    private static ObjectWriteFunction longTimestampWithTimezoneWriteFunction()
    {
        return ObjectWriteFunction.of(
                LongTimestampWithTimeZone.class,
                (statement, index, value) -> {
                    long epochMillis = value.getEpochMillis();
                    long epochSeconds = floorDiv(epochMillis, MILLISECONDS_PER_SECOND);
                    int nanoAdjustment = floorMod(epochMillis, MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND + value.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND;
                    ZoneId zoneId = getTimeZoneKey(value.getTimeZoneKey()).getZoneId();
                    Instant instant = Instant.ofEpochSecond(epochSeconds, nanoAdjustment);
                    statement.setString(index, SNOWFLAKE_DATE_TIME_FORMATTER.format(ZonedDateTime.ofInstant(instant, zoneId)));
                });
    }

    private static LongWriteFunction timestampWriteFunction()
    {
        return (statement, index, value) -> statement.setString(index, fromTrinoTimestamp(value).toString());
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
        return (statement, index, value) -> statement.setString(index, fromTrinoTime(value).toString());
    }

    private static LocalDateTime toLocalDateTime(ResultSet resultSet, int columnIndex)
            throws SQLException
    {
        Timestamp ts = resultSet.getTimestamp(columnIndex, UTC_TZ_PASSING_CALENDAR);
        return LocalDateTime.ofEpochSecond(floorDiv(ts.getTime(), MILLISECONDS_PER_SECOND), ts.getNanos(), UTC);
    }

    private static long toPrestoTime(Time sqlTime)
    {
        return PICOSECONDS_PER_MILLISECOND * sqlTime.getTime();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, TupleDomain<ColumnHandle> tupleDomain)
    {
        if (!statisticsEnabled) {
            return TableStatistics.empty();
        }
        if (!handle.isNamedRelation()) {
            return TableStatistics.empty();
        }
        try {
            return readTableStatistics(session, handle);
        }
        catch (SQLException | RuntimeException e) {
            throwIfInstanceOf(e, TrinoException.class);
            throwIfInvalidWarehouse(e);
            throw new TrinoException(JDBC_ERROR, "Failed fetching statistics for table: " + handle, e);
        }
    }

    private TableStatistics readTableStatistics(ConnectorSession session, JdbcTableHandle table)
            throws SQLException
    {
        checkArgument(table.isNamedRelation(), "Relation is not a table: %s", table);

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
                return TableStatistics.empty();
            }

            TableStatistics.Builder tableStatistics = TableStatistics.builder();
            tableStatistics.setRowCount(Estimate.of(rowCount));
            return tableStatistics.build();
        }
    }

    private static void throwIfInvalidWarehouse(Throwable throwable)
    {
        Throwable rootCause = getRootCause(throwable);
        if (rootCause instanceof SQLException && ((SQLException) rootCause).getErrorCode() == 606) {
            throw new TrinoException(JDBC_NON_TRANSIENT_ERROR, "Could not query Snowflake due to invalid warehouse configuration. " +
                    "Fix configuration or select an active Snowflake warehouse with 'warehouse' catalog session property.", throwable);
        }
    }
}
