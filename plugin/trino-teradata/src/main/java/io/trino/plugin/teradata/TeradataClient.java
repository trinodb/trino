/**
 * Unpublished work.
 * Copyright 2025 by Teradata Corporation. All rights reserved
 * TERADATA CORPORATION CONFIDENTIAL AND TRADE SECRET
 */

package io.trino.plugin.teradata;

import com.google.inject.Inject;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.CaseSensitivity;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcMetadata;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.PredicatePushdownController;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
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
import io.trino.plugin.jdbc.expression.ComparisonOperator;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.expression.RewriteCaseSensitiveComparison;
import io.trino.plugin.jdbc.expression.RewriteIn;
import io.trino.plugin.jdbc.expression.RewriteLikeEscapeWithCaseSensitivity;
import io.trino.plugin.jdbc.expression.RewriteLikeWithCaseSensitivity;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.Domain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.weakref.jmx.$internal.guava.collect.ImmutableMap;
import org.weakref.jmx.$internal.guava.collect.ImmutableSet;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_INSENSITIVE;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_SENSITIVE;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcJoinPushdownUtil.implementJoinCostAware;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.getDomainCompactionThreshold;
import static io.trino.plugin.jdbc.PredicatePushdownController.CASE_INSENSITIVE_CHARACTER_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateColumnMappingUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromTrinoTime;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.floorDiv;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

/**
 * TeradataClient is a JDBC client implementation for the Teradata database.
 * It extends BaseJdbcClient to provide Teradata-specific handling for
 * data types, case sensitivity, and SQL expression rewriting.
 * <p>
 * This client supports reading data from Teradata but does not support
 * schema creation or delete operations.
 * </p>
 * <p>
 * It includes custom mappings for Teradata-specific types such as
 * TIMESTAMP WITH TIME ZONE and TIME WITH TIME ZONE, as well as JSON.
 * </p>
 * <p>
 * The client also defines how case sensitivity is handled based on the Teradata
 * JDBC case sensitivity setting.
 * </p>
 *
 * <p>Unpublished work. Copyright 2025 by Teradata Corporation. All rights reserved.</p>
 */
public class TeradataClient
        extends BaseJdbcClient
{
    private static final PredicatePushdownController TERADATA_STRING_PUSHDOWN = (session, domain) -> {
        // 1. NULL-only filters are always safe
        if (domain.isOnlyNull()) {
            return FULL_PUSHDOWN.apply(session, domain);
        }

        // 2. If pushdown is explicitly enabled via session
        if (isEnableTeradataStringPushdown(session)) {
            return FULL_PUSHDOWN.apply(session, domain);
        }

        // 3. Simplify complex domains (like IN(...), ranges)
        Domain simplified = domain.simplify(getDomainCompactionThreshold(session));

        // 4. If this is a clean range predicate (e.g., <, <=, BETWEEN), push down
        if (isRangePredicate(simplified)) {
            return FULL_PUSHDOWN.apply(session, simplified);
        }

        // 5. Handle NOT NULL (non-null allowed and domain not only NULL)
        if (!domain.isOnlyNull() && !domain.isNullAllowed() && simplified.getValues().isAll()) {
            return FULL_PUSHDOWN.apply(session, simplified);
        }

        // 6. Handle NOT EQUAL and similar ranges (if safe range is expressible)
        if (isMultiRangePredicate(simplified)) {
            return FULL_PUSHDOWN.apply(session, simplified);
        }

        // 7. Push down discrete sets like IN ('a', 'b', 'c')
        if (simplified.getValues().isDiscreteSet()) {
            return FULL_PUSHDOWN.apply(session, simplified);
        }

        // 8. If nothing matched, fallback to no pushdown
        return DISABLE_PUSHDOWN.apply(session, domain);
    };
    private static final long DEFAULT_FALLBACK_NDV = 100_000L;
    private static final long MAX_FALLBACK_NDV = 1_000_000L; // max fallback NDV cap
    private static final double DEFAULT_FALLBACK_FRACTION = 0.1; // fallback = 10% of row count
    private final TeradataConfig.TeradataCaseSensitivity teradataJDBCCaseSensitivity;
    private final boolean statisticsEnabled;
    private ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;

    /**
     * Constructs a new TeradataClient instance.
     *
     * @param config base JDBC configuration
     * @param teradataConfig Teradata-specific configuration
     * @param connectionFactory factory to create JDBC connections
     * @param queryBuilder query builder for SQL queries
     * @param identifierMapping mapping for identifiers such as column names
     * @param remoteQueryModifier optional modifier for remote queries
     */
    @Inject
    public TeradataClient(BaseJdbcConfig config, TeradataConfig teradataConfig, JdbcStatisticsConfig statisticsConfig, ConnectionFactory connectionFactory, QueryBuilder queryBuilder, IdentifierMapping identifierMapping, RemoteQueryModifier remoteQueryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, remoteQueryModifier, true);
        this.teradataJDBCCaseSensitivity = teradataConfig.getTeradataCaseSensitivity();
        this.statisticsEnabled = statisticsConfig.isEnabled();
        buildExpressionRewriter();
        buildAggregateRewriter();
    }

    private static boolean isRangePredicate(Domain domain)
    {
        return !domain.isSingleValue() &&
                !domain.isAll() &&
                !domain.getValues().isDiscreteSet() &&
                !domain.getValues().isNone() &&
                domain.getValues().getRanges().getOrderedRanges().size() == 1;
    }

    private static boolean isMultiRangePredicate(Domain domain)
    {
        return !domain.getValues().isDiscreteSet() &&
                domain.getValues().getRanges().getOrderedRanges().size() > 1 &&
                !domain.getValues().isAll() &&
                !domain.getValues().isNone();
    }

    private static boolean isEnableTeradataStringPushdown(ConnectorSession session)
    {
        return session.getProperty("string_pushdown_enabled", Boolean.class);
    }

    /**
     * Creates a ColumnMapping for Teradata TIME type with specified precision.
     *
     * @param precision fractional seconds precision for the TIME column
     * @return ColumnMapping instance for TIME type
     */
    public static ColumnMapping timeColumnMapping(int precision)
    {
        TimeType timeType = createTimeType(precision);
        return ColumnMapping.longMapping(timeType, timeReadFunction(timeType), timeWriteFunction(precision), DISABLE_PUSHDOWN);
    }

    /**
     * Returns a function to read TIME values from JDBC result set,
     * converting SQL Timestamp to Trino's internal representation.
     *
     * @param timeType Trino TimeType
     * @return LongReadFunction for TIME values
     */
    public static LongReadFunction timeReadFunction(TimeType timeType)
    {
        requireNonNull(timeType, "timeType is null");
        return (resultSet, columnIndex) -> {
            Timestamp sqlTimestamp = resultSet.getTimestamp(columnIndex);
            LocalTime localTime = sqlTimestamp.toLocalDateTime().toLocalTime();
            long nsOfDay = localTime.toNanoOfDay();
            long picosOfDay = nsOfDay * PICOSECONDS_PER_NANOSECOND;
            long rounded = round(picosOfDay, 12 - timeType.getPrecision());
            if (rounded == PICOSECONDS_PER_DAY) {
                rounded = 0;
            }
            return rounded;
        };
    }

    /**
     * Returns a function to write TIME values to JDBC PreparedStatement,
     * converting Trino internal representation to JDBC object.
     *
     * @param precision fractional seconds precision
     * @return LongWriteFunction for TIME values
     */
    public static LongWriteFunction timeWriteFunction(int precision)
    {
        return LongWriteFunction.of(Types.TIME, (statement, index, picosOfDay) -> {
            picosOfDay = round(picosOfDay, 12 - precision);
            if (picosOfDay == PICOSECONDS_PER_DAY) {
                picosOfDay = 0;
            }
            statement.setObject(index, fromTrinoTime(picosOfDay));
        });
    }

    /**
     * Creates a ColumnMapping for Teradata TIME WITH TIME ZONE type with specified precision.
     *
     * @param precision fractional seconds precision
     * @return ColumnMapping instance for TIME WITH TIME ZONE type
     */
    public static ColumnMapping timeWithTimeZoneColumnMapping(int precision)
    {
        return ColumnMapping.longMapping(createTimeWithTimeZoneType(precision), shortTimeWithTimeZoneReadFunction(), shortTimeWithTimeZoneWriteFunction(), DISABLE_PUSHDOWN);
    }

    /**
     * Reads TIME WITH TIME ZONE values from JDBC ResultSet.
     *
     * @return LongReadFunction for TIME WITH TIME ZONE values
     */
    private static LongReadFunction shortTimeWithTimeZoneReadFunction()
    {
        return (resultSet, columnIndex) -> {
            Calendar calendar = Calendar.getInstance();
            Timestamp sqlTimestamp = resultSet.getTimestamp(columnIndex, calendar);
            LocalDateTime localDateTime = sqlTimestamp.toLocalDateTime();
            ZoneId zone = ZoneId.of(calendar.getTimeZone().getID());
            ZonedDateTime zdt = ZonedDateTime.of(localDateTime, zone);
            int offsetMinutes = zdt.getOffset().getTotalSeconds() / 60;
            long nanos = localDateTime.getLong(ChronoField.NANO_OF_DAY);
            return packTimeWithTimeZone(nanos, offsetMinutes);
        };
    }

    /**
     * Writes TIME WITH TIME ZONE values to JDBC PreparedStatement.
     *
     * @return LongWriteFunction for TIME WITH TIME ZONE values
     */
    private static LongWriteFunction shortTimeWithTimeZoneWriteFunction()
    {
        return (statement, index, value) -> {
            long millisUtc = unpackMillisUtc(value);
            TimeZoneKey timeZoneKey = unpackZoneKey(value);
            statement.setObject(index, OffsetTime.ofInstant(Instant.ofEpochMilli(millisUtc), timeZoneKey.getZoneId()));
        };
    }

    /**
     * Creates a ColumnMapping for Teradata TIMESTAMP WITH TIME ZONE type with specified precision.
     *
     * @param precision fractional seconds precision
     * @return ColumnMapping instance for TIMESTAMP WITH TIME ZONE type
     */
    public static ColumnMapping timestampWithTimeZoneColumnMapping(int precision)
    {
        if (precision <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
            return ColumnMapping.longMapping(createTimestampWithTimeZoneType(precision), shortTimestampWithTimeZoneReadFunction(), shortTimestampWithTimeZoneWriteFunction(), DISABLE_PUSHDOWN);
        }
        return ColumnMapping.objectMapping(createTimestampWithTimeZoneType(precision), longTimestampWithTimeZoneReadFunction(), longTimestampWithTimeZoneWriteFunction(), DISABLE_PUSHDOWN);
    }

    /**
     * Reads TIMESTAMP WITH TIME ZONE values with short precision from JDBC ResultSet.
     *
     * @return LongReadFunction for short precision TIMESTAMP WITH TIME ZONE values
     */
    private static LongReadFunction shortTimestampWithTimeZoneReadFunction()
    {
        return (resultSet, columnIndex) -> {
            Calendar calendar = Calendar.getInstance();
            Timestamp sqlTimestamp = resultSet.getTimestamp(columnIndex, calendar);
            ZonedDateTime zonedDateTime = ZonedDateTime.of(sqlTimestamp.toLocalDateTime(), calendar.getTimeZone().toZoneId());
            return packDateTimeWithZone(zonedDateTime.toInstant().toEpochMilli(), zonedDateTime.getZone().getId());
        };
    }

    /**
     * Writes TIMESTAMP WITH TIME ZONE values with short precision to JDBC PreparedStatement.
     *
     * @return LongWriteFunction for short precision TIMESTAMP WITH TIME ZONE values
     */
    private static LongWriteFunction shortTimestampWithTimeZoneWriteFunction()
    {
        return (statement, index, value) -> {
            long millisUtc = unpackMillisUtc(value);
            TimeZoneKey timeZoneKey = unpackZoneKey(value);
            statement.setObject(index, OffsetDateTime.ofInstant(Instant.ofEpochMilli(millisUtc), timeZoneKey.getZoneId()));
        };
    }

    // --- Static utility methods for Teradata time and timestamp types ---

    /**
     * Reads TIMESTAMP WITH TIME ZONE values with long precision from JDBC ResultSet.
     *
     * @return ObjectReadFunction for long precision TIMESTAMP WITH TIME ZONE values
     */
    private static ObjectReadFunction longTimestampWithTimeZoneReadFunction()
    {
        return ObjectReadFunction.of(LongTimestampWithTimeZone.class, (resultSet, columnIndex) -> {
            Calendar calendar = Calendar.getInstance();
            Timestamp sqlTimestamp = resultSet.getTimestamp(columnIndex, calendar);
            ZonedDateTime dateTime = ZonedDateTime.of(sqlTimestamp.toLocalDateTime(), calendar.getTimeZone().toZoneId());
            OffsetDateTime offsetDateTime = dateTime.toOffsetDateTime();
            long picosOfSecond = offsetDateTime.getNano() * ((long) PICOSECONDS_PER_NANOSECOND);

            return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(offsetDateTime.toEpochSecond(), picosOfSecond, getTimeZoneKey(offsetDateTime.toZonedDateTime().getZone().getId()));
        });
    }

    /**
     * Writes TIMESTAMP WITH TIME ZONE values with long precision to JDBC PreparedStatement.
     *
     * @return ObjectWriteFunction for long precision TIMESTAMP WITH TIME ZONE values
     */
    private static ObjectWriteFunction longTimestampWithTimeZoneWriteFunction()
    {
        return ObjectWriteFunction.of(LongTimestampWithTimeZone.class, (statement, index, value) -> {
            long epochMillis = value.getEpochMillis();
            long epochSeconds = floorDiv(epochMillis, MILLISECONDS_PER_SECOND);
            ZoneId zoneId = getTimeZoneKey(value.getTimeZoneKey()).getZoneId();
            Instant instant = Instant.ofEpochSecond(epochSeconds);
            statement.setObject(index, OffsetDateTime.ofInstant(instant, zoneId));
        });
    }

    private static boolean isCharacterType(JdbcColumnHandle column)
    {
        Type columnType = column.getColumnType();
        return columnType instanceof CharType || columnType instanceof VarcharType;
    }

    private static ColumnMapping charColumnMapping(int charLength, boolean isCaseSensitive)
    {
        if (charLength > CharType.MAX_LENGTH) {
            return varcharColumnMapping(charLength, isCaseSensitive);
        }
        CharType charType = createCharType(charLength);
        return ColumnMapping.sliceMapping(
                charType,
                charReadFunction(charType),
                charWriteFunction(),
                isCaseSensitive ? TERADATA_STRING_PUSHDOWN : CASE_INSENSITIVE_CHARACTER_PUSHDOWN);
    }

    private static ColumnMapping varcharColumnMapping(int varcharLength, boolean isCaseSensitive)
    {
        VarcharType varcharType = varcharLength <= VarcharType.MAX_LENGTH
                ? createVarcharType(varcharLength)
                : createUnboundedVarcharType();
        return ColumnMapping.sliceMapping(
                varcharType,
                varcharReadFunction(varcharType),
                varcharWriteFunction(),
                isCaseSensitive ? TERADATA_STRING_PUSHDOWN : CASE_INSENSITIVE_CHARACTER_PUSHDOWN);
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.NUMERIC, Optional.of("decimal"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> {
            // Apply TOP N directly to SELECT ... FROM ...
            System.out.println(sql.replaceFirst("(?i)^SELECT", "SELECT TOP " + limit));
            return sql.replaceFirst("(?i)^SELECT", "SELECT TOP " + limit);
        });
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        // Teradata supports TOP N with ORDER BY for all data types
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.of((query, sortItems, limit) -> {
            // Collect selected columns
            Set<String> selectColumns = new HashSet<>();
            Matcher matcher = Pattern.compile("(?i)SELECT\\s+(.*?)\\s+FROM").matcher(query);
            if (matcher.find()) {
                String[] cols = matcher.group(1).split(",");
                for (String col : cols) {
                    selectColumns.add(col.trim().replaceAll("\"", ""));
                }
            }

            // Add missing ORDER BY columns to SELECT
            List<String> extraColumns = new ArrayList<>();
            for (JdbcSortItem sortItem : sortItems) {
                String columnName = sortItem.column().getColumnName();
                if (!selectColumns.contains(columnName)) {
                    extraColumns.add("\"" + columnName + "\"");
                }
            }

            String modifiedQuery = query;
            if (!extraColumns.isEmpty()) {
                String allColumns = String.join(", ", selectColumns.stream().map(c -> "\"" + c + "\"").toList());
                allColumns += ", " + String.join(", ", extraColumns);
                modifiedQuery = query.replaceFirst("(?i)SELECT\\s+(.*?)\\s+FROM", "SELECT " + allColumns + " FROM");
            }

            String orderBy = sortItems.stream()
                    .map(sortItem -> {
                        String columnName = quoted(sortItem.column().getColumnName());
                        boolean asc = sortItem.sortOrder().isAscending();
                        String direction = asc ? "ASC" : "DESC";
                        String nullsHandling = sortItem.sortOrder().isNullsFirst() ? "NULLS FIRST" : "NULLS LAST";
                        return columnName + " " + direction + " " + nullsHandling;
                    })
                    .collect(Collectors.joining(", "));

            // Remove schema qualification (e.g. trino.nation → nation)
            String baseQuery = modifiedQuery.replaceAll("\\w+\\.\\w+\\.", "");

            return format("SELECT TOP %d * FROM (%s) AS t ORDER BY %s", limit, baseQuery, orderBy);
        });
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle)
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
            throw new TrinoException(JDBC_ERROR, "Failed fetching statistics for table: " + handle, e);
        }
    }

    private TableStatistics readTableStatistics(ConnectorSession session, JdbcTableHandle table)
            throws SQLException
    {
        checkArgument(table.isNamedRelation(), "Relation is not a table: %s", table);

        try (Connection connection = connectionFactory.openConnection(session);
                Handle handle = Jdbi.open(connection)) {

            TeradataStatisticsDao dao = new TeradataStatisticsDao(handle);
            long rowCount = dao.estimateRowCount(table);

            // Fallback to SAMPLE
            if (rowCount <= 0) {
                OptionalLong fallbackCount = dao.sampleRowCountEstimate(table, connection);
                if (fallbackCount.isEmpty()) {
                    return TableStatistics.empty();
                }
                rowCount = fallbackCount.getAsLong();
            }

            Map<String, TeradataStatisticsDao.ColumnIndexStatistics> stats = dao.getColumnIndexStatistics(table);
            TableStatistics.Builder tableStats = TableStatistics.builder().setRowCount(Estimate.of(rowCount));

            for (JdbcColumnHandle column : JdbcMetadata.getColumns(session, this, table)) {
                String columnName = column.getColumnName().toLowerCase();
                TeradataStatisticsDao.ColumnIndexStatistics stat = stats.get(columnName);

                ColumnStatistics.Builder columnStats = ColumnStatistics.builder();

                if (stat != null) {
                    columnStats.setNullsFraction(Estimate.of((double) stat.nullCount() / rowCount));

                    long distinctValues = stat.distinctValues();
                    if (distinctValues <= 0) {
                        // No NDV info from Teradata, fallback
                        columnStats.setDistinctValuesCount(Estimate.of(computeFallbackNDV(rowCount)));
                    }
                    else {
                        columnStats.setDistinctValuesCount(Estimate.of(distinctValues));
                    }
                }
                else {
                    // No stats at all for this column, fallback both null fraction and NDV
                    columnStats.setNullsFraction(Estimate.of(0.0));
                    columnStats.setDistinctValuesCount(Estimate.of(computeFallbackNDV(rowCount)));
                }

                tableStats.setColumnStatistics(column, columnStats.build());
            }

            return tableStats.build();
        }
    }

    /**
     * Compute fallback NDV based on table row count.
     * - Uses a fraction (e.g., 10%) of rowCount as fallback NDV,
     * - capped at MAX_FALLBACK_NDV,
     * - minimum fallback of 1 to avoid zero distinct count.
     */
    private long computeFallbackNDV(long rowCount)
    {
        if (rowCount <= 0) {
            return 1; // minimal fallback for empty or invalid row count
        }

        long fallback = (long) (rowCount * DEFAULT_FALLBACK_FRACTION);
        fallback = Math.max(fallback, 1); // at least 1 distinct value
        fallback = Math.min(fallback, MAX_FALLBACK_NDV); // cap at max fallback

        return fallback;
    }

    @Override
    public Optional<PreparedQuery> implementJoin(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            Map<JdbcColumnHandle, String> leftProjections,
            PreparedQuery rightSource,
            Map<JdbcColumnHandle, String> rightProjections,
            List<ParameterizedExpression> joinConditions,
            JoinStatistics statistics)
    {
        if (joinType == JoinType.FULL_OUTER) {
            // Teradata doesn't support FULL OUTER join pushdown well
            return Optional.empty();
        }

        return implementJoinCostAware(
                session,
                joinType,
                leftSource,
                rightSource,
                statistics,
                () -> super.implementJoin(session, joinType, leftSource, leftProjections, rightSource, rightProjections, joinConditions, statistics));
    }

    @Override
    public Optional<PreparedQuery> legacyImplementJoin(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            PreparedQuery rightSource,
            List<JdbcJoinCondition> joinConditions,
            Map<JdbcColumnHandle, String> rightAssignments,
            Map<JdbcColumnHandle, String> leftAssignments,
            JoinStatistics statistics)
    {
        if (joinType == JoinType.FULL_OUTER) {
            return Optional.empty();
        }

        return implementJoinCostAware(
                session,
                joinType,
                leftSource,
                rightSource,
                statistics,
                () -> super.legacyImplementJoin(session, joinType, leftSource, rightSource, joinConditions, rightAssignments, leftAssignments, statistics));
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        JoinCondition.Operator operator = joinCondition.getOperator();

        if (operator == JoinCondition.Operator.IDENTICAL) {
            return false;
        }

        boolean isVarcharJoin = Stream.of(joinCondition.getLeftColumn(), joinCondition.getRightColumn())
                .map(JdbcColumnHandle::getColumnType)
                .allMatch(type -> type instanceof CharType || type instanceof VarcharType);

        if (!isVarcharJoin) {
            // Non-VARCHAR join: allow common operators
            return switch (operator) {
                case EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL -> true;
                default -> false;
            };
        }

        // For VARCHAR joins: allow only equality checks
        return switch (operator) {
            case EQUAL, NOT_EQUAL -> true;
            default -> false;
        };
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    protected void createSchema(ConnectorSession session, Connection connection, String remoteSchemaName)
            throws SQLException
    {
        execute(session, format(
                "CREATE DATABASE %s AS PERMANENT = 60000000, SPOOL = 120000000",
                quoted(remoteSchemaName)));
    }

    @Override
    protected void verifySchemaName(DatabaseMetaData databaseMetadata, String schemaName)
            throws SQLException
    {
        int schemaNameLimit = databaseMetadata.getMaxSchemaNameLength();
        if (schemaName.length() > schemaNameLimit) {
            throw new TrinoException(NOT_SUPPORTED, format("Schema name must be shorter than or equal to '%s' characters but got '%s'", schemaNameLimit, schemaName.length()));
        }
    }

    @Override
    protected void verifyTableName(DatabaseMetaData databaseMetadata, String tableName)
            throws SQLException
    {
        // PostgreSQL truncates table name to 63 chars silently
        if (tableName.length() > databaseMetadata.getMaxTableNameLength()) {
            throw new TrinoException(NOT_SUPPORTED, format("Table name must be shorter than or equal to '%s' characters but got '%s'", databaseMetadata.getMaxTableNameLength(), tableName.length()));
        }
    }

    @Override
    protected void verifyColumnName(DatabaseMetaData databaseMetadata, String columnName)
            throws SQLException
    {
        // PostgreSQL truncates table name to 63 chars silently
        // PostgreSQL driver caches the max column name length in a DatabaseMetaData object. The cost to call this method per column is low.
        if (columnName.length() > databaseMetadata.getMaxColumnNameLength()) {
            throw new TrinoException(NOT_SUPPORTED, format("Column name must be shorter than or equal to '%s' characters but got '%s': '%s'", databaseMetadata.getMaxColumnNameLength(), columnName.length(), columnName));
        }
    }

    protected void dropSchema(ConnectorSession session, Connection connection, String remoteSchemaName, boolean cascade)
            throws SQLException
    {
        if (cascade) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas with CASCADE option");
        }
//        String deleteSchema = "DELETE DATABASE " + quoted(remoteSchemaName);
//        execute(session, connection, deleteSchema);
        String dropSchema = "DROP DATABASE " + quoted(remoteSchemaName);
        execute(session, connection, dropSchema);
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schema");
    }

    /**
     * Delete operations are not supported by the Teradata connector.
     *
     * @param session connector session
     * @param handle table handle identifying the target table
     * @return empty optional indicating no deletion occurred
     * @throws TrinoException always thrown with NOT_SUPPORTED error code
     */
    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support modifying table rows");
    }

    @Override
    public void truncateTable(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support truncating tables");
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping columns");
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming columns");
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables");
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column, ColumnPosition position)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support add column operations");
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping a not null constraint");
    }

    /**
     * Builds the expression rewriter for translating connector expressions
     * into SQL fragments understood by Teradata.
     * Currently supports numeric equality expressions and quoted identifiers.
     */
    private void buildExpressionRewriter()
    {
        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .add(new RewriteIn())
                .add(new RewriteLikeWithCaseSensitivity())
                .add(new RewriteLikeEscapeWithCaseSensitivity())
                .withTypeClass("integer_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint"))
                .withTypeClass("numeric_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint", "decimal", "real", "double"))
                .map("$equal(left: numeric_type, right: numeric_type)").to("left = right")
                .map("$not_equal(left: numeric_type, right: numeric_type)").to("left <> right")
                .map("$less_than(left: numeric_type, right: numeric_type)").to("left < right")
                .map("$less_than_or_equal(left: numeric_type, right: numeric_type)").to("left <= right")
                .map("$greater_than(left: numeric_type, right: numeric_type)").to("left > right")
                .map("$greater_than_or_equal(left: numeric_type, right: numeric_type)").to("left >= right")
                .add(new RewriteCaseSensitiveComparison(ImmutableSet.of(ComparisonOperator.EQUAL, ComparisonOperator.NOT_EQUAL)))
                .map("$add(left: integer_type, right: integer_type)").to("left + right")
                .map("$subtract(left: integer_type, right: integer_type)").to("left - right")
                .map("$multiply(left: integer_type, right: integer_type)").to("left * right")
                .map("$divide(left: integer_type, right: integer_type)").to("left / right")
                .map("$modulus(left: integer_type, right: integer_type)").to("left % right")
                .map("$negate(value: integer_type)").to("-value")
                .map("$not($is_null(value))").to("value IS NOT NULL")
                .map("$not(value: boolean)").to("NOT value")
                .map("$is_null(value)").to("value IS NULL")
                .map("$nullif(first, second)").to("NULLIF(first, second)")
                .build();
    }

    private void buildAggregateRewriter()
    {
        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                this.connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        // Basic aggregates
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementCountDistinct(bigintTypeHandle, false))
                        .add(new ImplementMinMax(false))
                        .add(new ImplementSum(TeradataClient::toTypeHandle))

                        // AVG
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementAvgBigint())

                        // Statistical aggregates (numeric types only)
                        .add(new ImplementStddevSamp())
                        .add(new ImplementStddevPop())
                        .add(new ImplementVarianceSamp())
                        .add(new ImplementVariancePop())

                        // Correlation and regression
                        .add(new ImplementCovarianceSamp())
                        .add(new ImplementCovariancePop())
                        .add(new ImplementCorr())
                        .add(new ImplementRegrIntercept())
                        .add(new ImplementRegrSlope())
                        .build()
        );
    }

    /**
     * Converts a predicate expression to a parameterized JDBC expression,
     * suitable for pushdown to the Teradata database.
     *
     * @param session connector session
     * @param expression connector expression representing the predicate
     * @param assignments mapping of column names to handles
     * @return optional parameterized expression if conversion is possible
     */
    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return this.connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    /**
     * Returns a mapping of column names to their case sensitivity,
     * derived from the metadata of a query "SELECT * FROM schema.table WHERE 0=1".
     *
     * @param session connector session
     * @param connection JDBC connection to the Teradata database
     * @param schemaTableName schema and table name within the connector
     * @param remoteTableName the fully qualified remote table name
     * @return map of column name to case sensitivity (case sensitive or insensitive)
     */
    @Override
    protected Map<String, CaseSensitivity> getCaseSensitivityForColumns(ConnectorSession session, Connection connection, SchemaTableName schemaTableName, RemoteTableName remoteTableName)
    {
        // try to use result set metadata from select * from table to populate the mapping
        try {
            HashMap<String, CaseSensitivity> caseMap = new HashMap<>();
            String sql = format("select * from %s.%s where 0=1", schemaTableName.getSchemaName(), schemaTableName.getTableName());
            PreparedStatement pstmt = connection.prepareStatement(sql);
            ResultSetMetaData rsmd = pstmt.getMetaData();
            int columnCount = rsmd.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                caseMap.put(rsmd.getColumnName(i), rsmd.isCaseSensitive(i) ? CASE_SENSITIVE : CASE_INSENSITIVE);
            }
            pstmt.close();
            return caseMap;
        }
        catch (SQLException e) {
            // behavior of base jdbc
            return ImmutableMap.of();
        }
    }

    /**
     * Determines the case sensitivity for a type based on Teradata configuration.
     *
     * @param typeHandleCaseSensitivity optional case sensitivity from type metadata
     * @return true if case sensitive, false otherwise
     */
    private boolean deriveCaseSensitivity(Optional<CaseSensitivity> typeHandleCaseSensitivity)
    {
        switch (teradataJDBCCaseSensitivity) {
            case NOT_CASE_SPECIFIC:
                return false;
            case CASE_SPECIFIC:
                return true;
            case AS_DEFINED:
            default:
                return typeHandleCaseSensitivity.orElse(CASE_INSENSITIVE) == CASE_SENSITIVE;
        }
    }

    /**
     * Maps JDBC types and Teradata-specific types to Trino column mappings.
     * Handles standard types as well as Teradata-specific types like
     * TIMESTAMP WITH TIME ZONE and JSON.
     *
     * @param session connector session
     * @param connection JDBC connection
     * @param typeHandle JDBC type handle describing the column type
     * @return optional column mapping for the given type
     */
    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        // this method should ultimately encompass all the expected teradata data types

        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        // switch by names as some types overlap other types going by jdbc type alone
        String jdbcTypeName = typeHandle.jdbcTypeName().orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));
        switch (jdbcTypeName.toUpperCase()) {
            case "TIMESTAMP WITH TIME ZONE":
                // TODO review correctness
                return Optional.of(timestampWithTimeZoneColumnMapping(typeHandle.requiredDecimalDigits()));
            case "TIME WITH TIME ZONE":
                // TODO review correctness
                return Optional.of(timeWithTimeZoneColumnMapping(typeHandle.requiredDecimalDigits()));
            case "JSON":
                // TODO map to trino json value
                Optional<ColumnMapping> cm = mapToUnboundedVarchar(typeHandle);
                if (cm.isPresent()) {
                    System.out.println(cm);
                }
                else {
                    System.out.println(typeHandle.jdbcTypeName() + "deosnt no have correct mapping");
                }
                return cm;
        }

        // switch by jdbc type
        // TODO missing types interval, array, etc
        switch (typeHandle.jdbcType()) {
            case Types.TINYINT:
                return Optional.of(tinyintColumnMapping());
            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());
            case Types.INTEGER:
                return Optional.of(integerColumnMapping());
            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());
            case Types.REAL:
            case Types.DOUBLE:
            case Types.FLOAT:
                // teradata float is 64 bit
                // trino double is 64 bit
                // teradata float / real / double precision all map to jdbc type float
                return Optional.of(doubleColumnMapping());
            case Types.NUMERIC:
            case Types.DECIMAL:
                // also applies to teradata number type
                // this is roughly logic see used by sql server
                int precision = typeHandle.requiredColumnSize();
                int scale = typeHandle.requiredDecimalDigits();
                if (precision > Decimals.MAX_PRECISION) {
                    // this will trigger for number(*) as precision is 40
                    break;
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, scale)));
            case Types.CHAR:
                return Optional.of(charColumnMapping(typeHandle.requiredColumnSize(), deriveCaseSensitivity(typeHandle.caseSensitivity())));
            case Types.VARCHAR:
                // see prior note on trino case sensitivity
                return Optional.of(varcharColumnMapping(typeHandle.requiredColumnSize(), deriveCaseSensitivity(typeHandle.caseSensitivity())));
            case Types.BINARY:
            case Types.VARBINARY:
                // trino only has varbinary
                return Optional.of(varbinaryColumnMapping());
            case Types.DATE:
                return Optional.of(dateColumnMappingUsingLocalDate());
            case Types.TIME:
                return Optional.of(timeColumnMapping(typeHandle.requiredDecimalDigits()));
            case Types.TIMESTAMP:
                return Optional.of(timestampColumnMapping(TimestampType.createTimestampType(typeHandle.requiredDecimalDigits())));
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }

        return Optional.empty();
    }

    /**
     * This connector is read-only and does not support writing to Teradata.
     * This method always throws a NOT_SUPPORTED exception.
     *
     * @param session connector session
     * @param type Trino type for the column
     * @return never returns normally
     * @throws TrinoException always thrown indicating unsupported operation
     */
    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("boolean", booleanWriteFunction());
        }

        if (type == TINYINT) {
            // PostgreSQL has no type corresponding to tinyint
            return WriteMapping.longMapping("tinyint", tinyintWriteFunction());
        }
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
            return WriteMapping.doubleMapping("double precision", doubleWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double precision", doubleWriteFunction());
        }

        if (type instanceof DecimalType decimalType) {
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
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
        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("bytea", varbinaryWriteFunction());
        }

        if (type == DATE) {
            return WriteMapping.longMapping("date", dateWriteFunctionUsingLocalDate());
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    private static class TeradataStatisticsDao
    {
        private final Handle handle;

        public TeradataStatisticsDao(Handle handle)
        {
            this.handle = requireNonNull(handle, "handle is null");
        }

        // 1. Estimate row count using max distinct values from DBC.StatsV
        public long estimateRowCount(JdbcTableHandle table)
        {
            RemoteTableName remote = table.getRequiredNamedRelation().getRemoteTableName();
            String schema = remote.getSchemaName().orElseThrow();
            String tableName = remote.getTableName();

            return handle.createQuery(
                            "SELECT MAX(RowCount) AS est_row_count " +
                                    "FROM DBC.StatsV " +
                                    "WHERE DatabaseName = :schema AND TableName = :table")
                    .bind("schema", schema)
                    .bind("table", tableName)
                    .mapTo(Long.class)
                    .findOne()
                    .orElse(0L);
        }

        // 2. Column-level stats from StatsV
        public Map<String, ColumnIndexStatistics> getColumnIndexStatistics(JdbcTableHandle table)
        {
            RemoteTableName remote = table.getRequiredNamedRelation().getRemoteTableName();
            String schema = remote.getSchemaName().orElseThrow();
            String tableName = remote.getTableName();

            return handle.createQuery(
                            "SELECT ColumnName, NullCount, UniqueValueCount " +
                                    "FROM DBC.StatsV " +
                                    "WHERE DatabaseName = :schema AND TableName = :table")
                    .bind("schema", schema)
                    .bind("table", tableName)
                    .map((rs, ctx) -> {
                        String column = rs.getString("ColumnName");
                        if (column == null) {
                            // skip this row by returning null
                            return null;
                        }
                        long nullCount = rs.getLong("NullCount");
                        long distinct = rs.getLong("UniqueValueCount");

                        return new SimpleEntry<>(
                                column.trim().toLowerCase(),
                                new ColumnIndexStatistics(nullCount > 0, distinct, nullCount));
                    })
                    // Filter out nulls before collecting to map
                    .filter(Objects::nonNull)
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        // 3. Fallback using SAMPLE query
        public OptionalLong sampleRowCountEstimate(JdbcTableHandle table, Connection connection)
        {
            RemoteTableName remote = table.getRequiredNamedRelation().getRemoteTableName();
            String schema = remote.getSchemaName().orElseThrow();
            String tableName = remote.getTableName();

            String sql = String.format("""
                    SELECT COUNT(*) * 100 AS estimated_count
                    FROM %s.%s
                    WHERE RANDOM(1, 10000) <= 100
                    """, schema, tableName);

            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery(sql)) {

                if (rs.next()) {
                    long estimated = rs.getLong("estimated_count");
                    return OptionalLong.of(estimated);
                }
            }
            catch (SQLException e) {
                System.err.printf("Sampling fallback failed: %s%n", e.getMessage());
            }

            return OptionalLong.empty();
        }

        public record ColumnIndexStatistics(boolean nullable, long distinctValues, long nullCount) {}
    }
}
