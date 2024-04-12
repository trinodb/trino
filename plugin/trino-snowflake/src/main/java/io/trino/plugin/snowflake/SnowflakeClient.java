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
package io.trino.plugin.snowflake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.PredicatePushdownController;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.StandardColumnMappings;
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
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Chars;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Timestamps;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

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
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_INSENSITIVE;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_SENSITIVE;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
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
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.math.RoundingMode.UNNECESSARY;
import static java.util.Objects.requireNonNull;

public class SnowflakeClient
        extends BaseJdbcClient
{
    /* TIME supports an optional precision parameter for fractional seconds, e.g. TIME(3). Time precision can range from 0 (seconds) to 9 (nanoseconds). The default precision is 9.
      All TIME values must be between 00:00:00 and 23:59:59.999999999. TIME internally stores “wallclock” time, and all operations on TIME values are performed without taking any time zone into consideration.
     */
    private static final int MAX_SUPPORTED_TEMPORAL_PRECISION = 9;

    private static final DateTimeFormatter SNOWFLAKE_DATETIME_FORMATTER = DateTimeFormatter.ofPattern("u-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX");
    private static final DateTimeFormatter SNOWFLAKE_DATE_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd");
    private static final DateTimeFormatter SNOWFLAKE_TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("u-MM-dd'T'HH:mm:ss.SSSSSSSSS");
    private static final DateTimeFormatter SNOWFLAKE_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS");
    private static final TimeZone UTC_TZ = TimeZone.getTimeZone(ZoneId.of("UTC"));
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;

    @Inject
    public SnowflakeClient(
            BaseJdbcConfig config,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier remoteQueryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, remoteQueryModifier, false);

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .build();

        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementCountDistinct(bigintTypeHandle, false))
                        .add(new ImplementMinMax(false))
                        .add(new ImplementSum(SnowflakeClient::decimalTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementAvgBigint())
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

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        String jdbcTypeName = typeHandle.jdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));
        jdbcTypeName = jdbcTypeName.toLowerCase(Locale.ENGLISH);
        int type = typeHandle.jdbcType();

        switch (type) {
            case Types.BOOLEAN:
                return Optional.of(booleanColumnMapping());
            case Types.TINYINT:
                return Optional.of(tinyintColumnMapping());
            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());
            case Types.INTEGER:
                return Optional.of(integerColumnMapping());
            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());
            case Types.REAL:
                return Optional.of(realColumnMapping());
            case Types.FLOAT:
            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());
            case Types.NUMERIC:
            case Types.DECIMAL: {
                int precision = typeHandle.requiredColumnSize();
                int scale = typeHandle.requiredDecimalDigits();
                if (precision > 38) {
                    break;
                }
                DecimalType decimalType = createDecimalType(precision, scale);
                return Optional.of(decimalColumnMapping(decimalType, UNNECESSARY));
            }
            case Types.VARCHAR:
                if (jdbcTypeName.equals("varchar")) {
                    return Optional.of(varcharColumnMapping(typeHandle.requiredColumnSize(), typeHandle.caseSensitivity()));
                }
                // Some other Snowflake types (ARRAY, VARIANT, GEOMETRY, etc.) are also mapped to Types.VARCHAR, but they're unsupported.
                break;
            case Types.BINARY:
                // Multiple Snowflake types are mapped into Types.BINARY
                if (jdbcTypeName.equals("binary")) {
                    return Optional.of(varbinaryColumnMapping());
                }
                // Some other Snowflake types (GEOMETRY in some cases, etc.) are also mapped to Types.BINARY, but they're unsupported.
                break;
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(varbinaryColumnMapping());
            case Types.DATE:
                return Optional.of(ColumnMapping.longMapping(DateType.DATE, ResultSet::getLong, snowFlakeDateWriteFunction()));
            case Types.TIME:
                return Optional.of(timeColumnMapping(typeHandle.requiredDecimalDigits()));
            case Types.TIMESTAMP:
                return Optional.of(timestampColumnMapping(typeHandle.requiredDecimalDigits()));
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return Optional.of(timestampWithTimeZoneColumnMapping(typeHandle.requiredDecimalDigits()));
        }
        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.empty();
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("BOOLEAN", booleanWriteFunction());
        }
        if (type == TINYINT) {
            return WriteMapping.longMapping("NUMBER(3, 0)", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("NUMBER(5, 0)", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("NUMBER(10, 0)", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("NUMBER(19, 0)", bigintWriteFunction());
        }
        if (type == REAL) {
            return WriteMapping.longMapping("DOUBLE", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("DOUBLE", doubleWriteFunction());
        }
        if (type instanceof DecimalType decimalType) {
            String dataType = format("NUMBER(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }
        if (type instanceof CharType charType) {
            return WriteMapping.sliceMapping("VARCHAR(" + charType.getLength() + ")", charWriteFunction(charType));
        }
        if (type instanceof VarcharType varcharType) {
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "VARCHAR";
            }
            else {
                dataType = "VARCHAR(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (type == VARBINARY) {
            return WriteMapping.sliceMapping("VARBINARY", varbinaryWriteFunction());
        }
        if (type == DATE) {
            return WriteMapping.longMapping("DATE", snowFlakeDateWriteFunction());
        }
        if (type instanceof TimeType timeType) {
            return WriteMapping.longMapping(format("TIME(%s)", timeType.getPrecision()), timeWriteFunction(timeType.getPrecision()));
        }
        if (type instanceof TimestampType timestampType) {
            return snowflakeTimestampWriteMapping(timestampType.getPrecision());
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            return snowflakeTimestampWithTimeZoneWriteMapping(timestampWithTimeZoneType.getPrecision());
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        // TODO support complex ConnectorExpressions
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        // Remote database can be case insensitive.
        return preventTextualTypeAggregationPushdown(groupingSets);
    }

    private static Optional<JdbcTypeHandle> decimalTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(
                Types.NUMERIC,
                Optional.of("NUMBER"),
                Optional.of(decimalType.getPrecision()),
                Optional.of(decimalType.getScale()),
                Optional.empty(),
                Optional.empty()));
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
        for (JdbcSortItem sortItem : sortOrder) {
            Type sortItemType = sortItem.column().getColumnType();
            if (sortItemType instanceof CharType || sortItemType instanceof VarcharType) {
                // Remote database can be case insensitive.
                return false;
            }
        }
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
        return true;
    }

    @Override
    public Optional<String> getTableComment(ResultSet resultSet)
            throws SQLException
    {
        // Empty remarks means that the table doesn't have a comment in Snowflake
        return Optional.ofNullable(emptyToNull(resultSet.getString("REMARKS")));
    }

    @Override
    protected List<String> createTableSqls(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        checkArgument(tableMetadata.getProperties().isEmpty(), "Unsupported table properties: %s", tableMetadata.getProperties());
        return ImmutableList.of(format("CREATE TABLE %s (%s) COMMENT = %s", quoted(remoteTableName), join(", ", columns), snowflakeVarcharLiteral(tableMetadata.getComment().orElse(""))));
    }

    @Override
    public void setTableComment(ConnectorSession session, JdbcTableHandle handle, Optional<String> comment)
    {
        String sql = "COMMENT ON TABLE %s IS %s".formatted(
                quoted(handle.asPlainTable().getRemoteTableName()),
                snowflakeVarcharLiteral(comment.orElse("")));
        execute(session, sql);
    }

    private static String snowflakeVarcharLiteral(String value)
    {
        requireNonNull(value, "value is null");
        return "'" + value.replace("'", "''").replace("\\", "\\\\") + "'";
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    private static ColumnMapping timeColumnMapping(int precision)
    {
        checkArgument(precision <= MAX_SUPPORTED_TEMPORAL_PRECISION, "The max timestamp precision in Snowflake is " + MAX_SUPPORTED_TEMPORAL_PRECISION);
        return ColumnMapping.longMapping(
                TimeType.createTimeType(precision),
                (resultSet, columnIndex) -> {
                    LocalTime time = SNOWFLAKE_TIME_FORMATTER.parse(resultSet.getString(columnIndex), LocalTime::from);
                    return Timestamps.round(time.toNanoOfDay() * PICOSECONDS_PER_NANOSECOND, 12 - precision);
                },
                timeWriteFunction(precision),
                PredicatePushdownController.FULL_PUSHDOWN);
    }

    private static ColumnMapping timestampWithTimeZoneColumnMapping(int precision)
    {
        if (precision <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
            return ColumnMapping.longMapping(createTimestampWithTimeZoneType(precision),
                    (resultSet, columnIndex) -> {
                        ZonedDateTime timestamp = SNOWFLAKE_DATETIME_FORMATTER.parse(resultSet.getString(columnIndex), ZonedDateTime::from);
                        return DateTimeEncoding.packDateTimeWithZone(timestamp.toInstant().toEpochMilli(), timestamp.getZone().getId());
                    },
                    shortTimestampWithTimeZoneWriteFunction(), PredicatePushdownController.FULL_PUSHDOWN);
        }
        return ColumnMapping.objectMapping(createTimestampWithTimeZoneType(precision), longTimestampWithTimezoneReadFunction(), longTimestampWithTimeZoneWriteFunction());
    }

    private static ObjectReadFunction longTimestampWithTimezoneReadFunction()
    {
        return ObjectReadFunction.of(LongTimestampWithTimeZone.class, (resultSet, columnIndex) -> {
            ZonedDateTime timestamp = SNOWFLAKE_DATETIME_FORMATTER.parse(resultSet.getString(columnIndex), ZonedDateTime::from);
            return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(timestamp.toEpochSecond(),
                    (long) timestamp.getNano() * Timestamps.PICOSECONDS_PER_NANOSECOND,
                    TimeZoneKey.getTimeZoneKey(timestamp.getZone().getId()));
        });
    }

    private static ColumnMapping timestampColumnMapping(int precision)
    {
        if (precision <= TimestampType.MAX_SHORT_PRECISION) {
            return ColumnMapping.longMapping(createTimestampType(precision), (resultSet, columnIndex) -> toTrinoTimestamp(createTimestampType(precision), toLocalDateTime(resultSet, columnIndex)), shortTimestampWriteFunction());
        }
        return ColumnMapping.objectMapping(createTimestampType(precision), longTimestampReader(), longTimestampWriteFunction(precision));
    }

    private static LocalDateTime toLocalDateTime(ResultSet resultSet, int columnIndex)
            throws SQLException
    {
        Calendar calendar = new GregorianCalendar(UTC_TZ, Locale.ENGLISH);
        calendar.setTime(new Date(0));
        Timestamp ts = resultSet.getTimestamp(columnIndex, calendar);
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(ts.getTime()), ZoneOffset.UTC);
    }

    private static ObjectReadFunction longTimestampReader()
    {
        return ObjectReadFunction.of(LongTimestamp.class, (resultSet, columnIndex) -> {
            Calendar calendar = new GregorianCalendar(UTC_TZ, Locale.ENGLISH);
            calendar.setTime(new Date(0));
            Timestamp ts = resultSet.getTimestamp(columnIndex, calendar);
            long epochMillis = ts.getTime();
            int nanosInTheSecond = ts.getNanos();
            int nanosInTheMilli = nanosInTheSecond % NANOSECONDS_PER_MILLISECOND;
            long micro = epochMillis * Timestamps.MICROSECONDS_PER_MILLISECOND + (nanosInTheMilli / Timestamps.NANOSECONDS_PER_MICROSECOND);
            int picosOfMicro = nanosInTheMilli % 1000 * 1000;
            return new LongTimestamp(micro, picosOfMicro);
        });
    }

    private static LongWriteFunction timeWriteFunction(int precision)
    {
        checkArgument(precision <= MAX_SUPPORTED_TEMPORAL_PRECISION, "Unsupported precision: %s", precision);
        return (statement, index, picosOfDay) -> {
            picosOfDay = Timestamps.round(picosOfDay, 12 - precision);
            if (picosOfDay == Timestamps.PICOSECONDS_PER_DAY) {
                picosOfDay = 0;
            }
            LocalTime localTime = LocalTime.ofNanoOfDay(picosOfDay / PICOSECONDS_PER_NANOSECOND);
            statement.setString(index, SNOWFLAKE_TIME_FORMATTER.format(localTime));
        };
    }

    private static ColumnMapping varcharColumnMapping(int varcharLength, Optional<CaseSensitivity> caseSensitivity)
    {
        VarcharType varcharType = varcharLength <= VarcharType.MAX_LENGTH ? createVarcharType(varcharLength) : createUnboundedVarcharType();
        return StandardColumnMappings.varcharColumnMapping(varcharType, caseSensitivity.orElse(CASE_INSENSITIVE) == CASE_SENSITIVE);
    }

    private static LongWriteFunction snowFlakeDateWriteFunction()
    {
        return (statement, index, day) -> statement.setString(index, SNOWFLAKE_DATE_FORMATTER.format(LocalDate.ofEpochDay(day)));
    }

    private static SliceWriteFunction charWriteFunction(CharType charType)
    {
        return (statement, index, value) -> statement.setString(index, Chars.padSpaces(value, charType).toStringUtf8());
    }

    private static WriteMapping snowflakeTimestampWriteMapping(int precision)
    {
        checkArgument(precision <= MAX_SUPPORTED_TEMPORAL_PRECISION, "The max timestamp precision in Snowflake is " + MAX_SUPPORTED_TEMPORAL_PRECISION);
        if (precision <= TimestampType.MAX_SHORT_PRECISION) {
            return WriteMapping.longMapping(format("timestamp_ntz(%d)", precision), shortTimestampWriteFunction());
        }
        return WriteMapping.objectMapping(format("timestamp_ntz(%d)", precision), longTimestampWriteFunction(precision));
    }

    private static LongWriteFunction shortTimestampWriteFunction()
    {
        return (statement, index, value) -> statement.setString(index, StandardColumnMappings.fromTrinoTimestamp(value).toString());
    }

    private static ObjectWriteFunction longTimestampWriteFunction(int precision)
    {
        return ObjectWriteFunction.of(
                LongTimestamp.class,
                (statement, index, value) -> statement.setString(index, SNOWFLAKE_TIMESTAMP_FORMATTER.format(StandardColumnMappings.fromLongTrinoTimestamp(value, precision))));
    }

    private static WriteMapping snowflakeTimestampWithTimeZoneWriteMapping(int precision)
    {
        checkArgument(precision <= MAX_SUPPORTED_TEMPORAL_PRECISION, "Max Snowflake precision is is " + MAX_SUPPORTED_TEMPORAL_PRECISION);
        if (precision <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
            return WriteMapping.longMapping(format("timestamp_tz(%d)", precision), shortTimestampWithTimeZoneWriteFunction());
        }
        return WriteMapping.objectMapping(format("timestamp_tz(%d)", precision), longTimestampWithTimeZoneWriteFunction());
    }

    private static LongWriteFunction shortTimestampWithTimeZoneWriteFunction()
    {
        return (statement, index, encodedTimeWithZone) -> {
            Instant instant = Instant.ofEpochMilli(DateTimeEncoding.unpackMillisUtc(encodedTimeWithZone));
            ZoneId zone = ZoneId.of(DateTimeEncoding.unpackZoneKey(encodedTimeWithZone).getId());
            statement.setString(index, SNOWFLAKE_DATETIME_FORMATTER.format(instant.atZone(zone)));
        };
    }

    private static ObjectWriteFunction longTimestampWithTimeZoneWriteFunction()
    {
        return ObjectWriteFunction.of(LongTimestampWithTimeZone.class, (statement, index, value) -> {
            long epochMillis = value.getEpochMillis();
            long epochSeconds = Math.floorDiv(epochMillis, MILLISECONDS_PER_SECOND);
            long adjustNanoSeconds = (long) Math.floorMod(epochMillis, MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND + value.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND;
            ZoneId zone = TimeZoneKey.getTimeZoneKey(value.getTimeZoneKey()).getZoneId();
            Instant instant = Instant.ofEpochSecond(epochSeconds, adjustNanoSeconds);
            statement.setString(index, SNOWFLAKE_DATETIME_FORMATTER.format(ZonedDateTime.ofInstant(instant, zone)));
        });
    }
}
