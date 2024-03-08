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
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.StandardColumnMappings;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgDecimal;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
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
import java.sql.PreparedStatement;
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
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
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
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;

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
                        .add(new ImplementMinMax(false))
                        .add(new ImplementSum(SnowflakeClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementAvgBigint())
                        .build());
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));
        jdbcTypeName = jdbcTypeName.toLowerCase(Locale.ENGLISH);
        int type = typeHandle.getJdbcType();

        switch (type) {
            case Types.BOOLEAN:
                return Optional.of(booleanColumnMapping());
            // This is used for synthetic columns generated by count() aggregation pushdown
            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());
            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());
            // In Snowflake all fixed-point numeric types are decimals. It always returns a DECIMAL type when
            // JDBC_TREAT_DECIMAL_AS_INT is set to False.
            case Types.DECIMAL:
            case Types.NUMERIC: {
                int precision = typeHandle.getRequiredColumnSize();
                int scale = typeHandle.getRequiredDecimalDigits();
                DecimalType decimalType = createDecimalType(precision, scale);
                return Optional.of(decimalColumnMapping(decimalType, UNNECESSARY));
            }
            case Types.VARCHAR:
                if (jdbcTypeName.equals("varchar")) {
                    return Optional.of(varcharColumnMapping(typeHandle.getRequiredColumnSize(), typeHandle.getCaseSensitivity()));
                }
                // Some other Snowflake types (ARRAY, VARIANT, GEOMETRY, etc.) are also mapped to Types.VARCHAR, but
                // they're unsupported.
                return Optional.empty();
            case Types.BINARY:
                // Multiple Snowflake types are mapped into Types.BINARY
                if (jdbcTypeName.equals("binary")) {
                    return Optional.of(varbinaryColumnMapping());
                }
                // Some other Snowflake types (GEOMETRY in some cases, etc.) are also mapped to Types.BINARY, but
                // they're unsupported.
                return Optional.empty();
            case Types.DATE:
                return Optional.of(ColumnMapping.longMapping(DateType.DATE, ResultSet::getLong, snowFlakeDateWriter()));
            case Types.TIME:
                return Optional.of(timeColumnMapping(typeHandle.getRequiredDecimalDigits()));
            case Types.TIMESTAMP:
                return Optional.of(timestampColumnMapping(typeHandle.getRequiredDecimalDigits()));
            case TIMESTAMP_WITH_TIMEZONE:
                return Optional.of(timestampTimeZoneColumnMapping(typeHandle.getRequiredDecimalDigits()));
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
            return WriteMapping.longMapping("date", snowFlakeDateWriter());
        }
        if (type instanceof TimeType timeType) {
            return WriteMapping.longMapping("time", timeWriteFunction(timeType.getPrecision()));
        }
        if (type instanceof TimestampType timestampType) {
            return snowflakeTimestampWriteMapping(timestampType);
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            return snowflakeTimestampWithTimeZoneWriteMapping(timestampWithTimeZoneType);
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

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.NUMERIC, Optional.of("decimal"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
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
            Type sortItemType = sortItem.getColumn().getColumnType();
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
    {
        // Don't return a comment until the connector supports creating tables with comment
        return Optional.empty();
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

    private static ColumnMapping timestampTimeZoneColumnMapping(int precision)
    {
        TimestampWithTimeZoneType trinoType = createTimestampWithTimeZoneType(precision);
        if (precision <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
            return ColumnMapping.longMapping(trinoType,
                    (resultSet, columnIndex) -> {
                        ZonedDateTime timestamp = SNOWFLAKE_DATETIME_FORMATTER.parse(resultSet.getString(columnIndex), ZonedDateTime::from);
                        return DateTimeEncoding.packDateTimeWithZone(timestamp.toInstant().toEpochMilli(), timestamp.getZone().getId());
                    },
                    shortTimestampWithTimeZoneWriteFunction(), PredicatePushdownController.FULL_PUSHDOWN);
        }
        return ColumnMapping.objectMapping(trinoType, longTimestampWithTimeZoneReadFunction(), longTimestampWithTimeZoneWriteFunction());
    }

    private static LongWriteFunction shortTimestampWithTimeZoneWriteFunction()
    {
        return (statement, index, encodedTimeWithZone) -> {
            Instant timeI = Instant.ofEpochMilli(DateTimeEncoding.unpackMillisUtc(encodedTimeWithZone));
            ZoneId zone = ZoneId.of(DateTimeEncoding.unpackZoneKey(encodedTimeWithZone).getId());
            statement.setString(index, SNOWFLAKE_DATETIME_FORMATTER.format(timeI.atZone(zone)));
        };
    }

    private static ObjectReadFunction longTimestampWithTimeZoneReadFunction()
    {
        return ObjectReadFunction.of(LongTimestampWithTimeZone.class, (resultSet, columnIndex) -> {
            ZonedDateTime timestamp = SNOWFLAKE_DATETIME_FORMATTER.parse(resultSet.getString(columnIndex), ZonedDateTime::from);
            return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(timestamp.toEpochSecond(),
                    (long) timestamp.getNano() * Timestamps.PICOSECONDS_PER_NANOSECOND,
                    TimeZoneKey.getTimeZoneKey(timestamp.getZone().getId()));
        });
    }

    private static ObjectWriteFunction longTimestampWithTimeZoneWriteFunction()
    {
        return ObjectWriteFunction.of(LongTimestampWithTimeZone.class, (statement, index, value) -> {
            long epoMilli = value.getEpochMillis();
            long epoSeconds = Math.floorDiv(epoMilli, Timestamps.MILLISECONDS_PER_SECOND);
            long adjNano = (long) Math.floorMod(epoMilli, Timestamps.MILLISECONDS_PER_SECOND) * Timestamps.NANOSECONDS_PER_MILLISECOND + value.getPicosOfMilli() / Timestamps.PICOSECONDS_PER_NANOSECOND;
            ZoneId zone = TimeZoneKey.getTimeZoneKey(value.getTimeZoneKey()).getZoneId();
            Instant timeI = Instant.ofEpochSecond(epoSeconds, adjNano);
            statement.setString(index, SNOWFLAKE_DATETIME_FORMATTER.format(ZonedDateTime.ofInstant(timeI, zone)));
        });
    }

    private static ColumnMapping timestampColumnMapping(int precision)
    {
        TimestampType trinoType = TimestampType.createTimestampType(precision);
        if (precision <= TimestampType.MAX_SHORT_PRECISION) {
            return ColumnMapping.longMapping(
                    trinoType,
                    (resultSet, columnIndex) -> toTrinoTimestamp(trinoType, toLocalDateTime(resultSet, columnIndex)),
                    shortTimestampWriteFunction());
        }

        return ColumnMapping.objectMapping(trinoType, longTimestampReader(), longTimestampWriteFunction(precision));
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
            int nanosInTheMilli = nanosInTheSecond % Timestamps.NANOSECONDS_PER_MILLISECOND;
            long micro = epochMillis * Timestamps.MICROSECONDS_PER_MILLISECOND + (nanosInTheMilli / Timestamps.NANOSECONDS_PER_MICROSECOND);
            int picosOfMicro = nanosInTheMilli % 1000 * 1000;
            return new LongTimestamp(micro, picosOfMicro);
        });
    }

    private static LongWriteFunction timeWriteFunction(int precision)
    {
        checkArgument(precision <= MAX_SUPPORTED_TEMPORAL_PRECISION, "Unsupported precision: %s", precision);
        return new LongWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                return format("CAST(? AS time(%s))", precision);
            }

            @Override
            public void set(PreparedStatement statement, int index, long picosOfDay)
                    throws SQLException
            {
                picosOfDay = Timestamps.round(picosOfDay, 12 - precision);
                if (picosOfDay == Timestamps.PICOSECONDS_PER_DAY) {
                    picosOfDay = 0;
                }
                LocalTime localTime = LocalTime.ofNanoOfDay(picosOfDay / PICOSECONDS_PER_NANOSECOND);
                // statement.setObject(.., localTime) would yield incorrect end result for 23:59:59.999000
                statement.setString(index, SNOWFLAKE_TIME_FORMATTER.format(localTime));
            }
        };
    }

    private static ColumnMapping varcharColumnMapping(int varcharLength, Optional<CaseSensitivity> caseSensitivity)
    {
        VarcharType varcharType = varcharLength <= VarcharType.MAX_LENGTH ? createVarcharType(varcharLength) : createUnboundedVarcharType();
        return StandardColumnMappings.varcharColumnMapping(varcharType, caseSensitivity.orElse(CASE_INSENSITIVE) == CASE_SENSITIVE);
    }

    private static ObjectWriteFunction longTimestampWithTzWriteFunction()
    {
        return ObjectWriteFunction.of(LongTimestampWithTimeZone.class, (statement, index, value) -> {
            long epochMilli = value.getEpochMillis();
            long epochSecond = Math.floorDiv(epochMilli, MILLISECONDS_PER_SECOND);
            int nanosOfSecond = Math.floorMod(epochMilli, MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND + value.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND;
            ZoneId zone = TimeZoneKey.getTimeZoneKey(value.getTimeZoneKey()).getZoneId();
            Instant instant = Instant.ofEpochSecond(epochSecond, nanosOfSecond);
            statement.setString(index, SNOWFLAKE_DATETIME_FORMATTER.format(ZonedDateTime.ofInstant(instant, zone)));
        });
    }

    private static LongWriteFunction snowFlakeDateWriter()
    {
        return (statement, index, day) -> statement.setString(index, SNOWFLAKE_DATE_FORMATTER.format(LocalDate.ofEpochDay(day)));
    }

    private static SliceWriteFunction charWriteFunction(CharType charType)
    {
        return (statement, index, value) -> statement.setString(index, Chars.padSpaces(value, charType).toStringUtf8());
    }

    private static WriteMapping snowflakeTimestampWriteMapping(Type type)
    {
        TimestampType timestampType = (TimestampType) type;
        checkArgument(
                timestampType.getPrecision() <= MAX_SUPPORTED_TEMPORAL_PRECISION,
                "The max timestamp precision in Snowflake is " + MAX_SUPPORTED_TEMPORAL_PRECISION);

        if (timestampType.isShort()) {
            return WriteMapping.longMapping(format("timestamp_ntz(%d)", timestampType.getPrecision()), shortTimestampWriteFunction());
        }
        return WriteMapping.objectMapping(format("timestamp_ntz(%d)", timestampType.getPrecision()), longTimestampWriteFunction(timestampType.getPrecision()));
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

    private static WriteMapping snowflakeTimestampWithTimeZoneWriteMapping(Type type)
    {
        TimestampWithTimeZoneType timeTZType = (TimestampWithTimeZoneType) type;

        checkArgument(timeTZType.getPrecision() <= MAX_SUPPORTED_TEMPORAL_PRECISION, "Max Snowflake precision is is " + MAX_SUPPORTED_TEMPORAL_PRECISION);
        if (timeTZType.isShort()) {
            return WriteMapping.longMapping(format("timestamp_tz(%d)", timeTZType.getPrecision()), timestampWithTimeZoneWriteFunction());
        }
        return WriteMapping.objectMapping(format("timestamp_tz(%d)", timeTZType.getPrecision()), longTimestampWithTzWriteFunction());
    }

    private static LongWriteFunction timestampWithTimeZoneWriteFunction()
    {
        return (statement, index, encodedTimeWithZone) -> {
            Instant instant = Instant.ofEpochMilli(DateTimeEncoding.unpackMillisUtc(encodedTimeWithZone));
            ZoneId zone = ZoneId.of(DateTimeEncoding.unpackZoneKey(encodedTimeWithZone).getId());
            statement.setString(index, SNOWFLAKE_DATETIME_FORMATTER.format(instant.atZone(zone)));
        };
    }
}
