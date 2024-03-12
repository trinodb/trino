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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
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

import java.math.RoundingMode;
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
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;

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

    private interface WriteMappingFunction
    {
        WriteMapping convert(Type type);
    }

    private interface ColumnMappingFunction
    {
        Optional<ColumnMapping> convert(JdbcTypeHandle typeHandle);
    }

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
                        .build());
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));
        jdbcTypeName = jdbcTypeName.toLowerCase(Locale.ENGLISH);
        int type = typeHandle.getJdbcType();

        // Mappings for JDBC column types to internal Trino types
        final Map<Integer, ColumnMapping> standardColumnMappings = ImmutableMap.<Integer, ColumnMapping>builder()
                .put(Types.BOOLEAN, StandardColumnMappings.booleanColumnMapping())
                .put(Types.TINYINT, StandardColumnMappings.tinyintColumnMapping())
                .put(Types.SMALLINT, StandardColumnMappings.smallintColumnMapping())
                .put(Types.INTEGER, StandardColumnMappings.integerColumnMapping())
                .put(Types.BIGINT, StandardColumnMappings.bigintColumnMapping())
                .put(Types.REAL, StandardColumnMappings.realColumnMapping())
                .put(Types.DOUBLE, StandardColumnMappings.doubleColumnMapping())
                .put(Types.FLOAT, StandardColumnMappings.doubleColumnMapping())
                .put(Types.BINARY, StandardColumnMappings.varbinaryColumnMapping())
                .put(Types.VARBINARY, StandardColumnMappings.varbinaryColumnMapping())
                .put(Types.LONGVARBINARY, StandardColumnMappings.varbinaryColumnMapping())
                .buildOrThrow();

        ColumnMapping columnMap = standardColumnMappings.get(type);
        if (columnMap != null) {
            return Optional.of(columnMap);
        }

        final Map<String, ColumnMappingFunction> snowflakeColumnMappings = ImmutableMap.<String, ColumnMappingFunction>builder()
                .put("time", handle -> Optional.of(timeColumnMapping(handle.getRequiredDecimalDigits())))
                .put("timestampntz", handle -> Optional.of(timestampColumnMapping(handle.getRequiredDecimalDigits())))
                .put("timestamptz", handle -> Optional.of(timestampTZColumnMapping(handle.getRequiredDecimalDigits())))
                .put("date", handle -> Optional.of(ColumnMapping.longMapping(DateType.DATE, (resultSet, columnIndex) -> LocalDate.ofEpochDay(resultSet.getLong(columnIndex)).toEpochDay(), snowFlakeDateWriter())))
                .put("varchar", handle -> Optional.of(varcharColumnMapping(handle.getRequiredColumnSize())))
                .put("number", handle -> {
                    int decimalDigits = handle.getRequiredDecimalDigits();
                    int precision = handle.getRequiredColumnSize() + Math.max(-decimalDigits, 0);
                    if (precision > 38) {
                        return Optional.empty();
                    }
                    return Optional.of(columnMappingPushdown(
                            StandardColumnMappings.decimalColumnMapping(DecimalType.createDecimalType(precision, Math.max(decimalDigits, 0)), RoundingMode.UNNECESSARY)));
                })
                .buildOrThrow();

        ColumnMappingFunction columnMappingFunction = snowflakeColumnMappings.get(jdbcTypeName);
        if (columnMappingFunction != null) {
            return columnMappingFunction.convert(typeHandle);
        }

        return Optional.empty();
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        Class<?> myClass = type.getClass();
        String simple = myClass.getSimpleName();

        // Mappings for internal Trino types to JDBC column types
        final Map<String, WriteMapping> standardWriteMappings = ImmutableMap.<String, WriteMapping>builder()
                .put("BooleanType", WriteMapping.booleanMapping("boolean", StandardColumnMappings.booleanWriteFunction()))
                .put("BigintType", WriteMapping.longMapping("number(19)", StandardColumnMappings.bigintWriteFunction()))
                .put("IntegerType", WriteMapping.longMapping("number(10)", StandardColumnMappings.integerWriteFunction()))
                .put("SmallintType", WriteMapping.longMapping("number(5)", StandardColumnMappings.smallintWriteFunction()))
                .put("TinyintType", WriteMapping.longMapping("number(3)", StandardColumnMappings.tinyintWriteFunction()))
                .put("DoubleType", WriteMapping.doubleMapping("double precision", StandardColumnMappings.doubleWriteFunction()))
                .put("RealType", WriteMapping.longMapping("real", StandardColumnMappings.realWriteFunction()))
                .put("VarbinaryType", WriteMapping.sliceMapping("varbinary", StandardColumnMappings.varbinaryWriteFunction()))
                .put("DateType", WriteMapping.longMapping("date", snowFlakeDateWriter()))
                .buildOrThrow();

        WriteMapping writeMapping = standardWriteMappings.get(simple);
        if (writeMapping != null) {
            return writeMapping;
        }

        final Map<String, WriteMappingFunction> snowflakeWriteMappings = ImmutableMap.<String, WriteMappingFunction>builder()
                .put("TimeType", writeType -> WriteMapping.longMapping("time", timeWriteFunction(((TimeType) writeType).getPrecision())))
                .put("ShortTimestampType", writeType -> SnowflakeClient.snowFlakeTimestampWriter(writeType))
                .put("ShortTimestampWithTimeZoneType", writeType -> SnowflakeClient.snowFlakeTimestampWithTZWriter(writeType))
                .put("LongTimestampType", writeType -> SnowflakeClient.snowFlakeTimestampWithTZWriter(writeType))
                .put("LongTimestampWithTimeZoneType", writeType -> SnowflakeClient.snowFlakeTimestampWithTZWriter(writeType))
                .put("VarcharType", writeType -> SnowflakeClient.snowFlakeVarCharWriter(writeType))
                .put("CharType", writeType -> SnowflakeClient.snowFlakeCharWriter(writeType))
                .put("LongDecimalType", writeType -> SnowflakeClient.snowFlakeDecimalWriter(writeType))
                .put("ShortDecimalType", writeType -> SnowflakeClient.snowFlakeDecimalWriter(writeType))
                .buildOrThrow();

        WriteMappingFunction writeMappingFunction = snowflakeWriteMappings.get(simple);
        if (writeMappingFunction != null) {
            return writeMappingFunction.convert(type);
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName() + ", simple:" + simple);
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        // TODO support complex ConnectorExpressions
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
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

    private static ColumnMapping columnMappingPushdown(ColumnMapping mapping)
    {
        if (mapping.getPredicatePushdownController() == PredicatePushdownController.DISABLE_PUSHDOWN) {
            throw new TrinoException(NOT_SUPPORTED, "mapping.getPredicatePushdownController() is DISABLE_PUSHDOWN. Type was " + mapping.getType());
        }

        return new ColumnMapping(mapping.getType(), mapping.getReadFunction(), mapping.getWriteFunction(), PredicatePushdownController.FULL_PUSHDOWN);
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

    private static ColumnMapping timestampTZColumnMapping(int precision)
    {
        if (precision <= 3) {
            return ColumnMapping.longMapping(TimestampWithTimeZoneType.createTimestampWithTimeZoneType(precision),
                    (resultSet, columnIndex) -> {
                        ZonedDateTime timestamp = SNOWFLAKE_DATETIME_FORMATTER.parse(resultSet.getString(columnIndex), ZonedDateTime::from);
                        return DateTimeEncoding.packDateTimeWithZone(timestamp.toInstant().toEpochMilli(), timestamp.getZone().getId());
                    },
                    timestampWithTZWriter(), PredicatePushdownController.FULL_PUSHDOWN);
        }
        else {
            return ColumnMapping.objectMapping(TimestampWithTimeZoneType.createTimestampWithTimeZoneType(precision), longTimestampWithTimezoneReadFunction(), longTimestampWithTZWriteFunction());
        }
    }

    private static LongWriteFunction timestampWithTZWriter()
    {
        return (statement, index, encodedTimeWithZone) -> {
            Instant timeI = Instant.ofEpochMilli(DateTimeEncoding.unpackMillisUtc(encodedTimeWithZone));
            ZoneId zone = ZoneId.of(DateTimeEncoding.unpackZoneKey(encodedTimeWithZone).getId());
            statement.setString(index, SNOWFLAKE_DATETIME_FORMATTER.format(timeI.atZone(zone)));
        };
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

    private static ObjectWriteFunction longTimestampWithTZWriteFunction()
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
        // <= 6 fits into a long
        if (precision <= 6) {
            return ColumnMapping.longMapping(TimestampType.createTimestampType(precision), (resultSet, columnIndex) -> StandardColumnMappings.toTrinoTimestamp(TimestampType.createTimestampType(precision), toLocalDateTime(resultSet, columnIndex)), timestampWriteFunction());
        }

        // Too big. Put it in an object
        return ColumnMapping.objectMapping(TimestampType.createTimestampType(precision), longTimestampReader(), longTimestampWriter(precision));
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

    private static ColumnMapping varcharColumnMapping(int varcharLength)
    {
        VarcharType varcharType = varcharLength <= VarcharType.MAX_LENGTH ? createVarcharType(varcharLength) : createUnboundedVarcharType();
        return ColumnMapping.sliceMapping(
                varcharType,
                StandardColumnMappings.varcharReadFunction(varcharType),
                StandardColumnMappings.varcharWriteFunction());
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

    private static WriteMapping snowFlakeDecimalWriter(Type type)
    {
        DecimalType decimalType = (DecimalType) type;
        String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());

        if (decimalType.isShort()) {
            return WriteMapping.longMapping(dataType, StandardColumnMappings.shortDecimalWriteFunction(decimalType));
        }
        return WriteMapping.objectMapping(dataType, StandardColumnMappings.longDecimalWriteFunction(decimalType));
    }

    private static LongWriteFunction snowFlakeDateWriter()
    {
        return (statement, index, day) -> statement.setString(index, SNOWFLAKE_DATE_FORMATTER.format(LocalDate.ofEpochDay(day)));
    }

    private static WriteMapping snowFlakeCharWriter(Type type)
    {
        CharType charType = (CharType) type;
        return WriteMapping.sliceMapping("char(" + charType.getLength() + ")", charWriteFunction(charType));
    }

    private static WriteMapping snowFlakeVarCharWriter(Type type)
    {
        String dataType;
        VarcharType varcharType = (VarcharType) type;

        if (varcharType.isUnbounded()) {
            dataType = "varchar";
        }
        else {
            dataType = "varchar(" + varcharType.getBoundedLength() + ")";
        }
        return WriteMapping.sliceMapping(dataType, StandardColumnMappings.varcharWriteFunction());
    }

    private static SliceWriteFunction charWriteFunction(CharType charType)
    {
        return (statement, index, value) -> statement.setString(index, Chars.padSpaces(value, charType).toStringUtf8());
    }

    private static WriteMapping snowFlakeTimestampWriter(Type type)
    {
        TimestampType timestampType = (TimestampType) type;
        checkArgument(
                timestampType.getPrecision() <= MAX_SUPPORTED_TEMPORAL_PRECISION,
                "The max timestamp precision in Snowflake is " + MAX_SUPPORTED_TEMPORAL_PRECISION);

        if (timestampType.isShort()) {
            return WriteMapping.longMapping(format("timestamp_ntz(%d)", timestampType.getPrecision()), timestampWriteFunction());
        }
        return WriteMapping.objectMapping(format("timestamp_ntz(%d)", timestampType.getPrecision()), longTimestampWriter(timestampType.getPrecision()));
    }

    private static LongWriteFunction timestampWriteFunction()
    {
        return (statement, index, value) -> statement.setString(index, StandardColumnMappings.fromTrinoTimestamp(value).toString());
    }

    private static ObjectWriteFunction longTimestampWriter(int precision)
    {
        return ObjectWriteFunction.of(
                LongTimestamp.class,
                (statement, index, value) -> statement.setString(index, SNOWFLAKE_TIMESTAMP_FORMATTER.format(StandardColumnMappings.fromLongTrinoTimestamp(value, precision))));
    }

    private static WriteMapping snowFlakeTimestampWithTZWriter(Type type)
    {
        TimestampWithTimeZoneType timeTZType = (TimestampWithTimeZoneType) type;

        checkArgument(timeTZType.getPrecision() <= MAX_SUPPORTED_TEMPORAL_PRECISION, "Max Snowflake precision is is " + MAX_SUPPORTED_TEMPORAL_PRECISION);
        if (timeTZType.isShort()) {
            return WriteMapping.longMapping(format("timestamp_tz(%d)", timeTZType.getPrecision()), timestampWithTimezoneWriteFunction());
        }
        return WriteMapping.objectMapping(format("timestamp_tz(%d)", timeTZType.getPrecision()), longTimestampWithTzWriteFunction());
    }

    private static LongWriteFunction timestampWithTimezoneWriteFunction()
    {
        return (statement, index, encodedTimeWithZone) -> {
            Instant instant = Instant.ofEpochMilli(DateTimeEncoding.unpackMillisUtc(encodedTimeWithZone));
            ZoneId zone = ZoneId.of(DateTimeEncoding.unpackZoneKey(encodedTimeWithZone).getId());
            statement.setString(index, SNOWFLAKE_DATETIME_FORMATTER.format(instant.atZone(zone)));
        };
    }
}
