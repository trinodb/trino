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
import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.PredicatePushdownController;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.SliceReadFunction;
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
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
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
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Timestamps;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;

import javax.inject.Inject;

import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
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
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.DecimalType.createDecimalType;

public class SnowflakeClient
        extends BaseJdbcClient
{
    private final Type jsonType;
    private final AggregateFunctionRewriter aggregateFunctionRewriter;
    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;

    /* TIME supports an optional precision parameter for fractional seconds, e.g. TIME(3). Time precision can range from 0 (seconds) to 9 (nanoseconds). The default precision is 9.
      All TIME values must be between 00:00:00 and 23:59:59.999999999. TIME internally stores “wallclock” time, and all operations on TIME values are performed without taking any time zone into consideration.
     */
    private static final int SNOWFLAKE_MAX_SUPPORTED_TIMESTAMP_PRECISION = 9;
    private static final Logger log = Logger.get(SnowflakeClient.class);
    private static final DateTimeFormatter snowballDateTimeFormatter = DateTimeFormatter.ofPattern("y-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX");
    private static final DateTimeFormatter snowballDateFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd");
    private static final DateTimeFormatter snowballTimestampFormatter = DateTimeFormatter.ofPattern("y-MM-dd'T'HH:mm:ss.SSSSSSSSS");
    private static final DateTimeFormatter snowballTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS");

    private interface WriteMappingFunction
    {
        WriteMapping convert(Type type);
    }

    private interface ColumnMappingFunction
    {
        Optional<ColumnMapping> convert(JdbcTypeHandle typeHandle);
    }

    private static final TimeZone UTC_TZ = TimeZone.getTimeZone(ZoneId.of("UTC"));
    // Mappings for JDBC column types to internal Trino types
    private static final Map<Integer, ColumnMapping> STANDARD_COLUMN_MAPPINGS = ImmutableMap.<Integer, ColumnMapping>builder()
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

    // Mappings for Pushdown JDBC column types to internal Trino types
//    private static final Map<Integer, ColumnMapping> PUSHDOWN_COLUMN_MAPPINGS = ImmutableMap.<Integer, ColumnMapping>builder()
//            .put(Types.TIME, columnMappingPushdown(timeColumnMapping()))
//            .buildOrThrow();

    private static final Map<String, ColumnMappingFunction> SHOWFLAKE_COLUMN_MAPPINGS = ImmutableMap.<String, ColumnMappingFunction>builder()
            .put("time", typeHandle -> {
                //return Optional.of(columnMappingPushdown(timeColumnMapping(typeHandle)));
                return Optional.of(timeColumnMapping(typeHandle));
            })
            .put("timestampntz", typeHandle -> {
                return Optional.of(timestampColumnMapping(typeHandle));
            })
            .put("timestamptz", typeHandle -> {
                return Optional.of(timestampTZColumnMapping(typeHandle));
            })
            .put("timestampltz", typeHandle -> {
                return Optional.of(timestampTZColumnMapping(typeHandle));
            })
            .put("date", typeHandle -> {
                return Optional.of(ColumnMapping.longMapping(
                        DateType.DATE, (resultSet, columnIndex) ->
                                LocalDate.ofEpochDay(resultSet.getLong(columnIndex)).toEpochDay(),
                        snowFlakeDateWriter()));
            })
            .put("object", typeHandle -> {
                return Optional.of(ColumnMapping.sliceMapping(
                        VarcharType.createUnboundedVarcharType(),
                        StandardColumnMappings.varcharReadFunction(VarcharType.createUnboundedVarcharType()),
                        StandardColumnMappings.varcharWriteFunction(),
                        PredicatePushdownController.DISABLE_PUSHDOWN));
            })
            .put("array", typeHandle -> {
                return Optional.of(ColumnMapping.sliceMapping(
                        VarcharType.createUnboundedVarcharType(),
                        StandardColumnMappings.varcharReadFunction(VarcharType.createUnboundedVarcharType()),
                        StandardColumnMappings.varcharWriteFunction(),
                        PredicatePushdownController.DISABLE_PUSHDOWN));
            })
            .put("variant", typeHandle -> {
                return Optional.of(ColumnMapping.sliceMapping(
                        VarcharType.createUnboundedVarcharType(), variantReadFunction(), StandardColumnMappings.varcharWriteFunction(),
                        PredicatePushdownController.FULL_PUSHDOWN));
            })
            .put("varchar", typeHandle -> {
                return Optional.of(varcharColumnMapping(typeHandle.getRequiredColumnSize()));
            })
            .put("number", typeHandle -> {
                int decimalDigits = typeHandle.getRequiredDecimalDigits();
                int precision = typeHandle.getRequiredColumnSize() + Math.max(-decimalDigits, 0);
                if (precision > 38) {
                    return Optional.empty();
                }
                return Optional.of(columnMappingPushdown(
                        StandardColumnMappings.decimalColumnMapping(DecimalType.createDecimalType(
                                precision, Math.max(decimalDigits, 0)), RoundingMode.UNNECESSARY)));
            })
            .buildOrThrow();

    // Mappings for internal Trino types to JDBC column types
    private static final Map<String, WriteMapping> STANDARD_WRITE_MAPPINGS = ImmutableMap.<String, WriteMapping>builder()
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

    private static final Map<String, WriteMappingFunction> SNOWFLAKE_WRITE_MAPPINGS = ImmutableMap.<String, WriteMappingFunction>builder()
            .put("TimeType", type -> {
                // return Optional.of(columnMappingPushdown(timeColumnMapping()));
                return WriteMapping.longMapping("time", SnowflakeClient.snowFlaketimeWriter(type));
            })
            .put("ShortTimestampType", type -> {
                WriteMapping myMap = SnowflakeClient.snowFlakeTimestampWriter(type);
                return myMap;
            })
            .put("ShortTimestampWithTimeZoneType", type -> {
                WriteMapping myMap = SnowflakeClient.snowFlakeTimestampWithTZWriter(type);
                return myMap;
            })
            .put("LongTimestampType", type -> {
                WriteMapping myMap = SnowflakeClient.snowFlakeTimestampWithTZWriter(type);
                return myMap;
            })
            .put("LongTimestampWithTimeZoneType", type -> {
                WriteMapping myMap = SnowflakeClient.snowFlakeTimestampWithTZWriter(type);
                return myMap;
            })
            .put("VarcharType", type -> {
                WriteMapping myMap = SnowflakeClient.snowFlakeVarCharWriter(type);
                return myMap;
            })
            .put("CharType", type -> {
                WriteMapping myMap = SnowflakeClient.snowFlakeCharWriter(type);
                return myMap;
            })
            .put("LongDecimalType", type -> {
                WriteMapping myMap = SnowflakeClient.snowFlakeDecimalWriter(type);
                return myMap;
            })
            .put("ShortDecimalType", type -> {
                WriteMapping myMap = SnowflakeClient.snowFlakeDecimalWriter(type);
                return myMap;
            })
            .buildOrThrow();

    @Inject
    public SnowflakeClient(BaseJdbcConfig config, ConnectionFactory connectionFactory, QueryBuilder queryBuilder,
                           TypeManager typeManager, IdentifierMapping identifierMapping,
                           RemoteQueryModifier remoteQueryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, remoteQueryModifier, false);
        this.jsonType = typeManager.getType(new TypeSignature(StandardTypes.JSON));

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .build();

        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                this.connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementMinMax(false))
                        .add(new ImplementSum(SnowflakeClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        // .add(new ImplementAvgBigint())
                        .build());
    }

    @Override
    public void abortReadConnection(Connection connection, ResultSet resultSet)
            throws SQLException
    {
        // Abort connection before closing. Without this, the Snowflake driver
        // attempts to drain the connection by reading all the results.
        connection.abort(directExecutor());
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));
        jdbcTypeName = jdbcTypeName.toLowerCase(Locale.ENGLISH);
        int type = typeHandle.getJdbcType();

        ColumnMapping columnMap = STANDARD_COLUMN_MAPPINGS.get(type);
        if (columnMap != null) {
            return Optional.of(columnMap);
        }

        ColumnMappingFunction columnMappingFunction = SHOWFLAKE_COLUMN_MAPPINGS.get(jdbcTypeName);
        if (columnMappingFunction != null) {
            return columnMappingFunction.convert(typeHandle);
        }

//        ColumnMapping pushColumnMap = PUSHDOWN_COLUMN_MAPPINGS.get(type);
//        if (pushColumnMap != null) {
//            return Optional.of(pushColumnMap);
//        }

        // Code should never reach here so throw an error.
        throw new TrinoException(NOT_SUPPORTED, "SNOWFLAKE_CONNECTOR_COLUMN_TYPE_NOT_SUPPORTED: Unsupported column type(" + type +
                "):" + jdbcTypeName);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        Class myClass = type.getClass();
        String simple = myClass.getSimpleName();

        WriteMapping writeMapping = STANDARD_WRITE_MAPPINGS.get(simple);
        if (writeMapping != null) {
            return writeMapping;
        }

        WriteMappingFunction writeMappingFunction = SNOWFLAKE_WRITE_MAPPINGS.get(simple);
        if (writeMappingFunction != null) {
            return writeMappingFunction.convert(type);
        }

        throw new TrinoException(NOT_SUPPORTED, "SNOWFLAKE_CONNECTOR_COLUMN_TYPE_NOT_SUPPORTED: Unsupported column type: " + type.getDisplayName() + ", simple:" + simple);
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

    private ColumnMapping jsonColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                jsonType,
                (resultSet, columnIndex) -> jsonParse(utf8Slice(resultSet.getString(columnIndex))),
                StandardColumnMappings.varcharWriteFunction(),
                DISABLE_PUSHDOWN);
    }

    private static SliceReadFunction variantReadFunction()
    {
        return (resultSet, columnIndex) -> Slices.utf8Slice(resultSet.getString(columnIndex).replaceAll("^\"|\"$", ""));
    }

    private static ColumnMapping columnMappingPushdown(ColumnMapping mapping)
    {
        if (mapping.getPredicatePushdownController() == PredicatePushdownController.DISABLE_PUSHDOWN) {
            throw new TrinoException(NOT_SUPPORTED, "mapping.getPredicatePushdownController() is DISABLE_PUSHDOWN. Type was " + mapping.getType());
        }

        return new ColumnMapping(mapping.getType(), mapping.getReadFunction(), mapping.getWriteFunction(),
                PredicatePushdownController.FULL_PUSHDOWN);
    }

    private static ColumnMapping timeColumnMapping(JdbcTypeHandle typeHandle)
    {
//        return ColumnMapping.longMapping(
//                TimeType.TIME, (resultSet, columnIndex) -> toTrinoTime(resultSet.getTime(columnIndex)), snowFlaketimeWriter());

        int precision = typeHandle.getRequiredDecimalDigits();
        checkArgument((precision <= SNOWFLAKE_MAX_SUPPORTED_TIMESTAMP_PRECISION),
                "The max timestamp precision in Snowflake is " + SNOWFLAKE_MAX_SUPPORTED_TIMESTAMP_PRECISION);
        return ColumnMapping.longMapping(
                TimeType.createTimeType(precision),
                (resultSet, columnIndex) -> {
                    LocalTime time = snowballTimeFormatter.parse(resultSet.getString(columnIndex), LocalTime::from);
                    long nanosOfDay = time.toNanoOfDay();
                    long picosOfDay = nanosOfDay * Timestamps.PICOSECONDS_PER_NANOSECOND;
                    return Timestamps.round(picosOfDay, 12 - precision);
                },
                timeWriteFunction(precision),
                PredicatePushdownController.FULL_PUSHDOWN);
    }

    private static LongWriteFunction snowFlaketimeWriter(Type type)
    {
        TimeType timeType = (TimeType) type;
        int precision = timeType.getPrecision();
        return timeWriteFunction(precision);
    }

    private static LongWriteFunction timeWriteFunction(int precision)
    {
        checkArgument(precision <= SNOWFLAKE_MAX_SUPPORTED_TIMESTAMP_PRECISION, "Unsupported precision: %s", precision);
        String bindExpression = String.format("CAST(? AS time(%s))", precision);
        return new LongWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                return bindExpression;
            }

            @Override
            public void set(PreparedStatement statement, int index, long picosOfDay)
                    throws SQLException
            {
                picosOfDay = Timestamps.round(picosOfDay, 12 - precision);
                if (picosOfDay == Timestamps.PICOSECONDS_PER_DAY) {
                    picosOfDay = 0;
                }
                LocalTime localTime = LocalTime.ofNanoOfDay(picosOfDay / Timestamps.PICOSECONDS_PER_NANOSECOND);
                // statement.setObject(.., localTime) would yield incorrect end result for 23:59:59.999000
                statement.setString(index, snowballTimeFormatter.format(localTime));
            }
        };
    }

    private static long toTrinoTime(Time sqlTime)
    {
        return Timestamps.PICOSECONDS_PER_SECOND * sqlTime.getTime();
    }

    // timestampWithTimezoneColumnMapping
    private static ColumnMapping timestampTZColumnMapping(JdbcTypeHandle typeHandle)
    {
        int precision = typeHandle.getRequiredDecimalDigits();
        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));
        int type = typeHandle.getJdbcType();
        log.debug("timestampTZColumnMapping: jdbcTypeName(%s):%s precision:precision", type, jdbcTypeName, precision);

        if (precision <= 3) {
            return ColumnMapping.longMapping(TimestampWithTimeZoneType.createTimestampWithTimeZoneType(precision),
                    (resultSet, columnIndex) -> {
                        ZonedDateTime timestamp = (ZonedDateTime) snowballDateTimeFormatter.parse(resultSet.getString(columnIndex), ZonedDateTime::from);
                        return DateTimeEncoding.packDateTimeWithZone(timestamp.toInstant().toEpochMilli(), timestamp.getZone().getId());
                    },
                    timestampWithTZWriter(), PredicatePushdownController.FULL_PUSHDOWN);
        }
        else {
            return ColumnMapping.objectMapping(TimestampWithTimeZoneType.createTimestampWithTimeZoneType(precision), longTimestampWithTimezoneReadFunction(), longTimestampWithTZWriteFunction());
        }
    }

    private static ColumnMapping varcharColumnMapping(int varcharLength)
    {
        VarcharType varcharType = varcharLength <= VarcharType.MAX_LENGTH
                ? VarcharType.createVarcharType(varcharLength)
                : VarcharType.createUnboundedVarcharType();
        return ColumnMapping.sliceMapping(
                varcharType,
                StandardColumnMappings.varcharReadFunction(varcharType),
                StandardColumnMappings.varcharWriteFunction());
    }

    private static ObjectReadFunction longTimestampWithTimezoneReadFunction()
    {
        return ObjectReadFunction.of(LongTimestampWithTimeZone.class, (resultSet, columnIndex) -> {
            ZonedDateTime timestamp = (ZonedDateTime) snowballDateTimeFormatter.parse(resultSet.getString(columnIndex), ZonedDateTime::from);
            return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(timestamp.toEpochSecond(),
                    (long) timestamp.getNano() * Timestamps.PICOSECONDS_PER_NANOSECOND,
                    TimeZoneKey.getTimeZoneKey(timestamp.getZone().getId()));
        });
    }

    // longTimestampWithTimezoneWriteFunction
    private static ObjectWriteFunction longTimestampWithTZWriteFunction()
    {
        return ObjectWriteFunction.of(LongTimestampWithTimeZone.class, (statement, index, value) -> {
            long epoMilli = value.getEpochMillis();
            long epoSeconds = Math.floorDiv(epoMilli, Timestamps.MILLISECONDS_PER_SECOND);
            long adjNano = Math.floorMod(epoMilli, Timestamps.MILLISECONDS_PER_SECOND) *
                    Timestamps.NANOSECONDS_PER_MILLISECOND + value.getPicosOfMilli() / Timestamps.PICOSECONDS_PER_NANOSECOND;
            ZoneId zone = TimeZoneKey.getTimeZoneKey(value.getTimeZoneKey()).getZoneId();
            Instant timeI = Instant.ofEpochSecond(epoSeconds, adjNano);
            statement.setString(index, snowballDateTimeFormatter.format(ZonedDateTime.ofInstant(timeI, zone)));
        });
    }

    private static LongWriteFunction snowFlakeDateTimeWriter()
    {
        return (statement, index, encodedTimeWithZone) -> {
            Instant time = Instant.ofEpochMilli(DateTimeEncoding.unpackMillisUtc(encodedTimeWithZone));
            ZoneId zone = ZoneId.of(DateTimeEncoding.unpackZoneKey(encodedTimeWithZone).getId());
            statement.setString(index, snowballDateTimeFormatter.format(time.atZone(zone)));
        };
    }

    private static WriteMapping snowFlakeDecimalWriter(Type type)
    {
        DecimalType decimalType = (DecimalType) type;
        String dataType = String.format("decimal(%s, %s)", new Object[] {
                Integer.valueOf(decimalType.getPrecision()), Integer.valueOf(decimalType.getScale())
        });

        if (decimalType.isShort()) {
            return WriteMapping.longMapping(dataType, StandardColumnMappings.shortDecimalWriteFunction(decimalType));
        }
        return WriteMapping.objectMapping(dataType, StandardColumnMappings.longDecimalWriteFunction(decimalType));
    }

    private static LongWriteFunction snowFlakeDateWriter()
    {
        return (statement, index, day) -> statement.setString(index, snowballDateFormatter.format(LocalDate.ofEpochDay(day)));
    }

    private static WriteMapping snowFlakeCharWriter(Type type)
    {
        CharType charType = (CharType) type;
        return WriteMapping.sliceMapping("char(" + charType.getLength() + ")",
                charWriteFunction(charType));
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
        checkArgument((timestampType.getPrecision() <= SNOWFLAKE_MAX_SUPPORTED_TIMESTAMP_PRECISION),
                "The max timestamp precision in Snowflake is " + SNOWFLAKE_MAX_SUPPORTED_TIMESTAMP_PRECISION);

        if (timestampType.isShort()) {
            return WriteMapping.longMapping(
                    String.format("timestamp_ntz(%d)", new Object[] {Integer.valueOf(timestampType.getPrecision()) }),
                    timestampWriteFunction());
        }
        return WriteMapping.objectMapping(
                String.format("timestamp_ntz(%d)", new Object[] {Integer.valueOf(timestampType.getPrecision()) }),
                longTimestampWriter(timestampType.getPrecision()));
    }

    private static LongWriteFunction timestampWriteFunction()
    {
        return (statement, index, value) -> statement.setString(index,
                StandardColumnMappings.fromTrinoTimestamp(value).toString());
    }

    private static ObjectWriteFunction longTimestampWriter(int precision)
    {
        return ObjectWriteFunction.of(LongTimestamp.class,
                (statement, index, value) -> statement.setString(index,
                        snowballTimestampFormatter.format(StandardColumnMappings.fromLongTrinoTimestamp(value,
                                precision))));
    }

    private static WriteMapping snowFlakeTimestampWithTZWriter(Type type)
    {
        TimestampWithTimeZoneType timeTZType = (TimestampWithTimeZoneType) type;

        checkArgument((timeTZType.getPrecision() <= SNOWFLAKE_MAX_SUPPORTED_TIMESTAMP_PRECISION),
                "Max Snowflake precision is is " + SNOWFLAKE_MAX_SUPPORTED_TIMESTAMP_PRECISION);
        if (timeTZType.isShort()) {
            return WriteMapping.longMapping(String.format("timestamp_tz(%d)",
                            new Object[] {Integer.valueOf(timeTZType.getPrecision()) }),
                    timestampWithTZWriter());
        }
        return WriteMapping.objectMapping(
                String.format("timestamp_tz(%d)", new Object[] {Integer.valueOf(timeTZType.getPrecision()) }),
                longTimestampWithTZWriteFunction());
    }

    private static LongWriteFunction timestampWithTZWriter()
    {
        return (statement, index, encodedTimeWithZone) -> {
            Instant timeI = Instant.ofEpochMilli(DateTimeEncoding.unpackMillisUtc(encodedTimeWithZone));
            ZoneId zone = ZoneId.of(DateTimeEncoding.unpackZoneKey(encodedTimeWithZone).getId());
            statement.setString(index, snowballDateTimeFormatter.format(timeI.atZone(zone)));
        };
    }

    private static ObjectReadFunction longTimestampWithTZReadFunction()
    {
        return ObjectReadFunction.of(LongTimestampWithTimeZone.class, (resultSet, columnIndex) -> {
            ZonedDateTime timestamp = snowballDateTimeFormatter.<ZonedDateTime>parse(resultSet.getString(columnIndex), ZonedDateTime::from);
            return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(timestamp.toEpochSecond(),
                    timestamp.getNano() * Timestamps.PICOSECONDS_PER_NANOSECOND, TimeZoneKey.getTimeZoneKey(timestamp.getZone().getId()));
        });
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

    private static ColumnMapping timestampColumnMapping(JdbcTypeHandle typeHandle)
    {
        int precision = typeHandle.getRequiredDecimalDigits();
        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));
        int type = typeHandle.getJdbcType();
        log.debug("timestampColumnMapping: jdbcTypeName(%s):%s precision:%s", type, jdbcTypeName, precision);

        // <= 6 fits into a long
        if (precision <= 6) {
            return ColumnMapping.longMapping(
                    (Type) TimestampType.createTimestampType(precision), (resultSet, columnIndex) ->
                            StandardColumnMappings.toTrinoTimestamp(TimestampType.createTimestampType(precision),
                                    toLocalDateTime(resultSet, columnIndex)),
                    timestampWriteFunction());
        }

        // Too big. Put it in an object
        return ColumnMapping.objectMapping(
                (Type) TimestampType.createTimestampType(precision),
                longTimestampReader(),
                longTimestampWriter(precision));
    }

    private static LocalDateTime toLocalDateTime(ResultSet resultSet, int columnIndex) throws SQLException
    {
        Calendar calendar = new GregorianCalendar(UTC_TZ, Locale.ENGLISH);
        calendar.setTime(new Date(0));
        Timestamp ts = resultSet.getTimestamp(columnIndex, calendar);
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(ts.getTime()), ZoneOffset.UTC);
    }

    private static LongWriteFunction timestampWriter()
    {
        return (statement, index, value) -> statement.setString(index, StandardColumnMappings.fromTrinoTimestamp(value).toString());
    }

    private static Optional<ColumnMapping> getUnsignedMapping(JdbcTypeHandle typeHandle)
    {
        if (!typeHandle.getJdbcTypeName().isPresent()) {
            return Optional.empty();
        }

        String typeName = typeHandle.getJdbcTypeName().get();
        if (typeName.equalsIgnoreCase("tinyint unsigned")) {
            return Optional.of(StandardColumnMappings.smallintColumnMapping());
        }
        if (typeName.equalsIgnoreCase("smallint unsigned")) {
            return Optional.of(StandardColumnMappings.integerColumnMapping());
        }
        if (typeName.equalsIgnoreCase("int unsigned")) {
            return Optional.of(StandardColumnMappings.bigintColumnMapping());
        }
        if (typeName.equalsIgnoreCase("bigint unsigned")) {
            return Optional.of(StandardColumnMappings.decimalColumnMapping(createDecimalType(20)));
        }

        return Optional.empty();
    }
}
