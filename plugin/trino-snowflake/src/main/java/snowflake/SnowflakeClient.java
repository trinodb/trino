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
package snowflake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.math.LongMath;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.BooleanReadFunction;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DoubleReadFunction;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
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
import io.trino.plugin.jdbc.ReadFunction;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.SliceReadFunction;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.StandardColumnMappings;
import io.trino.plugin.jdbc.UnsupportedTypeHandling;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.Domain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcJoinPushdownUtil.implementJoinCostAware;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.getDomainCompactionThreshold;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.IGNORE;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneParametricType.TIME_WITH_TIME_ZONE;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.sql.DatabaseMetaData.columnNoNulls;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;

public class SnowflakeClient
        extends BaseJdbcClient
{

    private static final Logger log = Logger.get(SnowflakeClient.class);
    /**
     * @see java.sql.Array#getResultSet()
     */
    private static final int ARRAY_RESULT_SET_VALUE_COLUMN = 2;
    private static final String DUPLICATE_TABLE_SQLSTATE = "42P07";
    private static final int POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION = 6;
    private static final int PRECISION_OF_UNSPECIFIED_DECIMAL = 0;
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS");
    private static final PredicatePushdownController POSTGRESQL_STRING_COLLATION_AWARE_PUSHDOWN = (session, domain) -> {
        if (domain.isOnlyNull()) {
            return FULL_PUSHDOWN.apply(session, domain);
        }
        Domain simplifiedDomain = domain.simplify(getDomainCompactionThreshold(session));
        if (!simplifiedDomain.getValues().isDiscreteSet()) {
            // Domain#simplify can turn a discrete set into a range predicate
            return DISABLE_PUSHDOWN.apply(session, domain);
        }

        return FULL_PUSHDOWN.apply(session, simplifiedDomain);
    };
    private final SnowflakeConfig snowflakeConfig;
    private final SnowflakeSessionPropertiesProvider propertiesProvider;

    @Inject
    public SnowflakeClient(
            BaseJdbcConfig config,
            SnowflakeConfig snowflakeConfig,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            TypeManager typeManager,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier,
            SnowflakeSessionPropertiesProvider propertiesProvider)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, true);
        statisticsConfig.setEnabled(true);
        this.snowflakeConfig = snowflakeConfig;
        this.propertiesProvider = propertiesProvider;
    }

    private static Map<String, Integer> getArrayColumnDimensions(Connection connection, JdbcTableHandle tableHandle)
            throws SQLException
    {
        String sql = "" +
                "SELECT att.attname, greatest(att.attndims, 1) AS attndims " +
                "FROM pg_attribute att " +
                "  JOIN pg_type attyp ON att.atttypid = attyp.oid" +
                "  JOIN pg_class tbl ON tbl.oid = att.attrelid " +
                "  JOIN pg_namespace ns ON tbl.relnamespace = ns.oid " +
                "WHERE ns.nspname = ? " +
                "AND tbl.relname = ? " +
                "AND attyp.typcategory = 'A' ";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            RemoteTableName remoteTableName = tableHandle.getRequiredNamedRelation().getRemoteTableName();
            statement.setString(1, remoteTableName.getSchemaName().orElse(null));
            statement.setString(2, remoteTableName.getTableName());

            Map<String, Integer> arrayColumnDimensions = new HashMap<>();
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    arrayColumnDimensions.put(resultSet.getString("attname"), resultSet.getInt("attndims"));
                }
            }
            return arrayColumnDimensions;
        }
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.NUMERIC, Optional.of("decimal"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
    }

    protected static boolean isCollatable(JdbcColumnHandle column)
    {
        if (column.getColumnType() instanceof CharType || column.getColumnType() instanceof VarcharType) {
            String jdbcTypeName = column.getJdbcTypeHandle().getJdbcTypeName()
                    .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + column.getJdbcTypeHandle()));
            return isCollatable(jdbcTypeName);
        }

        // non-textual types don't have the concept of collation
        return false;
    }

    private static boolean isCollatable(String jdbcTypeName)
    {
        // Only char (internally named bpchar)/varchar/text are the built-in collatable types
        return "bpchar".equals(jdbcTypeName) || "varchar".equals(jdbcTypeName) || "text".equals(jdbcTypeName);
    }

    private static Optional<Long> readRowCountTableStat(StatisticsDao statisticsDao, JdbcTableHandle table)
    {
        RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
        String schemaName = remoteTableName.getSchemaName().orElse(null);
        Optional<Long> rowCount = statisticsDao.getRowCountFromPgClass(schemaName, remoteTableName.getTableName());
        if (rowCount.isEmpty()) {
            // Table not found
            return Optional.empty();
        }

        if (statisticsDao.isPartitionedTable(schemaName, remoteTableName.getTableName())) {
            Optional<Long> partitionedTableRowCount = statisticsDao.getRowCountPartitionedTableFromPgClass(schemaName, remoteTableName.getTableName());
            if (partitionedTableRowCount.isPresent()) {
                return partitionedTableRowCount;
            }

            return statisticsDao.getRowCountPartitionedTableFromPgStats(schemaName, remoteTableName.getTableName());
        }

        if (rowCount.get() == 0) {
            // `pg_class.reltuples = 0` may mean an empty table or a recently populated table (CTAS, LOAD or INSERT)
            // `pg_stat_all_tables.n_live_tup` can be way off, so we use it only as a fallback
            rowCount = statisticsDao.getRowCountFromPgStat(schemaName, remoteTableName.getTableName());
        }

        return rowCount;
    }

    private static ColumnMapping charColumnMapping(int charLength)
    {
        if (charLength > CharType.MAX_LENGTH) {
            return varcharColumnMapping(charLength);
        }
        CharType charType = createCharType(charLength);
        return ColumnMapping.sliceMapping(
                charType,
                charReadFunction(charType),
                charWriteFunction(),
                POSTGRESQL_STRING_COLLATION_AWARE_PUSHDOWN);
    }

    private static ColumnMapping varcharColumnMapping(int varcharLength)
    {
        VarcharType varcharType = varcharLength <= VarcharType.MAX_LENGTH
                ? createVarcharType(varcharLength)
                : createUnboundedVarcharType();
        return ColumnMapping.sliceMapping(
                varcharType,
                varcharReadFunction(varcharType),
                varcharWriteFunction(),
                POSTGRESQL_STRING_COLLATION_AWARE_PUSHDOWN);
    }

    private static ColumnMapping timeColumnMapping(int precision)
    {
        verify(precision <= 6, "Unsupported precision: %s", precision); // PostgreSQL limit but also assumption within this method
        return ColumnMapping.longMapping(
                createTimeType(precision),
                (resultSet, columnIndex) -> {
                    LocalTime time = resultSet.getObject(columnIndex, LocalTime.class);
                    long nanosOfDay = time.toNanoOfDay();
                    if (nanosOfDay == NANOSECONDS_PER_DAY - 1) {
                        // PostgreSQL's 24:00:00 is returned as 23:59:59.999999999, regardless of column precision
                        nanosOfDay = NANOSECONDS_PER_DAY - LongMath.pow(10, 9 - precision);
                    }

                    long picosOfDay = nanosOfDay * PICOSECONDS_PER_NANOSECOND;
                    return round(picosOfDay, 12 - precision);
                },
                timeWriteFunction(precision),
                // Pushdown disabled because PostgreSQL distinguishes TIME '24:00:00' and TIME '00:00:00' whereas Trino does not.
                DISABLE_PUSHDOWN);
    }

    private static LongWriteFunction timeWriteFunction(int precision)
    {
        checkArgument(precision <= 6, "Unsupported precision: %s", precision); // PostgreSQL limit but also assumption within this method
        String bindExpression = format("CAST(? AS time(%s))", precision);
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
                picosOfDay = round(picosOfDay, 12 - precision);
                if (picosOfDay == PICOSECONDS_PER_DAY) {
                    picosOfDay = 0;
                }
                LocalTime localTime = LocalTime.ofNanoOfDay(picosOfDay / PICOSECONDS_PER_NANOSECOND);
                // statement.setObject(.., localTime) would yield incorrect end result for 23:59:59.999000
                statement.setString(index, TIME_FORMATTER.format(localTime));
            }
        };
    }

    // When writing with setObject() using LocalDateTime, driver converts the value to string representing date-time in JVM zone,
    // therefore cannot represent local date-time which is a "gap" in this zone.
    // TODO replace this method with StandardColumnMappings#timestampWriteFunction when https://github.com/pgjdbc/pgjdbc/issues/1390 is done
    private static void shortTimestampWriteFunction(PreparedStatement statement, int index, long epochMicros)
            throws SQLException
    {
        LocalDateTime localDateTime = fromTrinoTimestamp(epochMicros);
        statement.setObject(index, localDateTime.toString());
    }

    private static ObjectWriteFunction longTimestampWriteFunction()
    {
        return ObjectWriteFunction.of(LongTimestamp.class, ((statement, index, timestamp) -> {
            // PostgreSQL supports up to 6 digits of precision
            //noinspection ConstantConditions
            verify(POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION == 6);

            long epochMicros = timestamp.getEpochMicros();
            if (timestamp.getPicosOfMicro() >= PICOSECONDS_PER_MICROSECOND / 2) {
                epochMicros++;
            }
            shortTimestampWriteFunction(statement, index, epochMicros);
        }));
    }

    private static ColumnMapping timestampWithTimeZoneColumnMapping(int precision)
    {
        // PostgreSQL supports timestamptz precision up to microseconds
        checkArgument(precision <= POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION, "unsupported precision value %s", precision);
        TimestampWithTimeZoneType trinoType = createTimestampWithTimeZoneType(precision);
        if (precision <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
            return ColumnMapping.longMapping(
                    trinoType,
                    shortTimestampWithTimeZoneReadFunction(),
                    shortTimestampWithTimeZoneWriteFunction());
        }
        return ColumnMapping.objectMapping(
                trinoType,
                longTimestampWithTimeZoneReadFunction(),
                longTimestampWithTimeZoneWriteFunction());
    }

    private static LongReadFunction shortTimestampWithTimeZoneReadFunction()
    {
        return (resultSet, columnIndex) -> {
            // PostgreSQL does not store zone information in "timestamp with time zone" data type
            long millisUtc = resultSet.getTimestamp(columnIndex).getTime();
            return packDateTimeWithZone(millisUtc, UTC_KEY);
        };
    }

    private static LongWriteFunction shortTimestampWithTimeZoneWriteFunction()
    {
        return (statement, index, value) -> {
            // PostgreSQL does not store zone information in "timestamp with time zone" data type
            long millisUtc = unpackMillisUtc(value);
            statement.setTimestamp(index, new Timestamp(millisUtc));
        };
    }

    private static ObjectReadFunction longTimestampWithTimeZoneReadFunction()
    {
        return ObjectReadFunction.of(
                LongTimestampWithTimeZone.class,
                (resultSet, columnIndex) -> {
                    // PostgreSQL does not store zone information in "timestamp with time zone" data type
                    OffsetDateTime offsetDateTime = resultSet.getObject(columnIndex, OffsetDateTime.class);
                    return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(
                            offsetDateTime.toEpochSecond(),
                            (long) offsetDateTime.getNano() * PICOSECONDS_PER_NANOSECOND,
                            UTC_KEY);
                });
    }

    private static ObjectWriteFunction longTimestampWithTimeZoneWriteFunction()
    {
        return ObjectWriteFunction.of(
                LongTimestampWithTimeZone.class,
                (statement, index, value) -> {
                    // PostgreSQL does not store zone information in "timestamp with time zone" data type
                    long epochSeconds = floorDiv(value.getEpochMillis(), MILLISECONDS_PER_SECOND);
                    long nanosOfSecond = floorMod(value.getEpochMillis(), MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND + value.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND;
                    statement.setObject(index, OffsetDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds, nanosOfSecond), UTC_KEY.getZoneId()));
                });
    }

    private static ObjectReadFunction arrayReadFunction(Type elementType, ReadFunction elementReadFunction)
    {
        return ObjectReadFunction.of(Block.class, (resultSet, columnIndex) -> {
            Array array = resultSet.getArray(columnIndex);
            BlockBuilder builder = elementType.createBlockBuilder(null, 10);
            try (ResultSet arrayAsResultSet = array.getResultSet()) {
                while (arrayAsResultSet.next()) {
                    if (elementReadFunction.isNull(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN)) {
                        builder.appendNull();
                    }
                    else if (elementType.getJavaType() == boolean.class) {
                        elementType.writeBoolean(builder, ((BooleanReadFunction) elementReadFunction).readBoolean(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                    else if (elementType.getJavaType() == long.class) {
                        elementType.writeLong(builder, ((LongReadFunction) elementReadFunction).readLong(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                    else if (elementType.getJavaType() == double.class) {
                        elementType.writeDouble(builder, ((DoubleReadFunction) elementReadFunction).readDouble(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                    else if (elementType.getJavaType() == Slice.class) {
                        elementType.writeSlice(builder, ((SliceReadFunction) elementReadFunction).readSlice(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                    else {
                        elementType.writeObject(builder, ((ObjectReadFunction) elementReadFunction).readObject(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                }
            }

            return builder.build();
        });
    }

    private static ColumnMapping typedVarcharColumnMapping(String jdbcTypeName)
    {
        return ColumnMapping.sliceMapping(
                VARCHAR,
                (resultSet, columnIndex) -> utf8Slice(resultSet.getString(columnIndex)),
                typedVarcharWriteFunction(jdbcTypeName),
                POSTGRESQL_STRING_COLLATION_AWARE_PUSHDOWN);
    }

    private static SliceWriteFunction typedVarcharWriteFunction(String jdbcTypeName)
    {
        String bindExpression = format("CAST(? AS %s)", requireNonNull(jdbcTypeName, "jdbcTypeName is null"));

        return new SliceWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                return bindExpression;
            }

            @Override
            public void set(PreparedStatement statement, int index, Slice value)
                    throws SQLException
            {
                statement.setString(index, value.toStringUtf8());
            }
        };
    }

    private static ColumnMapping moneyColumnMapping()
    {
        /*
         * PostgreSQL JDBC maps "money" to Types.DOUBLE, but fails to retrieve double value for amounts
         * greater than or equal to 1000. Upon `ResultSet#getString`, the driver returns e.g. "$10.00" or "$10,000.00"
         * (currency symbol depends on the server side configuration).
         *
         * The following mapping maps PostgreSQL "money" to Trino "varchar".
         * Writing is disabled for simplicity.
         *
         * Money mapping can be improved when PostgreSQL JDBC gains explicit money type support.
         * See https://github.com/pgjdbc/pgjdbc/issues/425 for more information.
         */
        return ColumnMapping.sliceMapping(
                VARCHAR,
                new SliceReadFunction()
                {
                    @Override
                    public boolean isNull(ResultSet resultSet, int columnIndex)
                            throws SQLException
                    {
                        // super calls ResultSet#getObject(), which for money type calls .getDouble and the call may fail to parse the money value.
                        resultSet.getString(columnIndex);
                        return resultSet.wasNull();
                    }

                    @Override
                    public Slice readSlice(ResultSet resultSet, int columnIndex)
                            throws SQLException
                    {
                        return utf8Slice(resultSet.getString(columnIndex));
                    }
                },(statement, index, value) -> {throw new TrinoException(NOT_SUPPORTED, "Money type is not supported for INSERT");},
                DISABLE_PUSHDOWN);
    }

    private static SliceWriteFunction uuidWriteFunction()
    {
        return (statement, index, value) -> statement.setObject(index, trinoUuidToJavaUuid(value), Types.OTHER);
    }

    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle table)
    {
        System.out.println("PROPS WAS CALLED ");
        Map<String, Object> properties = new HashMap<>();
        properties.put(SnowflakeSessionPropertiesProvider.WRITE_FORMAT, propertiesProvider.getWriteFormat(session));
        System.out.println(properties);
        return properties;
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(io.trino.spi.connector.ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        int jdbcType = typeHandle.getJdbcType();
        System.out.println("Column mapping for type " + typeHandle);
        return switch (jdbcType) {
            //below case mapped as per https://docs.snowflake.com/en/sql-reference/data-types-numeric.html#int-integer-bigint-smallint-tinyint-byteint
            case Types.SMALLINT, Types.INTEGER, Types.TINYINT, Types.BIGINT, Types.NUMERIC -> Optional.of(StandardColumnMappings.bigintColumnMapping());
            case Types.REAL, Types.FLOAT, Types.DOUBLE -> Optional.of(doubleColumnMapping());
            case Types.DECIMAL -> Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, typeHandle.getRequiredColumnSize())));
            case Types.VARCHAR, Types.NVARCHAR, Types.LONGVARCHAR, Types.LONGNVARCHAR -> Optional.of(defaultVarcharColumnMapping(typeHandle.getRequiredColumnSize(), false));
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> Optional.of(ColumnMapping.sliceMapping(VARBINARY, varbinaryReadFunction(), varbinaryWriteFunction(), FULL_PUSHDOWN));
            /**
             * Ideally all date and time for other connectors is parsed as long, however this fails on the snowflake connector hence converted as varchar.
             * Tried converting to Trino Slice type, but that is just a buffered wrap over varchar, hence varchar is a safer choice for now.
             */
            case Types.DATE, Types.TIMESTAMP -> Optional.of(StandardColumnMappings.varcharColumnMapping(VarcharType.VARCHAR, false));
            default -> throw new TrinoException(SnowflakeErrorCode.SNOWFLAKE_COLUMN_MAPPING_ERROR, "Snowflake type mapper cannot build type mapping for JDBC type " + typeHandle.getJdbcType());
        };
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == TINYINT || type == SMALLINT || type == INTEGER || type == BIGINT) {
            return WriteMapping.longMapping("NUMBER(38,0)", bigintWriteFunction());
        }
        if (type == REAL || type == DOUBLE) {
            return WriteMapping.doubleMapping("double precision", doubleWriteFunction());
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, StandardColumnMappings.longDecimalWriteFunction(decimalType));
        }

        if (type == DATE) {
            return WriteMapping.longMapping("date",
                    StandardColumnMappings.dateWriteFunctionUsingLocalDate());
        }

        if (TIME_WITH_TIME_ZONE.equals(type)) {
            return WriteMapping.sliceMapping("TIMESTAMP_TZ", varcharWriteFunction());
        }
        if (TIMESTAMP_MILLIS.equals(type)) {
            // TODO use `timestampWriteFunction` (https://github.com/trinodb/trino/issues/6910)
            return WriteMapping.longMapping("NUMBER(38,0)", bigintWriteFunction());
        }
        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("varbinary", varbinaryWriteFunction());
        }

        if (type instanceof CharType) {
            return WriteMapping.sliceMapping("char(" + ((CharType) type).getLength() + ")", charWriteFunction());
        }

        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "varchar";
            }
            else if (varcharType.getBoundedLength() <= 16777215) {
                dataType = String.format("varchar(%s)", varcharType.getBoundedLength());
            }
            else {
                dataType = "varchar";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        throw new TrinoException(SnowflakeErrorCode.SNOWFLAKE_COLUMN_MAPPING_ERROR, "Cannot create write time mapping for " + type);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            System.out.println("CREATE T" + tableMetadata.getProperties());
            createTable(session, tableMetadata, tableMetadata.getTable().getTableName());
        }
        catch (SQLException e) {
            boolean exists = DUPLICATE_TABLE_SQLSTATE.equals(e.getSQLState());
            throw new TrinoException(exists ? ALREADY_EXISTS : JDBC_ERROR, e);
        }
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        System.out.println(tableMetadata.getClass());
        System.out.println("BEGIN C" + tableMetadata.getProperties());
        return super.beginCreateTable(session, tableMetadata);
    }

    @Override
    public void setTableProperties(ConnectorSession session, JdbcTableHandle handle, Map<String, Optional<Object>> properties)
    {
        super.setTableProperties(session, handle, properties);
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        return super.beginInsertTable(session, tableHandle, columns);
    }

    @Override
    public Optional<String> getTableComment(ResultSet resultSet)
    {
        // Don't return a comment until the connector supports creating tables with comment
        return Optional.empty();
    }

    @Override
    protected void renameTable(ConnectorSession session, Connection connection, String catalogName, String remoteSchemaName, String remoteTableName, String newRemoteSchemaName, String newRemoteTableName)
            throws SQLException
    {
        if (!remoteSchemaName.equals(newRemoteSchemaName)) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables across schemas");
        }

        execute(session, connection, format(
                "ALTER TABLE %s RENAME TO %s",
                quoted(catalogName, remoteSchemaName, remoteTableName),
                quoted(newRemoteTableName)));
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        // fetch-size is ignored when connection is in auto-commit
        connection.setAutoCommit(false);
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(1000);
        return statement;
    }

    protected String createTableSql(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        return format("CREATE TABLE %s (%s)", quoted(remoteTableName), join(", ", columns));
    }

    @Override
    public boolean supportsRetries()
    {
        return false;
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        if (tableHandle.getColumns().isPresent()) {
            return tableHandle.getColumns().get();
        }
        checkArgument(tableHandle.isNamedRelation(), "Cannot get columns for %s", tableHandle);
        SchemaTableName schemaTableName = tableHandle.getRequiredNamedRelation().getSchemaTableName();

        try (Connection connection = connectionFactory.openConnection(session)) {
            Map<String, Integer> arrayColumnDimensions = ImmutableMap.of();
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
                int allColumns = 0;
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    allColumns++;
                    String columnName = resultSet.getString("COLUMN_NAME");
                    JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                            getInteger(resultSet, "DATA_TYPE").orElseThrow(() -> new IllegalStateException("DATA_TYPE is null")),
                            Optional.of(resultSet.getString("TYPE_NAME")),
                            getInteger(resultSet, "COLUMN_SIZE"),
                            getInteger(resultSet, "DECIMAL_DIGITS"),
                            Optional.ofNullable(arrayColumnDimensions.get(columnName)),
                            Optional.empty());
                    Optional<ColumnMapping> columnMapping = toColumnMapping(session, connection, typeHandle);
                    log.debug("Mapping data type of '%s' column '%s': %s mapped to %s", schemaTableName, columnName, typeHandle, columnMapping);
                    // skip unsupported column types
                    if (columnMapping.isPresent()) {
                        boolean nullable = (resultSet.getInt("NULLABLE") != columnNoNulls);
                        Optional<String> comment = Optional.ofNullable(resultSet.getString("REMARKS"));
                        columns.add(JdbcColumnHandle.builder()
                                .setColumnName(columnName)
                                .setJdbcTypeHandle(typeHandle)
                                .setColumnType(columnMapping.get().getType())
                                .setNullable(nullable)
                                .setComment(comment)
                                .build());
                    }
                    if (columnMapping.isEmpty()) {
                        UnsupportedTypeHandling unsupportedTypeHandling = getUnsupportedTypeHandling(session);
                        verify(
                                unsupportedTypeHandling == IGNORE,
                                "Unsupported type handling is set to %s, but toColumnMapping() returned empty for %s",
                                unsupportedTypeHandling,
                                typeHandle);
                    }
                }
                if (columns.isEmpty()) {
                    // A table may have no supported columns. In rare cases a table might have no columns at all.
                    throw new TableNotFoundException(
                            schemaTableName,
                            format("Table '%s' has no supported columns (all %s columns are not supported)", schemaTableName, allColumns));
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet schemas = metaData.getSchemas(snowflakeConfig.getCatalog(), null);
            Set<String> outputSchemas = new HashSet<>();
            while (schemas.next()) {
                String schema = schemas.getString("TABLE_SCHEM");
                outputSchemas.add(schema);
            }
            return outputSchemas;
        }
        catch (SQLException e) {
            throw new TrinoException(SnowflakeErrorCode.SNOWFLAKE_COLUMN_MAPPING_ERROR,
                    e);
        }
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        // TODO support complex ConnectorExpressions
        //return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
        return super.implementAggregation(session, aggregate, assignments);
    }

    @Override
    public boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        // Postgres sorts textual types differently compared to Trino so we cannot safely pushdown any aggregations which take a text type as an input or as part of grouping set
        return preventTextualTypeAggregationPushdown(groupingSets);
    }

    @Override
    public Optional<String> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        // return connectorExpressionRewriter.rewrite(session, expression, assignments);
        return super.convertPredicate(session, expression, assignments);
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        for (JdbcSortItem sortItem : sortOrder) {
            Type sortItemType = sortItem.getColumn().getColumnType();
            if (sortItemType instanceof CharType || sortItemType instanceof VarcharType) {
                if (!isCollatable(sortItem.getColumn())) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.of((query, sortItems, limit) -> {
            String orderBy = sortItems.stream()
                    .map(sortItem -> {
                        String ordering = sortItem.getSortOrder().isAscending() ? "ASC" : "DESC";
                        String nullsHandling = sortItem.getSortOrder().isNullsFirst() ? "NULLS FIRST" : "NULLS LAST";
                        String collation = "";
                        if (isCollatable(sortItem.getColumn())) {
                            collation = "COLLATE \"C\"";
                        }
                        return format("%s %s %s %s", quoted(sortItem.getColumn().getColumnName()), collation, ordering, nullsHandling);
                    })
                    .collect(joining(", "));
            return format("%s ORDER BY %s LIMIT %d", query, orderBy, limit);
        });
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return true;
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
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        checkArgument(handle.isNamedRelation(), "Unable to delete from synthetic table: %s", handle);
        checkArgument(handle.getLimit().isEmpty(), "Unable to delete when limit is set: %s", handle);
        checkArgument(handle.getSortOrder().isEmpty(), "Unable to delete when sort order is set: %s", handle);
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            PreparedQuery preparedQuery = queryBuilder.prepareDeleteQuery(
                    this,
                    session,
                    connection,
                    handle.getRequiredNamedRelation(),
                    handle.getConstraint(),
                    getAdditionalPredicate(handle.getConstraintExpressions(), Optional.empty()));
            try (PreparedStatement preparedStatement = queryBuilder.prepareStatement(this, session, connection, preparedQuery)) {
                int affectedRowsCount = preparedStatement.executeUpdate();
                // In getPreparedStatement we set autocommit to false so here we need an explicit commit
                connection.commit();
                return OptionalLong.of(affectedRowsCount);
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle)
    {
        return super.getTableStatistics(session, handle);
    }

    private TableStatistics readTableStatistics(ConnectorSession session, JdbcTableHandle table)
            throws SQLException
    {
        checkArgument(table.isNamedRelation(), "Relation is not a table: %s", table);

        try (Connection connection = connectionFactory.openConnection(session);
                Handle handle = Jdbi.open(connection)) {
            StatisticsDao statisticsDao = new StatisticsDao(handle);

            Optional<Long> optionalRowCount = readRowCountTableStat(statisticsDao, table);
            if (optionalRowCount.isEmpty()) {
                // Table not found
                return TableStatistics.empty();
            }
            long rowCount = optionalRowCount.get();

            TableStatistics.Builder tableStatistics = TableStatistics.builder();
            tableStatistics.setRowCount(Estimate.of(rowCount));

            if (rowCount == 0) {
                return tableStatistics.build();
            }

            RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
            Map<String, ColumnStatisticsResult> columnStatistics = statisticsDao.getColumnStatistics(remoteTableName.getSchemaName().orElse(null), remoteTableName.getTableName()).stream()
                    .collect(toImmutableMap(ColumnStatisticsResult::getColumnName, identity()));

            for (JdbcColumnHandle column : this.getColumns(session, table)) {
                ColumnStatisticsResult result = columnStatistics.get(column.getColumnName());
                if (result == null) {
                    continue;
                }

                ColumnStatistics statistics = ColumnStatistics.builder()
                        .setNullsFraction(result.getNullsFraction()
                                .map(Estimate::of)
                                .orElseGet(Estimate::unknown))
                        .setDistinctValuesCount(result.getDistinctValuesIndicator()
                                .map(distinctValuesIndicator -> {
                                    if (distinctValuesIndicator >= 0.0) {
                                        return distinctValuesIndicator;
                                    }
                                    return -distinctValuesIndicator * rowCount;
                                })
                                .map(Estimate::of)
                                .orElseGet(Estimate::unknown))
                        .setDataSize(result.getAverageColumnLength()
                                .flatMap(averageColumnLength ->
                                        result.getNullsFraction().map(nullsFraction ->
                                                Estimate.of(1.0 * averageColumnLength * rowCount * (1 - nullsFraction))))
                                .orElseGet(Estimate::unknown))
                        .build();

                tableStatistics.setColumnStatistics(column, statistics);
            }

            return tableStatistics.build();
        }
    }

    @Override
    public Optional<PreparedQuery> implementJoin(
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
            // FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
            return Optional.empty();
        }
        return implementJoinCostAware(
                session,
                joinType,
                leftSource,
                rightSource,
                statistics,
                () -> super.implementJoin(session, joinType, leftSource, rightSource, joinConditions, rightAssignments, leftAssignments, statistics));
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        return false;
    }

    @Override
    protected void verifySchemaName(DatabaseMetaData databaseMetadata, String schemaName)
            throws SQLException
    {
        // PostgreSQL truncates schema name to 63 chars silently
        if (schemaName.length() > databaseMetadata.getMaxSchemaNameLength()) {
            throw new TrinoException(NOT_SUPPORTED, format("Schema name must be shorter than or equal to '%s' characters but got '%s'", databaseMetadata.getMaxSchemaNameLength(), schemaName.length()));
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

    @Override
    public void setColumnComment(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Optional<String> comment)
    {
        // PostgreSQL doesn't support prepared statement for COMMENT statement
        String sql = format(
                "COMMENT ON COLUMN %s.%s IS %s",
                quoted(handle.asPlainTable().getRemoteTableName()),
                quoted(column.getColumnName()),
                comment.map(BaseJdbcClient::varcharLiteral).orElse("NULL"));
        execute(session, sql);
    }

    public String getStageLocation()
    {
        return snowflakeConfig.getStageLocation();
    }

    public enum SnowflakeErrorCode
            implements ErrorCodeSupplier
    {
        SNOWFLAKE_COLUMN_MAPPING_ERROR(1, INTERNAL_ERROR);
        /**/;
        /**/;

        private final ErrorCode errorCode;

        SnowflakeErrorCode(int code, ErrorType type)
        {
            errorCode = new ErrorCode(code + 0x0506_0000, name(), type);
        }

        @Override
        public ErrorCode toErrorCode()
        {
            return errorCode;
        }
    }

    private static class StatisticsDao
    {
        private final Handle handle;

        public StatisticsDao(Handle handle)
        {
            this.handle = requireNonNull(handle, "handle is null");
        }

        Optional<Long> getRowCountFromPgClass(String schema, String tableName)
        {
            return handle.createQuery("" +
                            "SELECT reltuples " +
                            "FROM pg_class " +
                            "WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = :schema) " +
                            "AND relname = :table_name")
                    .bind("schema", schema)
                    .bind("table_name", tableName)
                    .mapTo(Long.class)
                    .findOne();
        }

        Optional<Long> getRowCountFromPgStat(String schema, String tableName)
        {
            return handle.createQuery("SELECT n_live_tup FROM pg_stat_all_tables WHERE schemaname = :schema AND relname = :table_name")
                    .bind("schema", schema)
                    .bind("table_name", tableName)
                    .mapTo(Long.class)
                    .findOne();
        }

        Optional<Long> getRowCountPartitionedTableFromPgClass(String schema, String tableName)
        {
            return handle.createQuery("" +
                            "SELECT SUM(child.reltuples) " +
                            "FROM pg_inherits " +
                            "JOIN pg_class parent ON pg_inherits.inhparent = parent.oid " +
                            "JOIN pg_class child ON pg_inherits.inhrelid = child.oid " +
                            "JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace " +
                            "JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace " +
                            "WHERE parent.oid = :schema_table_name::regclass")
                    .bind("schema_table_name", format("%s.%s", schema, tableName))
                    .mapTo(Long.class)
                    .findOne();
        }

        Optional<Long> getRowCountPartitionedTableFromPgStats(String schema, String tableName)
        {
            return handle.createQuery("" +
                            "SELECT SUM(stat.n_live_tup) " +
                            "FROM pg_inherits " +
                            "JOIN pg_class parent ON pg_inherits.inhparent = parent.oid " +
                            "JOIN pg_class child ON pg_inherits.inhrelid = child.oid " +
                            "JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace " +
                            "JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace " +
                            "JOIN pg_stat_all_tables stat ON stat.schemaname = child_ns.nspname AND stat.relname = child.relname " +
                            "WHERE parent.oid = :schema_table_name::regclass")
                    .bind("schema_table_name", format("%s.%s", schema, tableName))
                    .mapTo(Long.class)
                    .findOne();
        }

        List<ColumnStatisticsResult> getColumnStatistics(String schema, String tableName)
        {
            return handle.createQuery("SELECT attname, null_frac, n_distinct, avg_width FROM pg_stats WHERE schemaname = :schema AND tablename = :table_name")
                    .bind("schema", schema)
                    .bind("table_name", tableName)
                    .map((rs, ctx) -> new ColumnStatisticsResult(
                            requireNonNull(rs.getString("attname"), "attname is null"),
                            Optional.ofNullable(rs.getObject("null_frac", Float.class)),
                            Optional.ofNullable(rs.getObject("n_distinct", Float.class)),
                            Optional.ofNullable(rs.getObject("avg_width", Integer.class))))
                    .list();
        }

        boolean isPartitionedTable(String schema, String tableName)
        {
            return handle.createQuery("" +
                            "SELECT true " +
                            "FROM pg_class " +
                            "WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = :schema) " +
                            "AND relname = :table_name " +
                            "AND relkind = 'p'")
                    .bind("schema", schema)
                    .bind("table_name", tableName)
                    .mapTo(Boolean.class)
                    .findOne()
                    .orElse(false);
        }
    }

    private static class ColumnStatisticsResult
    {
        private final String columnName;
        private final Optional<Float> nullsFraction;
        private final Optional<Float> distinctValuesIndicator;
        private final Optional<Integer> averageColumnLength;

        public ColumnStatisticsResult(String columnName, Optional<Float> nullsFraction, Optional<Float> distinctValuesIndicator, Optional<Integer> averageColumnLength)
        {
            this.columnName = columnName;
            this.nullsFraction = nullsFraction;
            this.distinctValuesIndicator = distinctValuesIndicator;
            this.averageColumnLength = averageColumnLength;
        }

        public String getColumnName()
        {
            return columnName;
        }

        public Optional<Float> getNullsFraction()
        {
            return nullsFraction;
        }

        public Optional<Float> getDistinctValuesIndicator()
        {
            return distinctValuesIndicator;
        }

        public Optional<Integer> getAverageColumnLength()
        {
            return averageColumnLength;
        }
    }
}
