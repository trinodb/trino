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
package io.prestosql.plugin.postgresql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.BooleanReadFunction;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DoubleReadFunction;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcExpression;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.LongReadFunction;
import io.prestosql.plugin.jdbc.LongWriteFunction;
import io.prestosql.plugin.jdbc.ObjectReadFunction;
import io.prestosql.plugin.jdbc.ObjectWriteFunction;
import io.prestosql.plugin.jdbc.ReadFunction;
import io.prestosql.plugin.jdbc.SliceReadFunction;
import io.prestosql.plugin.jdbc.SliceWriteFunction;
import io.prestosql.plugin.jdbc.UnsupportedTypeHandling;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.plugin.jdbc.expression.AggregateFunctionRewriter;
import io.prestosql.plugin.jdbc.expression.AggregateFunctionRule;
import io.prestosql.plugin.jdbc.expression.ImplementAvgDecimal;
import io.prestosql.plugin.jdbc.expression.ImplementAvgFloatingPoint;
import io.prestosql.plugin.jdbc.expression.ImplementCount;
import io.prestosql.plugin.jdbc.expression.ImplementCountAll;
import io.prestosql.plugin.jdbc.expression.ImplementMinMax;
import io.prestosql.plugin.jdbc.expression.ImplementSum;
import io.prestosql.plugin.postgresql.PostgreSqlConfig.ArrayMapping;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.SingleMapBlock;
import io.prestosql.spi.connector.AggregateFunction;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.LongTimestampWithTimeZone;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import org.postgresql.core.TypeInfo;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.util.PGobject;

import javax.inject.Inject;

import java.io.IOException;
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
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedLongArray;
import static io.prestosql.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.prestosql.plugin.base.util.JsonTypeUtil.toJsonValue;
import static io.prestosql.plugin.jdbc.ColumnMapping.DISABLE_PUSHDOWN;
import static io.prestosql.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.prestosql.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.prestosql.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRounding;
import static io.prestosql.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.fromPrestoTimestamp;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.timeColumnMappingWithTruncation;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.timeWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.timestampReadFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.prestosql.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.prestosql.plugin.jdbc.UnsupportedTypeHandling.IGNORE;
import static io.prestosql.plugin.postgresql.PostgreSqlConfig.ArrayMapping.AS_ARRAY;
import static io.prestosql.plugin.postgresql.PostgreSqlConfig.ArrayMapping.AS_JSON;
import static io.prestosql.plugin.postgresql.PostgreSqlConfig.ArrayMapping.DISABLED;
import static io.prestosql.plugin.postgresql.PostgreSqlSessionProperties.getArrayMapping;
import static io.prestosql.plugin.postgresql.TypeUtils.arrayDepth;
import static io.prestosql.plugin.postgresql.TypeUtils.getArrayElementPgTypeName;
import static io.prestosql.plugin.postgresql.TypeUtils.getJdbcObjectArray;
import static io.prestosql.plugin.postgresql.TypeUtils.toPgTimestamp;
import static io.prestosql.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.StandardTypes.JSON;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.spi.type.TimestampType.createTimestampType;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.prestosql.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.prestosql.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.prestosql.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.prestosql.spi.type.TypeSignature.mapType;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.sql.DatabaseMetaData.columnNoNulls;
import static java.util.Collections.addAll;

public class PostgreSqlClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(PostgreSqlClient.class);

    /**
     * @see Array#getResultSet()
     */
    private static final int ARRAY_RESULT_SET_VALUE_COLUMN = 2;
    private static final String DUPLICATE_TABLE_SQLSTATE = "42P07";
    private static final int MAX_SUPPORTED_TIMESTAMP_PRECISION = 6;

    private final Type jsonType;
    private final Type uuidType;
    private final MapType varcharMapType;
    private final String[] tableTypes;
    private final AggregateFunctionRewriter aggregateFunctionRewriter;

    @Inject
    public PostgreSqlClient(
            BaseJdbcConfig config,
            PostgreSqlConfig postgreSqlConfig,
            ConnectionFactory connectionFactory,
            TypeManager typeManager)
    {
        super(config, "\"", connectionFactory);
        this.jsonType = typeManager.getType(new TypeSignature(JSON));
        this.uuidType = typeManager.getType(new TypeSignature(StandardTypes.UUID));
        this.varcharMapType = (MapType) typeManager.getType(mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()));

        List<String> tableTypes = new ArrayList<>();
        addAll(tableTypes, "TABLE", "VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE");
        if (postgreSqlConfig.isIncludeSystemTables()) {
            addAll(tableTypes, "SYSTEM TABLE", "SYSTEM VIEW");
        }
        this.tableTypes = tableTypes.toArray(new String[0]);

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), 0, Optional.empty(), Optional.empty(), Optional.empty());
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter(
                this::quoted,
                ImmutableSet.<AggregateFunctionRule>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementMinMax())
                        .add(new ImplementSum(PostgreSqlClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementAvgBigint())
                        .build());
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            createTable(session, tableMetadata, tableMetadata.getTable().getTableName());
        }
        catch (SQLException e) {
            boolean exists = DUPLICATE_TABLE_SQLSTATE.equals(e.getSQLState());
            throw new PrestoException(exists ? ALREADY_EXISTS : JDBC_ERROR, e);
        }
    }

    @Override
    protected void renameTable(JdbcIdentity identity, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        if (!schemaName.equals(newTable.getSchemaName())) {
            throw new PrestoException(NOT_SUPPORTED, "Table rename across schemas is not supported in PostgreSQL");
        }

        String sql = format(
                "ALTER TABLE %s RENAME TO %s",
                quoted(catalogName, schemaName, tableName),
                quoted(newTable.getTableName()));
        execute(identity, sql);
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        connection.setAutoCommit(false);
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(1000);
        return statement;
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, metadata.getSearchStringEscape()).orElse(null),
                escapeNamePattern(tableName, metadata.getSearchStringEscape()).orElse(null),
                tableTypes.clone());
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session))) {
            Map<String, Integer> arrayColumnDimensions = ImmutableMap.of();
            if (getArrayMapping(session) == AS_ARRAY) {
                arrayColumnDimensions = getArrayColumnDimensions(connection, tableHandle);
            }
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    String columnName = resultSet.getString("COLUMN_NAME");
                    JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                            resultSet.getInt("DATA_TYPE"),
                            Optional.of(resultSet.getString("TYPE_NAME")),
                            resultSet.getInt("COLUMN_SIZE"),
                            getInteger(resultSet, "DECIMAL_DIGITS"),
                            Optional.ofNullable(arrayColumnDimensions.get(columnName)),
                            Optional.empty());
                    Optional<ColumnMapping> columnMapping = toPrestoType(session, connection, typeHandle);
                    log.debug("Mapping data type of '%s' column '%s': %s mapped to %s", tableHandle.getSchemaTableName(), columnName, typeHandle, columnMapping);
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
                                "Unsupported type handling is set to %s, but toPrestoType() returned empty for %s",
                                unsupportedTypeHandling,
                                typeHandle);
                    }
                }
                if (columns.isEmpty()) {
                    // In rare cases a table might have no columns.
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
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
            statement.setString(1, tableHandle.getSchemaName());
            statement.setString(2, tableHandle.getTableName());

            Map<String, Integer> arrayColumnDimensions = new HashMap<>();
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    arrayColumnDimensions.put(resultSet.getString("attname"), resultSet.getInt("attndims"));
                }
            }
            return arrayColumnDimensions;
        }
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new PrestoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        switch (jdbcTypeName) {
            case "money":
                return Optional.of(moneyColumnMapping());
            case "uuid":
                return Optional.of(uuidColumnMapping());
            case "jsonb":
            case "json":
                return Optional.of(jsonColumnMapping());
            case "timestamptz":
                // PostgreSQL's "timestamp with time zone" is reported as Types.TIMESTAMP rather than Types.TIMESTAMP_WITH_TIMEZONE
                int decimalDigits = typeHandle.getDecimalDigits().orElseThrow(() -> new IllegalStateException("decimal digits not present"));
                return Optional.of(timestampWithTimeZoneColumnMapping(decimalDigits));
            case "hstore":
                return Optional.of(hstoreColumnMapping(session));
        }
        if (typeHandle.getJdbcType() == Types.VARCHAR && !jdbcTypeName.equals("varchar")) {
            // This can be e.g. an ENUM
            return Optional.of(typedVarcharColumnMapping(jdbcTypeName));
        }
        if (typeHandle.getJdbcType() == Types.TIME) {
            // When inserting a time such as 12:34:56.999, Postgres returns 12:34:56.999999999. If we use rounding semantics, the time turns into 00:00:00.000 when
            // reading it back into a time(3). Hence, truncate instead
            return Optional.of(timeColumnMappingWithTruncation());
        }
        if (typeHandle.getJdbcType() == Types.TIMESTAMP) {
            int decimalDigits = typeHandle.getDecimalDigits().orElseThrow(() -> new IllegalStateException("decimal digits not present"));
            TimestampType timestampType = createTimestampType(decimalDigits);
            return Optional.of(ColumnMapping.longMapping(
                    timestampType,
                    timestampReadFunction(timestampType),
                    timestampWriteFunction(timestampType)));
        }
        if (typeHandle.getJdbcType() == Types.NUMERIC && getDecimalRounding(session) == ALLOW_OVERFLOW) {
            if (typeHandle.getColumnSize() == 131089) {
                // decimal type with unspecified scale - up to 131072 digits before the decimal point; up to 16383 digits after the decimal point)
                // 131089 = SELECT LENGTH(pow(10::numeric,131071)::varchar); 131071 = 2^17-1  (org.postgresql.jdbc.TypeInfoCache#getDisplaySize)
                return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, getDecimalDefaultScale(session)), getDecimalRoundingMode(session)));
            }
            int precision = typeHandle.getColumnSize();
            int decimalDigits = typeHandle.getDecimalDigits().orElseThrow(() -> new IllegalStateException("decimal digits not present"));
            if (precision > Decimals.MAX_PRECISION) {
                int scale = min(decimalDigits, getDecimalDefaultScale(session));
                return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, scale), getDecimalRoundingMode(session)));
            }
        }
        if (typeHandle.getJdbcType() == Types.ARRAY) {
            Optional<ColumnMapping> columnMapping = arrayToPrestoType(session, connection, typeHandle);
            if (columnMapping.isPresent()) {
                return columnMapping;
            }
        }
        // TODO support PostgreSQL's TIME WITH TIME ZONE explicitly, otherwise predicate pushdown for these types may be incorrect
        return super.toPrestoType(session, connection, typeHandle);
    }

    private Optional<ColumnMapping> arrayToPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        checkArgument(typeHandle.getJdbcType() == Types.ARRAY, "Not array type");

        ArrayMapping arrayMapping = getArrayMapping(session);
        if (arrayMapping == DISABLED) {
            return Optional.empty();
        }
        // resolve and map base array element type
        JdbcTypeHandle baseElementTypeHandle = getArrayElementTypeHandle(connection, typeHandle);
        String baseElementTypeName = baseElementTypeHandle.getJdbcTypeName()
                .orElseThrow(() -> new PrestoException(JDBC_ERROR, "Element type name is missing: " + baseElementTypeHandle));
        if (baseElementTypeHandle.getJdbcType() == Types.BINARY) {
            // PostgreSQL jdbc driver doesn't currently support array of varbinary (bytea[])
            // https://github.com/pgjdbc/pgjdbc/pull/1184
            return Optional.empty();
        }
        Optional<ColumnMapping> baseElementMapping = toPrestoType(session, connection, baseElementTypeHandle);

        if (arrayMapping == AS_ARRAY) {
            if (typeHandle.getArrayDimensions().isEmpty()) {
                return Optional.empty();
            }
            return baseElementMapping
                    .map(elementMapping -> {
                        ArrayType prestoArrayType = new ArrayType(elementMapping.getType());
                        ColumnMapping arrayColumnMapping = arrayColumnMapping(session, prestoArrayType, elementMapping, baseElementTypeName);

                        int arrayDimensions = typeHandle.getArrayDimensions().get();
                        for (int i = 1; i < arrayDimensions; i++) {
                            prestoArrayType = new ArrayType(prestoArrayType);
                            arrayColumnMapping = arrayColumnMapping(session, prestoArrayType, arrayColumnMapping, baseElementTypeName);
                        }
                        return arrayColumnMapping;
                    });
        }
        if (arrayMapping == AS_JSON) {
            return baseElementMapping
                    .map(elementMapping -> arrayAsJsonColumnMapping(session, elementMapping));
        }
        throw new IllegalStateException("Unsupported array mapping type: " + arrayMapping);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("bytea", varbinaryWriteFunction());
        }
        if (TIME.equals(type)) {
            return WriteMapping.longMapping("time", timeWriteFunction());
        }
        if (type instanceof TimestampType && ((TimestampType) type).getPrecision() <= MAX_SUPPORTED_TIMESTAMP_PRECISION) {
            TimestampType timestampType = (TimestampType) type;
            return WriteMapping.longMapping(format("timestamp(%s)", timestampType.getPrecision()), timestampWriteFunction(timestampType));
        }
        if (type instanceof TimestampWithTimeZoneType && ((TimestampWithTimeZoneType) type).getPrecision() <= MAX_SUPPORTED_TIMESTAMP_PRECISION) {
            int precision = ((TimestampWithTimeZoneType) type).getPrecision();
            String postgresType = format("timestamptz(%d)", precision);
            if (precision <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
                return WriteMapping.longMapping(postgresType, shortTimestampWithTimeZoneWriteFunction());
            }
            else {
                return WriteMapping.objectMapping(postgresType, longTimestampWithTimeZoneWriteFunction());
            }
        }
        if (TinyintType.TINYINT.equals(type)) {
            return WriteMapping.longMapping("smallint", tinyintWriteFunction());
        }
        if (type.equals(jsonType)) {
            return WriteMapping.sliceMapping("jsonb", typedVarcharWriteFunction("json"));
        }
        if (type.equals(uuidType)) {
            return WriteMapping.sliceMapping("uuid", uuidWriteFunction());
        }
        if (type instanceof ArrayType && getArrayMapping(session) == AS_ARRAY) {
            Type elementType = ((ArrayType) type).getElementType();
            String elementDataType = toWriteMapping(session, elementType).getDataType();
            return WriteMapping.objectMapping(elementDataType + "[]", arrayWriteFunction(session, elementType, getArrayElementPgTypeName(session, this, elementType)));
        }
        return super.toWriteMapping(session, type);
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        // TODO support complex ConnectorExpressions
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.NUMERIC, Optional.of("decimal"), decimalType.getPrecision(), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
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

    // When writing with setObject() using LocalDateTime, driver converts the value to string representing date-time in JVM zone,
    // therefore cannot represent local date-time which is a "gap" in this zone.
    // TODO replace this method with StandardColumnMappings#timestampWriteFunction when https://github.com/pgjdbc/pgjdbc/issues/1390 is done
    private static LongWriteFunction timestampWriteFunction(TimestampType timestampType)
    {
        return (statement, index, value) -> {
            LocalDateTime localDateTime = fromPrestoTimestamp(value);
            statement.setObject(index, toPgTimestamp(localDateTime));
        };
    }

    private static ColumnMapping timestampWithTimeZoneColumnMapping(int precision)
    {
        // PosgreSQL supports timestamptz precision up to microseconds
        checkArgument(precision <= MAX_SUPPORTED_TIMESTAMP_PRECISION, "unsupported precision value %d", precision);
        TimestampWithTimeZoneType prestoType = createTimestampWithTimeZoneType(precision);
        if (precision <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
            return ColumnMapping.longMapping(
                    prestoType,
                    shortTimestampWithTimeZoneReadFunction(),
                    shortTimestampWithTimeZoneWriteFunction());
        }
        else {
            return ColumnMapping.objectMapping(
                    prestoType,
                    longTimestampWithTimeZoneReadFunction(),
                    longTimestampWithTimeZoneWriteFunction());
        }
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
                    long epochSeconds = value.getEpochMillis() / MILLISECONDS_PER_SECOND;
                    long nanosOfSecond = value.getEpochMillis() % MILLISECONDS_PER_SECOND * NANOSECONDS_PER_MILLISECOND + value.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND;
                    statement.setObject(index, OffsetDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds, nanosOfSecond), UTC_KEY.getZoneId()));
                });
    }

    private ColumnMapping hstoreColumnMapping(ConnectorSession session)
    {
        return ColumnMapping.objectMapping(
                varcharMapType,
                varcharMapReadFunction(),
                hstoreWriteFunction(session),
                DISABLE_PUSHDOWN);
    }

    private ObjectReadFunction varcharMapReadFunction()
    {
        return ObjectReadFunction.of(Block.class, (resultSet, columnIndex) -> {
            @SuppressWarnings("unchecked")
            Map<String, String> map = (Map<String, String>) resultSet.getObject(columnIndex);
            BlockBuilder keyBlockBuilder = varcharMapType.getKeyType().createBlockBuilder(null, map.size());
            BlockBuilder valueBlockBuilder = varcharMapType.getValueType().createBlockBuilder(null, map.size());
            for (Map.Entry<String, String> entry : map.entrySet()) {
                if (entry.getKey() == null) {
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "hstore key is null");
                }
                varcharMapType.getKeyType().writeSlice(keyBlockBuilder, utf8Slice(entry.getKey()));
                if (entry.getValue() == null) {
                    valueBlockBuilder.appendNull();
                }
                else {
                    varcharMapType.getValueType().writeSlice(valueBlockBuilder, utf8Slice(entry.getValue()));
                }
            }
            return varcharMapType.createBlockFromKeyValue(Optional.empty(), new int[] {0, map.size()}, keyBlockBuilder.build(), valueBlockBuilder.build())
                    .getObject(0, Block.class);
        });
    }

    private ObjectWriteFunction hstoreWriteFunction(ConnectorSession session)
    {
        return ObjectWriteFunction.of(Block.class, (statement, index, block) -> {
            checkArgument(block instanceof SingleMapBlock, "wrong block type: %s. expected SingleMapBlock", block.getClass().getSimpleName());
            Map<Object, Object> map = new HashMap<>();
            for (int i = 0; i < block.getPositionCount(); i += 2) {
                map.put(varcharMapType.getKeyType().getObjectValue(session, block, i), varcharMapType.getValueType().getObjectValue(session, block, i + 1));
            }
            statement.setObject(index, Collections.unmodifiableMap(map));
        });
    }

    private static ColumnMapping arrayColumnMapping(ConnectorSession session, ArrayType arrayType, ColumnMapping arrayElementMapping, String baseElementJdbcTypeName)
    {
        return ColumnMapping.objectMapping(
                arrayType,
                arrayReadFunction(arrayType.getElementType(), arrayElementMapping.getReadFunction()),
                arrayWriteFunction(session, arrayType.getElementType(), baseElementJdbcTypeName));
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

    private static ObjectWriteFunction arrayWriteFunction(ConnectorSession session, Type elementType, String baseElementJdbcTypeName)
    {
        return ObjectWriteFunction.of(Block.class, (statement, index, block) -> {
            Array jdbcArray = statement.getConnection().createArrayOf(baseElementJdbcTypeName, getJdbcObjectArray(session, elementType, block));
            statement.setArray(index, jdbcArray);
        });
    }

    private ColumnMapping arrayAsJsonColumnMapping(ConnectorSession session, ColumnMapping baseElementMapping)
    {
        return ColumnMapping.sliceMapping(
                jsonType,
                arrayAsJsonReadFunction(session, baseElementMapping),
                (statement, index, block) -> { throw new UnsupportedOperationException(); },
                DISABLE_PUSHDOWN);
    }

    private static SliceReadFunction arrayAsJsonReadFunction(ConnectorSession session, ColumnMapping baseElementMapping)
    {
        return (resultSet, columnIndex) -> {
            // resolve array type
            Object jdbcArray = resultSet.getArray(columnIndex).getArray();
            int arrayDimensions = arrayDepth(jdbcArray);

            ReadFunction readFunction = baseElementMapping.getReadFunction();
            Type type = baseElementMapping.getType();
            for (int i = 0; i < arrayDimensions; i++) {
                readFunction = arrayReadFunction(type, readFunction);
                type = new ArrayType(type);
            }

            // read array into a block
            Block block = (Block) ((ObjectReadFunction) readFunction).readObject(resultSet, columnIndex);

            // convert block to JSON slice
            BlockBuilder builder = type.createBlockBuilder(null, 1);
            type.writeObject(builder, block);
            Object value = type.getObjectValue(session, builder.build(), 0);

            try {
                return toJsonValue(value);
            }
            catch (IOException e) {
                throw new PrestoException(JDBC_ERROR, "Conversion to JSON failed for  " + type.getDisplayName(), e);
            }
        };
    }

    private static JdbcTypeHandle getArrayElementTypeHandle(Connection connection, JdbcTypeHandle arrayTypeHandle)
    {
        String jdbcTypeName = arrayTypeHandle.getJdbcTypeName()
                .orElseThrow(() -> new PrestoException(JDBC_ERROR, "Type name is missing: " + arrayTypeHandle));
        try {
            TypeInfo typeInfo = connection.unwrap(PgConnection.class).getTypeInfo();
            int pgElementOid = typeInfo.getPGArrayElement(typeInfo.getPGType(jdbcTypeName));
            verify(arrayTypeHandle.getCaseSensitivity().isEmpty(), "Case sensitivity not supported");
            return new JdbcTypeHandle(
                    typeInfo.getSQLType(pgElementOid),
                    Optional.of(typeInfo.getPGType(pgElementOid)),
                    arrayTypeHandle.getColumnSize(),
                    arrayTypeHandle.getDecimalDigits(),
                    arrayTypeHandle.getArrayDimensions(),
                    Optional.empty());
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    private ColumnMapping jsonColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                jsonType,
                (resultSet, columnIndex) -> jsonParse(utf8Slice(resultSet.getString(columnIndex))),
                typedVarcharWriteFunction("json"),
                DISABLE_PUSHDOWN);
    }

    private static ColumnMapping typedVarcharColumnMapping(String jdbcTypeName)
    {
        return ColumnMapping.sliceMapping(
                VARCHAR,
                (resultSet, columnIndex) -> utf8Slice(resultSet.getString(columnIndex)),
                typedVarcharWriteFunction(jdbcTypeName));
    }

    private static SliceWriteFunction typedVarcharWriteFunction(String jdbcTypeName)
    {
        return (statement, index, value) -> {
            PGobject pgObject = new PGobject();
            pgObject.setType(jdbcTypeName);
            pgObject.setValue(value.toStringUtf8());
            statement.setObject(index, pgObject);
        };
    }

    private static ColumnMapping moneyColumnMapping()
    {
        /*
         * PostgreSQL JDBC maps "money" to Types.DOUBLE, but fails to retrieve double value for amounts
         * greater than or equal to 1000. Upon `ResultSet#getString`, the driver returns e.g. "$10.00" or "$10,000.00"
         * (currency symbol depends on the server side configuration).
         *
         * The following mapping maps PostgreSQL "money" to Presto "varchar".
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
                },
                (statement, index, value) -> { throw new PrestoException(NOT_SUPPORTED, "Money type is not supported for INSERT"); },
                DISABLE_PUSHDOWN);
    }

    private static SliceWriteFunction uuidWriteFunction()
    {
        return (statement, index, value) -> {
            long high = Long.reverseBytes(value.getLong(0));
            long low = Long.reverseBytes(value.getLong(SIZE_OF_LONG));
            UUID uuid = new UUID(high, low);
            statement.setObject(index, uuid, Types.OTHER);
        };
    }

    private static Slice uuidSlice(UUID uuid)
    {
        return wrappedLongArray(
                Long.reverseBytes(uuid.getMostSignificantBits()),
                Long.reverseBytes(uuid.getLeastSignificantBits()));
    }

    private ColumnMapping uuidColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                uuidType,
                (resultSet, columnIndex) -> uuidSlice((UUID) resultSet.getObject(columnIndex)),
                uuidWriteFunction());
    }
}
