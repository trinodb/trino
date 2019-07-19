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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.BlockReadFunction;
import io.prestosql.plugin.jdbc.BlockWriteFunction;
import io.prestosql.plugin.jdbc.BooleanReadFunction;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DoubleReadFunction;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.LongReadFunction;
import io.prestosql.plugin.jdbc.LongWriteFunction;
import io.prestosql.plugin.jdbc.ReadFunction;
import io.prestosql.plugin.jdbc.SliceReadFunction;
import io.prestosql.plugin.jdbc.SliceWriteFunction;
import io.prestosql.plugin.jdbc.StatsCollecting;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.plugin.postgresql.PostgreSqlConfig.ArrayMapping;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import org.postgresql.core.TypeInfo;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.util.PGobject;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;

import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedLongArray;
import static io.prestosql.plugin.jdbc.ColumnMapping.DISABLE_PUSHDOWN;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.fromPrestoLegacyTimestamp;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.fromPrestoTimestamp;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.timestampReadFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
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
import static io.prestosql.spi.type.StandardTypes.JSON;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TypeSignature.mapType;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
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

    private final TypeManager typeManager;
    private final Type jsonType;
    private final Type uuidType;
    private final MapType varcharMapType;
    private final String[] tableTypes;

    @Inject
    public PostgreSqlClient(
            BaseJdbcConfig config,
            PostgreSqlConfig postgreSqlConfig,
            @StatsCollecting ConnectionFactory connectionFactory,
            TypeManager typeManager)
    {
        super(config, "\"", connectionFactory);
        this.typeManager = typeManager;
        this.jsonType = typeManager.getType(new TypeSignature(JSON));
        this.uuidType = typeManager.getType(new TypeSignature(StandardTypes.UUID));
        this.varcharMapType = (MapType) typeManager.getType(mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()));

        List<String> tableTypes = new ArrayList<>();
        addAll(tableTypes, "TABLE", "VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE");
        if (postgreSqlConfig.isIncludeSystemTables()) {
            addAll(tableTypes, "SYSTEM TABLE", "SYSTEM VIEW");
        }
        this.tableTypes = tableTypes.toArray(new String[0]);
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

        try (Connection connection = connectionFactory.openConnection(identity)) {
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
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
                            resultSet.getInt("DECIMAL_DIGITS"),
                            Optional.ofNullable(arrayColumnDimensions.get(columnName)));
                    Optional<ColumnMapping> columnMapping = toPrestoType(session, connection, typeHandle);
                    log.debug("Mapping data type of '%s' column '%s': %s mapped to %s", tableHandle.getSchemaTableName(), columnName, typeHandle, columnMapping);
                    // skip unsupported column types
                    if (columnMapping.isPresent()) {
                        boolean nullable = (resultSet.getInt("NULLABLE") != columnNoNulls);
                        columns.add(new JdbcColumnHandle(columnName, typeHandle, columnMapping.get().getType(), nullable));
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

    private Map<String, Integer> getArrayColumnDimensions(Connection connection, JdbcTableHandle tableHandle)
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
            case "uuid":
                return Optional.of(uuidColumnMapping());
            case "jsonb":
            case "json":
                return Optional.of(jsonColumnMapping());
            case "timestamptz":
                // PostgreSQL's "timestamp with time zone" is reported as Types.TIMESTAMP rather than Types.TIMESTAMP_WITH_TIMEZONE
                return Optional.of(timestampWithTimeZoneColumnMapping());
            case "hstore":
                return Optional.of(hstoreColumnMapping());
        }
        if (typeHandle.getJdbcType() == Types.VARCHAR && !jdbcTypeName.equals("varchar")) {
            // This can be e.g. an ENUM
            return Optional.of(typedVarcharColumnMapping(jdbcTypeName));
        }
        if (typeHandle.getJdbcType() == Types.TIMESTAMP) {
            return Optional.of(ColumnMapping.longMapping(
                    TIMESTAMP,
                    timestampReadFunction(session),
                    timestampWriteFunction(session)));
        }
        if (typeHandle.getJdbcType() == Types.ARRAY) {
            ArrayMapping arrayMapping = getArrayMapping(session);
            if (arrayMapping == DISABLED) {
                return Optional.empty();
            }
            // resolve and map base array element type
            JdbcTypeHandle baseElementTypeHandle = getArrayElementTypeHandle(connection, typeHandle);
            String baseElementTypeName = baseElementTypeHandle.getJdbcTypeName()
                    .orElseThrow(() -> new PrestoException(JDBC_ERROR, "Element type name is missing: " + baseElementTypeHandle));
            if (baseElementTypeHandle.getJdbcType() == Types.VARBINARY) {
                // PostgreSQL jdbc driver doesn't currently support array of varbinary (bytea[])
                // https://github.com/pgjdbc/pgjdbc/pull/1184
                return Optional.empty();
            }
            Optional<ColumnMapping> baseElementMapping = toPrestoType(session, connection, baseElementTypeHandle);

            if (arrayMapping == AS_ARRAY) {
                if (!typeHandle.getArrayDimensions().isPresent()) {
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
        // TODO support PostgreSQL's TIME WITH TIME ZONE explicitly, otherwise predicate pushdown for these types may be incorrect
        return super.toPrestoType(session, connection, typeHandle);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("bytea", varbinaryWriteFunction());
        }
        if (TIMESTAMP.equals(type)) {
            return WriteMapping.longMapping("timestamp", timestampWriteFunction(session));
        }
        if (TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            return WriteMapping.longMapping("timestamptz", timestampWithTimeZoneWriteFunction());
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
            return WriteMapping.blockMapping(elementDataType + "[]", arrayWriteFunction(session, elementType, getArrayElementPgTypeName(session, this, elementType)));
        }
        return super.toWriteMapping(session, type);
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed()
    {
        return true;
    }

    // When writing with setObject() using LocalDateTime, driver converts the value to string representing date-time in JVM zone,
    // therefore cannot represent local date-time which is a "gap" in this zone.
    // TODO replace this method with StandardColumnMappings#timestampWriteFunction when https://github.com/pgjdbc/pgjdbc/issues/1390 is done
    private static LongWriteFunction timestampWriteFunction(ConnectorSession session)
    {
        ZoneId sessionZone = ZoneId.of(session.getTimeZoneKey().getId());
        boolean legacyTimestamp = session.isLegacyTimestamp();
        return (statement, index, value) -> {
            LocalDateTime localDateTime = legacyTimestamp
                    ? fromPrestoLegacyTimestamp(value, sessionZone)
                    : fromPrestoTimestamp(value);
            statement.setObject(index, toPgTimestamp(localDateTime));
        };
    }

    private static ColumnMapping timestampWithTimeZoneColumnMapping()
    {
        return ColumnMapping.longMapping(
                TIMESTAMP_WITH_TIME_ZONE,
                (resultSet, columnIndex) -> {
                    // PostgreSQL does not store zone information in "timestamp with time zone" data type
                    long millisUtc = resultSet.getTimestamp(columnIndex).getTime();
                    return packDateTimeWithZone(millisUtc, UTC_KEY);
                },
                timestampWithTimeZoneWriteFunction());
    }

    private static LongWriteFunction timestampWithTimeZoneWriteFunction()
    {
        return (statement, index, value) -> {
            // PostgreSQL does not store zone information in "timestamp with time zone" data type
            long millisUtc = unpackMillisUtc(value);
            statement.setTimestamp(index, new java.sql.Timestamp(millisUtc));
        };
    }

    private ColumnMapping hstoreColumnMapping()
    {
        return ColumnMapping.blockMapping(
                varcharMapType,
                varcharMapReadFunction(),
                (statement, index, block) -> { throw new PrestoException(NOT_SUPPORTED, "PosgtreSQL hstore write is not supported"); },
                DISABLE_PUSHDOWN);
    }

    private BlockReadFunction varcharMapReadFunction()
    {
        return (resultSet, columnIndex) -> {
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
        };
    }

    private static ColumnMapping arrayColumnMapping(ConnectorSession session, ArrayType arrayType, ColumnMapping arrayElementMapping, String baseElementJdbcTypeName)
    {
        return ColumnMapping.blockMapping(
                arrayType,
                arrayReadFunction(arrayType.getElementType(), arrayElementMapping.getReadFunction()),
                arrayWriteFunction(session, arrayType.getElementType(), baseElementJdbcTypeName));
    }

    private static BlockReadFunction arrayReadFunction(Type elementType, ReadFunction elementReadFunction)
    {
        return (resultSet, columnIndex) -> {
            Array array = resultSet.getArray(columnIndex);
            BlockBuilder builder = elementType.createBlockBuilder(null, 10);
            try (ResultSet arrayAsResultSet = array.getResultSet()) {
                while (arrayAsResultSet.next()) {
                    arrayAsResultSet.getObject(ARRAY_RESULT_SET_VALUE_COLUMN);
                    if (arrayAsResultSet.wasNull()) {
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
                    else if (elementType.getJavaType() == Block.class) {
                        elementType.writeObject(builder, ((BlockReadFunction) elementReadFunction).readBlock(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                    else {
                        throw new IllegalStateException("Unsupported Java type: " + elementType.getJavaType());
                    }
                }
            }

            return builder.build();
        };
    }

    private static BlockWriteFunction arrayWriteFunction(ConnectorSession session, Type elementType, String baseElementJdbcTypeName)
    {
        return (statement, index, block) -> {
            Array jdbcArray = statement.getConnection().createArrayOf(baseElementJdbcTypeName, getJdbcObjectArray(session, elementType, block));
            statement.setArray(index, jdbcArray);
        };
    }

    private ColumnMapping arrayAsJsonColumnMapping(ConnectorSession session, ColumnMapping baseElementMapping)
    {
        return ColumnMapping.sliceMapping(
                jsonType,
                arrayAsJsonReadFunction(session, baseElementMapping),
                (statement, index, block) -> { throw new UnsupportedOperationException(); },
                DISABLE_PUSHDOWN);
    }

    private SliceReadFunction arrayAsJsonReadFunction(ConnectorSession session, ColumnMapping baseElementMapping)
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
            Block block = ((BlockReadFunction) readFunction).readBlock(resultSet, columnIndex);

            // cast block to JSON slice
            try {
                return (Slice) typeManager.getCoercion(type, jsonType)
                        .invokeExact(session, block);
            }
            catch (Throwable throwable) {
                throw new PrestoException(JDBC_ERROR, "Cast to JSON failed: " + throwable.getMessage(), throwable);
            }
        };
    }

    private JdbcTypeHandle getArrayElementTypeHandle(Connection connection, JdbcTypeHandle arrayTypeHandle)
    {
        String jdbcTypeName = arrayTypeHandle.getJdbcTypeName()
                .orElseThrow(() -> new PrestoException(JDBC_ERROR, "Type name is missing: " + arrayTypeHandle));
        try {
            TypeInfo typeInfo = connection.unwrap(PgConnection.class).getTypeInfo();
            int pgElementOid = typeInfo.getPGArrayElement(typeInfo.getPGType(jdbcTypeName));
            return new JdbcTypeHandle(
                    typeInfo.getSQLType(pgElementOid),
                    Optional.of(typeInfo.getPGType(pgElementOid)),
                    arrayTypeHandle.getColumnSize(),
                    arrayTypeHandle.getDecimalDigits(),
                    arrayTypeHandle.getArrayDimensions());
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

    private ColumnMapping typedVarcharColumnMapping(String jdbcTypeName)
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

    private static SliceWriteFunction uuidWriteFunction()
    {
        return (statement, index, value) -> {
            UUID uuid = new UUID(value.getLong(0), value.getLong(SIZE_OF_LONG));
            statement.setObject(index, uuid, Types.OTHER);
        };
    }

    private static Slice uuidSlice(UUID uuid)
    {
        return wrappedLongArray(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    private ColumnMapping uuidColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                uuidType,
                (resultSet, columnIndex) -> uuidSlice((UUID) resultSet.getObject(columnIndex)),
                uuidWriteFunction());
    }

    private static final JsonFactory JSON_FACTORY = new JsonFactory()
            .disable(CANONICALIZE_FIELD_NAMES);

    private static final ObjectMapper SORTED_MAPPER = new ObjectMapperProvider().get().configure(ORDER_MAP_ENTRIES_BY_KEYS, true);

    private static Slice jsonParse(Slice slice)
    {
        try (JsonParser parser = createJsonParser(slice)) {
            byte[] in = slice.getBytes();
            SliceOutput dynamicSliceOutput = new DynamicSliceOutput(in.length);
            SORTED_MAPPER.writeValue((OutputStream) dynamicSliceOutput, SORTED_MAPPER.readValue(parser, Object.class));
            // nextToken() returns null if the input is parsed correctly,
            // but will throw an exception if there are trailing characters.
            parser.nextToken();
            return dynamicSliceOutput.slice();
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Cannot convert '%s' to JSON", slice.toStringUtf8()));
        }
    }

    private static JsonParser createJsonParser(Slice json)
            throws IOException
    {
        // Jackson tries to detect the character encoding automatically when using InputStream
        // so we pass an InputStreamReader instead.
        return JSON_FACTORY.createParser(new InputStreamReader(json.getInput(), UTF_8));
    }
}
