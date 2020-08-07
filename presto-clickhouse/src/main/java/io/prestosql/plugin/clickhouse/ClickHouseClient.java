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
package io.prestosql.plugin.clickhouse;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcOutputTableHandle;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.UnsupportedTypeHandling;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.except.ClickHouseErrorCode;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.plugin.jdbc.ColumnMapping.DISABLE_PUSHDOWN;
import static io.prestosql.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.prestosql.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.timestampWriteFunctionUsingSqlTimestamp;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.prestosql.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.prestosql.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.prestosql.plugin.jdbc.UnsupportedTypeHandling.IGNORE;
import static io.prestosql.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.sql.ResultSetMetaData.columnNoNulls;
import static java.util.Collections.nCopies;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;

public class ClickHouseClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(ClickHouseClient.class);
    private final Type jsonType;
    public static final String ESCAPE_CHARACTER = "\"";

    @Inject
    public ClickHouseClient(BaseJdbcConfig config, ConnectionFactory connectionFactory, TypeManager typeManager)
    {
//        super(config, "`", connectionFactory);

        super(
                ESCAPE_CHARACTER,
                connectionFactory,
                ImmutableSet.of(),
                config.isCaseInsensitiveNameMatching(),
                config.getCaseInsensitiveNameMatchingCacheTtl());
        this.jsonType = typeManager.getType(new TypeSignature(StandardTypes.JSON));
    }

    @Override
    public void abortReadConnection(Connection connection)
            throws SQLException
    {
        connection.abort(directExecutor());
    }

    @Override
    public void finishInsertTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        String temporaryTable = quoted(null, handle.getSchemaName(), handle.getTemporaryTableName());
        String targetTable = quoted(null, handle.getSchemaName(), handle.getTableName());
        String columnNames = handle.getColumnNames().stream()
                .map(this::quoted)
                .collect(joining(", "));
        String insertSql = format("INSERT INTO %s (%s) SELECT %s FROM %s", targetTable, columnNames, columnNames, temporaryTable);
        String cleanupSql = "DROP TABLE " + temporaryTable;

        try (Connection connection = getConnection(identity, handle)) {
            execute(connection, insertSql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }

        try (Connection connection = getConnection(identity, handle)) {
            execute(connection, cleanupSql);
        }
        catch (SQLException e) {
            log.warn(e, "Failed to cleanup temporary table: %s", temporaryTable);
        }
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        SchemaTableName schemaTableName = tableHandle.getSchemaTableName();
        JdbcIdentity identity = JdbcIdentity.from(session);

        try (Connection connection = connectionFactory.openConnection(identity)) {
            boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
            String remoteSchema = toRemoteSchemaName(identity, connection, schemaTableName.getSchemaName());
            String remoteTable = toRemoteTableName(identity, connection, remoteSchema, schemaTableName.getTableName());
            String tableName = generateTemporaryTableName();
            if (uppercase) {
                tableName = tableName.toUpperCase(ENGLISH);
            }
            String catalog = connection.getCatalog();

            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<JdbcTypeHandle> jdbcColumnTypes = ImmutableList.builder();
            for (JdbcColumnHandle column : columns) {
                columnNames.add(column.getColumnName());
                columnTypes.add(column.getColumnType());
                jdbcColumnTypes.add(column.getJdbcTypeHandle());
            }

            copyTableSchema(connection, catalog, remoteSchema, remoteTable, tableName, columnNames.build());

            return new JdbcOutputTableHandle(
                    catalog,
                    remoteSchema,
                    remoteTable,
                    columnNames.build(),
                    columnTypes.build(),
                    Optional.of(jdbcColumnTypes.build()),
                    tableName);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        // clickhouse maps their "database" to SQL catalogs and does not have schemas
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getTables(
                schemaName.orElse(null),
                null,
                escapeNamePattern(tableName, metadata.getSearchStringEscape()).orElse(null),
                new String[] {"TABLE", "VIEW"});
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
        if (jdbcTypeName.equals("String")) {
            if (typeHandle.getColumnSize() == 0) {
                return Optional.of(varcharColumnMapping(createUnboundedVarcharType()));
            }
        }
        if (typeHandle.getJdbcType() == Types.DATE) {
            if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
                return mapToUnboundedVarchar(typeHandle);
            }
            return Optional.empty();
        }
        if (typeHandle.getJdbcType() == Types.TIMESTAMP) {
            if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
                return mapToUnboundedVarchar(typeHandle);
            }
            return Optional.empty();
        }
        if (typeHandle.getJdbcType() == Types.DECIMAL) {
            int precision = typeHandle.getColumnSize();
            if (precision > Decimals.MAX_PRECISION) {
                int scale = min(typeHandle.getDecimalDigits(), getDecimalDefaultScale(session));
                return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, scale), getDecimalRoundingMode(session)));
            }
        }
        return super.toPrestoType(session, connection, typeHandle);
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        List<JdbcColumnHandle> columns = new ArrayList<>();
        try {
            ClickHouseConnection connection = connectionFactory.openConnection(JdbcIdentity.from(session)).unwrap(ClickHouseConnection.class);
            Map<String, Integer> arrayColumnDimensions = ImmutableMap.of();
            ResultSet resultSet = connection.getMetaData().getColumns(null, tableHandle.getSchemaName(), tableHandle.getTableName(), null);
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
                    verify(unsupportedTypeHandling == IGNORE, "Unsupported type handling is set to %s, but toPrestoType() returned empty", unsupportedTypeHandling);
                }
            }
            if (columns.isEmpty()) {
                // In rare cases a table might have no columns.
                throw new TableNotFoundException(tableHandle.getSchemaTableName());
            }
            return ImmutableList.copyOf(columns);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (REAL.equals(type)) {
            return WriteMapping.longMapping("float", realWriteFunction());
        }
        if (TIMESTAMP.equals(type)) {
            // TODO use `timestampWriteFunction`
            return WriteMapping.longMapping("datetime", timestampWriteFunctionUsingSqlTimestamp(session));
        }
        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("String", varbinaryWriteFunction());
        }
        if (isVarcharType(type)) {
            // VarcharType varcharType = (VarcharType) type;
            // String dataType;
            // if (varcharType.isUnbounded()) {
            //     dataType = "longtext";
            // }
            // else if (varcharType.getBoundedLength() <= 255) {
            //     dataType = "tinytext";
            // }
            // else if (varcharType.getBoundedLength() <= 65535) {
            //     dataType = "text";
            // }
            // else if (varcharType.getBoundedLength() <= 16777215) {
            //     dataType = "mediumtext";
            // }
            // else {
            //     dataType = "longtext";
            // }
            return WriteMapping.sliceMapping("String", varcharWriteFunction());
        }
        return super.toWriteMapping(session, type);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        log.info("entering createTable.........");
        Map<String, Object> props = tableMetadata.getProperties();
        log.info("props: {}", props.toString());
        try {
            createTable(session, tableMetadata, tableMetadata.getTable().getTableName());
        }
        catch (SQLException e) {
            boolean exists = ClickHouseErrorCode.TABLE_ALREADY_EXISTS.toString().equals(e.getSQLState());
            throw new PrestoException(exists ? ALREADY_EXISTS : JDBC_ERROR, e);
        }
    }

    @Override
    public void renameColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers()) {
                newColumnName = newColumnName.toUpperCase(ENGLISH);
            }
            String sql = format(
                    "ALTER TABLE %s RENAME COLUMN %s TO %s ",
                    quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()),
                    quoted(jdbcColumn.getColumnName()),
                    quoted(newColumnName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            // clickhouse versions earlier than 8 do not support the above RENAME COLUMN syntax
            if (ClickHouseErrorCode.UNKNOWN_EXCEPTION.toString().equals(e.getSQLState())) {
                throw new PrestoException(NOT_SUPPORTED, format("Rename column not supported in catalog: '%s'", handle.getCatalogName()), e);
            }
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void copyTableSchema(Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        String sql = format(
                "CREATE TABLE %s  ENGINE = Log AS SELECT %s FROM %s WHERE 0 = 1 ",
                quoted(null, schemaName, newTableName),
                columnNames.stream()
                        .map(this::quoted)
                        .collect(joining(", ")),
                quoted(null, schemaName, tableName));
        execute(connection, sql);
    }

    @Override
    public void renameTable(JdbcIdentity identity, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        // clickhouse doesn't support specifying the catalog name in a rename. By setting the
        // catalogName parameter to null, it will be omitted in the ALTER TABLE statement.
        verify(handle.getSchemaName() == null);
        renameTable(identity, null, handle.getCatalogName(), handle.getTableName(), newTableName);
    }

    @Override
    public void dropTable(JdbcIdentity identity, JdbcTableHandle handle)
    {
        String sql = "DROP TABLE " + quoted(null, handle.getSchemaName(), handle.getTableName());

        try (Connection connection = connectionFactory.openConnection(identity)) {
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle)
    {
        return format(
                "INSERT INTO %s (%s) VALUES (%s)",
                quoted(null, handle.getSchemaName(), handle.getTemporaryTableName()),
                handle.getColumnNames().stream()
                        .map(this::quoted)
                        .collect(joining(", ")),
                join(",", nCopies(handle.getColumnNames().size(), "?")));
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
                varcharWriteFunction(),
                DISABLE_PUSHDOWN);
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

    public void execute(ConnectorSession session, String statement)
    {
        execute(JdbcIdentity.from(session), statement);
    }
}
