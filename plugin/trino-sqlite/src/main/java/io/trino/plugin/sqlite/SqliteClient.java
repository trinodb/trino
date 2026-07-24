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
package io.trino.plugin.sqlite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.CaseSensitivity;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcMergeTableHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_INSENSITIVE;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_SENSITIVE;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.isNonTransactionalMerge;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateColumnMappingUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.sqlite.SqliteHelper.fromRemoteSchemaName;
import static io.trino.plugin.sqlite.SqliteHelper.getDefaultValue;
import static io.trino.plugin.sqlite.SqliteHelper.getSqliteDataType;
import static io.trino.plugin.sqlite.SqliteHelper.getSqliteTypeName;
import static io.trino.plugin.sqlite.SqliteTableProperties.PRIMARY_KEY_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.stream.Collectors.joining;

public class SqliteClient
        extends BaseJdbcClient
{
    protected static final String MAIN_SCHEMA = "main";
    protected static final String TEMP_SCHEMA = "temp";
    protected static final Set<String> SCHEMAS = ImmutableSet.of(MAIN_SCHEMA, TEMP_SCHEMA);
    private static final List<String> AUTOINCREMENT_TYPES = ImmutableList.of("integer");
    private static final String PK_AUTOINCREMENT = " PRIMARY KEY AUTOINCREMENT";
    private final boolean useTypeAffinity;
    private final List<String> tableTypes;
    private final Map<String, String> customDataTypes;

    @Inject
    public SqliteClient(
            BaseJdbcConfig config,
            SqliteConfig sqliteConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        // SQLite does not supports retries
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, false);

        ImmutableList.Builder<String> tableTypes = ImmutableList.builder();
        tableTypes.add("TABLE", "VIEW");
        if (sqliteConfig.isIncludeSystemTables()) {
            tableTypes.add("SYSTEM TABLE", "SYSTEM VIEW");
        }
        this.tableTypes = tableTypes.build();
        this.useTypeAffinity = sqliteConfig.getUseTypeAffinity();
        this.customDataTypes = sqliteConfig.getCustomDataTypes();
    }

    @Override
    public Connection getConnection(ConnectorSession session)
            throws SQLException
    {
        // FIXME: The method calls Connection.setReadOnly method, but Sqlite
        // FIXME: does not support changing read-only status after connection.
        // FIXME: This must be done on connection with connection's properties.
        // FIXME: and some time Trino seems to need a read-only connection.
        return connectionFactory.openConnection(session);
    }

    @Override // FIXME: Must be override if connector does not support SCHEMA
    public Collection<String> listSchemas(Connection connection)
    {
        return SCHEMAS;
    }

    @Override // FIXME: Must be override if connector does not support SCHEMA
    protected String getTableRemoteSchemaName(ResultSet resultSet)
            throws SQLException
    {
        return fromRemoteSchemaName(resultSet, MAIN_SCHEMA);
    }

    @Override
    protected Optional<String> getColumnDefaultValue(ResultSet resultSet)
            throws SQLException
    {
        String defaultValue = emptyToNull(resultSet.getString("COLUMN_DEF"));
        if (defaultValue != null && defaultValue.startsWith("'") && defaultValue.endsWith("'")) {
            defaultValue = defaultValue.substring(1, defaultValue.length() - 2).replace("''", "'");
        }
        return Optional.ofNullable(defaultValue);
    }

    @Override // FIXME: Must be override if connector does not support SCHEMA
    public ResultSet getTables(Connection connection, Optional<String> remoteSchemaName, Optional<String> remoteTableName)
            throws SQLException
    {
        // this method is called by IdentifierMapping, so cannot use IdentifierMapping here as this would cause an endless loop
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getTables(
                null,
                getRemoteSchemaName(metadata, remoteSchemaName),
                escapeObjectNameForMetadataQuery(remoteTableName, metadata.getSearchStringEscape()).orElse(null),
                getTableTypes().map(types -> types.toArray(String[]::new)).orElse(null));
    }

    private String getRemoteSchemaName(DatabaseMetaData metadata, Optional<String> remoteSchemaName)
            throws SQLException
    {
        if (remoteSchemaName.isEmpty() || remoteSchemaName.get().isEmpty()) {
            return MAIN_SCHEMA;
        }
        return escapeObjectNameForMetadataQuery(remoteSchemaName, metadata.getSearchStringEscape()).orElse(null);
    }

    @Override
    protected RemoteTableName getRemoteTable(ResultSet resultSet)
            throws SQLException
    {
        return new RemoteTableName(
                Optional.ofNullable(resultSet.getString("TABLE_CAT")),
                Optional.ofNullable(getTableRemoteSchemaName(resultSet)),
                resultSet.getString("TABLE_NAME"));
    }

    @Override
    protected ResultSet getAllTableColumns(Connection connection, Optional<String> remoteSchemaName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getColumns(
                null,
                escapeObjectNameForMetadataQuery(remoteSchemaName, metadata.getSearchStringEscape()).orElse(null),
                null,
                null);
    }

    @Override // FIXME: Must be override if connector does not support SCHEMA
    protected ResultSet getColumns(RemoteTableName remoteTableName, DatabaseMetaData metadata)
            throws SQLException
    {
        String schemaName = getRemoteSchemaName(metadata, remoteTableName.getSchemaName());

        return metadata.getColumns(
                null,
                schemaName,
                escapeObjectNameForMetadataQuery(remoteTableName.getTableName(), metadata.getSearchStringEscape()),
                null);
    }

    @Override // FIXME: This method is override to allow Trino to handle SQL Types better than Sqlite offers.
    protected JdbcTypeHandle getColumnTypeHandle(ResultSet resultSet, Optional<CaseSensitivity> caseSensitivity)
            throws SQLException
    {
        Optional<String> typeName = Optional.ofNullable(resultSet.getString("TYPE_NAME"));
        int dataType = getSqliteDataType(customDataTypes, useTypeAffinity, resultSet, typeName);
        return new JdbcTypeHandle(
                dataType,
                getSqliteTypeName(typeName, dataType),
                getInteger(resultSet, "COLUMN_SIZE"),
                getInteger(resultSet, "DECIMAL_DIGITS"),
                // arrayDimensions
                Optional.<Integer>empty(),
                caseSensitivity);
    }

    @Override // FIXME: This method is override to allow Trino to handle SQL Types better than Sqlite offers.
    protected JdbcTypeHandle getColumnTypeHandle(ResultSetMetaData metadata, int column)
            throws SQLException
    {
        Optional<String> typeName = Optional.ofNullable(metadata.getColumnTypeName(column));
        int dataType = getSqliteDataType(customDataTypes, useTypeAffinity, metadata.getColumnType(column), typeName);
        return new JdbcTypeHandle(
                dataType,
                getSqliteTypeName(typeName, dataType),
                Optional.of(metadata.getPrecision(column)),
                Optional.of(metadata.getScale(column)),
                Optional.empty(), // TODO support arrays
                Optional.of(metadata.isCaseSensitive(column) ? CASE_SENSITIVE : CASE_INSENSITIVE));
    }

    @Override
    protected Optional<List<String>> getTableTypes()
    {
        return Optional.of(tableTypes);
    }

    @Override
    // XXX: see: https://www.sqlite.org/datatype3.html#affinity_name_examples
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        switch (typeHandle.jdbcType()) {
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
            case Types.FLOAT:
            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());
            case Types.DECIMAL:
            case Types.NUMERIC:
                int decimalDigits = typeHandle.requiredDecimalDigits();
                int precision = typeHandle.requiredColumnSize();
                // FIXME: This seems necessary if we want to pass the test testDecimal(), I don't really understand why?
                // FIXME: And perhaps it would be preferable to do this in getColumns() so that typeHandle offers the correct precision?
                if (precision > decimalDigits) {
                    precision -= decimalDigits;
                }
                return Optional.of(decimalColumnMapping(createDecimalType(max(precision, 1), max(decimalDigits, 0))));
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.CLOB:
                return Optional.of(defaultVarcharColumnMapping(typeHandle.requiredColumnSize(), true));
            case Types.BLOB:
            case Types.VARBINARY:
                return Optional.of(varbinaryColumnMapping());
            case Types.DATE:
                return Optional.of(dateColumnMappingUsingLocalDate());
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
            return WriteMapping.booleanMapping("boolean", booleanWriteFunction());
        }
        if (type == TINYINT) {
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
            return WriteMapping.longMapping("float", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double", doubleWriteFunction());
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
        if (type == VARBINARY) {
            return WriteMapping.sliceMapping("blob", varbinaryWriteFunction());
        }
        if (type == DATE) {
            return WriteMapping.longMapping("date", dateWriteFunctionUsingLocalDate());
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    public void truncateTable(ConnectorSession session, JdbcTableHandle handle)
    {
        verify(handle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s", handle);
        String sql = "DELETE FROM " + quoted(handle.asPlainTable().getRemoteTableName());
        execute(session, sql);
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping a not null constraint");
    }

    @Override
    protected List<String> createTableSqls(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        if (tableMetadata.getComment().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with table comment");
        }

        ImmutableList.Builder<String> columnDefinitions = ImmutableList.builder();
        columnDefinitions.addAll(columns);

        Optional<List<String>> primaryKeys = SqliteTableProperties.getPrimaryKey(tableMetadata.getProperties());
        // FIXME: The auto-incrementing column takes precedence over the primary key declaration.
        if (primaryKeys.isPresent() && !hasAutoIncrement(columns)) {
            setPrimaryKey(columnDefinitions, remoteTableName, tableMetadata, primaryKeys.get());
        }

        return ImmutableList.of(format("CREATE TABLE %s (%s)",
                quoted(remoteTableName),
                join(", ", columnDefinitions.build())));
    }

    private boolean hasAutoIncrement(List<String> columns)
    {
        return columns.stream().anyMatch(e -> e.endsWith(PK_AUTOINCREMENT));
    }

    @Override
    protected String getColumnDefinitionSql(ConnectorSession session, ColumnMetadata column, String columnName)
    {
        if (column.getComment().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with column comment");
        }

        StringBuilder sb = new StringBuilder()
                .append(quoted(columnName))
                .append(" ")
                .append(toWriteMapping(session, column.getType()).getDataType());
        Optional<String> defaultValue = column.getDefaultValue();
        if (defaultValue.isPresent()) {
            sb.append(" DEFAULT ");
            sb.append(getDefaultValue(column.getType(), defaultValue.get()));
        }
        if (!column.isNullable()) {
            sb.append(" NOT NULL");
        }
        if (SqliteColumnProperties.isAutoIncrement(column.getProperties())) {
            checkAutoIncrementColumn(column.getType().getBaseName(), columnName);
            sb.append(PK_AUTOINCREMENT);
        }
        return sb.toString();
    }

    private void checkAutoIncrementColumn(String columnType, String columnName)
    {
        checkArgument(AUTOINCREMENT_TYPES.contains(columnType), "Unsupported type %s for autoincrement properties on column %s", columnType, columnName);
    }

    private void setPrimaryKey(ImmutableList.Builder<String> columnDefinitions, RemoteTableName remoteTableName, ConnectorTableMetadata tableMetadata, List<String> primaryKeys)
    {
        if (!primaryKeys.isEmpty()) {
            verifyPrimaryKey(remoteTableName, primaryKeys, tableMetadata.getColumns());
            columnDefinitions.add(format("PRIMARY KEY (%s)",
                    primaryKeys.stream().map(this::quoted).collect(joining(", "))));
        }
    }

    private void verifyPrimaryKey(RemoteTableName remoteTableName, List<String> primaryKeys, List<ColumnMetadata> columns)
    {
        Set<String> columnNames = columns.stream()
                .map(ColumnMetadata::getName)
                .collect(toImmutableSet());
        for (String primaryKey : primaryKeys) {
            if (!columnNames.contains(primaryKey)) {
                throw new TrinoException(INVALID_TABLE_PROPERTY,
                        format("Column '%s' specified in property '%s' doesn't exist in table '%s'",
                                primaryKey,
                                PRIMARY_KEY_PROPERTY,
                                remoteTableName.getTableName()));
            }
        }
    }

    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        List<String> primaryKeys = getPrimaryKeys(session, tableHandle.getRequiredNamedRelation().getRemoteTableName()).stream()
                .map(JdbcColumnHandle::getColumnName)
                .collect(toImmutableList());
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        if (!primaryKeys.isEmpty()) {
            properties.put(PRIMARY_KEY_PROPERTY, primaryKeys);
        }
        return properties.buildOrThrow();
    }

    @Override
    public List<JdbcColumnHandle> getPrimaryKeys(ConnectorSession session, RemoteTableName remoteTableName)
    {
        List<JdbcColumnHandle> columns = getColumns(session, remoteTableName.getSchemaTableName(), remoteTableName);
        try (Connection connection = connectionFactory.openConnection(session);
                ResultSet resultSet = getPrimaryKeyResultSet(connection, remoteTableName)) {
            Set<String> primaryKeys = new HashSet<>();
            while (resultSet.next()) {
                String columnName = resultSet.getString("COLUMN_NAME");
                primaryKeys.add(columnName);
            }
            if (primaryKeys.isEmpty()) {
                return ImmutableList.of();
            }
            return columns.stream()
                    .filter(column -> primaryKeys.contains(column.getColumnName()))
                    .collect(toImmutableList());
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private ResultSet getPrimaryKeyResultSet(Connection connection, RemoteTableName remoteTableName)
            throws SQLException
    {
        return connection.getMetaData().getPrimaryKeys(remoteTableName.getCatalogName().orElse(null),
                remoteTableName.getSchemaName().orElse(null),
                remoteTableName.getTableName());
    }

    @Override
    protected void copyTableSchema(ConnectorSession session, Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        // If we want to get the original column definitions, we should not use CTAS here, but the 'sqlite_schema' table.
        String sql = format("CREATE TABLE %s.%s %s",
                schemaName,
                quoted(newTableName),
                getTableDefinition(connection, schemaName, tableName)
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, format("Unable to find the table '%s' in '%s.sqlite_schema'", tableName, schemaName))));

        try {
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private Optional<String> getTableDefinition(Connection connection, String schemaName, String tableName)
    {
        Optional<String> tableDefinition;
        String sql = format("SELECT substr(sql, instr(sql, '(')) FROM %s.sqlite_schema WHERE type='table' AND name=?", schemaName);
        try (PreparedStatement statement = connection.prepareStatement(sql);
                ResultSet resultSet = getTableDefinitionResultSet(statement, tableName)) {
            if (resultSet.next()) {
                tableDefinition = Optional.ofNullable(resultSet.getString(1));
            }
            else {
                tableDefinition = Optional.empty();
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
        return tableDefinition;
    }

    private ResultSet getTableDefinitionResultSet(PreparedStatement statement, String tableName)
            throws SQLException
    {
        statement.setString(1, tableName);
        return statement.executeQuery();
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating schemas");
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas");
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
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
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> format("%s LIMIT %d", sql, limit));
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public boolean supportsMerge()
    {
        return true;
    }

    @Override
    public JdbcMergeTableHandle beginMerge(
            ConnectorSession session,
            JdbcTableHandle handle,
            Map<Integer, Collection<ColumnHandle>> updateColumnHandles,
            Consumer<Runnable> rollbackActionCollector,
            RetryMode retryMode)
    {
        if (retryMode != NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support MERGE with fault-tolerant execution");
        }

        if (!isNonTransactionalMerge(session)) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support MERGE with transactional execution");
        }

        return super.beginMerge(session, handle, updateColumnHandles, rollbackActionCollector, retryMode);
    }

    @Override
    public void finishMerge(ConnectorSession session, JdbcMergeTableHandle handle, Set<Long> pageSinkIds)
    {
        // When the connector retry mode is NO_RETRIES but isNonTransactionalInsert is false
        // the insert of merge still will first create temporary table
        finishInsertTable(session, handle.getOutputTableHandle(), pageSinkIds);
    }
}
