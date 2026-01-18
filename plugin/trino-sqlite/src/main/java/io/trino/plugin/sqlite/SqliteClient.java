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
import com.google.inject.Inject;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.base.mapping.RemoteIdentifiers;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.CaseSensitivity;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcRemoteIdentifiers;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.UnsupportedTypeHandling;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.Type;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateColumnMappingUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.IGNORE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.sql.DatabaseMetaData.columnNoNulls;
import static java.util.stream.Collectors.joining;

public class SqliteClient
        extends BaseJdbcClient
{
    static final List<String> SQLITE_DATA_TYPES = Arrays.asList(
            "BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT",
            "REAL", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC",
            "CHAR", "VARCHAR", "CLOB",
            "BLOB", "VARBINARY",
            "DATE");

    private static final String SCHEMA = "main";
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
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, false);

        ImmutableList.Builder<String> tableTypes = ImmutableList.builder();
        tableTypes.add("TABLE", "VIEW");
        if (sqliteConfig.isIncludeSystemTables()) {
            tableTypes.add("SYSTEM TABLE", "SYSTEM VIEW");
        }
        this.tableTypes = tableTypes.build();
        this.useTypeAffinity = sqliteConfig.isUseTypeAffinity();
        this.customDataTypes = sqliteConfig.getCustomDataTypes();
    }

    @Override
    public Connection getConnection(ConnectorSession session, boolean readOnly)
            throws SQLException
    {
        // FIXME: The method calls Connection.setReadOnly method, but Sqlite
        // FIXME: does not support changing read-only status after connection.
        // FIXME: This must be done on connection with connection's properties.
        // FIXME: and some time Trino seems to need a read-only connection.
        Properties properties = new Properties();
        if (readOnly) {
            properties.setProperty("open_mode", "1"); //1 == readonly
        }
        return connectionFactory.openConnection(session, properties);
    }

    @Override // FIXME: Must be override if connector does not support SCHEMA
    public Collection<String> listSchemas(Connection connection)
    {
        return ImmutableList.of(SCHEMA);
    }

    @Override // FIXME: Must be override if connector does not support SCHEMA
    protected String getTableSchemaName(ResultSet resultSet)
            throws SQLException
    {
        return SCHEMA;
    }

    @Override // FIXME: Must be override if connector does not support SCHEMA
    public ResultSet getTables(Connection connection, Optional<String> remoteSchemaName, Optional<String> remoteTableName)
            throws SQLException
    {
        // this method is called by IdentifierMapping, so cannot use IdentifierMapping here as this would cause an endless loop
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getTables(
                null,
                null,
                escapeObjectNameForMetadataQuery(remoteTableName, metadata.getSearchStringEscape()).orElse(null),
                getTableTypes().map(types -> types.toArray(String[]::new)).orElse(null));
    }

    @Override // FIXME: Must be override if connector does not support SCHEMA
    protected ResultSet getColumns(RemoteTableName remoteTableName, DatabaseMetaData metadata)
            throws SQLException
    {
        return metadata.getColumns(
                null,
                null,
                escapeObjectNameForMetadataQuery(remoteTableName.getTableName(), metadata.getSearchStringEscape()),
                null);
    }

    @Override // FIXME: This method is override to allow Trino to handle SQL Types better than Sqlite offers.
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, SchemaTableName schemaTableName, RemoteTableName remoteTableName)
    {
        try (Connection connection = connectionFactory.openConnection(session);
                ResultSet resultSet = getColumns(remoteTableName, connection.getMetaData())) {
            Map<String, CaseSensitivity> caseSensitivityMapping = getCaseSensitivityForColumns(session, connection, schemaTableName, remoteTableName);
            int allColumns = 0;
            List<JdbcColumnHandle> columns = new ArrayList<>();
            while (resultSet.next()) {
                // skip if table doesn't match expected
                if (!Objects.equals(remoteTableName, getRemoteTable(resultSet))) {
                    continue;
                }
                allColumns++;
                String columnName = resultSet.getString("COLUMN_NAME");
                // SQLite manages types by affinity, see: https://www.sqlite.org/datatype3.html#affinity_name_examples
                // Use type name here because Sqlite does not report the exact JDBC type in the metadata
                Optional<String> typeName = Optional.ofNullable(resultSet.getString("TYPE_NAME"));
                int dataType = getInteger(resultSet, "DATA_TYPE").orElseThrow(() -> new IllegalStateException("DATA_TYPE is null"));
                dataType = getSqliteDataType(typeName, dataType);
                JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                        dataType,
                        getSqliteTypeName(typeName, dataType),
                        getInteger(resultSet, "COLUMN_SIZE"),
                        getInteger(resultSet, "DECIMAL_DIGITS"),
                        Optional.empty(),
                        Optional.ofNullable(caseSensitivityMapping.get(columnName)));
                Optional<ColumnMapping> columnMapping = toColumnMapping(session, connection, typeHandle);
                boolean nullable = (resultSet.getInt("NULLABLE") != columnNoNulls);
                // Note: some databases (e.g. SQL Server) do not return column remarks/comment here.
                Optional<String> comment = Optional.ofNullable(emptyToNull(resultSet.getString("REMARKS")));
                // skip unsupported column types
                columnMapping.ifPresent(mapping -> columns.add(JdbcColumnHandle.builder()
                        .setColumnName(columnName)
                        .setJdbcTypeHandle(typeHandle)
                        .setColumnType(mapping.getType())
                        .setNullable(nullable)
                        .setComment(comment)
                        .build()));
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
                // A table may have no supported columns. In rare cases (e.g. PostgreSQL) a table might have no columns at all.
                throw new TableNotFoundException(
                        schemaTableName,
                        format("Table '%s' has no supported columns (all %s columns are not supported)", schemaTableName, allColumns));
            }
            return ImmutableList.copyOf(columns);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private int getSqliteDataType(Optional<String> typeName, int dataType)
    {
        // Here we aim to follow the logic of defining column affinities in SQLite.
        // If the type is not specified in the column definition of a table,
        // which is possible with SQLite, then TYPENAME can be empty and will be considered as BLOB.
        if (typeName.isPresent() && !typeName.get().isEmpty()) {
            if (SQLITE_DATA_TYPES.contains(typeName.get())) {
                return JDBCType.valueOf(typeName.get()).getVendorTypeNumber();
            }
            return getCustomDataType(typeName.get(), dataType);
        }
        return Types.BLOB;
    }

    private int getCustomDataType(String typeName, int dataType)
    {
        if (customDataTypes.containsKey(typeName)) {
            return JDBCType.valueOf(customDataTypes.get(typeName)).getVendorTypeNumber();
        }
        if (useTypeAffinity) {
            return dataType;
        }
        // If the type is not recognized, then it is not managed in Trino and NULL is used for that.
        return Types.NULL;
    }

    private Optional<String> getSqliteTypeName(Optional<String> typeName, int dataType)
    {
        if (typeName.isEmpty() || typeName.get().isEmpty()) {
            return Optional.of(JDBCType.valueOf(dataType).getName());
        }
        return typeName;
    }

    @Override // FIXME: This method is override because it requires overriding the used private method getRemoteTable().
    public Optional<JdbcTableHandle> getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            ConnectorIdentity identity = session.getIdentity();
            RemoteIdentifiers remoteIdentifiers = getRemoteIdentifiers(connection);
            String remoteSchema = getIdentifierMapping().toRemoteSchemaName(remoteIdentifiers, identity, schemaTableName.getSchemaName());
            String remoteTable = getIdentifierMapping().toRemoteTableName(remoteIdentifiers, identity, remoteSchema, schemaTableName.getTableName());
            try (ResultSet resultSet = getTables(connection, Optional.of(remoteSchema), Optional.of(remoteTable))) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new JdbcTableHandle(schemaTableName, getRemoteTable(resultSet), getTableComment(resultSet)));
                }
                if (tableHandles.isEmpty()) {
                    return Optional.empty();
                }
                if (tableHandles.size() > 1) {
                    throw new TrinoException(NOT_SUPPORTED, "Multiple tables matched: " + schemaTableName);
                }
                return Optional.of(getOnlyElement(tableHandles));
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    // FIXME: Must be override if connector does not support SCHEMA
    private static RemoteTableName getRemoteTable(ResultSet resultSet)
            throws SQLException
    {
        return new RemoteTableName(
                Optional.empty(),
                Optional.of(SCHEMA),
                resultSet.getString("TABLE_NAME"));
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
        // SQLite manages types by affinity, see: https://www.sqlite.org/datatype3.html#affinity_name_examples
        // Use type name because Sqlite does not report the exact JDBC type in the metadata
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
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, MODIFYING_ROWS_MESSAGE);
    }

    @Override
    public void truncateTable(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support truncating tables");
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    @Override
    public void commitCreateTable(ConnectorSession session, JdbcOutputTableHandle handle, Set<Long> pageSinkIds)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column, ColumnPosition position)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns");
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming columns");
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping columns");
    }

    @Override
    public void dropTable(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping tables");
    }

    @Override
    public void rollbackCreateTable(ConnectorSession session, JdbcOutputTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle, List<WriteFunction> columnWriters)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support inserts");
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
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables");
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        // Deactivated because test 'testJoinPushdown()' requires write access which is not implemented yet for Sqlite
        return false;
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        // Deactivated because test 'testCaseSensitiveAggregationPushdown()' requires write access which is not implemented yet for Sqlite
        return Optional.empty();
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support writing");
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Consumer<Runnable> rollbackActionConsumer)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with data");
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support inserts");
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> format("%s LIMIT %d", sql, limit));
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.of((query, sortItems, limit) -> {
            String orderBy = sortItems.stream()
                    .map(sortItem -> {
                        String ordering = sortItem.sortOrder().isAscending() ? "ASC" : "DESC";
                        String nullsHandling = sortItem.sortOrder().isNullsFirst() ? "NULLS FIRST" : "NULLS LAST";
                        return format("%s %s %s", quoted(sortItem.column().getColumnName()), ordering, nullsHandling);
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
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        return true;
    }

    @Override // Must be override if connector does not support SCHEMA
    public RemoteIdentifiers getRemoteIdentifiers(Connection connection)
    {
        return new JdbcRemoteIdentifiers(this, connection, false);
    }
}
