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
package io.trino.plugin.jdbc;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import io.airlift.log.Logger;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import javax.annotation.Nullable;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.isNonTransactionalInsert;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.IGNORE;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.sql.DatabaseMetaData.columnNoNulls;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public abstract class BaseJdbcClient
        implements JdbcClient
{
    private static final Logger log = Logger.get(BaseJdbcClient.class);

    protected final ConnectionFactory connectionFactory;
    protected final String identifierQuote;
    protected final Set<String> jdbcTypesMappedToVarchar;
    private final IdentifierMapping identifierMapping;

    public BaseJdbcClient(
            BaseJdbcConfig config,
            String identifierQuote,
            ConnectionFactory connectionFactory,
            IdentifierMapping identifierMapping)
    {
        this(
                identifierQuote,
                connectionFactory,
                config.getJdbcTypesMappedToVarchar(),
                identifierMapping);
    }

    public BaseJdbcClient(
            String identifierQuote,
            ConnectionFactory connectionFactory,
            Set<String> jdbcTypesMappedToVarchar,
            IdentifierMapping identifierMapping)
    {
        this.identifierQuote = requireNonNull(identifierQuote, "identifierQuote is null");
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
        this.jdbcTypesMappedToVarchar = ImmutableSortedSet.orderedBy(CASE_INSENSITIVE_ORDER)
                .addAll(requireNonNull(jdbcTypesMappedToVarchar, "jdbcTypesMappedToVarchar is null"))
                .build();
        this.identifierMapping = requireNonNull(identifierMapping, "identifierMapping is null");
    }

    protected IdentifierMapping getIdentifierMapping()
    {
        return identifierMapping;
    }

    @Override
    public final Set<String> getSchemaNames(ConnectorSession session)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            return listSchemas(connection).stream()
                    .map(remoteSchemaName -> identifierMapping.fromRemoteSchemaName(remoteSchemaName))
                    .collect(toImmutableSet());
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    public Collection<String> listSchemas(Connection connection)
    {
        try (ResultSet resultSet = connection.getMetaData().getSchemas(connection.getCatalog(), null)) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM");
                // skip internal schemas
                if (filterSchema(schemaName)) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected boolean filterSchema(String schemaName)
    {
        return !schemaName.equalsIgnoreCase("information_schema");
    }

    @Override
    public List<SchemaTableName> getTableNames(ConnectorSession session, Optional<String> schema)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            ConnectorIdentity identity = session.getIdentity();
            Optional<String> remoteSchema = schema.map(schemaName -> identifierMapping.toRemoteSchemaName(identity, connection, schemaName));
            if (remoteSchema.isPresent() && !filterSchema(remoteSchema.get())) {
                return ImmutableList.of();
            }

            try (ResultSet resultSet = getTables(connection, remoteSchema, Optional.empty())) {
                ImmutableList.Builder<SchemaTableName> list = ImmutableList.builder();
                while (resultSet.next()) {
                    String remoteSchemaFromResultSet = getTableSchemaName(resultSet);
                    String tableSchema = identifierMapping.fromRemoteSchemaName(remoteSchemaFromResultSet);
                    String tableName = identifierMapping.fromRemoteTableName(remoteSchemaFromResultSet, resultSet.getString("TABLE_NAME"));
                    if (filterSchema(tableSchema)) {
                        list.add(new SchemaTableName(tableSchema, tableName));
                    }
                }
                return list.build();
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public Optional<JdbcTableHandle> getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            ConnectorIdentity identity = session.getIdentity();
            String remoteSchema = identifierMapping.toRemoteSchemaName(identity, connection, schemaTableName.getSchemaName());
            String remoteTable = identifierMapping.toRemoteTableName(identity, connection, remoteSchema, schemaTableName.getTableName());
            try (ResultSet resultSet = getTables(connection, Optional.of(remoteSchema), Optional.of(remoteTable))) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new JdbcTableHandle(schemaTableName, getRemoteTable(resultSet)));
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

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        if (tableHandle.getColumns().isPresent()) {
            return tableHandle.getColumns().get();
        }
        checkArgument(tableHandle.isNamedRelation(), "Cannot get columns for %s", tableHandle);
        SchemaTableName schemaTableName = tableHandle.getRequiredNamedRelation().getSchemaTableName();
        RemoteTableName remoteTableName = tableHandle.getRequiredNamedRelation().getRemoteTableName();

        try (Connection connection = connectionFactory.openConnection(session);
                ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
            int allColumns = 0;
            List<JdbcColumnHandle> columns = new ArrayList<>();
            while (resultSet.next()) {
                // skip if table doesn't match expected
                if (!(Objects.equals(remoteTableName, getRemoteTable(resultSet)))) {
                    continue;
                }
                allColumns++;
                String columnName = resultSet.getString("COLUMN_NAME");
                JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                        getInteger(resultSet, "DATA_TYPE").orElseThrow(() -> new IllegalStateException("DATA_TYPE is null")),
                        Optional.ofNullable(resultSet.getString("TYPE_NAME")),
                        getInteger(resultSet, "COLUMN_SIZE"),
                        getInteger(resultSet, "DECIMAL_DIGITS"),
                        Optional.empty(),
                        Optional.empty());
                Optional<ColumnMapping> columnMapping = toColumnMapping(session, connection, typeHandle);
                log.debug("Mapping data type of '%s' column '%s': %s mapped to %s", schemaTableName, columnName, typeHandle, columnMapping);
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
                            "Unsupported type handling is set to %s, but toTrinoType() returned empty for %s",
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

    protected static Optional<Integer> getInteger(ResultSet resultSet, String columnLabel)
            throws SQLException
    {
        int value = resultSet.getInt(columnLabel);
        if (resultSet.wasNull()) {
            return Optional.empty();
        }
        return Optional.of(value);
    }

    protected ResultSet getColumns(JdbcTableHandle tableHandle, DatabaseMetaData metadata)
            throws SQLException
    {
        RemoteTableName remoteTableName = tableHandle.getRequiredNamedRelation().getRemoteTableName();
        return metadata.getColumns(
                remoteTableName.getCatalogName().orElse(null),
                escapeNamePattern(remoteTableName.getSchemaName(), metadata.getSearchStringEscape()).orElse(null),
                escapeNamePattern(Optional.of(remoteTableName.getTableName()), metadata.getSearchStringEscape()).orElse(null),
                null);
    }

    @Override
    public List<ColumnMapping> toColumnMappings(ConnectorSession session, List<JdbcTypeHandle> typeHandles)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            return typeHandles.stream()
                    .map(typeHandle -> toColumnMapping(session, connection, typeHandle)
                            .orElseThrow(() -> new VerifyException(format("Unsupported type handle %s", typeHandle))))
                    .collect(toImmutableList());
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected Optional<ColumnMapping> getForcedMappingToVarchar(JdbcTypeHandle typeHandle)
    {
        if (typeHandle.getJdbcTypeName().isPresent() && jdbcTypesMappedToVarchar.contains(typeHandle.getJdbcTypeName().get())) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.empty();
    }

    protected static Optional<ColumnMapping> mapToUnboundedVarchar(JdbcTypeHandle typeHandle)
    {
        VarcharType unboundedVarcharType = createUnboundedVarcharType();
        return Optional.of(ColumnMapping.sliceMapping(
                unboundedVarcharType,
                varcharReadFunction(unboundedVarcharType),
                (statement, index, value) -> {
                    throw new TrinoException(
                            NOT_SUPPORTED,
                            "Underlying type that is mapped to VARCHAR is not supported for INSERT: " + typeHandle.getJdbcTypeName().get());
                },
                DISABLE_PUSHDOWN));
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return new FixedSplitSource(ImmutableList.of(new JdbcSplit(Optional.empty())));
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcSplit split)
            throws SQLException
    {
        Connection connection = connectionFactory.openConnection(session);
        try {
            connection.setReadOnly(true);
        }
        catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }

    @Override
    public PreparedQuery prepareQuery(
            ConnectorSession session,
            JdbcTableHandle table,
            Optional<List<List<JdbcColumnHandle>>> groupingSets,
            List<JdbcColumnHandle> columns,
            Map<String, String> columnExpressions)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            return prepareQuery(session, connection, table, groupingSets, columns, columnExpressions, Optional.empty());
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle table, List<JdbcColumnHandle> columns)
            throws SQLException
    {
        PreparedQuery preparedQuery = prepareQuery(session, connection, table, Optional.empty(), columns, ImmutableMap.of(), Optional.of(split));
        return new QueryBuilder(this).prepareStatement(session, connection, preparedQuery);
    }

    protected PreparedQuery prepareQuery(
            ConnectorSession session,
            Connection connection,
            JdbcTableHandle table,
            Optional<List<List<JdbcColumnHandle>>> groupingSets,
            List<JdbcColumnHandle> columns,
            Map<String, String> columnExpressions,
            Optional<JdbcSplit> split)
    {
        return applyQueryTransformations(table, new QueryBuilder(this).prepareQuery(
                session,
                connection,
                table.getRelationHandle(),
                groupingSets,
                columns,
                columnExpressions,
                table.getConstraint(),
                getAdditionalPredicate(table.getConstraintExpressions(), split.flatMap(JdbcSplit::getAdditionalPredicate))));
    }

    protected static Optional<String> getAdditionalPredicate(List<String> constraintExpressions, Optional<String> splitPredicate)
    {
        if (constraintExpressions.isEmpty() && splitPredicate.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(Stream.concat(
                        constraintExpressions.stream(),
                        splitPredicate.stream())
                .collect(joining(" AND ")));
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
        for (JdbcJoinCondition joinCondition : joinConditions) {
            if (!isSupportedJoinCondition(joinCondition)) {
                return Optional.empty();
            }
        }

        QueryBuilder queryBuilder = new QueryBuilder(this);
        return Optional.of(queryBuilder.prepareJoinQuery(
                session,
                joinType,
                leftSource,
                rightSource,
                joinConditions,
                leftAssignments,
                rightAssignments));
    }

    protected boolean isSupportedJoinCondition(JdbcJoinCondition joinCondition)
    {
        return false;
    }

    protected PreparedQuery applyQueryTransformations(JdbcTableHandle tableHandle, PreparedQuery query)
    {
        PreparedQuery preparedQuery = query;

        if (tableHandle.getLimit().isPresent()) {
            if (tableHandle.getSortOrder().isPresent()) {
                preparedQuery = preparedQuery.transformQuery(applyTopN(tableHandle.getSortOrder().get(), tableHandle.getLimit().getAsLong()));
            }
            else {
                preparedQuery = preparedQuery.transformQuery(applyLimit(tableHandle.getLimit().getAsLong()));
            }
        }

        return preparedQuery;
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            createTable(session, tableMetadata, tableMetadata.getTable().getTableName());
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            return createTable(session, tableMetadata, generateTemporaryTableName());
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected JdbcOutputTableHandle createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, String targetTableName)
            throws SQLException
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();

        ConnectorIdentity identity = session.getIdentity();
        if (!getSchemaNames(session).contains(schemaTableName.getSchemaName())) {
            throw new TrinoException(NOT_FOUND, "Schema not found: " + schemaTableName.getSchemaName());
        }

        try (Connection connection = connectionFactory.openConnection(session)) {
            String remoteSchema = identifierMapping.toRemoteSchemaName(identity, connection, schemaTableName.getSchemaName());
            String remoteTable = identifierMapping.toRemoteTableName(identity, connection, remoteSchema, schemaTableName.getTableName());
            String remoteTargetTableName = identifierMapping.toRemoteTableName(identity, connection, remoteSchema, targetTableName);
            String catalog = connection.getCatalog();

            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<String> columnList = ImmutableList.builder();
            for (ColumnMetadata column : tableMetadata.getColumns()) {
                String columnName = identifierMapping.toRemoteColumnName(connection, column.getName());
                columnNames.add(columnName);
                columnTypes.add(column.getType());
                columnList.add(getColumnDefinitionSql(session, column, columnName));
            }

            RemoteTableName remoteTableName = new RemoteTableName(Optional.ofNullable(catalog), Optional.ofNullable(remoteSchema), remoteTargetTableName);
            String sql = createTableSql(remoteTableName, columnList.build(), tableMetadata);
            execute(connection, sql);

            return new JdbcOutputTableHandle(
                    catalog,
                    remoteSchema,
                    remoteTable,
                    columnNames.build(),
                    columnTypes.build(),
                    Optional.empty(),
                    remoteTargetTableName);
        }
    }

    protected String createTableSql(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        checkArgument(tableMetadata.getProperties().isEmpty(), "Unsupported table properties: %s", tableMetadata.getProperties());
        return format("CREATE TABLE %s (%s)", quoted(remoteTableName), join(", ", columns));
    }

    protected String getColumnDefinitionSql(ConnectorSession session, ColumnMetadata column, String columnName)
    {
        StringBuilder sb = new StringBuilder()
                .append(quoted(columnName))
                .append(" ")
                .append(toWriteMapping(session, column.getType()).getDataType());
        if (!column.isNullable()) {
            sb.append(" NOT NULL");
        }
        return sb.toString();
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        SchemaTableName schemaTableName = tableHandle.asPlainTable().getSchemaTableName();
        ConnectorIdentity identity = session.getIdentity();

        try (Connection connection = connectionFactory.openConnection(session)) {
            String remoteSchema = identifierMapping.toRemoteSchemaName(identity, connection, schemaTableName.getSchemaName());
            String remoteTable = identifierMapping.toRemoteTableName(identity, connection, remoteSchema, schemaTableName.getTableName());
            String catalog = connection.getCatalog();

            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<JdbcTypeHandle> jdbcColumnTypes = ImmutableList.builder();
            for (JdbcColumnHandle column : columns) {
                columnNames.add(column.getColumnName());
                columnTypes.add(column.getColumnType());
                jdbcColumnTypes.add(column.getJdbcTypeHandle());
            }

            if (isNonTransactionalInsert(session)) {
                return new JdbcOutputTableHandle(
                        catalog,
                        remoteSchema,
                        remoteTable,
                        columnNames.build(),
                        columnTypes.build(),
                        Optional.of(jdbcColumnTypes.build()),
                        remoteTable);
            }

            String remoteTemporaryTableName = identifierMapping.toRemoteTableName(identity, connection, remoteSchema, generateTemporaryTableName());
            copyTableSchema(connection, catalog, remoteSchema, remoteTable, remoteTemporaryTableName, columnNames.build());

            return new JdbcOutputTableHandle(
                    catalog,
                    remoteSchema,
                    remoteTable,
                    columnNames.build(),
                    columnTypes.build(),
                    Optional.of(jdbcColumnTypes.build()),
                    remoteTemporaryTableName);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected void copyTableSchema(Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        String sql = format(
                "CREATE TABLE %s AS SELECT %s FROM %s WHERE 0 = 1",
                quoted(catalogName, schemaName, newTableName),
                columnNames.stream()
                        .map(this::quoted)
                        .collect(joining(", ")),
                quoted(catalogName, schemaName, tableName));
        execute(connection, sql);
    }

    protected String generateTemporaryTableName()
    {
        return "tmp_trino_" + UUID.randomUUID().toString().replace("-", "");
    }

    @Override
    public void commitCreateTable(ConnectorSession session, JdbcOutputTableHandle handle)
    {
        renameTable(
                session,
                handle.getCatalogName(),
                handle.getSchemaName(),
                handle.getTemporaryTableName(),
                new SchemaTableName(handle.getSchemaName(), handle.getTableName()));
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        renameTable(session, handle.getCatalogName(), handle.getSchemaName(), handle.getTableName(), newTableName);
    }

    protected void renameTable(ConnectorSession session, String catalogName, String remoteSchemaName, String remoteTableName, SchemaTableName newTable)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            String newSchemaName = newTable.getSchemaName();
            String newTableName = newTable.getTableName();
            ConnectorIdentity identity = session.getIdentity();
            String newRemoteSchemaName = identifierMapping.toRemoteSchemaName(identity, connection, newSchemaName);
            String newRemoteTableName = identifierMapping.toRemoteTableName(identity, connection, newRemoteSchemaName, newTableName);
            String sql = format(
                    "ALTER TABLE %s RENAME TO %s",
                    quoted(catalogName, remoteSchemaName, remoteTableName),
                    quoted(catalogName, newRemoteSchemaName, newRemoteTableName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void finishInsertTable(ConnectorSession session, JdbcOutputTableHandle handle)
    {
        if (isNonTransactionalInsert(session)) {
            checkState(handle.getTemporaryTableName().equals(handle.getTableName()), "Unexpected use of temporary table when non transactional inserts are enabled");
            return;
        }

        RemoteTableName temporaryTable = new RemoteTableName(
                Optional.ofNullable(handle.getCatalogName()),
                Optional.ofNullable(handle.getSchemaName()),
                handle.getTemporaryTableName());
        RemoteTableName targetTable = new RemoteTableName(
                Optional.ofNullable(handle.getCatalogName()),
                Optional.ofNullable(handle.getSchemaName()),
                handle.getTableName());
        String insertSql = buildInsertSql(session, targetTable, temporaryTable, handle.getColumnNames());

        try (Connection connection = getConnection(session, handle)) {
            execute(connection, insertSql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
        finally {
            dropTable(session, temporaryTable);
        }
    }

    protected String buildInsertSql(ConnectorSession session, RemoteTableName targetTable, RemoteTableName sourceTable, List<String> columnNames)
    {
        String columns = columnNames.stream()
                .map(this::quoted)
                .collect(joining(", "));
        return format("INSERT INTO %s (%s) SELECT %s FROM %s", quoted(targetTable), columns, columns, quoted(sourceTable));
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            String columnName = column.getName();
            String remoteColumnName = identifierMapping.toRemoteColumnName(connection, columnName);
            String sql = format(
                    "ALTER TABLE %s ADD %s",
                    quoted(handle.asPlainTable().getRemoteTableName()),
                    getColumnDefinitionSql(session, column, remoteColumnName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            String newRemoteColumnName = identifierMapping.toRemoteColumnName(connection, newColumnName);
            String sql = format(
                    "ALTER TABLE %s RENAME COLUMN %s TO %s",
                    quoted(handle.asPlainTable().getRemoteTableName()),
                    jdbcColumn.getColumnName(),
                    newRemoteColumnName);
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        String sql = format(
                "ALTER TABLE %s DROP COLUMN %s",
                quoted(handle.asPlainTable().getRemoteTableName()),
                column.getColumnName());
        execute(session, sql);
    }

    @Override
    public void dropTable(ConnectorSession session, JdbcTableHandle handle)
    {
        dropTable(session, handle.asPlainTable().getRemoteTableName());
    }

    private void dropTable(ConnectorSession session, RemoteTableName remoteTableName)
    {
        String sql = "DROP TABLE " + quoted(remoteTableName);
        execute(session, sql);
    }

    @Override
    public void rollbackCreateTable(ConnectorSession session, JdbcOutputTableHandle handle)
    {
        dropTable(session, new JdbcTableHandle(
                new SchemaTableName(handle.getSchemaName(), handle.getTemporaryTableName()),
                handle.getCatalogName(),
                handle.getSchemaName(),
                handle.getTemporaryTableName()));
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle, List<WriteFunction> columnWriters)
    {
        checkArgument(handle.getColumnNames().size() == columnWriters.size(), "handle and columnWriters mismatch: %s, %s", handle, columnWriters);
        return format(
                "INSERT INTO %s (%s) VALUES (%s)",
                quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTemporaryTableName()),
                handle.getColumnNames().stream()
                        .map(this::quoted)
                        .collect(joining(", ")),
                columnWriters.stream()
                        .map(WriteFunction::getBindExpression)
                        .collect(joining(",")));
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcOutputTableHandle handle)
            throws SQLException
    {
        return connectionFactory.openConnection(session);
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        return connection.prepareStatement(sql);
    }

    public ResultSet getTables(Connection connection, Optional<String> remoteSchemaName, Optional<String> remoteTableName)
            throws SQLException
    {
        // this method is called by IdentifierMapping, so cannot use IdentifierMapping here as this woudl cause an endless loop
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(remoteSchemaName, metadata.getSearchStringEscape()).orElse(null),
                escapeNamePattern(remoteTableName, metadata.getSearchStringEscape()).orElse(null),
                getTableTypes().map(types -> types.toArray(String[]::new)).orElse(null));
    }

    protected Optional<List<String>> getTableTypes()
    {
        return Optional.of(ImmutableList.of("TABLE", "VIEW"));
    }

    protected String getTableSchemaName(ResultSet resultSet)
            throws SQLException
    {
        return resultSet.getString("TABLE_SCHEM");
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, TupleDomain<ColumnHandle> tupleDomain)
    {
        return TableStatistics.empty();
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        ConnectorIdentity identity = session.getIdentity();
        try (Connection connection = connectionFactory.openConnection(session)) {
            schemaName = identifierMapping.toRemoteSchemaName(identity, connection, schemaName);
            execute(connection, createSchemaSql(schemaName));
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected String createSchemaSql(String schemaName)
    {
        return "CREATE SCHEMA " + quoted(schemaName);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        ConnectorIdentity identity = session.getIdentity();
        try (Connection connection = connectionFactory.openConnection(session)) {
            schemaName = identifierMapping.toRemoteSchemaName(identity, connection, schemaName);
            execute(connection, dropSchemaSql(schemaName));
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected String dropSchemaSql(String schemaName)
    {
        return "DROP SCHEMA " + quoted(schemaName);
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        ConnectorIdentity identity = session.getIdentity();
        try (Connection connection = connectionFactory.openConnection(session)) {
            String remoteSchemaName = identifierMapping.toRemoteSchemaName(identity, connection, schemaName);
            String newRemoteSchemaName = identifierMapping.toRemoteSchemaName(identity, connection, newSchemaName);
            execute(connection, renameSchemaSql(remoteSchemaName, newRemoteSchemaName));
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected String renameSchemaSql(String remoteSchemaName, String newRemoteSchemaName)
    {
        return "ALTER SCHEMA " + quoted(remoteSchemaName) + " RENAME TO " + quoted(newRemoteSchemaName);
    }

    protected void execute(ConnectorSession session, String query)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            execute(connection, query);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected void execute(Connection connection, String query)
    {
        try (Statement statement = connection.createStatement()) {
            log.info("Execute: %s", query);
            statement.execute(query);
        }
        catch (SQLException e) {
            TrinoException exception = new TrinoException(JDBC_ERROR, e);
            exception.addSuppressed(new RuntimeException("Query: " + query));
            throw exception;
        }
    }

    protected static boolean preventTextualTypeAggregationPushdown(List<List<ColumnHandle>> groupingSets)
    {
        // Remote database can be case insensitive or sorts textual types differently than Trino.
        // In such cases we should not pushdown aggregations if the grouping set contains a textual type.
        if (!groupingSets.isEmpty()) {
            for (List<ColumnHandle> groupingSet : groupingSets) {
                boolean hasCaseSensitiveGroupingSet = groupingSet.stream()
                        .map(columnHandle -> ((JdbcColumnHandle) columnHandle).getColumnType())
                        // this may catch more cases than required (e.g. MONEY in Postgres) but doesn't affect correctness
                        .anyMatch(type -> type instanceof VarcharType || type instanceof CharType);
                if (hasCaseSensitiveGroupingSet) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        if (topNFunction().isEmpty()) {
            return false;
        }
        throw new UnsupportedOperationException("topNFunction() implemented without implementing supportsTopN()");
    }

    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.empty();
    }

    private Function<String, String> applyTopN(List<JdbcSortItem> sortOrder, long limit)
    {
        return query -> topNFunction()
                .orElseThrow()
                .apply(query, sortOrder, limit);
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        throw new UnsupportedOperationException("topNFunction() implemented without implementing isTopNLimitGuaranteed()");
    }

    @Override
    public boolean supportsLimit()
    {
        return limitFunction().isPresent();
    }

    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.empty();
    }

    private Function<String, String> applyLimit(long limit)
    {
        return query -> limitFunction()
                .orElseThrow()
                .apply(query, limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        throw new TrinoException(JDBC_ERROR, "limitFunction() is implemented without isLimitGuaranteed()");
    }

    @Override
    public String quoted(String name)
    {
        name = name.replace(identifierQuote, identifierQuote + identifierQuote);
        return identifierQuote + name + identifierQuote;
    }

    @Override
    public String quoted(RemoteTableName remoteTableName)
    {
        return quoted(
                remoteTableName.getCatalogName().orElse(null),
                remoteTableName.getSchemaName().orElse(null),
                remoteTableName.getTableName());
    }

    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return emptyMap();
    }

    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        checkArgument(handle.isNamedRelation(), "Unable to delete from synthetic table: %s", handle);
        checkArgument(handle.getLimit().isEmpty(), "Unable to delete when limit is set: %s", handle);
        checkArgument(handle.getSortOrder().isEmpty(), "Unable to delete when sort order is set: %s", handle);
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            QueryBuilder queryBuilder = new QueryBuilder(this);
            PreparedQuery preparedQuery = queryBuilder.prepareDelete(session, connection, handle.getRequiredNamedRelation(), handle.getConstraint());
            try (PreparedStatement preparedStatement = queryBuilder.prepareStatement(session, connection, preparedQuery)) {
                return OptionalLong.of(preparedStatement.executeUpdate());
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void truncateTable(ConnectorSession session, JdbcTableHandle handle)
    {
        String sql = "TRUNCATE TABLE " + quoted(handle.asPlainTable().getRemoteTableName());
        execute(session, sql);
    }

    protected String quoted(@Nullable String catalog, @Nullable String schema, String table)
    {
        StringBuilder sb = new StringBuilder();
        if (!isNullOrEmpty(catalog)) {
            sb.append(quoted(catalog)).append(".");
        }
        if (!isNullOrEmpty(schema)) {
            sb.append(quoted(schema)).append(".");
        }
        sb.append(quoted(table));
        return sb.toString();
    }

    protected static Optional<String> escapeNamePattern(Optional<String> name, String escape)
    {
        return name.map(string -> escapeNamePattern(string, escape));
    }

    private static String escapeNamePattern(String name, String escape)
    {
        requireNonNull(name, "name is null");
        requireNonNull(escape, "escape is null");
        checkArgument(!escape.isEmpty(), "Escape string must not be empty");
        checkArgument(!escape.equals("_"), "Escape string must not be '_'");
        checkArgument(!escape.equals("%"), "Escape string must not be '%'");
        name = name.replace(escape, escape + escape);
        name = name.replace("_", escape + "_");
        name = name.replace("%", escape + "%");
        return name;
    }

    private static RemoteTableName getRemoteTable(ResultSet resultSet)
            throws SQLException
    {
        return new RemoteTableName(
                Optional.ofNullable(resultSet.getString("TABLE_CAT")),
                Optional.ofNullable(resultSet.getString("TABLE_SCHEM")),
                resultSet.getString("TABLE_NAME"));
    }

    @FunctionalInterface
    public interface TopNFunction
    {
        String apply(String query, List<JdbcSortItem> sortItems, long limit);

        static TopNFunction sqlStandard(Function<String, String> quote)
        {
            return (query, sortItems, limit) -> {
                String orderBy = sortItems.stream()
                        .map(sortItem -> {
                            String ordering = sortItem.getSortOrder().isAscending() ? "ASC" : "DESC";
                            String nullsHandling = sortItem.getSortOrder().isNullsFirst() ? "NULLS FIRST" : "NULLS LAST";
                            return format("%s %s %s", quote.apply(sortItem.getColumn().getColumnName()), ordering, nullsHandling);
                        })
                        .collect(joining(", "));

                return format("%s ORDER BY %s OFFSET 0 ROWS FETCH NEXT %s ROWS ONLY", query, orderBy, limit);
            };
        }
    }
}
