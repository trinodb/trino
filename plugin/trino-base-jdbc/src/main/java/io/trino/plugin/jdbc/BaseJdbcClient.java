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

import com.google.common.base.CharMatcher;
import com.google.common.base.VerifyException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
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
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
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
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.legacyDefaultColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.IGNORE;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.sql.DatabaseMetaData.columnNoNulls;
import static java.util.Collections.emptyMap;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;

public abstract class BaseJdbcClient
        implements JdbcClient
{
    private static final Logger log = Logger.get(BaseJdbcClient.class);

    /**
     * @deprecated To be removed after {{@link #legacyToWriteMapping} is removed.
     */
    @Deprecated
    private static final Map<Type, WriteMapping> WRITE_MAPPINGS = ImmutableMap.<Type, WriteMapping>builder()
            .put(BOOLEAN, WriteMapping.booleanMapping("boolean", booleanWriteFunction()))
            .put(BIGINT, WriteMapping.longMapping("bigint", bigintWriteFunction()))
            .put(INTEGER, WriteMapping.longMapping("integer", integerWriteFunction()))
            .put(SMALLINT, WriteMapping.longMapping("smallint", smallintWriteFunction()))
            .put(TINYINT, WriteMapping.longMapping("tinyint", tinyintWriteFunction()))
            .put(DOUBLE, WriteMapping.doubleMapping("double precision", doubleWriteFunction()))
            .put(REAL, WriteMapping.longMapping("real", realWriteFunction()))
            .put(VARBINARY, WriteMapping.sliceMapping("varbinary", varbinaryWriteFunction()))
            .put(DATE, WriteMapping.longMapping("date", dateWriteFunction()))
            .build();

    protected final ConnectionFactory connectionFactory;
    protected final String identifierQuote;
    protected final Set<String> jdbcTypesMappedToVarchar;
    protected final boolean caseInsensitiveNameMatching;
    protected final Cache<JdbcIdentity, Map<String, String>> remoteSchemaNames;
    protected final Cache<RemoteTableNameCacheKey, Map<String, String>> remoteTableNames;

    public BaseJdbcClient(BaseJdbcConfig config, String identifierQuote, ConnectionFactory connectionFactory)
    {
        this(
                identifierQuote,
                connectionFactory,
                config.getJdbcTypesMappedToVarchar(),
                requireNonNull(config, "config is null").isCaseInsensitiveNameMatching(),
                config.getCaseInsensitiveNameMatchingCacheTtl());
    }

    public BaseJdbcClient(
            String identifierQuote,
            ConnectionFactory connectionFactory,
            Set<String> jdbcTypesMappedToVarchar,
            boolean caseInsensitiveNameMatching,
            Duration caseInsensitiveNameMatchingCacheTtl)
    {
        this.identifierQuote = requireNonNull(identifierQuote, "identifierQuote is null");
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
        this.jdbcTypesMappedToVarchar = ImmutableSortedSet.orderedBy(CASE_INSENSITIVE_ORDER)
                .addAll(requireNonNull(jdbcTypesMappedToVarchar, "jdbcTypesMappedToVarchar is null"))
                .build();
        requireNonNull(caseInsensitiveNameMatchingCacheTtl, "caseInsensitiveNameMatchingCacheTtl is null");

        this.caseInsensitiveNameMatching = caseInsensitiveNameMatching;
        CacheBuilder<Object, Object> remoteNamesCacheBuilder = CacheBuilder.newBuilder()
                .expireAfterWrite(caseInsensitiveNameMatchingCacheTtl.toMillis(), MILLISECONDS);
        this.remoteSchemaNames = remoteNamesCacheBuilder.build();
        this.remoteTableNames = remoteNamesCacheBuilder.build();
    }

    @Override
    public final Set<String> getSchemaNames(ConnectorSession session)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            return listSchemas(connection).stream()
                    .map(schemaName -> schemaName.toLowerCase(ENGLISH))
                    .collect(toImmutableSet());
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected Collection<String> listSchemas(Connection connection)
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
            JdbcIdentity identity = JdbcIdentity.from(session);
            Optional<String> remoteSchema = schema.map(schemaName -> toRemoteSchemaName(identity, connection, schemaName));
            if (remoteSchema.isPresent() && !filterSchema(remoteSchema.get())) {
                return ImmutableList.of();
            }

            try (ResultSet resultSet = getTables(connection, remoteSchema, Optional.empty())) {
                ImmutableList.Builder<SchemaTableName> list = ImmutableList.builder();
                while (resultSet.next()) {
                    String tableSchema = getTableSchemaName(resultSet);
                    String tableName = resultSet.getString("TABLE_NAME");
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
            JdbcIdentity identity = JdbcIdentity.from(session);
            String remoteSchema = toRemoteSchemaName(identity, connection, schemaTableName.getSchemaName());
            String remoteTable = toRemoteTableName(identity, connection, remoteSchema, schemaTableName.getTableName());
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
                // skip unsupported column types
                boolean nullable = (resultSet.getInt("NULLABLE") != columnNoNulls);
                // Note: some databases (e.g. SQL Server) do not return column remarks/comment here.
                Optional<String> comment = Optional.ofNullable(emptyToNull(resultSet.getString("REMARKS")));
                if (columnMapping.isPresent()) {
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

    /**
     * @deprecated Each connector should provide its own explicit type mapping, along with respective tests.
     */
    @Deprecated
    protected Optional<ColumnMapping> legacyToPrestoType(ConnectorSession session, @SuppressWarnings("unused") Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        Optional<ColumnMapping> connectorMapping = legacyDefaultColumnMapping(typeHandle);
        if (connectorMapping.isPresent()) {
            return connectorMapping;
        }
        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.empty();
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
            return applyQueryTransformations(table, new QueryBuilder(this).prepareQuery(
                    session,
                    connection,
                    table.getRelationHandle(),
                    groupingSets,
                    columns,
                    columnExpressions,
                    table.getConstraint(),
                    Optional.empty()));
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
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

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle table, List<JdbcColumnHandle> columns)
            throws SQLException
    {
        QueryBuilder queryBuilder = new QueryBuilder(this);
        PreparedQuery preparedQuery = applyQueryTransformations(table, queryBuilder.prepareQuery(
                session,
                connection,
                table.getRelationHandle(),
                Optional.empty(),
                columns,
                ImmutableMap.of(),
                table.getConstraint(),
                split.getAdditionalPredicate()));
        return queryBuilder.prepareStatement(session, connection, preparedQuery);
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

    protected JdbcOutputTableHandle createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, String tableName)
            throws SQLException
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();

        JdbcIdentity identity = JdbcIdentity.from(session);
        if (!getSchemaNames(session).contains(schemaTableName.getSchemaName())) {
            throw new TrinoException(NOT_FOUND, "Schema not found: " + schemaTableName.getSchemaName());
        }

        try (Connection connection = connectionFactory.openConnection(session)) {
            boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
            String remoteSchema = toRemoteSchemaName(identity, connection, schemaTableName.getSchemaName());
            String remoteTable = toRemoteTableName(identity, connection, remoteSchema, schemaTableName.getTableName());
            if (uppercase) {
                tableName = tableName.toUpperCase(ENGLISH);
            }
            String catalog = connection.getCatalog();

            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<String> columnList = ImmutableList.builder();
            for (ColumnMetadata column : tableMetadata.getColumns()) {
                String columnName = column.getName();
                if (uppercase) {
                    columnName = columnName.toUpperCase(ENGLISH);
                }
                columnNames.add(columnName);
                columnTypes.add(column.getType());
                columnList.add(getColumnDefinitionSql(session, column, columnName));
            }

            RemoteTableName remoteTableName = new RemoteTableName(Optional.ofNullable(catalog), Optional.ofNullable(remoteSchema), tableName);
            String sql = createTableSql(remoteTableName, columnList.build(), tableMetadata);
            execute(connection, sql);

            return new JdbcOutputTableHandle(
                    catalog,
                    remoteSchema,
                    remoteTable,
                    columnNames.build(),
                    columnTypes.build(),
                    Optional.empty(),
                    tableName);
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
        JdbcIdentity identity = JdbcIdentity.from(session);

        try (Connection connection = connectionFactory.openConnection(session)) {
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

    protected void renameTable(ConnectorSession session, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            String newSchemaName = newTable.getSchemaName();
            String newTableName = newTable.getTableName();
            if (connection.getMetaData().storesUpperCaseIdentifiers()) {
                newSchemaName = newSchemaName.toUpperCase(ENGLISH);
                newTableName = newTableName.toUpperCase(ENGLISH);
            }
            String sql = format(
                    "ALTER TABLE %s RENAME TO %s",
                    quoted(catalogName, schemaName, tableName),
                    quoted(catalogName, newSchemaName, newTableName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void finishInsertTable(ConnectorSession session, JdbcOutputTableHandle handle)
    {
        String temporaryTable = quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTemporaryTableName());
        String targetTable = quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName());
        String columnNames = handle.getColumnNames().stream()
                .map(this::quoted)
                .collect(joining(", "));
        String insertSql = format("INSERT INTO %s (%s) SELECT %s FROM %s", targetTable, columnNames, columnNames, temporaryTable);
        String cleanupSql = "DROP TABLE " + temporaryTable;

        try (Connection connection = getConnection(session, handle)) {
            execute(connection, insertSql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }

        try (Connection connection = getConnection(session, handle)) {
            execute(connection, cleanupSql);
        }
        catch (SQLException e) {
            log.warn(e, "Failed to cleanup temporary table: %s", temporaryTable);
        }
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            String columnName = column.getName();
            if (connection.getMetaData().storesUpperCaseIdentifiers()) {
                columnName = columnName.toUpperCase(ENGLISH);
            }
            String sql = format(
                    "ALTER TABLE %s ADD %s",
                    quoted(handle.asPlainTable().getRemoteTableName()),
                    getColumnDefinitionSql(session, column, columnName));
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
            if (connection.getMetaData().storesUpperCaseIdentifiers()) {
                newColumnName = newColumnName.toUpperCase(ENGLISH);
            }
            String sql = format(
                    "ALTER TABLE %s RENAME COLUMN %s TO %s",
                    quoted(handle.asPlainTable().getRemoteTableName()),
                    jdbcColumn.getColumnName(),
                    newColumnName);
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
        String sql = "DROP TABLE " + quoted(handle.asPlainTable().getRemoteTableName());
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

    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, metadata.getSearchStringEscape()).orElse(null),
                escapeNamePattern(tableName, metadata.getSearchStringEscape()).orElse(null),
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

    protected String toRemoteSchemaName(JdbcIdentity identity, Connection connection, String schemaName)
    {
        requireNonNull(schemaName, "schemaName is null");
        verify(CharMatcher.forPredicate(Character::isUpperCase).matchesNoneOf(schemaName), "Expected schema name from internal metadata to be lowercase: %s", schemaName);

        if (caseInsensitiveNameMatching) {
            try {
                Map<String, String> mapping = remoteSchemaNames.getIfPresent(identity);
                if (mapping != null && !mapping.containsKey(schemaName)) {
                    // This might be a schema that has just been created. Force reload.
                    mapping = null;
                }
                if (mapping == null) {
                    mapping = listSchemasByLowerCase(connection);
                    remoteSchemaNames.put(identity, mapping);
                }
                String remoteSchema = mapping.get(schemaName);
                if (remoteSchema != null) {
                    return remoteSchema;
                }
            }
            catch (RuntimeException e) {
                throw new TrinoException(JDBC_ERROR, "Failed to find remote schema name: " + firstNonNull(e.getMessage(), e), e);
            }
        }

        try {
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers()) {
                return schemaName.toUpperCase(ENGLISH);
            }
            return schemaName;
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected Map<String, String> listSchemasByLowerCase(Connection connection)
    {
        return listSchemas(connection).stream()
                // will throw on collisions to avoid ambiguity
                .collect(toImmutableMap(schemaName -> schemaName.toLowerCase(ENGLISH), schemaName -> schemaName));
    }

    protected String toRemoteTableName(JdbcIdentity identity, Connection connection, String remoteSchema, String tableName)
    {
        requireNonNull(remoteSchema, "remoteSchema is null");
        requireNonNull(tableName, "tableName is null");
        verify(CharMatcher.forPredicate(Character::isUpperCase).matchesNoneOf(tableName), "Expected table name from internal metadata to be lowercase: %s", tableName);

        if (caseInsensitiveNameMatching) {
            try {
                RemoteTableNameCacheKey cacheKey = new RemoteTableNameCacheKey(identity, remoteSchema);
                Map<String, String> mapping = remoteTableNames.getIfPresent(cacheKey);
                if (mapping != null && !mapping.containsKey(tableName)) {
                    // This might be a table that has just been created. Force reload.
                    mapping = null;
                }
                if (mapping == null) {
                    mapping = listTablesByLowerCase(connection, remoteSchema);
                    remoteTableNames.put(cacheKey, mapping);
                }
                String remoteTable = mapping.get(tableName);
                if (remoteTable != null) {
                    return remoteTable;
                }
            }
            catch (RuntimeException e) {
                throw new TrinoException(JDBC_ERROR, "Failed to find remote table name: " + firstNonNull(e.getMessage(), e), e);
            }
        }

        try {
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers()) {
                return tableName.toUpperCase(ENGLISH);
            }
            return tableName;
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected Map<String, String> listTablesByLowerCase(Connection connection, String remoteSchema)
    {
        try (ResultSet resultSet = getTables(connection, Optional.of(remoteSchema), Optional.empty())) {
            ImmutableMap.Builder<String, String> map = ImmutableMap.builder();
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                map.put(tableName.toLowerCase(ENGLISH), tableName);
            }
            // will throw on collisions to avoid ambiguity
            return map.build();
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, TupleDomain<ColumnHandle> tupleDomain)
    {
        return TableStatistics.empty();
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        execute(session, "CREATE SCHEMA " + quoted(schemaName));
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        execute(session, "DROP SCHEMA " + quoted(schemaName));
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
            log.debug("Execute: %s", query);
            statement.execute(query);
        }
        catch (SQLException e) {
            TrinoException exception = new TrinoException(JDBC_ERROR, e);
            exception.addSuppressed(new RuntimeException("Query: " + query));
            throw exception;
        }
    }

    /**
     * @deprecated Each connector should provide its own explicit type mapping, along with respective tests.
     */
    @Deprecated
    protected WriteMapping legacyToWriteMapping(@SuppressWarnings("unused") ConnectorSession session, Type type)
    {
        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "varchar";
            }
            else {
                dataType = "varchar(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (type instanceof CharType) {
            return WriteMapping.sliceMapping("char(" + ((CharType) type).getLength() + ")", charWriteFunction());
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.sliceMapping(dataType, longDecimalWriteFunction(decimalType));
        }

        WriteMapping writeMapping = WRITE_MAPPINGS.get(type);
        if (writeMapping != null) {
            return writeMapping;
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<SortItem> sortOrder)
    {
        return topNFunction().isPresent();
    }

    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.empty();
    }

    private Function<String, String> applyTopN(List<SortItem> sortOrder, long limit)
    {
        return query -> topNFunction()
                .orElseThrow()
                .apply(query, sortOrder, limit);
    }

    @Override
    public boolean isTopNLimitGuaranteed(ConnectorSession session)
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
        String apply(String query, List<SortItem> sortItems, long limit);
    }
}
