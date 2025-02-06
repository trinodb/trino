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
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.io.Closer;
import dev.failsafe.function.CheckedRunnable;
import io.airlift.log.Logger;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.base.mapping.RemoteIdentifiers;
import io.trino.plugin.jdbc.JdbcProcedureHandle.ProcedureQuery;
import io.trino.plugin.jdbc.JdbcRemoteIdentifiers.JdbcRemoteIdentifiersFactory;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.base.TemporaryTables.generateTemporaryTableName;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_INSENSITIVE;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_SENSITIVE;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.getWriteBatchSize;
import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.getWriteParallelism;
import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.isNonTransactionalInsert;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.IGNORE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Boolean.TRUE;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.sql.DatabaseMetaData.columnNoNulls;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public abstract class BaseJdbcClient
        implements JdbcClient
{
    private static final Logger log = Logger.get(BaseJdbcClient.class);

    static final Type TRINO_PAGE_SINK_ID_COLUMN_TYPE = BigintType.BIGINT;

    protected final ConnectionFactory connectionFactory;
    protected final QueryBuilder queryBuilder;
    protected final String identifierQuote;
    protected final Set<String> jdbcTypesMappedToVarchar;
    protected final RemoteQueryModifier queryModifier;
    private final IdentifierMapping identifierMapping;
    private final boolean supportsRetries;
    private final JdbcRemoteIdentifiersFactory jdbcRemoteIdentifiersFactory = new JdbcRemoteIdentifiersFactory(this);
    private Integer maxColumnNameLength;

    public BaseJdbcClient(
            String identifierQuote,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            Set<String> jdbcTypesMappedToVarchar,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier remoteQueryModifier,
            boolean supportsRetries)
    {
        this.identifierQuote = requireNonNull(identifierQuote, "identifierQuote is null");
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
        this.queryBuilder = requireNonNull(queryBuilder, "queryBuilder is null");
        this.jdbcTypesMappedToVarchar = ImmutableSortedSet.orderedBy(CASE_INSENSITIVE_ORDER)
                .addAll(requireNonNull(jdbcTypesMappedToVarchar, "jdbcTypesMappedToVarchar is null"))
                .build();
        this.identifierMapping = requireNonNull(identifierMapping, "identifierMapping is null");
        this.queryModifier = requireNonNull(remoteQueryModifier, "remoteQueryModifier is null");
        this.supportsRetries = supportsRetries;
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
                    .map(identifierMapping::fromRemoteSchemaName)
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
        return getAllTableComments(session, schema).stream()
                .map(RelationCommentMetadata::name)
                .collect(toImmutableList());
    }

    @Override
    public List<RelationCommentMetadata> getAllTableComments(ConnectorSession session, Optional<String> schema)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            ConnectorIdentity identity = session.getIdentity();
            Optional<String> remoteSchema = schema.map(schemaName -> identifierMapping.toRemoteSchemaName(getRemoteIdentifiers(connection), identity, schemaName));
            if (remoteSchema.isPresent() && !filterSchema(remoteSchema.get())) {
                return ImmutableList.of();
            }

            try (ResultSet resultSet = getTables(connection, remoteSchema, Optional.empty())) {
                ImmutableList.Builder<RelationCommentMetadata> list = ImmutableList.builder();
                while (resultSet.next()) {
                    String remoteSchemaFromResultSet = getTableSchemaName(resultSet);
                    String tableSchema = identifierMapping.fromRemoteSchemaName(remoteSchemaFromResultSet);
                    String tableName = identifierMapping.fromRemoteTableName(remoteSchemaFromResultSet, resultSet.getString("TABLE_NAME"));
                    if (filterSchema(tableSchema)) {
                        list.add(RelationCommentMetadata.forRelation(new SchemaTableName(tableSchema, tableName), getTableComment(resultSet)));
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
            RemoteIdentifiers remoteIdentifiers = getRemoteIdentifiers(connection);
            String remoteSchema = identifierMapping.toRemoteSchemaName(remoteIdentifiers, identity, schemaTableName.getSchemaName());
            String remoteTable = identifierMapping.toRemoteTableName(remoteIdentifiers, identity, remoteSchema, schemaTableName.getTableName());
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

    @Override
    public JdbcTableHandle getTableHandle(ConnectorSession session, PreparedQuery preparedQuery)
    {
        try (Connection connection = connectionFactory.openConnection(session);
                PreparedStatement preparedStatement = queryBuilder.prepareStatement(this, session, connection, preparedQuery, Optional.empty())) {
            ResultSetMetaData metadata = preparedStatement.getMetaData();
            if (metadata == null) {
                throw new TrinoException(NOT_SUPPORTED, "Query not supported: ResultSetMetaData not available for query: " + preparedQuery.query());
            }
            return new JdbcTableHandle(
                    new JdbcQueryRelationHandle(preparedQuery),
                    TupleDomain.all(),
                    ImmutableList.of(),
                    Optional.empty(),
                    OptionalLong.empty(),
                    Optional.of(getColumns(session, connection, metadata)),
                    // The query is opaque, so we don't know referenced tables
                    Optional.empty(),
                    0,
                    Optional.empty(),
                    ImmutableList.of());
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, "Failed to get table handle for prepared query. " + firstNonNull(e.getMessage(), e), e);
        }
    }

    @Override
    public JdbcProcedureHandle getProcedureHandle(ConnectorSession session, ProcedureQuery procedureQuery)
    {
        throw new TrinoException(NOT_SUPPORTED, "Procedure is not supported");
    }

    protected List<JdbcColumnHandle> getColumns(ConnectorSession session, Connection connection, ResultSetMetaData metadata)
            throws SQLException
    {
        ImmutableList.Builder<JdbcColumnHandle> columns = ImmutableList.builder();
        for (int column = 1; column <= metadata.getColumnCount(); column++) {
            // Use getColumnLabel method because query pass-through table function may contain column aliases
            String name = metadata.getColumnLabel(column);
            JdbcTypeHandle jdbcTypeHandle = new JdbcTypeHandle(
                    metadata.getColumnType(column),
                    Optional.ofNullable(metadata.getColumnTypeName(column)),
                    Optional.of(metadata.getPrecision(column)),
                    Optional.of(metadata.getScale(column)),
                    Optional.empty(), // TODO support arrays
                    Optional.of(metadata.isCaseSensitive(column) ? CASE_SENSITIVE : CASE_INSENSITIVE));
            Type type = toColumnMapping(session, connection, jdbcTypeHandle)
                    .orElseThrow(() -> new UnsupportedOperationException(format("Unsupported type: %s of column: %s", jdbcTypeHandle, name)))
                    .getType();
            columns.add(new JdbcColumnHandle(name, jdbcTypeHandle, type));
        }
        return columns.build();
    }

    @Override
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
                JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                        getInteger(resultSet, "DATA_TYPE").orElseThrow(() -> new IllegalStateException("DATA_TYPE is null")),
                        Optional.ofNullable(resultSet.getString("TYPE_NAME")),
                        getInteger(resultSet, "COLUMN_SIZE"),
                        getInteger(resultSet, "DECIMAL_DIGITS"),
                        Optional.empty(),
                        Optional.ofNullable(caseSensitivityMapping.get(columnName)));
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

    @Override
    public Iterator<RelationColumnsMetadata> getAllTableColumns(ConnectorSession session, Optional<String> schema)
    {
        Connection connection = null;
        ResultSet resultSet = null;
        try {
            connection = connectionFactory.openConnection(session);
            Connection connectionFinal = connection;
            Optional<String> remoteSchema = schema.map(name -> {
                RemoteIdentifiers remoteIdentifiers = getRemoteIdentifiers(connectionFinal);
                return identifierMapping.toRemoteSchemaName(remoteIdentifiers, session.getIdentity(), name);
            });
            if (remoteSchema.isPresent() && !filterSchema(remoteSchema.get())) {
                return emptyIterator();
            }

            // getTables filter tables by table_type. This is not possible to do when reading columns result set.
            ImmutableSet.Builder<RemoteTableName> visibleTables = ImmutableSet.builder();
            try (ResultSet tablesResultSet = getTables(connection, remoteSchema, Optional.empty())) {
                while (tablesResultSet.next()) {
                    if (filterSchema(getTableSchemaName(tablesResultSet))) {
                        visibleTables.add(getRemoteTable(tablesResultSet));
                    }
                }
            }

            resultSet = getAllTableColumns(connection, remoteSchema);
            return new IterateTableColumns(session, connection, visibleTables.build(), resultSet);
        }
        catch (RuntimeException | SQLException e) {
            if (resultSet != null) {
                ResultSet resultSetFinal = resultSet;
                Connection connectionFinal = connection;
                cleanupSuppressing(e, () -> abortReadConnection(connectionFinal, resultSetFinal));
                cleanupSuppressing(e, resultSet::close);
            }
            if (connection != null) {
                cleanupSuppressing(e, connection::close);
            }
            throwIfUnchecked(e);
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private class IterateTableColumns
            extends AbstractIterator<RelationColumnsMetadata>
    {
        private final ConnectorSession session;
        private final Connection connection;
        private final Set<RemoteTableName> visibleTables;
        private final ResultSet resultSet;

        private RemoteTableName currentTable;
        private boolean currentTableVisible;
        // Not set when current table not visible
        private SchemaTableName currentTableName;
        // Not set when current table not visible
        private ImmutableList.Builder<ColumnMetadata> currentTableColumns;

        public IterateTableColumns(ConnectorSession session, Connection connection, Set<RemoteTableName> visibleTables, ResultSet resultSet)
        {
            this.session = requireNonNull(session, "session is null");
            this.connection = requireNonNull(connection, "connection is null");
            this.visibleTables = requireNonNull(visibleTables, "visibleTables is null");
            this.resultSet = requireNonNull(resultSet, "resultSet is null");
        }

        @Override
        protected RelationColumnsMetadata computeNext()
        {
            try {
                RelationColumnsMetadata computedNext = null;
                while (computedNext == null && resultSet.next()) {
                    RemoteTableName nextTable = getRemoteTable(resultSet);
                    if (currentTable != null && !currentTable.equals(nextTable)) {
                        computedNext = finishCurrentTable().orElse(null);
                    }

                    try {
                        if (currentTable == null) {
                            currentTable = nextTable;
                            String remoteSchemaFromResultSet = getTableSchemaName(resultSet);
                            currentTableVisible = visibleTables.contains(nextTable);
                            if (currentTableVisible) {
                                currentTableName = new SchemaTableName(
                                        identifierMapping.fromRemoteSchemaName(remoteSchemaFromResultSet),
                                        identifierMapping.fromRemoteTableName(remoteSchemaFromResultSet, resultSet.getString("TABLE_NAME")));
                                currentTableColumns = ImmutableList.builder();
                            }
                        }
                        if (!currentTableVisible) {
                            continue;
                        }

                        String columnName = resultSet.getString("COLUMN_NAME");
                        JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                                getInteger(resultSet, "DATA_TYPE").orElseThrow(() -> new IllegalStateException("DATA_TYPE is null")),
                                Optional.ofNullable(resultSet.getString("TYPE_NAME")),
                                getInteger(resultSet, "COLUMN_SIZE"),
                                getInteger(resultSet, "DECIMAL_DIGITS"),
                                // arrayDimensions
                                Optional.<Integer>empty(),
                                // This code doesn't do getCaseSensitivityForColumns. However, this does not impact the ColumnMetadata returned.
                                Optional.<CaseSensitivity>empty());
                        boolean nullable = (resultSet.getInt("NULLABLE") != columnNoNulls);
                        Optional<String> comment = Optional.ofNullable(emptyToNull(resultSet.getString("REMARKS")));
                        toColumnMapping(session, connection, typeHandle).ifPresent(columnMapping -> {
                            currentTableColumns.add(ColumnMetadata.builder()
                                    .setName(columnName)
                                    .setType(columnMapping.getType())
                                    .setNullable(nullable)
                                    .setComment(comment)
                                    .build());
                        });
                    }
                    catch (RuntimeException | SQLException e) {
                        throwIfInstanceOf(e, TrinoException.class);
                        throw new RuntimeException("Failure when processing column information for table %s: %s".formatted(currentTable, firstNonNull(e.getMessage(), e)), e);
                    }
                }
                if (computedNext == null) {
                    // Last table
                    computedNext = finishCurrentTable().orElse(null);
                }
                if (computedNext == null) {
                    // We will not be called again.
                    resultSet.close();
                    connection.close();
                    return endOfData();
                }
                return computedNext;
            }
            catch (RuntimeException | SQLException e) {
                cleanupSuppressing(e, () -> abortReadConnection(connection, resultSet));
                cleanupSuppressing(e, resultSet::close);
                cleanupSuppressing(e, connection::close);
                throwIfUnchecked(e);
                throw new TrinoException(JDBC_ERROR, e);
            }
        }

        private Optional<RelationColumnsMetadata> finishCurrentTable()
        {
            if (currentTable == null) {
                return Optional.empty();
            }
            Optional<RelationColumnsMetadata> currentTableMetadata = Optional.empty();
            if (currentTableVisible) {
                List<ColumnMetadata> columnMetadata = currentTableColumns.build();
                if (!columnMetadata.isEmpty()) { // Ignore tables with no supported columns
                    currentTableMetadata = Optional.of(RelationColumnsMetadata.forTable(currentTableName, columnMetadata));
                }
            }
            currentTable = null;
            currentTableName = null;
            currentTableColumns = null;
            return currentTableMetadata;
        }
    }

    private static void cleanupSuppressing(Throwable inflight, CheckedRunnable cleanup)
    {
        try {
            cleanup.run();
        }
        catch (Throwable cleanupException) {
            if (cleanupException instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            if (inflight != cleanupException) {
                inflight.addSuppressed(cleanupException);
            }
        }
    }

    protected Map<String, CaseSensitivity> getCaseSensitivityForColumns(ConnectorSession session, Connection connection, SchemaTableName schemaTableName, RemoteTableName remoteTableName)
    {
        return ImmutableMap.of();
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

    protected ResultSet getColumns(RemoteTableName remoteTableName, DatabaseMetaData metadata)
            throws SQLException
    {
        return metadata.getColumns(
                remoteTableName.getCatalogName().orElse(null),
                escapeObjectNameForMetadataQuery(remoteTableName.getSchemaName(), metadata.getSearchStringEscape()).orElse(null),
                escapeObjectNameForMetadataQuery(remoteTableName.getTableName(), metadata.getSearchStringEscape()),
                null);
    }

    protected ResultSet getAllTableColumns(Connection connection, Optional<String> remoteSchemaName)
            throws SQLException
    {
        if (TRUE) {
            // A really compliant database would have the implementation as below.
            // However, any subclass overriding
            //  - listing tables (getTables(Connection, ...)) OR
            //  - listing table's columns (getColumns(..., DatabaseMetaData))
            // would need to override this method. So, to be on the safe side,
            // there is no default implementation for this method, and the capability remains opt-in.
            throw new TrinoException(NOT_SUPPORTED, "The requested column listing mode is not supported");
        }
        // Unreachable (see comment above). Kept for illustration purposes.
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getColumns(
                metadata.getConnection().getCatalog(),
                escapeObjectNameForMetadataQuery(remoteSchemaName, metadata.getSearchStringEscape()).orElse(null),
                null,
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
        if (typeHandle.jdbcTypeName().isPresent() && jdbcTypesMappedToVarchar.contains(typeHandle.jdbcTypeName().get())) {
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
                            "Underlying type that is mapped to VARCHAR is not supported for INSERT: " + typeHandle.jdbcTypeName().get());
                },
                DISABLE_PUSHDOWN));
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return new FixedSplitSource(new JdbcSplit(Optional.empty()));
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorSession session, JdbcProcedureHandle procedureHandle)
    {
        return new FixedSplitSource(new JdbcSplit(Optional.empty()));
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcSplit split, JdbcTableHandle tableHandle)
            throws SQLException
    {
        verify(tableHandle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s".formatted(tableHandle));
        return getConnection(session);
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcSplit split, JdbcProcedureHandle procedureHandle)
            throws SQLException
    {
        return getConnection(session);
    }

    @Override
    public Connection getConnection(ConnectorSession session)
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
            Map<String, ParameterizedExpression> columnExpressions)
    {
        verify(table.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s".formatted(table));
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
        return queryBuilder.prepareStatement(this, session, connection, preparedQuery, Optional.of(columns.size()));
    }

    @Override
    public CallableStatement buildProcedure(ConnectorSession session, Connection connection, JdbcSplit split, JdbcProcedureHandle procedureHandle)
            throws SQLException
    {
        return queryBuilder.callProcedure(this, session, connection, procedureHandle.getProcedureQuery());
    }

    protected PreparedQuery prepareQuery(
            ConnectorSession session,
            Connection connection,
            JdbcTableHandle table,
            Optional<List<List<JdbcColumnHandle>>> groupingSets,
            List<JdbcColumnHandle> columns,
            Map<String, ParameterizedExpression> columnExpressions,
            Optional<JdbcSplit> split)
    {
        return applyQueryTransformations(table, queryBuilder.prepareSelectQuery(
                this,
                session,
                connection,
                table.getRelationHandle(),
                groupingSets,
                columns,
                columnExpressions,
                table.getConstraint(),
                getAdditionalPredicate(table.getConstraintExpressions(), split.flatMap(JdbcSplit::getAdditionalPredicate))));
    }

    protected static Optional<ParameterizedExpression> getAdditionalPredicate(List<ParameterizedExpression> constraintExpressions, Optional<String> splitPredicate)
    {
        if (constraintExpressions.isEmpty() && splitPredicate.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new ParameterizedExpression(
                Stream.concat(constraintExpressions.stream().map(ParameterizedExpression::expression), splitPredicate.stream())
                        .collect(joining(") AND (", "(", ")")),
                constraintExpressions.stream()
                        .flatMap(expressionRewrite -> expressionRewrite.parameters().stream())
                        .collect(toImmutableList())));
    }

    @Override
    public Optional<PreparedQuery> implementJoin(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            Map<JdbcColumnHandle, String> leftProjections,
            PreparedQuery rightSource,
            Map<JdbcColumnHandle, String> rightProjections,
            List<ParameterizedExpression> joinConditions,
            JoinStatistics statistics)
    {
        try (Connection connection = this.connectionFactory.openConnection(session)) {
            return Optional.of(queryBuilder.prepareJoinQuery(
                    this,
                    session,
                    connection,
                    joinType,
                    leftSource,
                    leftProjections,
                    rightSource,
                    rightProjections,
                    joinConditions));
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Deprecated
    @Override
    public Optional<PreparedQuery> legacyImplementJoin(
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
            if (!isSupportedJoinCondition(session, joinCondition)) {
                return Optional.empty();
            }
        }

        try (Connection connection = this.connectionFactory.openConnection(session)) {
            return Optional.of(queryBuilder.legacyPrepareJoinQuery(
                    this,
                    session,
                    connection,
                    joinType,
                    leftSource,
                    rightSource,
                    joinConditions,
                    leftAssignments,
                    rightAssignments));
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
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
            if (shouldUseFaultTolerantExecution(session)) {
                // Create the target table
                createTable(session, tableMetadata);
                // Create the temporary table
                ColumnMetadata pageSinkIdColumn = getPageSinkIdColumn(
                        tableMetadata.getColumns().stream().map(ColumnMetadata::getName).toList());
                return createTable(session, tableMetadata, generateTemporaryTableName(session), Optional.of(pageSinkIdColumn));
            }
            else {
                return createTable(session, tableMetadata, generateTemporaryTableName(session));
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected JdbcOutputTableHandle createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, String targetTableName)
            throws SQLException
    {
        return createTable(session, tableMetadata, targetTableName, Optional.empty());
    }

    protected JdbcOutputTableHandle createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, String targetTableName, Optional<ColumnMetadata> pageSinkIdColumn)
            throws SQLException
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();

        ConnectorIdentity identity = session.getIdentity();
        if (!getSchemaNames(session).contains(schemaTableName.getSchemaName())) {
            throw new SchemaNotFoundException(schemaTableName.getSchemaName());
        }

        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            RemoteIdentifiers remoteIdentifiers = getRemoteIdentifiers(connection);
            String remoteSchema = identifierMapping.toRemoteSchemaName(remoteIdentifiers, identity, schemaTableName.getSchemaName());
            String remoteTable = identifierMapping.toRemoteTableName(remoteIdentifiers, identity, remoteSchema, schemaTableName.getTableName());
            String remoteTargetTableName = identifierMapping.toRemoteTableName(remoteIdentifiers, identity, remoteSchema, targetTableName);
            String catalog = connection.getCatalog();

            verifyTableName(connection.getMetaData(), remoteTargetTableName);

            return createTable(
                    session,
                    connection,
                    tableMetadata,
                    remoteIdentifiers,
                    catalog,
                    remoteSchema,
                    remoteTable,
                    remoteTargetTableName,
                    pageSinkIdColumn);
        }
    }

    protected JdbcOutputTableHandle createTable(
            ConnectorSession session,
            Connection connection,
            ConnectorTableMetadata tableMetadata,
            RemoteIdentifiers remoteIdentifiers,
            String catalog,
            String remoteSchema,
            String remoteTable,
            String remoteTargetTableName,
            Optional<ColumnMetadata> pageSinkIdColumn)
            throws SQLException
    {
        List<ColumnMetadata> columns = tableMetadata.getColumns();
        ImmutableList.Builder<String> columnNames = ImmutableList.builderWithExpectedSize(columns.size());
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builderWithExpectedSize(columns.size());
        // columnList is only used for createTableSql - the extraColumns are not included on the JdbcOutputTableHandle
        ImmutableList.Builder<String> columnList = ImmutableList.builderWithExpectedSize(columns.size() + (pageSinkIdColumn.isPresent() ? 1 : 0));

        for (ColumnMetadata column : columns) {
            String columnName = identifierMapping.toRemoteColumnName(remoteIdentifiers, column.getName());
            verifyColumnName(connection.getMetaData(), columnName);
            columnNames.add(columnName);
            columnTypes.add(column.getType());
            columnList.add(getColumnDefinitionSql(session, column, columnName));
        }

        Optional<String> pageSinkIdColumnName = Optional.empty();
        if (pageSinkIdColumn.isPresent()) {
            String columnName = identifierMapping.toRemoteColumnName(remoteIdentifiers, pageSinkIdColumn.get().getName());
            pageSinkIdColumnName = Optional.of(columnName);
            verifyColumnName(connection.getMetaData(), columnName);
            columnList.add(getColumnDefinitionSql(session, pageSinkIdColumn.get(), columnName));
        }

        RemoteTableName remoteTableName = new RemoteTableName(Optional.ofNullable(catalog), Optional.ofNullable(remoteSchema), remoteTargetTableName);
        for (String sql : createTableSqls(remoteTableName, columnList.build(), tableMetadata)) {
            execute(session, connection, sql);
        }

        return new JdbcOutputTableHandle(
                new RemoteTableName(Optional.ofNullable(catalog), Optional.ofNullable(remoteSchema), remoteTable),
                columnNames.build(),
                columnTypes.build(),
                Optional.empty(),
                Optional.of(remoteTargetTableName),
                pageSinkIdColumnName);
    }

    protected List<String> createTableSqls(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        if (tableMetadata.getComment().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with table comment");
        }
        checkArgument(tableMetadata.getProperties().isEmpty(), "Unsupported table properties: %s", tableMetadata.getProperties());
        return ImmutableList.of(format("CREATE TABLE %s (%s)", quoted(remoteTableName), join(", ", columns)));
    }

    protected String getColumnDefinitionSql(ConnectorSession session, ColumnMetadata column, String columnName)
    {
        if (column.getComment() != null) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with column comment");
        }
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

        verify(tableHandle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s".formatted(tableHandle));
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            RemoteIdentifiers remoteIdentifiers = getRemoteIdentifiers(connection);
            String remoteSchema = identifierMapping.toRemoteSchemaName(remoteIdentifiers, identity, schemaTableName.getSchemaName());
            String remoteTable = identifierMapping.toRemoteTableName(remoteIdentifiers, identity, remoteSchema, schemaTableName.getTableName());
            String catalog = connection.getCatalog();

            return beginInsertTable(
                    session,
                    connection,
                    remoteIdentifiers,
                    catalog,
                    remoteSchema,
                    remoteTable,
                    columns);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected JdbcOutputTableHandle beginInsertTable(
            ConnectorSession session,
            Connection connection,
            RemoteIdentifiers remoteIdentifiers,
            String catalog,
            String remoteSchema,
            String remoteTable,
            List<JdbcColumnHandle> columns)
            throws SQLException
    {
        ConnectorIdentity identity = session.getIdentity();
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
                    new RemoteTableName(Optional.ofNullable(catalog), Optional.ofNullable(remoteSchema), remoteTable),
                    columnNames.build(),
                    columnTypes.build(),
                    Optional.of(jdbcColumnTypes.build()),
                    Optional.empty(),
                    Optional.empty());
        }

        String remoteTemporaryTableName = identifierMapping.toRemoteTableName(remoteIdentifiers, identity, remoteSchema, generateTemporaryTableName(session));
        copyTableSchema(session, connection, catalog, remoteSchema, remoteTable, remoteTemporaryTableName, columnNames.build());

        Optional<ColumnMetadata> pageSinkIdColumn = Optional.empty();
        if (shouldUseFaultTolerantExecution(session)) {
            pageSinkIdColumn = Optional.of(getPageSinkIdColumn(columnNames.build()));
            addColumn(session, connection, new RemoteTableName(
                    Optional.ofNullable(catalog),
                    Optional.ofNullable(remoteSchema),
                    remoteTemporaryTableName
            ), pageSinkIdColumn.get());
        }

        return new JdbcOutputTableHandle(
                new RemoteTableName(Optional.ofNullable(catalog), Optional.ofNullable(remoteSchema), remoteTable),
                columnNames.build(),
                columnTypes.build(),
                Optional.of(jdbcColumnTypes.build()),
                Optional.of(remoteTemporaryTableName),
                pageSinkIdColumn.map(column -> identifierMapping.toRemoteColumnName(remoteIdentifiers, column.getName())));
    }

    protected void copyTableSchema(ConnectorSession session, Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        String sql = format(
                "CREATE TABLE %s AS SELECT %s FROM %s WHERE 0 = 1",
                quoted(catalogName, schemaName, newTableName),
                columnNames.stream()
                        .map(this::quoted)
                        .collect(joining(", ")),
                quoted(catalogName, schemaName, tableName));
        try {
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void commitCreateTable(ConnectorSession session, JdbcOutputTableHandle handle, Set<Long> pageSinkIds)
    {
        if (handle.getPageSinkIdColumnName().isPresent()) {
            finishInsertTable(session, handle, pageSinkIds);
        }
        else {
            renameTable(
                    session,
                    handle.getRemoteTableName().getCatalogName().orElse(null),
                    handle.getRemoteTableName().getSchemaName().orElse(null),
                    handle.getTemporaryTableName().orElseThrow(() -> new IllegalStateException("Temporary table name missing")),
                    handle.getRemoteTableName().getSchemaTableName());
        }
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        verify(handle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s".formatted(handle));
        RemoteTableName remoteTableName = handle.asPlainTable().getRemoteTableName();
        renameTable(session, remoteTableName.getCatalogName().orElse(null), remoteTableName.getSchemaName().orElse(null), remoteTableName.getTableName(), newTableName);
    }

    protected void renameTable(ConnectorSession session, String catalogName, String remoteSchemaName, String remoteTableName, SchemaTableName newTable)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            String newSchemaName = newTable.getSchemaName();
            String newTableName = newTable.getTableName();
            verifyTableName(connection.getMetaData(), newTableName);
            ConnectorIdentity identity = session.getIdentity();
            RemoteIdentifiers remoteIdentifiers = getRemoteIdentifiers(connection);
            String newRemoteSchemaName = identifierMapping.toRemoteSchemaName(remoteIdentifiers, identity, newSchemaName);
            String newRemoteTableName = identifierMapping.toRemoteTableName(remoteIdentifiers, identity, newRemoteSchemaName, newTableName);
            renameTable(session, connection, catalogName, remoteSchemaName, remoteTableName, newRemoteSchemaName, newRemoteTableName);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected void renameTable(ConnectorSession session, Connection connection, String catalogName, String remoteSchemaName, String remoteTableName, String newRemoteSchemaName, String newRemoteTableName)
            throws SQLException
    {
        execute(session, connection, format(
                "ALTER TABLE %s RENAME TO %s",
                quoted(catalogName, remoteSchemaName, remoteTableName),
                quoted(catalogName, newRemoteSchemaName, newRemoteTableName)));
    }

    private RemoteTableName constructPageSinkIdsTable(ConnectorSession session, Connection connection, JdbcOutputTableHandle handle, Set<Long> pageSinkIds, Closer closer)
            throws SQLException
    {
        verify(handle.getPageSinkIdColumnName().isPresent(), "Output table handle's pageSinkIdColumn is empty");

        RemoteTableName pageSinkTable = new RemoteTableName(
                handle.getRemoteTableName().getCatalogName(),
                handle.getRemoteTableName().getSchemaName(),
                generateTemporaryTableName(session));

        int maxBatchSize = getWriteBatchSize(session);

        String pageSinkIdColumnName = handle.getPageSinkIdColumnName().get();

        String pageSinkTableSql = format("CREATE TABLE %s (%s)",
                quoted(pageSinkTable),
                getColumnDefinitionSql(session, new ColumnMetadata(pageSinkIdColumnName, TRINO_PAGE_SINK_ID_COLUMN_TYPE), pageSinkIdColumnName));
        String pageSinkInsertSql = format("INSERT INTO %s (%s) VALUES (?)",
                quoted(pageSinkTable),
                pageSinkIdColumnName);
        pageSinkInsertSql = queryModifier.apply(session, pageSinkInsertSql);
        LongWriteFunction pageSinkIdWriter = (LongWriteFunction) toWriteMapping(session, TRINO_PAGE_SINK_ID_COLUMN_TYPE).getWriteFunction();

        execute(session, connection, pageSinkTableSql);
        closer.register(() -> dropTable(session, pageSinkTable, true));

        try (PreparedStatement statement = connection.prepareStatement(pageSinkInsertSql)) {
            int batchSize = 0;
            for (Long pageSinkId : pageSinkIds) {
                pageSinkIdWriter.set(statement, 1, pageSinkId);

                statement.addBatch();
                batchSize++;

                if (batchSize >= maxBatchSize) {
                    statement.executeBatch();
                    batchSize = 0;
                }
            }
            if (batchSize > 0) {
                statement.executeBatch();
            }
        }

        return pageSinkTable;
    }

    @Override
    public void finishInsertTable(ConnectorSession session, JdbcOutputTableHandle handle, Set<Long> pageSinkIds)
    {
        if (isNonTransactionalInsert(session)) {
            checkState(handle.getTemporaryTableName().isEmpty(), "Unexpected use of temporary table when non transactional inserts are enabled");
            return;
        }

        RemoteTableName temporaryTable = new RemoteTableName(
                handle.getRemoteTableName().getCatalogName(),
                handle.getRemoteTableName().getSchemaName(),
                handle.getTemporaryTableName().orElseThrow());

        // We conditionally create more than the one table, so keep a list of the tables that need to be dropped.
        Closer closer = Closer.create();
        closer.register(() -> dropTable(session, temporaryTable, true));

        try (Connection connection = getConnection(session, handle)) {
            verify(connection.getAutoCommit());
            String columns = handle.getColumnNames().stream()
                    .map(this::quoted)
                    .collect(joining(", "));

            String insertSql = format("INSERT INTO %s (%s) SELECT %s FROM %s temp_table",
                    postProcessInsertTableNameClause(session, quoted(handle.getRemoteTableName())),
                    columns,
                    columns,
                    quoted(temporaryTable));

            if (handle.getPageSinkIdColumnName().isPresent()) {
                RemoteTableName pageSinkTable = constructPageSinkIdsTable(session, connection, handle, pageSinkIds, closer);

                insertSql += format(" WHERE EXISTS (SELECT 1 FROM %s page_sink_table WHERE page_sink_table.%s = temp_table.%s)",
                        quoted(pageSinkTable),
                        handle.getPageSinkIdColumnName().get(),
                        handle.getPageSinkIdColumnName().get());
            }

            execute(session, connection, insertSql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
        finally {
            try {
                closer.close();
            }
            catch (IOException e) {
                throw new TrinoException(JDBC_ERROR, e);
            }
        }
    }

    protected String postProcessInsertTableNameClause(ConnectorSession session, String tableName)
    {
        return tableName;
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column, ColumnPosition position)
    {
        verify(handle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s".formatted(handle));

        switch (position) {
            case ColumnPosition.First _ -> throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns with FIRST clause");
            case ColumnPosition.After _ -> throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns with AFTER clause");
            case ColumnPosition.Last _ -> addColumn(session, handle.asPlainTable().getRemoteTableName(), column);
        }
    }

    private void addColumn(ConnectorSession session, RemoteTableName table, ColumnMetadata column)
    {
        if (column.getComment() != null) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns with comments");
        }

        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            addColumn(session, connection, table, column);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected void addColumn(ConnectorSession session, Connection connection, RemoteTableName table, ColumnMetadata column)
            throws SQLException
    {
        String columnName = column.getName();
        verifyColumnName(connection.getMetaData(), columnName);
        String remoteColumnName = identifierMapping.toRemoteColumnName(getRemoteIdentifiers(connection), columnName);
        String sql = format(
                "ALTER TABLE %s ADD %s",
                quoted(table),
                getColumnDefinitionSql(session, column, remoteColumnName));
        execute(session, connection, sql);
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        verify(handle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s".formatted(handle));
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            String newRemoteColumnName = identifierMapping.toRemoteColumnName(getRemoteIdentifiers(connection), newColumnName);
            verifyColumnName(connection.getMetaData(), newRemoteColumnName);
            renameColumn(session, connection, handle.asPlainTable().getRemoteTableName(), jdbcColumn.getColumnName(), newRemoteColumnName);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected void renameColumn(ConnectorSession session, Connection connection, RemoteTableName remoteTableName, String remoteColumnName, String newRemoteColumnName)
            throws SQLException
    {
        execute(session, connection, format(
                "ALTER TABLE %s RENAME COLUMN %s TO %s",
                quoted(remoteTableName),
                quoted(remoteColumnName),
                quoted(newRemoteColumnName)));
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        verify(handle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s".formatted(handle));
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            String remoteColumnName = identifierMapping.toRemoteColumnName(getRemoteIdentifiers(connection), column.getColumnName());
            String sql = format(
                    "ALTER TABLE %s DROP COLUMN %s",
                    quoted(handle.asPlainTable().getRemoteTableName()),
                    quoted(remoteColumnName));
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            String remoteColumnName = identifierMapping.toRemoteColumnName(getRemoteIdentifiers(connection), column.getColumnName());
            String sql = format(
                    "ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s",
                    quoted(handle.asPlainTable().getRemoteTableName()),
                    quoted(remoteColumnName),
                    toWriteMapping(session, type).getDataType());
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            String remoteColumnName = identifierMapping.toRemoteColumnName(getRemoteIdentifiers(connection), column.getColumnName());
            String sql = format(
                    "ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL",
                    quoted(handle.asPlainTable().getRemoteTableName()),
                    quoted(remoteColumnName));
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void dropTable(ConnectorSession session, JdbcTableHandle handle)
    {
        verify(handle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s".formatted(handle));
        dropTable(session, handle.asPlainTable().getRemoteTableName(), false);
    }

    protected void dropTable(ConnectorSession session, RemoteTableName remoteTableName, boolean temporaryTable)
    {
        String sql = "DROP TABLE " + quoted(remoteTableName);
        execute(session, sql);
    }

    @Override
    public void rollbackCreateTable(ConnectorSession session, JdbcOutputTableHandle handle)
    {
        if (handle.getTemporaryTableName().isPresent()) {
            dropTable(session,
                    new RemoteTableName(
                            handle.getRemoteTableName().getCatalogName(),
                            handle.getRemoteTableName().getSchemaName(),
                            handle.getTemporaryTableName().get()),
                    true);
        }
    }

    @Override
    public boolean supportsRetries()
    {
        return supportsRetries;
    }

    private boolean shouldUseFaultTolerantExecution(ConnectorSession session)
    {
        return supportsRetries() && !isNonTransactionalInsert(session);
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle, List<WriteFunction> columnWriters)
    {
        boolean hasPageSinkIdColumn = handle.getPageSinkIdColumnName().isPresent();
        checkArgument(handle.getColumnNames().size() == columnWriters.size(), "handle and columnWriters mismatch: %s, %s", handle, columnWriters);
        return format(
                "INSERT INTO %s (%s%s) VALUES (%s%s)",
                quoted(
                        handle.getRemoteTableName().getCatalogName().orElse(null),
                        handle.getRemoteTableName().getSchemaName().orElse(null),
                        handle.getTemporaryTableName().orElseGet(() -> handle.getRemoteTableName().getTableName())),
                handle.getColumnNames().stream()
                        .map(this::quoted)
                        .collect(joining(", ")),
                hasPageSinkIdColumn ? ", " + quoted(handle.getPageSinkIdColumnName().get()) : "",
                columnWriters.stream()
                        .map(WriteFunction::getBindExpression)
                        .collect(joining(",")),
                hasPageSinkIdColumn ? ", ?" : "");
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcOutputTableHandle handle)
            throws SQLException
    {
        return connectionFactory.openConnection(session);
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql, Optional<Integer> columnCount)
            throws SQLException
    {
        return connection.prepareStatement(sql);
    }

    public ResultSet getTables(Connection connection, Optional<String> remoteSchemaName, Optional<String> remoteTableName)
            throws SQLException
    {
        // this method is called by IdentifierMapping, so cannot use IdentifierMapping here as this would cause an endless loop
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getTables(
                connection.getCatalog(),
                escapeObjectNameForMetadataQuery(remoteSchemaName, metadata.getSearchStringEscape()).orElse(null),
                escapeObjectNameForMetadataQuery(remoteTableName, metadata.getSearchStringEscape()).orElse(null),
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
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle)
    {
        verify(handle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s".formatted(handle));
        return TableStatistics.empty();
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        ConnectorIdentity identity = session.getIdentity();
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            schemaName = identifierMapping.toRemoteSchemaName(getRemoteIdentifiers(connection), identity, schemaName);
            verifySchemaName(connection.getMetaData(), schemaName);
            createSchema(session, connection, schemaName);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected void createSchema(ConnectorSession session, Connection connection, String remoteSchemaName)
            throws SQLException
    {
        execute(session, connection, "CREATE SCHEMA " + quoted(remoteSchemaName));
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        ConnectorIdentity identity = session.getIdentity();
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            schemaName = identifierMapping.toRemoteSchemaName(getRemoteIdentifiers(connection), identity, schemaName);
            dropSchema(session, connection, schemaName, cascade);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected void dropSchema(ConnectorSession session, Connection connection, String remoteSchemaName, boolean cascade)
            throws SQLException
    {
        String dropSchema = "DROP SCHEMA " + quoted(remoteSchemaName);
        if (cascade) {
            dropSchema += " CASCADE";
        }
        execute(session, connection, dropSchema);
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        ConnectorIdentity identity = session.getIdentity();
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            RemoteIdentifiers remoteIdentifiers = getRemoteIdentifiers(connection);
            String remoteSchemaName = identifierMapping.toRemoteSchemaName(remoteIdentifiers, identity, schemaName);
            String newRemoteSchemaName = identifierMapping.toRemoteSchemaName(remoteIdentifiers, identity, newSchemaName);
            verifySchemaName(connection.getMetaData(), newRemoteSchemaName);
            renameSchema(session, connection, remoteSchemaName, newRemoteSchemaName);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected void renameSchema(ConnectorSession session, Connection connection, String remoteSchemaName, String newRemoteSchemaName)
            throws SQLException
    {
        execute(session, connection, "ALTER SCHEMA " + quoted(remoteSchemaName) + " RENAME TO " + quoted(newRemoteSchemaName));
    }

    protected void execute(ConnectorSession session, String query)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            execute(session, connection, query);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected void execute(ConnectorSession session, Connection connection, String query)
            throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            String modifiedQuery = queryModifier.apply(session, query);
            log.debug("Execute: %s", modifiedQuery);
            statement.execute(modifiedQuery);
        }
        catch (SQLException e) {
            e.addSuppressed(new RuntimeException("Query: " + query));
            throw e;
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
    public boolean supportsMerge()
    {
        return false;
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
        verify(tableHandle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s".formatted(tableHandle));
        return emptyMap();
    }

    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        checkArgument(handle.isNamedRelation(), "Unable to delete from synthetic table: %s", handle);
        checkArgument(handle.getLimit().isEmpty(), "Unable to delete when limit is set: %s", handle);
        checkArgument(handle.getSortOrder().isEmpty(), "Unable to delete when sort order is set: %s", handle);
        checkArgument(handle.getUpdateAssignments().isEmpty(), "Unable to delete when update assignments are set: %s", handle);
        verify(handle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s".formatted(handle));
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            PreparedQuery preparedQuery = queryBuilder.prepareDeleteQuery(
                    this,
                    session,
                    connection,
                    handle.getRequiredNamedRelation(),
                    handle.getConstraint(),
                    getAdditionalPredicate(handle.getConstraintExpressions(), Optional.empty()));
            try (PreparedStatement preparedStatement = queryBuilder.prepareStatement(this, session, connection, preparedQuery, Optional.empty())) {
                return OptionalLong.of(preparedStatement.executeUpdate());
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public OptionalLong update(ConnectorSession session, JdbcTableHandle handle)
    {
        checkArgument(handle.isNamedRelation(), "Unable to update from synthetic table: %s", handle);
        checkArgument(handle.getLimit().isEmpty(), "Unable to update when limit is set: %s", handle);
        checkArgument(handle.getSortOrder().isEmpty(), "Unable to update when sort order is set: %s", handle);
        checkArgument(!handle.getUpdateAssignments().isEmpty(), "Unable to update when update assignments are not set: %s", handle);
        verify(handle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s".formatted(handle));
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            PreparedQuery preparedQuery = queryBuilder.prepareUpdateQuery(
                    this,
                    session,
                    connection,
                    handle.getRequiredNamedRelation(),
                    handle.getConstraint(),
                    getAdditionalPredicate(handle.getConstraintExpressions(), Optional.empty()),
                    handle.getUpdateAssignments());
            try (PreparedStatement preparedStatement = queryBuilder.prepareStatement(this, session, connection, preparedQuery, Optional.empty())) {
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
        verify(handle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s".formatted(handle));
        String sql = "TRUNCATE TABLE " + quoted(handle.asPlainTable().getRemoteTableName());
        execute(session, sql);
    }

    @Override
    public OptionalInt getMaxWriteParallelism(ConnectorSession session)
    {
        return OptionalInt.of(getWriteParallelism(session));
    }

    protected OptionalInt getMaxColumnNameLengthFromDatabaseMetaData(ConnectorSession session)
    {
        if (maxColumnNameLength != null) {
            // According to JavaDoc of DatabaseMetaData#getMaxColumnNameLength a value of 0 signifies that the limit is unknown
            if (maxColumnNameLength == 0) {
                return OptionalInt.empty();
            }

            return OptionalInt.of(maxColumnNameLength);
        }

        try (Connection connection = connectionFactory.openConnection(session)) {
            maxColumnNameLength = connection.getMetaData().getMaxColumnNameLength();
            return OptionalInt.of(maxColumnNameLength);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    protected void verifySchemaName(DatabaseMetaData databaseMetadata, String schemaName)
            throws SQLException
    {
        // expect remote databases throw an exception for unsupported schema names
    }

    protected void verifyTableName(DatabaseMetaData databaseMetadata, String tableName)
            throws SQLException
    {
        // expect remote databases throw an exception for unsupported table names
    }

    protected void verifyColumnName(DatabaseMetaData databaseMetadata, String columnName)
            throws SQLException
    {
        // expect remote databases throw an exception for unsupported column names
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

    public static String varcharLiteral(String value)
    {
        requireNonNull(value, "value is null");
        return "'" + value.replace("'", "''") + "'";
    }

    protected Optional<String> escapeObjectNameForMetadataQuery(Optional<String> name, String escape)
    {
        return name.map(string -> escapeObjectNameForMetadataQuery(string, escape));
    }

    protected String escapeObjectNameForMetadataQuery(String name, String escape)
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
                            String ordering = sortItem.sortOrder().isAscending() ? "ASC" : "DESC";
                            String nullsHandling = sortItem.sortOrder().isNullsFirst() ? "NULLS FIRST" : "NULLS LAST";
                            return format("%s %s %s", quote.apply(sortItem.column().getColumnName()), ordering, nullsHandling);
                        })
                        .collect(joining(", "));

                return format("%s ORDER BY %s OFFSET 0 ROWS FETCH NEXT %s ROWS ONLY", query, orderBy, limit);
            };
        }
    }

    private static ColumnMetadata getPageSinkIdColumn(List<String> otherColumnNames)
    {
        // While it's unlikely this column name will collide with client table columns,
        // guarantee it will not by appending a deterministic suffix to it.
        String baseColumnName = "trino_page_sink_id";
        String columnName = baseColumnName;
        int suffix = 1;
        while (otherColumnNames.contains(columnName)) {
            columnName = baseColumnName + "_" + suffix;
            suffix++;
        }
        return new ColumnMetadata(columnName, TRINO_PAGE_SINK_ID_COLUMN_TYPE);
    }

    public RemoteIdentifiers getRemoteIdentifiers(Connection connection)
    {
        return jdbcRemoteIdentifiersFactory.createJdbcRemoteIdentifies(connection);
    }
}
