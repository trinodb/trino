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
package io.trino.plugin.cassandra;

import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.plugin.cassandra.ptf.Query.QueryHandle;
import io.trino.plugin.cassandra.util.CassandraCqlUtils;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.NotFoundException;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.ptf.ConnectorTableFunctionHandle;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.Type;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.truncate;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.toOptional;
import static io.trino.plugin.cassandra.util.CassandraCqlUtils.ID_COLUMN_NAME;
import static io.trino.plugin.cassandra.util.CassandraCqlUtils.cqlNameToSqlName;
import static io.trino.plugin.cassandra.util.CassandraCqlUtils.quoteStringLiteral;
import static io.trino.plugin.cassandra.util.CassandraCqlUtils.validColumnName;
import static io.trino.plugin.cassandra.util.CassandraCqlUtils.validSchemaName;
import static io.trino.plugin.cassandra.util.CassandraCqlUtils.validTableName;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class CassandraMetadata
        implements ConnectorMetadata
{
    public static final String PRESTO_COMMENT_METADATA = "Presto Metadata:";

    private final CassandraSession cassandraSession;
    private final CassandraPartitionManager partitionManager;
    private final boolean allowDropTable;

    private final JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec;
    private final CassandraTypeManager cassandraTypeManager;

    @Inject
    public CassandraMetadata(
            CassandraSession cassandraSession,
            CassandraPartitionManager partitionManager,
            JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec,
            CassandraTypeManager cassandraTypeManager,
            CassandraClientConfig config)
    {
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.cassandraSession = requireNonNull(cassandraSession, "cassandraSession is null");
        this.cassandraTypeManager = requireNonNull(cassandraTypeManager, "cassandraTypeManager is null");
        this.allowDropTable = config.getAllowDropTable();
        this.extraColumnMetadataCodec = requireNonNull(extraColumnMetadataCodec, "extraColumnMetadataCodec is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return cassandraSession.getCaseSensitiveSchemaNames().stream()
                .map(name -> name.toLowerCase(ENGLISH))
                .collect(toImmutableList());
    }

    @Override
    public CassandraTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        try {
            return new CassandraTableHandle(cassandraSession.getTable(tableName).getTableHandle());
        }
        catch (TableNotFoundException | SchemaNotFoundException e) {
            // table was not found
            return null;
        }
    }

    private static SchemaTableName getTableName(ConnectorTableHandle tableHandle)
    {
        return ((CassandraTableHandle) tableHandle).getRequiredNamedRelation().getSchemaTableName();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        CassandraTableHandle handle = (CassandraTableHandle) tableHandle;
        if (handle.getRelationHandle() instanceof CassandraQueryRelationHandle queryRelationHandle) {
            List<ColumnMetadata> columns = getColumnHandles(queryRelationHandle.getQuery()).stream()
                    .map(CassandraColumnHandle.class::cast)
                    .map(CassandraColumnHandle::getColumnMetadata)
                    .collect(toImmutableList());
            return new ConnectorTableMetadata(getSchemaTableName(handle), columns);
        }
        return getTableMetadata(getTableName(tableHandle));
    }

    private static SchemaTableName getSchemaTableName(CassandraTableHandle handle)
    {
        return handle.isNamedRelation()
                ? handle.getRequiredNamedRelation().getSchemaTableName()
                // TODO (https://github.com/trinodb/trino/issues/6694) SchemaTableName should not be required for synthetic ConnectorTableHandle
                : new SchemaTableName("_generated", "_generated_query");
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        CassandraTable table = cassandraSession.getTable(tableName);
        List<ColumnMetadata> columns = table.getColumns().stream()
                .map(CassandraColumnHandle::getColumnMetadata)
                .collect(toList());
        return new ConnectorTableMetadata(tableName, columns);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName1)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        List<String> schemaNames = listSchemas(session, schemaName1);
        for (String schemaName : schemaNames) {
            try {
                for (String tableName : cassandraSession.getCaseSensitiveTableNames(schemaName)) {
                    tableNames.add(new SchemaTableName(schemaName, tableName.toLowerCase(ENGLISH)));
                }
            }
            catch (SchemaNotFoundException e) {
                // schema disappeared during listing operation
            }
        }
        return tableNames.build();
    }

    private List<String> listSchemas(ConnectorSession session, Optional<String> schemaName)
    {
        return schemaName.map(ImmutableList::of)
                .orElseGet(() -> (ImmutableList<String>) listSchemaNames(session));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(session, "session is null");
        requireNonNull(tableHandle, "tableHandle is null");
        CassandraTable table = cassandraSession.getTable(getTableName(tableHandle));
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (CassandraColumnHandle columnHandle : table.getColumns()) {
            columnHandles.put(cqlNameToSqlName(columnHandle.getName()).toLowerCase(ENGLISH), columnHandle);
        }
        return columnHandles.buildOrThrow();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            try {
                columns.put(tableName, getTableMetadata(tableName).getColumns());
            }
            catch (NotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.buildOrThrow();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        return ImmutableList.of(prefix.toSchemaTableName());
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((CassandraColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle connectorTableHandle, Constraint constraint)
    {
        CassandraTableHandle tableHandle = (CassandraTableHandle) connectorTableHandle;
        if (tableHandle.isSynthetic()) {
            // filter pushdown currently not supported for passthrough query
            return Optional.empty();
        }
        CassandraNamedRelationHandle handle = tableHandle.getRequiredNamedRelation();
        if (handle.getPartitions().isPresent() || !handle.getClusteringKeyPredicates().isEmpty()) {
            // TODO support repeated applyFilter
            return Optional.empty();
        }

        CassandraPartitionResult partitionResult = partitionManager.getPartitions(handle, constraint.getSummary());

        String clusteringKeyPredicates = "";
        TupleDomain<ColumnHandle> unenforcedConstraint;
        if (partitionResult.isUnpartitioned()) {
            unenforcedConstraint = partitionResult.getUnenforcedConstraint();
        }
        else {
            CassandraClusteringPredicatesExtractor clusteringPredicatesExtractor = new CassandraClusteringPredicatesExtractor(
                    cassandraTypeManager,
                    cassandraSession.getTable(handle.getSchemaTableName()).getClusteringKeyColumns(),
                    partitionResult.getUnenforcedConstraint(),
                    cassandraSession.getCassandraVersion());
            clusteringKeyPredicates = clusteringPredicatesExtractor.getClusteringKeyPredicates();
            unenforcedConstraint = clusteringPredicatesExtractor.getUnenforcedConstraints();
        }

        Optional<List<CassandraPartition>> currentPartitions = handle.getPartitions();
        if (currentPartitions.isPresent() &&
                // TODO: we should skip only when new table handle does not narrow down enforced predicate
                currentPartitions.get().containsAll(partitionResult.getPartitions()) &&
                handle.getClusteringKeyPredicates().equals(clusteringKeyPredicates)) {
            return Optional.empty();
        }

        return Optional.of(
                new ConstraintApplicationResult<>(new CassandraTableHandle(new CassandraNamedRelationHandle(
                        handle.getSchemaName(),
                        handle.getTableName(),
                        Optional.of(partitionResult.getPartitions()),
                        // TODO this should probably be AND-ed with handle.getClusteringKeyPredicates()
                        clusteringKeyPredicates)),
                        unenforcedConstraint,
                        false));
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        createTable(tableMetadata);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        checkArgument(tableHandle instanceof CassandraTableHandle, "tableHandle is not an instance of CassandraTableHandle");

        if (!allowDropTable) {
            throw new TrinoException(PERMISSION_DENIED, "DROP TABLE is disabled in this Cassandra catalog");
        }

        CassandraNamedRelationHandle cassandraTableHandle = ((CassandraTableHandle) tableHandle).getRequiredNamedRelation();
        if (cassandraSession.isMaterializedView(cassandraTableHandle.getSchemaTableName())) {
            throw new TrinoException(NOT_SUPPORTED, "Dropping materialized views not yet supported");
        }

        cassandraSession.execute(format("DROP TABLE \"%s\".\"%s\"", cassandraTableHandle.getSchemaName(), cassandraTableHandle.getTableName()));
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode)
    {
        if (retryMode != NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }

        return createTable(tableMetadata);
    }

    private CassandraOutputTableHandle createTable(ConnectorTableMetadata tableMetadata)
    {
        if (tableMetadata.getComment().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with table comment");
        }
        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        ImmutableList.Builder<ExtraColumnMetadata> columnExtra = ImmutableList.builder();
        columnExtra.add(new ExtraColumnMetadata(ID_COLUMN_NAME, true));
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            if (column.getComment() != null) {
                throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with column comment");
            }
            columnNames.add(column.getName());
            columnTypes.add(column.getType());
            columnExtra.add(new ExtraColumnMetadata(column.getName(), column.isHidden()));
        }

        SchemaTableName table = tableMetadata.getTable();
        String schemaName = cassandraSession.getCaseSensitiveSchemaName(table.getSchemaName());
        String tableName = table.getTableName();
        List<String> columns = columnNames.build();
        List<Type> types = columnTypes.build();
        StringBuilder queryBuilder = new StringBuilder(format("CREATE TABLE \"%s\".\"%s\"(%s uuid primary key", schemaName, tableName, ID_COLUMN_NAME));
        for (int i = 0; i < columns.size(); i++) {
            String name = columns.get(i);
            Type type = types.get(i);
            queryBuilder.append(", ")
                    .append(validColumnName(name))
                    .append(" ")
                    .append(cassandraTypeManager.toCassandraType(type, cassandraSession.getProtocolVersion()).getName().toLowerCase(ENGLISH));
        }
        queryBuilder.append(") ");

        // encode column ordering in the cassandra table comment field since there is no better place to store this
        String columnMetadata = extraColumnMetadataCodec.toJson(columnExtra.build());
        queryBuilder.append("WITH comment=").append(quoteStringLiteral(PRESTO_COMMENT_METADATA + " " + columnMetadata));

        // We need to create the Cassandra table before commit because the record needs to be written to the table.
        cassandraSession.execute(queryBuilder.toString());
        return new CassandraOutputTableHandle(
                schemaName,
                tableName,
                columnNames.build(),
                columnTypes.build());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        CassandraNamedRelationHandle table = ((CassandraTableHandle) tableHandle).getRequiredNamedRelation();
        cassandraSession.execute(truncate(validSchemaName(table.getSchemaName()), validTableName(table.getTableName())).build());
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> insertedColumns, RetryMode retryMode)
    {
        if (retryMode != NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }

        CassandraNamedRelationHandle table = ((CassandraTableHandle) tableHandle).getRequiredNamedRelation();
        if (cassandraSession.isMaterializedView(table.getSchemaTableName())) {
            throw new TrinoException(NOT_SUPPORTED, "Inserting into materialized views not yet supported");
        }

        SchemaTableName schemaTableName = new SchemaTableName(table.getSchemaName(), table.getTableName());
        List<CassandraColumnHandle> columns = cassandraSession.getTable(schemaTableName).getColumns();
        List<String> columnNames = columns.stream()
                .filter(columnHandle -> !isHiddenIdColumn(columnHandle))
                .map(CassandraColumnHandle::getName)
                .collect(Collectors.toList());
        List<Type> columnTypes = columns.stream()
                .filter(columnHandle -> !isHiddenIdColumn(columnHandle))
                .map(CassandraColumnHandle::getType)
                .collect(Collectors.toList());
        boolean generateUuid = columns.stream()
                .filter(CassandraMetadata::isHiddenIdColumn)
                .collect(toOptional()) // must be at most one
                .isPresent();

        return new CassandraInsertTableHandle(
                table.getSchemaName(),
                table.getTableName(),
                columnNames,
                columnTypes,
                generateUuid);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    private static boolean isHiddenIdColumn(CassandraColumnHandle columnHandle)
    {
        return columnHandle.isHidden() && ID_COLUMN_NAME.equals(columnHandle.getName());
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle, RetryMode retryMode)
    {
        throw new TrinoException(NOT_SUPPORTED, "Delete without primary key or partition key is not supported");
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return new CassandraColumnHandle("$update_row_id", 0, CassandraTypes.TEXT, false, false, false, true);
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return Optional.of(handle);
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle deleteHandle)
    {
        CassandraNamedRelationHandle handle = ((CassandraTableHandle) deleteHandle).getRequiredNamedRelation();
        List<CassandraPartition> partitions = handle.getPartitions()
                .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Deleting without partition key is not supported"));
        if (partitions.isEmpty()) {
            // there are no records of a given partition key
            return OptionalLong.empty();
        }
        for (String cql : CassandraCqlUtils.getDeleteQueries(handle)) {
            cassandraSession.execute(cql);
        }
        return OptionalLong.empty();
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
    {
        if (!(handle instanceof QueryHandle queryHandle)) {
            return Optional.empty();
        }

        CassandraTableHandle tableHandle = queryHandle.getTableHandle();
        List<ColumnHandle> columnHandles = getColumnHandles(((CassandraQueryRelationHandle) tableHandle.getRelationHandle()).getQuery());
        return Optional.of(new TableFunctionApplicationResult<>(tableHandle, columnHandles));
    }

    public List<ColumnHandle> getColumnHandles(String query)
    {
        PreparedStatement statement = cassandraSession.prepare(SimpleStatement.newInstance(query));
        int position = 0;
        ImmutableList.Builder<ColumnHandle> columnsBuilder = ImmutableList.builderWithExpectedSize(statement.getResultSetDefinitions().size());
        for (ColumnDefinition column : statement.getResultSetDefinitions()) {
            CassandraType cassandraType = cassandraTypeManager.toCassandraType(column.getType())
                    .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Unsupported type: " + column.getType()));
            columnsBuilder.add(new CassandraColumnHandle(column.getName().asInternal(), position++, cassandraType, false, false, false, false));
        }
        return columnsBuilder.build();
    }
}
