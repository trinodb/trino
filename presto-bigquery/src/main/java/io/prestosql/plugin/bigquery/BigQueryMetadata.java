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
package io.prestosql.plugin.bigquery;

import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.airlift.log.Logger;
import io.prestosql.spi.connector.Assignment;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.LimitApplicationResult;
import io.prestosql.spi.connector.NotFoundException;
import io.prestosql.spi.connector.ProjectionApplicationResult;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.predicate.TupleDomain;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.cloud.bigquery.TableDefinition.Type.TABLE;
import static com.google.cloud.bigquery.TableDefinition.Type.VIEW;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class BigQueryMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(BigQueryMetadata.class);

    static final int NUMERIC_DATA_TYPE_PRECISION = 38;
    static final int NUMERIC_DATA_TYPE_SCALE = 9;
    static final String INFORMATION_SCHEMA = "information_schema";

    private final BigQueryClient bigQueryClient;
    private final String projectId;

    @Inject
    public BigQueryMetadata(BigQueryClient bigQueryClient, BigQueryConfig config)
    {
        this.bigQueryClient = requireNonNull(bigQueryClient, "bigQueryClient is null");
        this.projectId = requireNonNull(config, "config is null").getProjectId().orElse(bigQueryClient.getProjectId());
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        log.debug("listSchemaNames(session=%s)", session);
        return Streams.stream(bigQueryClient.listDatasets(projectId))
                .map(dataset -> dataset.getDatasetId().getDataset())
                .filter(schemaName -> !schemaName.equalsIgnoreCase(INFORMATION_SCHEMA))
                .collect(toImmutableList());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        log.debug("listTables(session=%s, schemaName=%s)", session, schemaName);
        return listTablesWithTypes(session, schemaName, TABLE);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        log.debug("listViews(session=%s, schemaName=%s)", session, schemaName);
        return listTablesWithTypes(session, schemaName, VIEW);
    }

    private List<SchemaTableName> listTablesWithTypes(ConnectorSession session, Optional<String> schemaName, TableDefinition.Type... types)
    {
        if (schemaName.isPresent() && schemaName.get().equalsIgnoreCase(INFORMATION_SCHEMA)) {
            return ImmutableList.of();
        }
        Set<String> schemaNames = schemaName.map(ImmutableSet::of)
                .orElseGet(() -> ImmutableSet.copyOf(listSchemaNames(session)));

        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String datasetId : schemaNames) {
            for (Table table : bigQueryClient.listTables(DatasetId.of(projectId, datasetId), types)) {
                tableNames.add(new SchemaTableName(datasetId, table.getTableId().getTable()));
            }
        }
        return tableNames.build();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        log.debug("getTableHandle(session=%s, tableName=%s)", session, tableName);
        TableInfo tableInfo = getBigQueryTable(tableName);
        if (tableInfo == null) {
            log.debug("Table [%s.%s] was not found", tableName.getSchemaName(), tableName.getTableName());
            return null;
        }
        return BigQueryTableHandle.from(tableInfo);
    }

    // May return null
    private TableInfo getBigQueryTable(SchemaTableName tableName)
    {
        return bigQueryClient.getTable(TableId.of(projectId, tableName.getSchemaName(), tableName.getTableName()));
    }

    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName schemaTableName)
    {
        ConnectorTableHandle table = getTableHandle(session, schemaTableName);
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }
        return getTableMetadata(session, table);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        log.debug("getTableMetadata(session=%s, tableHandle=%s)", session, tableHandle);
        TableInfo table = bigQueryClient.getTable(((BigQueryTableHandle) tableHandle).getTableId());
        SchemaTableName schemaTableName = new SchemaTableName(table.getTableId().getDataset(), table.getTableId().getTable());
        Schema schema = table.getDefinition().getSchema();
        List<ColumnMetadata> columns = schema == null ?
                ImmutableList.of() :
                schema.getFields().stream()
                        .map(Conversions::toColumnMetadata)
                        .collect(toImmutableList());
        return new ConnectorTableMetadata(schemaTableName, columns);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        log.debug("getColumnHandles(session=%s, tableHandle=%s)", session, tableHandle);
        List<BigQueryColumnHandle> columnHandles = getTableColumns(((BigQueryTableHandle) tableHandle).getTableId());
        return columnHandles.stream().collect(toMap(BigQueryColumnHandle::getName, identity()));
    }

    List<BigQueryColumnHandle> getTableColumns(TableId tableId)
    {
        return getTableColumns(bigQueryClient.getTable(tableId));
    }

    private List<BigQueryColumnHandle> getTableColumns(TableInfo table)
    {
        ImmutableList.Builder<BigQueryColumnHandle> columns = ImmutableList.builder();
        TableDefinition tableDefinition = table.getDefinition();
        Schema schema = tableDefinition.getSchema();
        if (schema != null) {
            for (Field field : schema.getFields()) {
                columns.add(Conversions.toColumnHandle(field));
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        log.debug("getColumnMetadata(session=%s, tableHandle=%s, columnHandle=%s)", session, columnHandle, columnHandle);
        return ((BigQueryColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        log.debug("listTableColumns(session=%s, prefix=%s)", session, prefix);
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            try {
                columns.put(tableName, getTableMetadata(session, tableName).getColumns());
            }
            catch (NotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        SchemaTableName tableName = prefix.toSchemaTableName();
        TableInfo tableInfo = getBigQueryTable(tableName);
        return tableInfo == null ?
                ImmutableList.of() : // table does not exist
                ImmutableList.of(tableName);
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        log.debug("getTableProperties(session=%s, prefix=%s)", session, table);
        return new ConnectorTableProperties();
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session,
            ConnectorTableHandle handle,
            long limit)
    {
        log.debug("applyLimit(session=%s, handle=%s, limit=%s)", session, handle, limit);
        BigQueryTableHandle bigQueryTableHandle = (BigQueryTableHandle) handle;

        if (bigQueryTableHandle.getLimit().isPresent() && bigQueryTableHandle.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }

        bigQueryTableHandle = bigQueryTableHandle.withLimit(limit);

        return Optional.of(new LimitApplicationResult<>(bigQueryTableHandle, false));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        log.debug("applyProjection(session=%s, handle=%s, projections=%s, assignments=%s)",
                session, handle, projections, assignments);
        BigQueryTableHandle bigQueryTableHandle = (BigQueryTableHandle) handle;

        if (bigQueryTableHandle.getProjectedColumns().isPresent()) {
            return Optional.empty();
        }

        ImmutableList.Builder<ColumnHandle> projectedColumns = ImmutableList.builder();
        ImmutableList.Builder<Assignment> assignmentList = ImmutableList.builder();
        assignments.forEach((name, column) -> {
            projectedColumns.add(column);
            assignmentList.add(new Assignment(name, column, ((BigQueryColumnHandle) column).getPrestoType()));
        });

        bigQueryTableHandle = bigQueryTableHandle.withProjectedColumns(projectedColumns.build());

        return Optional.of(new ProjectionApplicationResult<>(bigQueryTableHandle, projections, assignmentList.build()));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle handle,
            Constraint constraint)
    {
        log.debug("applyFilter(session=%s, handle=%s, summary=%s, predicate=%s, columns=%s)",
                session, handle, constraint.getSummary(), constraint.predicate(), constraint.getPredicateColumns());
        BigQueryTableHandle bigQueryTableHandle = (BigQueryTableHandle) handle;

        TupleDomain<ColumnHandle> oldDomain = bigQueryTableHandle.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        BigQueryTableHandle updatedHandle = bigQueryTableHandle.withConstraint(newDomain);

        return Optional.of(new ConstraintApplicationResult<>(updatedHandle, constraint.getSummary()));
    }
}
