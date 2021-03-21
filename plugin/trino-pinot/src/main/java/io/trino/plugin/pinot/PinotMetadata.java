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
package io.trino.plugin.pinot;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.plugin.pinot.client.PinotClient;
import io.trino.plugin.pinot.query.AggregationExpression;
import io.trino.plugin.pinot.query.DynamicTable;
import io.trino.plugin.pinot.query.DynamicTableBuilder;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnNotFoundException;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTablePartitioning;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ComputedStatistics;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.Schema;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.pinot.PinotColumn.getPinotColumnsForPinotSchema;
import static io.trino.plugin.pinot.client.PinotClient.getFromCache;
import static io.trino.plugin.pinot.query.DynamicTableBuilder.BIGINT_AGGREGATIONS;
import static io.trino.plugin.pinot.query.DynamicTableBuilder.DOUBLE_AGGREGATIONS;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class PinotMetadata
        implements ConnectorMetadata
{
    public static final String SCHEMA_NAME = "default";
    private static final String PINOT_COLUMN_NAME_PROPERTY = "pinotColumnName";

    private final LoadingCache<String, List<PinotColumn>> pinotTableColumnCache;
    private final PinotClient pinotClient;
    private final NodeManager nodeManager;
    private final boolean forbidDropTable;

    @Inject
    public PinotMetadata(
            PinotClient pinotClient,
            PinotConfig pinotConfig,
            @ForPinot ExecutorService executor,
            NodeManager nodeManager)
    {
        requireNonNull(pinotConfig, "pinot config");
        requireNonNull(executor, "executor is null");
        this.forbidDropTable = pinotConfig.getForbidDropTable();
        long metadataCacheExpiryMillis = pinotConfig.getMetadataCacheExpiry().roundTo(TimeUnit.MILLISECONDS);
        this.pinotClient = requireNonNull(pinotClient, "pinotClient is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.pinotTableColumnCache =
                CacheBuilder.newBuilder()
                        .refreshAfterWrite(metadataCacheExpiryMillis, TimeUnit.MILLISECONDS)
                        .build(asyncReloading(new CacheLoader<>()
                        {
                            @Override
                            public List<PinotColumn> load(String tableName)
                                    throws Exception
                            {
                                Schema tablePinotSchema = pinotClient.getTableSchema(tableName);
                                return getPinotColumnsForPinotSchema(tablePinotSchema);
                            }
                        }, executor));
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(SCHEMA_NAME);
    }

    @Override
    public PinotTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        if (tableName.getTableName().trim().startsWith("select ")) {
            DynamicTable dynamicTable = DynamicTableBuilder.buildFromPql(this, pinotClient, tableName);
            return new PinotTableHandle(tableName.getSchemaName(), dynamicTable.getTableName(), TupleDomain.all(), OptionalLong.empty(), Optional.of(dynamicTable), Optional.empty(), OptionalInt.empty(), Optional.empty());
        }
        String pinotTableName = pinotClient.getPinotTableNameFromTrinooTableNameIfExists(tableName.getTableName());
        if (pinotTableName == null) {
            return null;
        }
        return new PinotTableHandle(
                tableName.getSchemaName(),
                pinotTableName,
                TupleDomain.all(),
                OptionalLong.empty(),
                Optional.empty(),
                Optional.of(getNodes()),
                OptionalInt.of(pinotClient.getSegments(pinotTableName).size()),
                Optional.of(getPinotDateTimeField(pinotTableName)));
    }

    private List<String> getNodes()
    {
        List<String> nodes = nodeManager.getRequiredWorkerNodes().stream().map(Node::getNodeIdentifier).collect(toList());
        return nodes;
    }

    private PinotDateTimeField getPinotDateTimeField(String pinotTableName)
    {
        Schema schema = pinotClient.getTableSchema(pinotTableName);
        TableConfig tableConfig = pinotClient.getTableConfig(pinotTableName);
        DateTimeFieldSpec dateTimeFieldSpec = schema.getDateTimeSpec(tableConfig.getValidationConfig().getTimeColumnName());
        return new PinotDateTimeField(dateTimeFieldSpec);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        PinotTableHandle pinotTableHandle = (PinotTableHandle) table;
        if (pinotTableHandle.getQuery().isPresent()) {
            DynamicTable dynamicTable = pinotTableHandle.getQuery().get();
            Map<String, ColumnHandle> columnHandles = getColumnHandles(session, table);
            ImmutableList.Builder<ColumnMetadata> columnMetadataBuilder = ImmutableList.builder();
            for (String columnName : dynamicTable.getSelections()) {
                PinotColumnHandle pinotColumnHandle = (PinotColumnHandle) columnHandles.get(columnName.toLowerCase(ENGLISH));
                columnMetadataBuilder.add(pinotColumnHandle.getColumnMetadata());
            }

            for (String columnName : dynamicTable.getGroupingColumns()) {
                PinotColumnHandle pinotColumnHandle = (PinotColumnHandle) columnHandles.get(columnName.toLowerCase(ENGLISH));
                columnMetadataBuilder.add(pinotColumnHandle.getColumnMetadata());
            }

            for (AggregationExpression aggregationExpression : dynamicTable.getAggregateColumns()) {
                PinotColumnHandle pinotColumnHandle = (PinotColumnHandle) columnHandles.get(aggregationExpression.getOutputColumnName().toLowerCase(ENGLISH));
                columnMetadataBuilder.add(pinotColumnHandle.getColumnMetadata());
            }
            SchemaTableName schemaTableName = new SchemaTableName(pinotTableHandle.getSchemaName(), dynamicTable.getTableName());
            return new ConnectorTableMetadata(schemaTableName, columnMetadataBuilder.build());
        }
        SchemaTableName tableName = new SchemaTableName(pinotTableHandle.getSchemaName(), pinotTableHandle.getTableName());

        return getTableMetadata(tableName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String table : pinotClient.getPinotTableNames()) {
            builder.add(new SchemaTableName(SCHEMA_NAME, table));
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PinotTableHandle pinotTableHandle = (PinotTableHandle) tableHandle;
        if (pinotTableHandle.getQuery().isPresent()) {
            return getDynamicTableColumnHandles(pinotTableHandle);
        }
        return getPinotColumnHandles(pinotTableHandle.getTableName());
    }

    public Map<String, ColumnHandle> getPinotColumnHandles(String tableName)
    {
        ImmutableMap.Builder<String, ColumnHandle> columnHandlesBuilder = ImmutableMap.builder();
        for (ColumnMetadata columnMetadata : getColumnsMetadata(tableName)) {
            columnHandlesBuilder.put(columnMetadata.getName(),
                    new PinotColumnHandle(getPinotColumnName(columnMetadata), columnMetadata.getType()));
        }
        return columnHandlesBuilder.build();
    }

    private static String getPinotColumnName(ColumnMetadata columnMetadata)
    {
        Object pinotColumnName = requireNonNull(columnMetadata.getProperties().get(PINOT_COLUMN_NAME_PROPERTY), "Pinot column name is missing");
        return pinotColumnName.toString();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
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
        return ((PinotColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle table)
    {
        return Optional.empty();
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        PinotTableHandle pinotTableHandle = (PinotTableHandle) table;
        if (pinotTableHandle.getDateTimeField().isPresent()) {
            PinotDateTimeField dateTimeField = pinotTableHandle.getDateTimeField().get();
            PinotColumnHandle partitionColumn = new PinotColumnHandle(dateTimeField.getColumnName().toLowerCase(ENGLISH), dateTimeField.getType());
            return new ConnectorTableProperties(
                    TupleDomain.all(),
                    Optional.of(new ConnectorTablePartitioning(new PinotPartitioningHandle(pinotTableHandle.getNodes(), pinotTableHandle.getDateTimeField(), pinotTableHandle.getSegmentCount()), ImmutableList.of(partitionColumn))),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableList.of());
        }
        return new ConnectorTableProperties();
    }

    @Override
    public ConnectorTableHandle makeCompatiblePartitioning(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorPartitioningHandle partitioningHandle)
    {
        PinotTableHandle pinotTableHandle = (PinotTableHandle) tableHandle;
        PinotPartitioningHandle pinotPartitioningHandle = (PinotPartitioningHandle) partitioningHandle;
        return new PinotTableHandle(
                pinotTableHandle.getSchemaName(),
                pinotTableHandle.getTableName(),
                pinotTableHandle.getConstraint(),
                pinotTableHandle.getLimit(),
                pinotTableHandle.getQuery(),
                pinotPartitioningHandle.getNodes(),
                pinotTableHandle.getSegmentCount(),
                pinotPartitioningHandle.getDateTimeField());
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle table, long limit)
    {
        PinotTableHandle handle = (PinotTableHandle) table;
        if (handle.getLimit().isPresent() && handle.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }
        Optional<DynamicTable> dynamicTable = handle.getQuery();
        if (dynamicTable.isPresent() &&
                (dynamicTable.get().getLimit().isEmpty() || dynamicTable.get().getLimit().getAsLong() > limit)) {
            dynamicTable = Optional.of(new DynamicTable(dynamicTable.get().getTableName(),
                    dynamicTable.get().getSuffix(),
                    dynamicTable.get().getSelections(),
                    dynamicTable.get().getGroupingColumns(),
                    dynamicTable.get().getFilter(),
                    dynamicTable.get().getAggregateColumns(),
                    dynamicTable.get().getOrderBy(),
                    OptionalLong.of(limit),
                    dynamicTable.get().getOffset(),
                    dynamicTable.get().getQuery()));
        }

        handle = new PinotTableHandle(
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getConstraint(),
                OptionalLong.of(limit),
                dynamicTable,
                handle.getNodes(),
                handle.getSegmentCount(),
                handle.getDateTimeField());
        return Optional.of(new LimitApplicationResult<>(handle, false));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        PinotTableHandle handle = (PinotTableHandle) table;
        TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        handle = new PinotTableHandle(
                handle.getSchemaName(),
                handle.getTableName(),
                newDomain,
                handle.getLimit(),
                handle.getQuery(),
                handle.getNodes(),
                handle.getSegmentCount(),
                handle.getDateTimeField());
        return Optional.of(new ConstraintApplicationResult<>(handle, constraint.getSummary()));
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns)
    {
        PinotTableHandle pinotTableHandle = (PinotTableHandle) tableHandle;
        String pinotTableName = pinotClient.getPinotTableNameFromTrinoTableName(((PinotTableHandle) tableHandle).getTableName());
        List<PinotColumnHandle> pinotColumnHandles = columns.stream()
                .map(column -> (PinotColumnHandle) column)
                .collect(toImmutableList());
        return new PinotInsertTableHandle(pinotTableName, pinotTableHandle.getDateTimeField(), pinotColumnHandles);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        checkState(!forbidDropTable, "Drop table forbidden");
        PinotTableHandle pinotTableHandle = (PinotTableHandle) tableHandle;
        pinotClient.dropTableAndSchema(pinotTableHandle.getTableName());
    }

    @VisibleForTesting
    public List<PinotColumn> getPinotColumns(String tableName)
    {
        String pinotTableName = pinotClient.getPinotTableNameFromTrinoTableName(tableName);
        return getFromCache(pinotTableColumnCache, pinotTableName);
    }

    private Map<String, ColumnHandle> getDynamicTableColumnHandles(PinotTableHandle pinotTableHandle)
    {
        checkState(pinotTableHandle.getQuery().isPresent(), "dynamic table not present");
        String schemaName = pinotTableHandle.getSchemaName();
        DynamicTable dynamicTable = pinotTableHandle.getQuery().get();

        Map<String, ColumnHandle> columnHandles = getPinotColumnHandles(dynamicTable.getTableName());
        ImmutableMap.Builder<String, ColumnHandle> columnHandlesBuilder = ImmutableMap.builder();
        for (String columnName : dynamicTable.getSelections()) {
            PinotColumnHandle columnHandle = (PinotColumnHandle) columnHandles.get(columnName.toLowerCase(ENGLISH));
            if (columnHandle == null) {
                throw new ColumnNotFoundException(new SchemaTableName(schemaName, dynamicTable.getTableName()), columnName);
            }
            columnHandlesBuilder.put(columnName.toLowerCase(ENGLISH), columnHandle);
        }

        for (String columnName : dynamicTable.getGroupingColumns()) {
            PinotColumnHandle columnHandle = (PinotColumnHandle) columnHandles.get(columnName.toLowerCase(ENGLISH));
            if (columnHandle == null) {
                throw new ColumnNotFoundException(new SchemaTableName(schemaName, dynamicTable.getTableName()), columnName);
            }
            columnHandlesBuilder.put(columnName.toLowerCase(ENGLISH), columnHandle);
        }

        for (AggregationExpression aggregationExpression : dynamicTable.getAggregateColumns()) {
            if (DOUBLE_AGGREGATIONS.contains(aggregationExpression.getAggregationType().toLowerCase(ENGLISH))) {
                columnHandlesBuilder.put(aggregationExpression.getOutputColumnName().toLowerCase(ENGLISH),
                        new PinotColumnHandle(aggregationExpression.getOutputColumnName(), DOUBLE));
            }
            else if (BIGINT_AGGREGATIONS.contains(aggregationExpression.getAggregationType().toLowerCase(ENGLISH))) {
                columnHandlesBuilder.put(aggregationExpression.getOutputColumnName().toLowerCase(ENGLISH),
                        new PinotColumnHandle(aggregationExpression.getOutputColumnName(), BIGINT));
            }
            else {
                PinotColumnHandle columnHandle = (PinotColumnHandle) columnHandles.get(aggregationExpression.getBaseColumnName().toLowerCase(ENGLISH));
                if (columnHandle == null) {
                    throw new ColumnNotFoundException(new SchemaTableName(schemaName, dynamicTable.getTableName()), aggregationExpression.getBaseColumnName());
                }
                columnHandlesBuilder.put(aggregationExpression.getOutputColumnName().toLowerCase(ENGLISH),
                        new PinotColumnHandle(aggregationExpression.getOutputColumnName(),
                                columnHandle.getDataType()));
            }
        }
        return columnHandlesBuilder.build();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        return new ConnectorTableMetadata(tableName, getColumnsMetadata(tableName.getTableName()));
    }

    private List<ColumnMetadata> getColumnsMetadata(String tableName)
    {
        List<PinotColumn> columns = getPinotColumns(tableName);
        return columns.stream()
                .map(PinotMetadata::createPinotColumnMetadata)
                .collect(toImmutableList());
    }

    private static ColumnMetadata createPinotColumnMetadata(PinotColumn pinotColumn)
    {
        return ColumnMetadata.builder()
                .setName(pinotColumn.getName().toLowerCase(ENGLISH))
                .setType(pinotColumn.getType())
                .setProperties(ImmutableMap.<String, Object>builder()
                        .put(PINOT_COLUMN_NAME_PROPERTY, pinotColumn.getName())
                        .build())
                .build();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchema().isEmpty() || prefix.getTable().isEmpty()) {
            return listTables(session, Optional.empty());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchema().get(), prefix.getTable().get()));
    }
}
