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
package io.trino.plugin.lakehouse;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import io.airlift.slice.Slice;
import io.trino.metastore.Table;
import io.trino.plugin.deltalake.DeltaLakeInsertTableHandle;
import io.trino.plugin.deltalake.DeltaLakeMergeTableHandle;
import io.trino.plugin.deltalake.DeltaLakeMetadata;
import io.trino.plugin.deltalake.DeltaLakeOutputTableHandle;
import io.trino.plugin.deltalake.DeltaLakePartitioningHandle;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.deltalake.procedure.DeltaLakeTableExecuteHandle;
import io.trino.plugin.hive.HiveInsertTableHandle;
import io.trino.plugin.hive.HiveMergeTableHandle;
import io.trino.plugin.hive.HiveOutputTableHandle;
import io.trino.plugin.hive.HivePartitioningHandle;
import io.trino.plugin.hive.HiveTableExecuteHandle;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hive.TransactionalMetadata;
import io.trino.plugin.hudi.HudiMetadata;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.iceberg.IcebergMergeTableHandle;
import io.trino.plugin.iceberg.IcebergMetadata;
import io.trino.plugin.iceberg.IcebergPartitioningHandle;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.iceberg.IcebergWritableTableHandle;
import io.trino.plugin.iceberg.procedure.IcebergTableExecuteHandle;
import io.trino.spi.RefreshType;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.BeginTableExecuteResult;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorAnalyzeMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.RelationType;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SampleApplicationResult;
import io.trino.spi.connector.SampleType;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.connector.WriterScalingOptions;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.Type;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.UnaryOperator;

import static io.trino.plugin.hive.util.HiveUtil.isDeltaLakeTable;
import static io.trino.plugin.hive.util.HiveUtil.isHudiTable;
import static io.trino.plugin.hive.util.HiveUtil.isIcebergTable;
import static io.trino.plugin.iceberg.IcebergTableName.isIcebergTableName;
import static io.trino.plugin.iceberg.IcebergTableName.isMaterializedViewStorage;
import static io.trino.plugin.lakehouse.LakehouseTableProperties.getTableType;
import static java.util.Objects.requireNonNull;

public class LakehouseMetadata
        implements ConnectorMetadata
{
    private final LakehouseTableProperties tableProperties;
    private final TransactionalMetadata hiveMetadata;
    private final IcebergMetadata icebergMetadata;
    private final DeltaLakeMetadata deltaMetadata;
    private final HudiMetadata hudiMetadata;

    public LakehouseMetadata(
            LakehouseTableProperties tableProperties,
            TransactionalMetadata hiveMetadata,
            IcebergMetadata icebergMetadata,
            DeltaLakeMetadata deltaMetadata,
            HudiMetadata hudiMetadata)
    {
        this.tableProperties = requireNonNull(tableProperties, "tableProperties is null");
        this.hiveMetadata = requireNonNull(hiveMetadata, "hiveMetadata is null");
        this.icebergMetadata = requireNonNull(icebergMetadata, "icebergMetadata is null");
        this.deltaMetadata = requireNonNull(deltaMetadata, "deltaMetadata is null");
        this.hudiMetadata = requireNonNull(hudiMetadata, "hudiMetadata is null");
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return hiveMetadata.schemaExists(session, schemaName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return hiveMetadata.listSchemaNames(session);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (isIcebergTableName(tableName.getTableName()) && isMaterializedViewStorage(tableName.getTableName())) {
            return icebergMetadata.getTableHandle(session, tableName, startVersion, endVersion);
        }

        Table table = hiveMetadata.getMetastore()
                .getTable(tableName.getSchemaName(), tableName.getTableName())
                .orElse(null);
        if (table == null) {
            return null;
        }
        if (isIcebergTable(table)) {
            return icebergMetadata.getTableHandle(session, tableName, startVersion, endVersion);
        }
        if (isDeltaLakeTable(table)) {
            return deltaMetadata.getTableHandle(session, tableName, startVersion, endVersion);
        }
        if (isHudiTable(table)) {
            return hudiMetadata.getTableHandle(session, tableName, startVersion, endVersion);
        }
        return hiveMetadata.getTableHandle(session, tableName, startVersion, endVersion);
    }

    @Override
    public Optional<ConnectorTableExecuteHandle> getTableHandleForExecute(ConnectorSession session, ConnectorAccessControl accessControl, ConnectorTableHandle tableHandle, String procedureName, Map<String, Object> executeProperties, RetryMode retryMode)
    {
        return forHandle(tableHandle).getTableHandleForExecute(session, accessControl, tableHandle, procedureName, executeProperties, retryMode);
    }

    @Override
    public Optional<ConnectorTableLayout> getLayoutForTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        return forHandle(tableExecuteHandle).getLayoutForTableExecute(session, tableExecuteHandle);
    }

    @Override
    public BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, ConnectorTableHandle updatedSourceTableHandle)
    {
        return forHandle(tableExecuteHandle).beginTableExecute(session, tableExecuteHandle, updatedSourceTableHandle);
    }

    @Override
    public void finishTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, Collection<Slice> fragments, List<Object> tableExecuteState)
    {
        forHandle(tableExecuteHandle).finishTableExecute(session, tableExecuteHandle, fragments, tableExecuteState);
    }

    @Override
    public Map<String, Long> executeTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        return forHandle(tableExecuteHandle).executeTableExecute(session, tableExecuteHandle);
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return hiveMetadata.getSystemTable(session, tableName)
                .or(() -> icebergMetadata.getSystemTable(session, tableName))
                .or(() -> deltaMetadata.getSystemTable(session, tableName))
                .or(() -> hudiMetadata.getSystemTable(session, tableName));
    }

    @Override
    public Optional<ConnectorTableHandle> applyPartitioning(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorPartitioningHandle> partitioningHandle, List<ColumnHandle> columns)
    {
        return forHandle(tableHandle).applyPartitioning(session, tableHandle, partitioningHandle, columns);
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getCommonPartitioningHandle(ConnectorSession session, ConnectorPartitioningHandle left, ConnectorPartitioningHandle right)
    {
        if (left.getClass() != right.getClass()) {
            return Optional.empty();
        }
        return forHandle(left).getCommonPartitioningHandle(session, left, right);
    }

    @Override
    public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle table)
    {
        return forHandle(table).getTableName(session, table);
    }

    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle table)
    {
        return forHandle(table).getTableSchema(session, table);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        return wrapTableMetadata(tableTypeForHandle(table), forHandle(table).getTableMetadata(session, table));
    }

    @Override
    public Optional<Object> getInfo(ConnectorSession session, ConnectorTableHandle table)
    {
        return forHandle(table).getInfo(session, table);
    }

    @Override
    public Metrics getMetrics(ConnectorSession session)
    {
        ImmutableMap.Builder<String, Metric<?>> metrics = ImmutableMap.<String, Metric<?>>builder();
        hiveMetadata.getMetrics(session).getMetrics().forEach((metricName, metric) -> {
            metrics.put("hive." + metricName, metric);
        });
        icebergMetadata.getMetrics(session).getMetrics().forEach((metricName, metric) -> {
            metrics.put("iceberg." + metricName, metric);
        });
        deltaMetadata.getMetrics(session).getMetrics().forEach((metricName, metric) -> {
            metrics.put("delta." + metricName, metric);
        });
        hudiMetadata.getMetrics(session).getMetrics().forEach((metricName, metric) -> {
            metrics.put("hudi." + metricName, metric);
        });
        return new Metrics(metrics.buildOrThrow());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return hiveMetadata.listTables(session, schemaName);
    }

    @Override
    public Map<SchemaTableName, RelationType> getRelationTypes(ConnectorSession session, Optional<String> schemaName)
    {
        return hiveMetadata.getRelationTypes(session, schemaName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return forHandle(tableHandle).getColumnHandles(session, tableHandle);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return forHandle(tableHandle).getColumnMetadata(session, tableHandle, columnHandle);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        throw new UnsupportedOperationException("The deprecated listTableColumns is not supported because streamTableColumns is implemented instead");
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        throw new UnsupportedOperationException("The deprecated streamTableColumns is not supported because streamRelationColumns is implemented instead");
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        return Iterators.concat(
                hiveMetadata.streamRelationColumns(session, schemaName, relationFilter),
                icebergMetadata.streamRelationColumns(session, schemaName, relationFilter),
                deltaMetadata.streamRelationColumns(session, schemaName, relationFilter),
                hudiMetadata.streamRelationColumns(session, schemaName, relationFilter));
    }

    @Override
    public Iterator<RelationCommentMetadata> streamRelationComments(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        return Iterators.concat(
                hiveMetadata.streamRelationComments(session, schemaName, relationFilter),
                icebergMetadata.streamRelationComments(session, schemaName, relationFilter),
                deltaMetadata.streamRelationComments(session, schemaName, relationFilter),
                hudiMetadata.streamRelationComments(session, schemaName, relationFilter));
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return forHandle(tableHandle).getTableStatistics(session, tableHandle);
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        hiveMetadata.createSchema(session, schemaName, properties, owner);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        // use Iceberg to allow dropping materialized views
        icebergMetadata.dropSchema(session, schemaName, cascade);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        hiveMetadata.renameSchema(session, source, target);
    }

    @Override
    public void setSchemaAuthorization(ConnectorSession session, String schemaName, TrinoPrincipal principal)
    {
        hiveMetadata.setSchemaAuthorization(session, schemaName, principal);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        forProperties(tableMetadata.getProperties()).createTable(session, unwrapTableMetadata(tableMetadata), saveMode);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        forHandle(tableHandle).dropTable(session, tableHandle);
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        forHandle(tableHandle).truncateTable(session, tableHandle);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        forHandle(tableHandle).renameTable(session, tableHandle, newTableName);
    }

    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        forHandle(tableHandle).setTableProperties(session, tableHandle, properties);
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        forHandle(tableHandle).setTableComment(session, tableHandle, comment);
    }

    @Override
    public void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        hiveMetadata.setViewComment(session, viewName, comment);
    }

    @Override
    public void setViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        hiveMetadata.setViewColumnComment(session, viewName, columnName, comment);
    }

    @Override
    public void setMaterializedViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        icebergMetadata.setMaterializedViewColumnComment(session, viewName, columnName, comment);
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        forHandle(tableHandle).setColumnComment(session, tableHandle, column, comment);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column, ColumnPosition position)
    {
        forHandle(tableHandle).addColumn(session, tableHandle, column, position);
    }

    @Override
    public void addField(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> parentPath, String fieldName, Type type, boolean ignoreExisting)
    {
        forHandle(tableHandle).addField(session, tableHandle, parentPath, fieldName, type, ignoreExisting);
    }

    @Override
    public void setDefaultValue(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, String defaultValue)
    {
        forHandle(tableHandle).setDefaultValue(session, tableHandle, column, defaultValue);
    }

    @Override
    public void dropDefaultValue(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        forHandle(tableHandle).dropDefaultValue(session, tableHandle, columnHandle);
    }

    @Override
    public void setColumnType(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Type type)
    {
        forHandle(tableHandle).setColumnType(session, tableHandle, column, type);
    }

    @Override
    public void setFieldType(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> fieldPath, Type type)
    {
        forHandle(tableHandle).setFieldType(session, tableHandle, fieldPath, type);
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        forHandle(tableHandle).dropNotNullConstraint(session, tableHandle, column);
    }

    @Override
    public void setTableAuthorization(ConnectorSession session, SchemaTableName tableName, TrinoPrincipal principal)
    {
        hiveMetadata.setTableAuthorization(session, tableName, principal);
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        forHandle(tableHandle).renameColumn(session, tableHandle, source, target);
    }

    @Override
    public void renameField(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> fieldPath, String target)
    {
        forHandle(tableHandle).renameField(session, tableHandle, fieldPath, target);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        forHandle(tableHandle).dropColumn(session, tableHandle, column);
    }

    @Override
    public void dropField(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, List<String> fieldPath)
    {
        forHandle(tableHandle).dropField(session, tableHandle, column, fieldPath);
    }

    @Override
    public Optional<ConnectorTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return forProperties(tableMetadata.getProperties()).getNewTableLayout(session, unwrapTableMetadata(tableMetadata));
    }

    @Override
    public Optional<Type> getSupportedType(ConnectorSession session, Map<String, Object> tableProperties, Type type)
    {
        return forProperties(tableProperties).getSupportedType(session, tableProperties, type);
    }

    @Override
    public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return forHandle(tableHandle).getInsertLayout(session, tableHandle);
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean tableReplace)
    {
        return forProperties(tableMetadata.getProperties()).getStatisticsCollectionMetadataForWrite(session, unwrapTableMetadata(tableMetadata), tableReplace);
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException("This variant of getStatisticsCollectionMetadataForWrite is unsupported");
    }

    @Override
    public ConnectorAnalyzeMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Object> analyzeProperties)
    {
        return forHandle(tableHandle).getStatisticsCollectionMetadata(session, tableHandle, analyzeProperties);
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return forHandle(tableHandle).beginStatisticsCollection(session, tableHandle);
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        forHandle(tableHandle).finishStatisticsCollection(session, tableHandle, computedStatistics);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
    {
        return forProperties(tableMetadata.getProperties()).beginCreateTable(session, unwrapTableMetadata(tableMetadata), layout, retryMode, replace);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return forHandle(tableHandle).finishCreateTable(session, tableHandle, fragments, computedStatistics);
    }

    @Override
    public void beginQuery(ConnectorSession session)
    {
        hiveMetadata.beginQuery(session);
    }

    @Override
    public void cleanupQuery(ConnectorSession session)
    {
        hiveMetadata.cleanupQuery(session);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        return forHandle(tableHandle).beginInsert(session, tableHandle, columns, retryMode);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, List<ConnectorTableHandle> sourceTableHandles, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return forHandle(insertHandle).finishInsert(session, insertHandle, sourceTableHandles, fragments, computedStatistics);
    }

    @Override
    public boolean delegateMaterializedViewRefreshToConnector(ConnectorSession session, SchemaTableName viewName)
    {
        return icebergMetadata.delegateMaterializedViewRefreshToConnector(session, viewName);
    }

    @Override
    public ConnectorInsertTableHandle beginRefreshMaterializedView(ConnectorSession session, ConnectorTableHandle tableHandle, List<ConnectorTableHandle> sourceTableHandles, boolean hasForeignSourceTables, RetryMode retryMode, RefreshType refreshType)
    {
        List<ConnectorTableHandle> icebergSourceHandles = sourceTableHandles.stream()
                .filter(IcebergTableHandle.class::isInstance)
                .toList();
        hasForeignSourceTables |= icebergSourceHandles.size() < sourceTableHandles.size();
        return icebergMetadata.beginRefreshMaterializedView(session, tableHandle, icebergSourceHandles, hasForeignSourceTables, retryMode, refreshType);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics, List<ConnectorTableHandle> sourceTableHandles, boolean hasForeignSourceTables, boolean hasSourceTableFunctions)
    {
        List<ConnectorTableHandle> icebergSourceHandles = sourceTableHandles.stream()
                .filter(IcebergTableHandle.class::isInstance)
                .toList();
        hasForeignSourceTables |= icebergSourceHandles.size() < sourceTableHandles.size();
        return icebergMetadata.finishRefreshMaterializedView(session, tableHandle, insertHandle, fragments, computedStatistics, icebergSourceHandles, hasForeignSourceTables, hasSourceTableFunctions);
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return forHandle(tableHandle).getRowChangeParadigm(session, tableHandle);
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return forHandle(tableHandle).getMergeRowIdColumnHandle(session, tableHandle);
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getUpdateLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return forHandle(tableHandle).getUpdateLayout(session, tableHandle);
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle, Map<Integer, Collection<ColumnHandle>> updateCaseColumns, RetryMode retryMode)
    {
        return forHandle(tableHandle).beginMerge(session, tableHandle, updateCaseColumns, retryMode);
    }

    @Override
    public void finishMerge(ConnectorSession session, ConnectorMergeTableHandle mergeTableHandle, List<ConnectorTableHandle> sourceTableHandles, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        forHandle(mergeTableHandle).finishMerge(session, mergeTableHandle, sourceTableHandles, fragments, computedStatistics);
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, Map<String, Object> viewProperties, boolean replace)
    {
        hiveMetadata.createView(session, viewName, definition, viewProperties, replace);
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        hiveMetadata.renameView(session, source, target);
    }

    @Override
    public void setViewAuthorization(ConnectorSession session, SchemaTableName viewName, TrinoPrincipal principal)
    {
        hiveMetadata.setViewAuthorization(session, viewName, principal);
    }

    @Override
    public void refreshView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition viewDefinition)
    {
        hiveMetadata.refreshView(session, viewName, viewDefinition);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        hiveMetadata.dropView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        return hiveMetadata.listViews(session, schemaName);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        return hiveMetadata.getViews(session, schemaName);
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return hiveMetadata.getView(session, viewName);
    }

    @Override
    public boolean isView(ConnectorSession session, SchemaTableName viewName)
    {
        return hiveMetadata.isView(session, viewName);
    }

    @Override
    public Map<String, Object> getViewProperties(ConnectorSession session, SchemaTableName viewName)
    {
        return hiveMetadata.getViewProperties(session, viewName);
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, String schemaName)
    {
        return hiveMetadata.getSchemaProperties(session, schemaName);
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(ConnectorSession session, String schemaName)
    {
        return hiveMetadata.getSchemaOwner(session, schemaName);
    }

    @Override
    public Optional<ConnectorTableHandle> applyUpdate(ConnectorSession session, ConnectorTableHandle handle, Map<ColumnHandle, Constant> assignments)
    {
        return forHandle(handle).applyUpdate(session, handle, assignments);
    }

    @Override
    public OptionalLong executeUpdate(ConnectorSession session, ConnectorTableHandle handle)
    {
        return forHandle(handle).executeUpdate(session, handle);
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return forHandle(handle).applyDelete(session, handle);
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return forHandle(handle).executeDelete(session, handle);
    }

    @Override
    public Collection<LanguageFunction> listLanguageFunctions(ConnectorSession session, String schemaName)
    {
        return hiveMetadata.listLanguageFunctions(session, schemaName);
    }

    @Override
    public Collection<LanguageFunction> getLanguageFunctions(ConnectorSession session, SchemaFunctionName name)
    {
        return hiveMetadata.getLanguageFunctions(session, name);
    }

    @Override
    public boolean languageFunctionExists(ConnectorSession session, SchemaFunctionName name, String signatureToken)
    {
        return hiveMetadata.languageFunctionExists(session, name, signatureToken);
    }

    @Override
    public void createLanguageFunction(ConnectorSession session, SchemaFunctionName name, LanguageFunction function, boolean replace)
    {
        hiveMetadata.createLanguageFunction(session, name, function, replace);
    }

    @Override
    public void dropLanguageFunction(ConnectorSession session, SchemaFunctionName name, String signatureToken)
    {
        hiveMetadata.dropLanguageFunction(session, name, signatureToken);
    }

    @Override
    public boolean roleExists(ConnectorSession session, String role)
    {
        return hiveMetadata.roleExists(session, role);
    }

    @Override
    public void createRole(ConnectorSession session, String role, Optional<TrinoPrincipal> grantor)
    {
        hiveMetadata.createRole(session, role, grantor);
    }

    @Override
    public void dropRole(ConnectorSession session, String role)
    {
        hiveMetadata.dropRole(session, role);
    }

    @Override
    public Set<String> listRoles(ConnectorSession session)
    {
        return hiveMetadata.listRoles(session);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(ConnectorSession session, TrinoPrincipal principal)
    {
        return hiveMetadata.listRoleGrants(session, principal);
    }

    @Override
    public void grantRoles(ConnectorSession connectorSession, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        hiveMetadata.grantRoles(connectorSession, roles, grantees, adminOption, grantor);
    }

    @Override
    public void revokeRoles(ConnectorSession connectorSession, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        hiveMetadata.revokeRoles(connectorSession, roles, grantees, adminOption, grantor);
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(ConnectorSession session, TrinoPrincipal principal)
    {
        return hiveMetadata.listApplicableRoles(session, principal);
    }

    @Override
    public Set<String> listEnabledRoles(ConnectorSession session)
    {
        return hiveMetadata.listEnabledRoles(session);
    }

    @Override
    public void grantSchemaPrivileges(ConnectorSession session, String schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        hiveMetadata.grantSchemaPrivileges(session, schemaName, privileges, grantee, grantOption);
    }

    @Override
    public void denySchemaPrivileges(ConnectorSession session, String schemaName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        hiveMetadata.denySchemaPrivileges(session, schemaName, privileges, grantee);
    }

    @Override
    public void revokeSchemaPrivileges(ConnectorSession session, String schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        hiveMetadata.revokeSchemaPrivileges(session, schemaName, privileges, grantee, grantOption);
    }

    @Override
    public void grantTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        hiveMetadata.grantTablePrivileges(session, tableName, privileges, grantee, grantOption);
    }

    @Override
    public void denyTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        hiveMetadata.denyTablePrivileges(session, tableName, privileges, grantee);
    }

    @Override
    public void revokeTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        hiveMetadata.revokeTablePrivileges(session, tableName, privileges, grantee, grantOption);
    }

    @Override
    public List<GrantInfo> listTablePrivileges(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return hiveMetadata.listTablePrivileges(session, prefix);
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return forHandle(table).getTableProperties(session, table);
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit)
    {
        return forHandle(handle).applyLimit(session, handle, limit);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        return forHandle(handle).applyFilter(session, handle, constraint);
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session, ConnectorTableHandle handle, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        return forHandle(handle).applyProjection(session, handle, projections, assignments);
    }

    @Override
    public Optional<SampleApplicationResult<ConnectorTableHandle>> applySample(ConnectorSession session, ConnectorTableHandle handle, SampleType sampleType, double sampleRatio)
    {
        return forHandle(handle).applySample(session, handle, sampleType, sampleRatio);
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(ConnectorSession session, ConnectorTableHandle handle, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        return forHandle(handle).applyAggregation(session, handle, aggregates, assignments, groupingSets);
    }

    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(ConnectorSession session, ConnectorTableHandle handle, long topNCount, List<SortItem> sortItems, Map<String, ColumnHandle> assignments)
    {
        return forHandle(handle).applyTopN(session, handle, topNCount, sortItems, assignments);
    }

    @Override
    public void validateScan(ConnectorSession session, ConnectorTableHandle handle)
    {
        forHandle(handle).validateScan(session, handle);
    }

    @Override
    public void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, Map<String, Object> properties, boolean replace, boolean ignoreExisting)
    {
        icebergMetadata.createMaterializedView(session, viewName, definition, properties, replace, ignoreExisting);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        icebergMetadata.dropMaterializedView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        return icebergMetadata.listMaterializedViews(session, schemaName);
    }

    @Override
    public void setMaterializedViewAuthorization(ConnectorSession session, SchemaTableName viewName, TrinoPrincipal principal)
    {
        icebergMetadata.setMaterializedViewAuthorization(session, viewName, principal);
    }

    @Override
    public Map<SchemaTableName, ConnectorMaterializedViewDefinition> getMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        return icebergMetadata.getMaterializedViews(session, schemaName);
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return icebergMetadata.getMaterializedView(session, viewName);
    }

    @Override
    public Map<String, Object> getMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition materializedViewDefinition)
    {
        return icebergMetadata.getMaterializedViewProperties(session, viewName, materializedViewDefinition);
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session, SchemaTableName name)
    {
        return icebergMetadata.getMaterializedViewFreshness(session, name);
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        icebergMetadata.renameMaterializedView(session, source, target);
    }

    @Override
    public void setMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, Map<String, Optional<Object>> properties)
    {
        icebergMetadata.setMaterializedViewProperties(session, viewName, properties);
    }

    @Override
    public boolean allowSplittingReadIntoMultipleSubQueries(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return forHandle(tableHandle).allowSplittingReadIntoMultipleSubQueries(session, tableHandle);
    }

    @Override
    public WriterScalingOptions getNewTableWriterScalingOptions(ConnectorSession session, SchemaTableName tableName, Map<String, Object> tableProperties)
    {
        return forProperties(tableProperties).getNewTableWriterScalingOptions(session, tableName, tableProperties);
    }

    @Override
    public WriterScalingOptions getInsertWriterScalingOptions(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return forHandle(tableHandle).getInsertWriterScalingOptions(session, tableHandle);
    }

    private ConnectorMetadata forHandle(ConnectorTableHandle handle)
    {
        return switch (handle) {
            case HiveTableHandle _ -> hiveMetadata;
            case IcebergTableHandle _ -> icebergMetadata;
            case DeltaLakeTableHandle _ -> deltaMetadata;
            case HudiTableHandle _ -> hudiMetadata;
            default -> throw new IllegalArgumentException("Unsupported table handle: " + handle.getClass().getName());
        };
    }

    private ConnectorMetadata forHandle(ConnectorTableExecuteHandle handle)
    {
        return switch (handle) {
            case HiveTableExecuteHandle _ -> hiveMetadata;
            case IcebergTableExecuteHandle _ -> icebergMetadata;
            case DeltaLakeTableExecuteHandle _ -> deltaMetadata;
            default -> throw new IllegalArgumentException("Unsupported execute handle: " + handle.getClass().getName());
        };
    }

    private ConnectorMetadata forHandle(ConnectorPartitioningHandle handle)
    {
        return switch (handle) {
            case HivePartitioningHandle _ -> hiveMetadata;
            case IcebergPartitioningHandle _ -> icebergMetadata;
            case DeltaLakePartitioningHandle _ -> deltaMetadata;
            default -> throw new IllegalArgumentException("Unsupported partitioning handle: " + handle.getClass().getName());
        };
    }

    private ConnectorMetadata forHandle(ConnectorInsertTableHandle handle)
    {
        return switch (handle) {
            case HiveInsertTableHandle _ -> hiveMetadata;
            case IcebergWritableTableHandle _ -> icebergMetadata;
            case DeltaLakeInsertTableHandle _ -> deltaMetadata;
            default -> throw new IllegalArgumentException("Unsupported insert handle: " + handle.getClass().getName());
        };
    }

    private ConnectorMetadata forHandle(ConnectorOutputTableHandle handle)
    {
        return switch (handle) {
            case HiveOutputTableHandle _ -> hiveMetadata;
            case IcebergWritableTableHandle _ -> icebergMetadata;
            case DeltaLakeOutputTableHandle _ -> deltaMetadata;
            default -> throw new IllegalArgumentException("Unsupported output handle: " + handle.getClass().getName());
        };
    }

    private ConnectorMetadata forHandle(ConnectorMergeTableHandle handle)
    {
        return switch (handle) {
            case HiveMergeTableHandle _ -> hiveMetadata;
            case IcebergMergeTableHandle _ -> icebergMetadata;
            case DeltaLakeMergeTableHandle _ -> deltaMetadata;
            default -> throw new IllegalArgumentException("Unsupported merge handle: " + handle.getClass().getName());
        };
    }

    private ConnectorMetadata forProperties(Map<String, Object> properties)
    {
        return switch (getTableType(properties)) {
            case HIVE -> hiveMetadata;
            case ICEBERG -> icebergMetadata;
            case DELTA -> deltaMetadata;
            case HUDI -> hudiMetadata;
        };
    }

    private ConnectorTableMetadata unwrapTableMetadata(ConnectorTableMetadata metadata)
    {
        return withProperties(metadata, tableProperties.unwrapProperties(metadata.getProperties()));
    }

    private ConnectorTableMetadata wrapTableMetadata(TableType tableType, ConnectorTableMetadata metadata)
    {
        return withProperties(metadata, tableProperties.wrapProperties(tableType, metadata.getProperties()));
    }

    private static ConnectorTableMetadata withProperties(ConnectorTableMetadata metadata, Map<String, Object> properties)
    {
        return new ConnectorTableMetadata(
                metadata.getTable(),
                metadata.getColumns(),
                properties,
                metadata.getComment(),
                metadata.getCheckConstraints());
    }

    private static TableType tableTypeForHandle(ConnectorTableHandle handle)
    {
        return switch (handle) {
            case HiveTableHandle _ -> TableType.HIVE;
            case IcebergTableHandle _ -> TableType.ICEBERG;
            case DeltaLakeTableHandle _ -> TableType.DELTA;
            case HudiTableHandle _ -> TableType.HUDI;
            default -> throw new IllegalArgumentException("Unsupported table handle: " + handle.getClass().getName());
        };
    }
}
