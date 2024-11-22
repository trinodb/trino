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
package io.trino.tracing;

import io.airlift.slice.Slice;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.trino.spi.RefreshType;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.BeginTableExecuteResult;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorAnalyzeMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorResolvedIndex;
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
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
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
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.connector.WriterScalingOptions;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.function.AggregationFunctionMetadata;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.TupleDomain;
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
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;

import static io.trino.tracing.ScopedSpan.scopedSpan;
import static java.util.Objects.requireNonNull;

public class TracingConnectorMetadata
        implements ConnectorMetadata
{
    private final Tracer tracer;
    private final String catalogName;
    private final ConnectorMetadata delegate;

    public TracingConnectorMetadata(Tracer tracer, String catalogName, ConnectorMetadata delegate)
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        Span span = startSpan("schemaExists", schemaName);
        try (var _ = scopedSpan(span)) {
            return delegate.schemaExists(session, schemaName);
        }
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        Span span = startSpan("listSchemaNames");
        try (var _ = scopedSpan(span)) {
            return delegate.listSchemaNames(session);
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        Span span = startSpan("getTableHandle", tableName);
        try (var _ = scopedSpan(span)) {
            return delegate.getTableHandle(session, tableName, startVersion, endVersion);
        }
    }

    @Override
    public Optional<ConnectorTableExecuteHandle> getTableHandleForExecute(ConnectorSession session, ConnectorTableHandle tableHandle, String procedureName, Map<String, Object> executeProperties, RetryMode retryMode)
    {
        Span span = startSpan("getTableHandleForExecute", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.getTableHandleForExecute(session, tableHandle, procedureName, executeProperties, retryMode);
        }
    }

    @Override
    public Optional<ConnectorTableExecuteHandle> getTableHandleForExecute(ConnectorSession session, ConnectorAccessControl accessControl, ConnectorTableHandle tableHandle, String procedureName, Map<String, Object> executeProperties, RetryMode retryMode)
    {
        Span span = startSpan("getTableHandleForExecute", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.getTableHandleForExecute(session, accessControl, tableHandle, procedureName, executeProperties, retryMode);
        }
    }

    @Override
    public Optional<ConnectorTableLayout> getLayoutForTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        Span span = startSpan("getLayoutForTableExecute", tableExecuteHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.getLayoutForTableExecute(session, tableExecuteHandle);
        }
    }

    @Override
    public BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, ConnectorTableHandle updatedSourceTableHandle)
    {
        Span span = startSpan("beginTableExecute", tableExecuteHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.beginTableExecute(session, tableExecuteHandle, updatedSourceTableHandle);
        }
    }

    @Override
    public void finishTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, Collection<Slice> fragments, List<Object> tableExecuteState)
    {
        Span span = startSpan("finishTableExecute", tableExecuteHandle);
        try (var _ = scopedSpan(span)) {
            delegate.finishTableExecute(session, tableExecuteHandle, fragments, tableExecuteState);
        }
    }

    @Override
    public void executeTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        Span span = startSpan("executeTableExecute", tableExecuteHandle);
        try (var _ = scopedSpan(span)) {
            delegate.executeTableExecute(session, tableExecuteHandle);
        }
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        Span span = startSpan("getSystemTable", tableName);
        try (var _ = scopedSpan(span)) {
            return delegate.getSystemTable(session, tableName);
        }
    }

    @Override
    public ConnectorTableHandle makeCompatiblePartitioning(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorPartitioningHandle partitioningHandle)
    {
        Span span = startSpan("makeCompatiblePartitioning", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.makeCompatiblePartitioning(session, tableHandle, partitioningHandle);
        }
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getCommonPartitioningHandle(ConnectorSession session, ConnectorPartitioningHandle left, ConnectorPartitioningHandle right)
    {
        Span span = startSpan("getCommonPartitioning");
        try (var _ = scopedSpan(span)) {
            return delegate.getCommonPartitioningHandle(session, left, right);
        }
    }

    @Override
    public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle table)
    {
        Span span = startSpan("getTableName", table);
        try (var _ = scopedSpan(span)) {
            return delegate.getTableName(session, table);
        }
    }

    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle table)
    {
        Span span = startSpan("getTableSchema", table);
        try (var _ = scopedSpan(span)) {
            return delegate.getTableSchema(session, table);
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        Span span = startSpan("getTableMetadata", table);
        try (var _ = scopedSpan(span)) {
            return delegate.getTableMetadata(session, table);
        }
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle table)
    {
        Span span = startSpan("getInfo", table);
        try (var _ = scopedSpan(span)) {
            return delegate.getInfo(table);
        }
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        Span span = startSpan("listTables", schemaName);
        try (var _ = scopedSpan(span)) {
            return delegate.listTables(session, schemaName);
        }
    }

    @Override
    public Map<SchemaTableName, RelationType> getRelationTypes(ConnectorSession session, Optional<String> schemaName)
    {
        Span span = startSpan("getRelationTypes", schemaName);
        try (var _ = scopedSpan(span)) {
            return delegate.getRelationTypes(session, schemaName);
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        Span span = startSpan("getColumnHandles", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.getColumnHandles(session, tableHandle);
        }
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        Span span = startSpan("getColumnMetadata", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.getColumnMetadata(session, tableHandle, columnHandle);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        Span span = startSpan("listTableColumns", prefix);
        try (var _ = scopedSpan(span)) {
            return delegate.listTableColumns(session, prefix);
        }
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        Span span = startSpan("streamTableColumns", prefix);
        try (var _ = scopedSpan(span)) {
            return delegate.streamTableColumns(session, prefix);
        }
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        Span span = startSpan("streamRelationColumns", schemaName);
        try (var _ = scopedSpan(span)) {
            return delegate.streamRelationColumns(session, schemaName, relationFilter);
        }
    }

    @Override
    public Iterator<RelationCommentMetadata> streamRelationComments(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        Span span = startSpan("streamRelationComments", schemaName);
        try (var _ = scopedSpan(span)) {
            return delegate.streamRelationComments(session, schemaName, relationFilter);
        }
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        Span span = startSpan("getTableStatistics", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.getTableStatistics(session, tableHandle);
        }
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        Span span = startSpan("createSchema", schemaName);
        try (var _ = scopedSpan(span)) {
            delegate.createSchema(session, schemaName, properties, owner);
        }
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        Span span = startSpan("dropSchema", schemaName)
                .setAttribute(TrinoAttributes.CASCADE, cascade);
        try (var _ = scopedSpan(span)) {
            delegate.dropSchema(session, schemaName, cascade);
        }
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        Span span = startSpan("renameSchema", source);
        try (var _ = scopedSpan(span)) {
            delegate.renameSchema(session, source, target);
        }
    }

    @Override
    public void setSchemaAuthorization(ConnectorSession session, String schemaName, TrinoPrincipal principal)
    {
        Span span = startSpan("setSchemaAuthorization", schemaName);
        try (var _ = scopedSpan(span)) {
            delegate.setSchemaAuthorization(session, schemaName, principal);
        }
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        Span span = startSpan("createTable", tableMetadata.getTable());
        try (var _ = scopedSpan(span)) {
            delegate.createTable(session, tableMetadata, saveMode);
        }
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        Span span = startSpan("dropTable", tableHandle);
        try (var _ = scopedSpan(span)) {
            delegate.dropTable(session, tableHandle);
        }
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        Span span = startSpan("truncateTable", tableHandle);
        try (var _ = scopedSpan(span)) {
            delegate.truncateTable(session, tableHandle);
        }
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        Span span = startSpan("renameTable", tableHandle);
        try (var _ = scopedSpan(span)) {
            delegate.renameTable(session, tableHandle, newTableName);
        }
    }

    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        Span span = startSpan("setTableProperties", tableHandle);
        try (var _ = scopedSpan(span)) {
            delegate.setTableProperties(session, tableHandle, properties);
        }
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        Span span = startSpan("setTableComment", tableHandle);
        try (var _ = scopedSpan(span)) {
            delegate.setTableComment(session, tableHandle, comment);
        }
    }

    @Override
    public void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        Span span = startSpan("setViewComment", viewName);
        try (var _ = scopedSpan(span)) {
            delegate.setViewComment(session, viewName, comment);
        }
    }

    @Override
    public void setViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        Span span = startSpan("setViewColumnComment", viewName);
        try (var _ = scopedSpan(span)) {
            delegate.setViewColumnComment(session, viewName, columnName, comment);
        }
    }

    @Override
    public void setMaterializedViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        Span span = startSpan("setMaterializedViewColumnComment", viewName);
        try (var _ = scopedSpan(span)) {
            delegate.setMaterializedViewColumnComment(session, viewName, columnName, comment);
        }
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        Span span = startSpan("setColumnComment", tableHandle);
        try (var _ = scopedSpan(span)) {
            delegate.setColumnComment(session, tableHandle, column, comment);
        }
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        Span span = startSpan("addColumn", tableHandle);
        try (var _ = scopedSpan(span)) {
            delegate.addColumn(session, tableHandle, column);
        }
    }

    @Override
    public void addField(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> parentPath, String fieldName, Type type, boolean ignoreExisting)
    {
        Span span = startSpan("addField", tableHandle);
        try (var _ = scopedSpan(span)) {
            delegate.addField(session, tableHandle, parentPath, fieldName, type, ignoreExisting);
        }
    }

    @Override
    public void setColumnType(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Type type)
    {
        Span span = startSpan("setColumnType", tableHandle);
        try (var _ = scopedSpan(span)) {
            delegate.setColumnType(session, tableHandle, column, type);
        }
    }

    @Override
    public void setFieldType(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> fieldPath, Type type)
    {
        Span span = startSpan("setFieldType", tableHandle);
        try (var _ = scopedSpan(span)) {
            delegate.setFieldType(session, tableHandle, fieldPath, type);
        }
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        Span span = startSpan("dropNotNull", tableHandle);
        try (var _ = scopedSpan(span)) {
            delegate.dropNotNullConstraint(session, tableHandle, column);
        }
    }

    @Override
    public void setTableAuthorization(ConnectorSession session, SchemaTableName tableName, TrinoPrincipal principal)
    {
        Span span = startSpan("setTableAuthorization", tableName);
        try (var _ = scopedSpan(span)) {
            delegate.setTableAuthorization(session, tableName, principal);
        }
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        Span span = startSpan("renameColumn", tableHandle);
        try (var _ = scopedSpan(span)) {
            delegate.renameColumn(session, tableHandle, source, target);
        }
    }

    @Override
    public void renameField(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> fieldPath, String target)
    {
        Span span = startSpan("renameField", tableHandle);
        try (var _ = scopedSpan(span)) {
            delegate.renameField(session, tableHandle, fieldPath, target);
        }
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        Span span = startSpan("dropColumn", tableHandle);
        try (var _ = scopedSpan(span)) {
            delegate.dropColumn(session, tableHandle, column);
        }
    }

    @Override
    public void dropField(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, List<String> fieldPath)
    {
        Span span = startSpan("dropField", tableHandle);
        try (var _ = scopedSpan(span)) {
            delegate.dropField(session, tableHandle, column, fieldPath);
        }
    }

    @Override
    public Optional<ConnectorTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        Span span = startSpan("getNewTableLayout", tableMetadata.getTable());
        try (var _ = scopedSpan(span)) {
            return delegate.getNewTableLayout(session, tableMetadata);
        }
    }

    @Override
    public Optional<Type> getSupportedType(ConnectorSession session, Map<String, Object> tableProperties, Type type)
    {
        Span span = startSpan("getSupportedType");
        try (var _ = scopedSpan(span)) {
            return delegate.getSupportedType(session, tableProperties, type);
        }
    }

    @Override
    public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        Span span = startSpan("getInsertLayout", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.getInsertLayout(session, tableHandle);
        }
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        Span span = startSpan("getStatisticsCollectionMetadataForWrite", tableMetadata.getTable());
        try (var _ = scopedSpan(span)) {
            return delegate.getStatisticsCollectionMetadataForWrite(session, tableMetadata);
        }
    }

    @Override
    public ConnectorAnalyzeMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Object> analyzeProperties)
    {
        Span span = startSpan("getStatisticsCollectionMetadata", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.getStatisticsCollectionMetadata(session, tableHandle, analyzeProperties);
        }
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        Span span = startSpan("beginStatisticsCollection", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.beginStatisticsCollection(session, tableHandle);
        }
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        Span span = startSpan("finishStatisticsCollection", tableHandle);
        try (var _ = scopedSpan(span)) {
            delegate.finishStatisticsCollection(session, tableHandle, computedStatistics);
        }
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
    {
        Span span = startSpan("beginCreateTable", tableMetadata.getTable());
        try (var _ = scopedSpan(span)) {
            return delegate.beginCreateTable(session, tableMetadata, layout, retryMode, replace);
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        Span span = startSpan("finishCreateTable");
        if (span.isRecording()) {
            span.setAttribute(TrinoAttributes.HANDLE, tableHandle.toString());
        }
        try (var _ = scopedSpan(span)) {
            return delegate.finishCreateTable(session, tableHandle, fragments, computedStatistics);
        }
    }

    @Override
    public void beginQuery(ConnectorSession session)
    {
        Span span = startSpan("beginQuery");
        try (var _ = scopedSpan(span)) {
            delegate.beginQuery(session);
        }
    }

    @Override
    public void cleanupQuery(ConnectorSession session)
    {
        Span span = startSpan("cleanupQuery");
        try (var _ = scopedSpan(span)) {
            delegate.cleanupQuery(session);
        }
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        Span span = startSpan("beginInsert", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.beginInsert(session, tableHandle, columns, retryMode);
        }
    }

    @Override
    public boolean supportsMissingColumnsOnInsert()
    {
        Span span = startSpan("supportsMissingColumnsOnInsert");
        try (var _ = scopedSpan(span)) {
            return delegate.supportsMissingColumnsOnInsert();
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, List<ConnectorTableHandle> sourceTableHandles, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        Span span = startSpan("finishInsert");
        if (span.isRecording()) {
            span.setAttribute(TrinoAttributes.HANDLE, insertHandle.toString());
        }
        try (var _ = scopedSpan(span)) {
            return delegate.finishInsert(session, insertHandle, sourceTableHandles, fragments, computedStatistics);
        }
    }

    @Override
    public boolean delegateMaterializedViewRefreshToConnector(ConnectorSession session, SchemaTableName viewName)
    {
        Span span = startSpan("delegateMaterializedViewRefreshToConnector", viewName);
        try (var _ = scopedSpan(span)) {
            return delegate.delegateMaterializedViewRefreshToConnector(session, viewName);
        }
    }

    @Override
    public CompletableFuture<?> refreshMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        Span span = startSpan("refreshMaterializedView", viewName);
        try (var _ = scopedSpan(span)) {
            return delegate.refreshMaterializedView(session, viewName);
        }
    }

    @Override
    public ConnectorInsertTableHandle beginRefreshMaterializedView(ConnectorSession session, ConnectorTableHandle tableHandle, List<ConnectorTableHandle> sourceTableHandles, RetryMode retryMode, RefreshType refreshType)
    {
        Span span = startSpan("beginRefreshMaterializedView", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.beginRefreshMaterializedView(session, tableHandle, sourceTableHandles, retryMode, refreshType);
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics,
            List<ConnectorTableHandle> sourceTableHandles,
            List<String> sourceTableFunctions)
    {
        Span span = startSpan("finishRefreshMaterializedView", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.finishRefreshMaterializedView(session, tableHandle, insertHandle, fragments, computedStatistics, sourceTableHandles, sourceTableFunctions);
        }
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        Span span = startSpan("getRowChangeParadigm", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.getRowChangeParadigm(session, tableHandle);
        }
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        Span span = startSpan("getMergeRowIdColumnHandle", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.getMergeRowIdColumnHandle(session, tableHandle);
        }
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getUpdateLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        Span span = startSpan("getUpdateLayout", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.getUpdateLayout(session, tableHandle);
        }
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle, RetryMode retryMode)
    {
        Span span = startSpan("beginMerge", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.beginMerge(session, tableHandle, retryMode);
        }
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle, Map<Integer, Collection<ColumnHandle>> updateCaseColumns, RetryMode retryMode)
    {
        Span span = startSpan("beginMerge", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.beginMerge(session, tableHandle, updateCaseColumns, retryMode);
        }
    }

    @Override
    public void finishMerge(ConnectorSession session, ConnectorMergeTableHandle tableHandle, List<ConnectorTableHandle> sourceTableHandles, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        Span span = startSpan("finishMerge", tableHandle.getTableHandle());
        try (var ignored = scopedSpan(span)) {
            delegate.finishMerge(session, tableHandle, sourceTableHandles, fragments, computedStatistics);
        }
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, Map<String, Object> viewProperties, boolean replace)
    {
        Span span = startSpan("createView", viewName);
        try (var _ = scopedSpan(span)) {
            delegate.createView(session, viewName, definition, viewProperties, replace);
        }
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        Span span = startSpan("renameView", source);
        try (var _ = scopedSpan(span)) {
            delegate.renameView(session, source, target);
        }
    }

    @Override
    public void setViewAuthorization(ConnectorSession session, SchemaTableName viewName, TrinoPrincipal principal)
    {
        Span span = startSpan("setViewAuthorization", viewName);
        try (var _ = scopedSpan(span)) {
            delegate.setViewAuthorization(session, viewName, principal);
        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        Span span = startSpan("dropView", viewName);
        try (var _ = scopedSpan(span)) {
            delegate.dropView(session, viewName);
        }
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        Span span = startSpan("listViews", schemaName);
        try (var _ = scopedSpan(span)) {
            return delegate.listViews(session, schemaName);
        }
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        Span span = startSpan("getViews", schemaName);
        try (var _ = scopedSpan(span)) {
            return delegate.getViews(session, schemaName);
        }
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        Span span = startSpan("getView", viewName);
        try (var _ = scopedSpan(span)) {
            return delegate.getView(session, viewName);
        }
    }

    @Override
    public boolean isView(ConnectorSession session, SchemaTableName viewName)
    {
        Span span = startSpan("isView", viewName);
        try (var _ = scopedSpan(span)) {
            return delegate.isView(session, viewName);
        }
    }

    @Override
    public Map<String, Object> getViewProperties(ConnectorSession session, SchemaTableName viewName)
    {
        Span span = startSpan("getViewProperties", viewName);
        try (var _ = scopedSpan(span)) {
            return delegate.getViewProperties(session, viewName);
        }
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, String schemaName)
    {
        Span span = startSpan("getSchemaProperties", schemaName);
        try (var _ = scopedSpan(span)) {
            return delegate.getSchemaProperties(session, schemaName);
        }
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(ConnectorSession session, String schemaName)
    {
        Span span = startSpan("getSchemaOwner", schemaName);
        try (var _ = scopedSpan(span)) {
            return delegate.getSchemaOwner(session, schemaName);
        }
    }

    @Override
    public Optional<ConnectorTableHandle> applyUpdate(ConnectorSession session, ConnectorTableHandle handle, Map<ColumnHandle, Constant> assignments)
    {
        Span span = startSpan("applyUpdate", handle);
        try (var _ = scopedSpan(span)) {
            return delegate.applyUpdate(session, handle, assignments);
        }
    }

    @Override
    public OptionalLong executeUpdate(ConnectorSession session, ConnectorTableHandle handle)
    {
        Span span = startSpan("executeUpdate", handle);
        try (var _ = scopedSpan(span)) {
            return delegate.executeUpdate(session, handle);
        }
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        Span span = startSpan("applyDelete", handle);
        try (var _ = scopedSpan(span)) {
            return delegate.applyDelete(session, handle);
        }
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        Span span = startSpan("executeDelete", handle);
        try (var _ = scopedSpan(span)) {
            return delegate.executeDelete(session, handle);
        }
    }

    @Override
    public Optional<ConnectorResolvedIndex> resolveIndex(ConnectorSession session, ConnectorTableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        Span span = startSpan("resolveIndex", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.resolveIndex(session, tableHandle, indexableColumns, outputColumns, tupleDomain);
        }
    }

    @Override
    public Collection<FunctionMetadata> listFunctions(ConnectorSession session, String schemaName)
    {
        Span span = startSpan("listFunctions", schemaName);
        try (var _ = scopedSpan(span)) {
            return delegate.listFunctions(session, schemaName);
        }
    }

    @Override
    public Collection<FunctionMetadata> getFunctions(ConnectorSession session, SchemaFunctionName name)
    {
        Span span = startSpan("getFunctions", name.getSchemaName())
                .setAttribute(TrinoAttributes.FUNCTION, name.getFunctionName());
        try (var _ = scopedSpan(span)) {
            return delegate.getFunctions(session, name);
        }
    }

    @Override
    public FunctionMetadata getFunctionMetadata(ConnectorSession session, FunctionId functionId)
    {
        Span span = startSpan("getFunctionMetadata", functionId);
        try (var _ = scopedSpan(span)) {
            return delegate.getFunctionMetadata(session, functionId);
        }
    }

    @Override
    public AggregationFunctionMetadata getAggregationFunctionMetadata(ConnectorSession session, FunctionId functionId)
    {
        Span span = startSpan("getAggregationFunctionMetadata", functionId);
        try (var _ = scopedSpan(span)) {
            return delegate.getAggregationFunctionMetadata(session, functionId);
        }
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies(ConnectorSession session, FunctionId functionId, BoundSignature boundSignature)
    {
        Span span = startSpan("getFunctionDependencies", functionId);
        try (var _ = scopedSpan(span)) {
            return delegate.getFunctionDependencies(session, functionId, boundSignature);
        }
    }

    @Override
    public Collection<LanguageFunction> listLanguageFunctions(ConnectorSession session, String schemaName)
    {
        Span span = startSpan("listLanguageFunctions", schemaName);
        try (var _ = scopedSpan(span)) {
            return delegate.listLanguageFunctions(session, schemaName);
        }
    }

    @Override
    public Collection<LanguageFunction> getLanguageFunctions(ConnectorSession session, SchemaFunctionName name)
    {
        Span span = startSpan("getLanguageFunctions", name.getSchemaName())
                .setAttribute(TrinoAttributes.FUNCTION, name.getFunctionName());
        try (var _ = scopedSpan(span)) {
            return delegate.getLanguageFunctions(session, name);
        }
    }

    @Override
    public boolean languageFunctionExists(ConnectorSession session, SchemaFunctionName name, String signatureToken)
    {
        Span span = startSpan("languageFunctionExists", name.getSchemaName())
                .setAttribute(TrinoAttributes.FUNCTION, name.getFunctionName());
        try (var _ = scopedSpan(span)) {
            return delegate.languageFunctionExists(session, name, signatureToken);
        }
    }

    @Override
    public void createLanguageFunction(ConnectorSession session, SchemaFunctionName name, LanguageFunction function, boolean replace)
    {
        Span span = startSpan("createLanguageFunction", name.getSchemaName())
                .setAttribute(TrinoAttributes.FUNCTION, name.getFunctionName());
        try (var _ = scopedSpan(span)) {
            delegate.createLanguageFunction(session, name, function, replace);
        }
    }

    @Override
    public void dropLanguageFunction(ConnectorSession session, SchemaFunctionName name, String signatureToken)
    {
        Span span = startSpan("dropLanguageFunction", name.getSchemaName())
                .setAttribute(TrinoAttributes.FUNCTION, name.getFunctionName());
        try (var _ = scopedSpan(span)) {
            delegate.dropLanguageFunction(session, name, signatureToken);
        }
    }

    @Override
    public boolean roleExists(ConnectorSession session, String role)
    {
        Span span = startSpan("roleExists");
        try (var _ = scopedSpan(span)) {
            return delegate.roleExists(session, role);
        }
    }

    @Override
    public void createRole(ConnectorSession session, String role, Optional<TrinoPrincipal> grantor)
    {
        Span span = startSpan("createRole");
        try (var _ = scopedSpan(span)) {
            delegate.createRole(session, role, grantor);
        }
    }

    @Override
    public void dropRole(ConnectorSession session, String role)
    {
        Span span = startSpan("dropRole");
        try (var _ = scopedSpan(span)) {
            delegate.dropRole(session, role);
        }
    }

    @Override
    public Set<String> listRoles(ConnectorSession session)
    {
        Span span = startSpan("listRoles");
        try (var _ = scopedSpan(span)) {
            return delegate.listRoles(session);
        }
    }

    @Override
    public Set<RoleGrant> listRoleGrants(ConnectorSession session, TrinoPrincipal principal)
    {
        Span span = startSpan("listRoleGrants");
        try (var _ = scopedSpan(span)) {
            return delegate.listRoleGrants(session, principal);
        }
    }

    @Override
    public void grantRoles(ConnectorSession connectorSession, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        Span span = startSpan("grantRoles");
        try (var _ = scopedSpan(span)) {
            delegate.grantRoles(connectorSession, roles, grantees, adminOption, grantor);
        }
    }

    @Override
    public void revokeRoles(ConnectorSession connectorSession, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        Span span = startSpan("revokeRoles");
        try (var _ = scopedSpan(span)) {
            delegate.revokeRoles(connectorSession, roles, grantees, adminOption, grantor);
        }
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(ConnectorSession session, TrinoPrincipal principal)
    {
        Span span = startSpan("listApplicableRoles");
        try (var _ = scopedSpan(span)) {
            return delegate.listApplicableRoles(session, principal);
        }
    }

    @Override
    public Set<String> listEnabledRoles(ConnectorSession session)
    {
        Span span = startSpan("listEnabledRoles");
        try (var _ = scopedSpan(span)) {
            return delegate.listEnabledRoles(session);
        }
    }

    @Override
    public void grantSchemaPrivileges(ConnectorSession session, String schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        Span span = startSpan("grantSchemaPrivileges", schemaName);
        try (var _ = scopedSpan(span)) {
            delegate.grantSchemaPrivileges(session, schemaName, privileges, grantee, grantOption);
        }
    }

    @Override
    public void denySchemaPrivileges(ConnectorSession session, String schemaName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        Span span = startSpan("denySchemaPrivileges", schemaName);
        try (var _ = scopedSpan(span)) {
            delegate.denySchemaPrivileges(session, schemaName, privileges, grantee);
        }
    }

    @Override
    public void revokeSchemaPrivileges(ConnectorSession session, String schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        Span span = startSpan("revokeSchemaPrivileges", schemaName);
        try (var _ = scopedSpan(span)) {
            delegate.revokeSchemaPrivileges(session, schemaName, privileges, grantee, grantOption);
        }
    }

    @Override
    public void grantTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        Span span = startSpan("grantTablePrivileges", tableName);
        try (var _ = scopedSpan(span)) {
            delegate.grantTablePrivileges(session, tableName, privileges, grantee, grantOption);
        }
    }

    @Override
    public void denyTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        Span span = startSpan("denyTablePrivileges", tableName);
        try (var _ = scopedSpan(span)) {
            delegate.denyTablePrivileges(session, tableName, privileges, grantee);
        }
    }

    @Override
    public void revokeTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        Span span = startSpan("revokeTablePrivileges", tableName);
        try (var _ = scopedSpan(span)) {
            delegate.revokeTablePrivileges(session, tableName, privileges, grantee, grantOption);
        }
    }

    @Override
    public List<GrantInfo> listTablePrivileges(ConnectorSession session, SchemaTablePrefix prefix)
    {
        Span span = startSpan("listTablePrivileges", prefix);
        try (var _ = scopedSpan(span)) {
            return delegate.listTablePrivileges(session, prefix);
        }
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        Span span = startSpan("getTableProperties", table);
        try (var _ = scopedSpan(span)) {
            return delegate.getTableProperties(session, table);
        }
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit)
    {
        Span span = startSpan("applyLimit", handle);
        try (var _ = scopedSpan(span)) {
            return delegate.applyLimit(session, handle, limit);
        }
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        Span span = startSpan("applyFilter", handle);
        try (var _ = scopedSpan(span)) {
            return delegate.applyFilter(session, handle, constraint);
        }
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session, ConnectorTableHandle handle, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        Span span = startSpan("applyProjection", handle);
        try (var _ = scopedSpan(span)) {
            return delegate.applyProjection(session, handle, projections, assignments);
        }
    }

    @Override
    public Optional<SampleApplicationResult<ConnectorTableHandle>> applySample(ConnectorSession session, ConnectorTableHandle handle, SampleType sampleType, double sampleRatio)
    {
        Span span = startSpan("applySample", handle);
        try (var _ = scopedSpan(span)) {
            return delegate.applySample(session, handle, sampleType, sampleRatio);
        }
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(ConnectorSession session, ConnectorTableHandle handle, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        Span span = startSpan("applyAggregation", handle);
        try (var _ = scopedSpan(span)) {
            return delegate.applyAggregation(session, handle, aggregates, assignments, groupingSets);
        }
    }

    @Override
    public Optional<JoinApplicationResult<ConnectorTableHandle>> applyJoin(ConnectorSession session, JoinType joinType, ConnectorTableHandle left, ConnectorTableHandle right, ConnectorExpression joinCondition, Map<String, ColumnHandle> leftAssignments, Map<String, ColumnHandle> rightAssignments, JoinStatistics statistics)
    {
        Span span = startSpan("applyJoin");
        try (var _ = scopedSpan(span)) {
            return delegate.applyJoin(session, joinType, left, right, joinCondition, leftAssignments, rightAssignments, statistics);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public Optional<JoinApplicationResult<ConnectorTableHandle>> applyJoin(ConnectorSession session, JoinType joinType, ConnectorTableHandle left, ConnectorTableHandle right, List<JoinCondition> joinConditions, Map<String, ColumnHandle> leftAssignments, Map<String, ColumnHandle> rightAssignments, JoinStatistics statistics)
    {
        Span span = startSpan("applyJoin");
        try (var _ = scopedSpan(span)) {
            return delegate.applyJoin(session, joinType, left, right, joinConditions, leftAssignments, rightAssignments, statistics);
        }
    }

    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(ConnectorSession session, ConnectorTableHandle handle, long topNCount, List<SortItem> sortItems, Map<String, ColumnHandle> assignments)
    {
        Span span = startSpan("applyTopN", handle);
        try (var _ = scopedSpan(span)) {
            return delegate.applyTopN(session, handle, topNCount, sortItems, assignments);
        }
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
    {
        Span span = startSpan("applyTableFunction");
        try (var _ = scopedSpan(span)) {
            return delegate.applyTableFunction(session, handle);
        }
    }

    @Override
    public void validateScan(ConnectorSession session, ConnectorTableHandle handle)
    {
        Span span = startSpan("validateScan", handle);
        try (var _ = scopedSpan(span)) {
            delegate.validateScan(session, handle);
        }
    }

    @Override
    public void createMaterializedView(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> properties,
            boolean replace,
            boolean ignoreExisting)
    {
        Span span = startSpan("createMaterializedView", viewName);
        try (var _ = scopedSpan(span)) {
            delegate.createMaterializedView(session, viewName, definition, properties, replace, ignoreExisting);
        }
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        Span span = startSpan("dropMaterializedView", viewName);
        try (var _ = scopedSpan(span)) {
            delegate.dropMaterializedView(session, viewName);
        }
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        Span span = startSpan("listMaterializedViews", schemaName);
        try (var _ = scopedSpan(span)) {
            return delegate.listMaterializedViews(session, schemaName);
        }
    }

    @Override
    public Map<SchemaTableName, ConnectorMaterializedViewDefinition> getMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        Span span = startSpan("getMaterializedViews", schemaName);
        try (var _ = scopedSpan(span)) {
            return delegate.getMaterializedViews(session, schemaName);
        }
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        Span span = startSpan("getMaterializedView", viewName);
        try (var _ = scopedSpan(span)) {
            return delegate.getMaterializedView(session, viewName);
        }
    }

    @Override
    public Map<String, Object> getMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition materializedViewDefinition)
    {
        Span span = startSpan("getMaterializedViewProperties", viewName);
        try (var _ = scopedSpan(span)) {
            return delegate.getMaterializedViewProperties(session, viewName, materializedViewDefinition);
        }
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session, SchemaTableName name)
    {
        Span span = startSpan("getMaterializedViewFreshness", name);
        try (var _ = scopedSpan(span)) {
            return delegate.getMaterializedViewFreshness(session, name);
        }
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        Span span = startSpan("renameMaterializedView", source);
        try (var _ = scopedSpan(span)) {
            delegate.renameMaterializedView(session, source, target);
        }
    }

    @Override
    public void setMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, Map<String, Optional<Object>> properties)
    {
        Span span = startSpan("setMaterializedViewProperties", viewName);
        try (var _ = scopedSpan(span)) {
            delegate.setMaterializedViewProperties(session, viewName, properties);
        }
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        Span span = startSpan("applyTableScanRedirect", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.applyTableScanRedirect(session, tableHandle);
        }
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName)
    {
        Span span = startSpan("redirectTable", tableName);
        try (var _ = scopedSpan(span)) {
            return delegate.redirectTable(session, tableName);
        }
    }

    @Override
    public OptionalInt getMaxWriterTasks(ConnectorSession session)
    {
        Span span = startSpan("getMaxWriterTasks");
        try (var _ = scopedSpan(span)) {
            return delegate.getMaxWriterTasks(session);
        }
    }

    @Override
    public boolean allowSplittingReadIntoMultipleSubQueries(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        Span span = startSpan("allowSplittingReadIntoMultipleSubQueries");
        try (var ignored = scopedSpan(span)) {
            return delegate.allowSplittingReadIntoMultipleSubQueries(session, tableHandle);
        }
    }

    @Override
    public WriterScalingOptions getNewTableWriterScalingOptions(ConnectorSession session, SchemaTableName tableName, Map<String, Object> tableProperties)
    {
        Span span = startSpan("getNewTableWriterScalingOptions", tableName);
        try (var _ = scopedSpan(span)) {
            return delegate.getNewTableWriterScalingOptions(session, tableName, tableProperties);
        }
    }

    @Override
    public WriterScalingOptions getInsertWriterScalingOptions(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        Span span = startSpan("getInsertWriterScalingOptions", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.getInsertWriterScalingOptions(session, tableHandle);
        }
    }

    private Span startSpan(String methodName)
    {
        return tracer.spanBuilder("ConnectorMetadata." + methodName)
                .setAttribute(TrinoAttributes.CATALOG, catalogName)
                .startSpan();
    }

    private Span startSpan(String methodName, String schemaName)
    {
        return startSpan(methodName)
                .setAttribute(TrinoAttributes.SCHEMA, schemaName);
    }

    private Span startSpan(String methodName, Optional<String> schemaName)
    {
        return startSpan(methodName)
                .setAttribute(TrinoAttributes.SCHEMA, schemaName.orElse(null));
    }

    private Span startSpan(String methodName, SchemaTableName table)
    {
        return startSpan(methodName)
                .setAttribute(TrinoAttributes.SCHEMA, table.getSchemaName())
                .setAttribute(TrinoAttributes.TABLE, table.getTableName());
    }

    private Span startSpan(String methodName, SchemaTablePrefix prefix)
    {
        return startSpan(methodName)
                .setAttribute(TrinoAttributes.SCHEMA, prefix.getSchema().orElse(null))
                .setAttribute(TrinoAttributes.TABLE, prefix.getTable().orElse(null));
    }

    private Span startSpan(String methodName, ConnectorTableHandle handle)
    {
        Span span = startSpan(methodName);
        if (span.isRecording()) {
            span.setAttribute(TrinoAttributes.HANDLE, handle.toString());
        }
        return span;
    }

    private Span startSpan(String methodName, ConnectorTableExecuteHandle handle)
    {
        Span span = startSpan(methodName);
        if (span.isRecording()) {
            span.setAttribute(TrinoAttributes.HANDLE, handle.toString());
        }
        return span;
    }

    private Span startSpan(String methodName, FunctionId functionId)
    {
        Span span = startSpan(methodName);
        if (span.isRecording()) {
            span.setAttribute(TrinoAttributes.FUNCTION, functionId.toString());
        }
        return span;
    }
}
