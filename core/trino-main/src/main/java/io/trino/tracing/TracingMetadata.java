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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.trino.Session;
import io.trino.metadata.AnalyzeMetadata;
import io.trino.metadata.AnalyzeTableHandle;
import io.trino.metadata.CatalogFunctionMetadata;
import io.trino.metadata.CatalogInfo;
import io.trino.metadata.InsertTableHandle;
import io.trino.metadata.MaterializedViewDefinition;
import io.trino.metadata.MergeHandle;
import io.trino.metadata.Metadata;
import io.trino.metadata.OperatorNotFoundException;
import io.trino.metadata.OutputTableHandle;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.metadata.RedirectionAwareTableHandle;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.ResolvedIndex;
import io.trino.metadata.TableExecuteHandle;
import io.trino.metadata.TableFunctionHandle;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableLayout;
import io.trino.metadata.TableMetadata;
import io.trino.metadata.TableProperties;
import io.trino.metadata.TableSchema;
import io.trino.metadata.TableVersion;
import io.trino.metadata.ViewDefinition;
import io.trino.metadata.ViewInfo;
import io.trino.spi.RefreshType;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.BeginTableExecuteResult;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.EntityKindAndName;
import io.trino.spi.connector.EntityPrivilege;
import io.trino.spi.connector.JoinApplicationResult;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.RelationType;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SampleApplicationResult;
import io.trino.spi.connector.SampleType;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
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
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.function.OperatorType;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.Type;
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.sql.planner.PartitioningHandle;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static io.trino.tracing.ScopedSpan.scopedSpan;
import static java.util.Objects.requireNonNull;

public class TracingMetadata
        implements Metadata
{
    private final Tracer tracer;
    private final Metadata delegate;

    @Inject
    public TracingMetadata(Tracer tracer, @ForTracing Metadata delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
    }

    @VisibleForTesting
    public Metadata getDelegate()
    {
        return delegate;
    }

    @Override
    public Set<ConnectorCapabilities> getConnectorCapabilities(Session session, CatalogHandle catalogHandle)
    {
        Span span = startSpan("getConnectorCapabilities", catalogHandle.getCatalogName());
        try (var _ = scopedSpan(span)) {
            return delegate.getConnectorCapabilities(session, catalogHandle);
        }
    }

    @Override
    public boolean catalogExists(Session session, String catalogName)
    {
        Span span = startSpan("catalogExists", catalogName);
        try (var _ = scopedSpan(span)) {
            return delegate.catalogExists(session, catalogName);
        }
    }

    @Override
    public boolean schemaExists(Session session, CatalogSchemaName schema)
    {
        Span span = startSpan("schemaExists", schema);
        try (var _ = scopedSpan(span)) {
            return delegate.schemaExists(session, schema);
        }
    }

    @Override
    public List<String> listSchemaNames(Session session, String catalogName)
    {
        Span span = startSpan("listSchemaNames", catalogName);
        try (var _ = scopedSpan(span)) {
            return delegate.listSchemaNames(session, catalogName);
        }
    }

    @Override
    public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName)
    {
        Span span = startSpan("getTableHandle", tableName);
        try (var _ = scopedSpan(span)) {
            return delegate.getTableHandle(session, tableName);
        }
    }

    @Override
    public Optional<SystemTable> getSystemTable(Session session, QualifiedObjectName tableName)
    {
        Span span = startSpan("getSystemTable", tableName);
        try (var _ = scopedSpan(span)) {
            return delegate.getSystemTable(session, tableName);
        }
    }

    @Override
    public Optional<TableExecuteHandle> getTableHandleForExecute(Session session, TableHandle tableHandle, String procedureName, Map<String, Object> executeProperties)
    {
        Span span = startSpan("getTableHandleForExecute", tableHandle)
                .setAttribute(TrinoAttributes.PROCEDURE, procedureName);
        try (var _ = scopedSpan(span)) {
            return delegate.getTableHandleForExecute(session, tableHandle, procedureName, executeProperties);
        }
    }

    @Override
    public Optional<TableLayout> getLayoutForTableExecute(Session session, TableExecuteHandle tableExecuteHandle)
    {
        Span span = startSpan("getLayoutForTableExecute", tableExecuteHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.getLayoutForTableExecute(session, tableExecuteHandle);
        }
    }

    @Override
    public BeginTableExecuteResult<TableExecuteHandle, TableHandle> beginTableExecute(Session session, TableExecuteHandle handle, TableHandle updatedSourceTableHandle)
    {
        Span span = startSpan("beginTableExecute", handle);
        try (var _ = scopedSpan(span)) {
            return delegate.beginTableExecute(session, handle, updatedSourceTableHandle);
        }
    }

    @Override
    public void finishTableExecute(Session session, TableExecuteHandle handle, Collection<Slice> fragments, List<Object> tableExecuteState)
    {
        Span span = startSpan("finishTableExecute", handle);
        try (var _ = scopedSpan(span)) {
            delegate.finishTableExecute(session, handle, fragments, tableExecuteState);
        }
    }

    @Override
    public void executeTableExecute(Session session, TableExecuteHandle handle)
    {
        Span span = startSpan("executeTableExecute", handle);
        try (var _ = scopedSpan(span)) {
            delegate.executeTableExecute(session, handle);
        }
    }

    @Override
    public TableProperties getTableProperties(Session session, TableHandle handle)
    {
        Span span = startSpan("getTableProperties", handle);
        try (var _ = scopedSpan(span)) {
            return delegate.getTableProperties(session, handle);
        }
    }

    @Override
    public TableHandle makeCompatiblePartitioning(Session session, TableHandle table, PartitioningHandle partitioningHandle)
    {
        Span span = startSpan("makeCompatiblePartitioning", table);
        try (var _ = scopedSpan(span)) {
            return delegate.makeCompatiblePartitioning(session, table, partitioningHandle);
        }
    }

    @Override
    public Optional<PartitioningHandle> getCommonPartitioning(Session session, PartitioningHandle left, PartitioningHandle right)
    {
        Span span = startSpan("getCommonPartitioning");
        if (span.isRecording() && left.getCatalogHandle().equals(right.getCatalogHandle()) && left.getCatalogHandle().isPresent()) {
            span.setAttribute(TrinoAttributes.CATALOG, left.getCatalogHandle().get().getCatalogName().toString());
        }
        try (var _ = scopedSpan(span)) {
            return delegate.getCommonPartitioning(session, left, right);
        }
    }

    @Override
    public Optional<Object> getInfo(Session session, TableHandle handle)
    {
        Span span = startSpan("getInfo", handle);
        try (var _ = scopedSpan(span)) {
            return delegate.getInfo(session, handle);
        }
    }

    @Override
    public CatalogSchemaTableName getTableName(Session session, TableHandle tableHandle)
    {
        Span span = startSpan("getTableName", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.getTableName(session, tableHandle);
        }
    }

    @Override
    public TableSchema getTableSchema(Session session, TableHandle tableHandle)
    {
        Span span = startSpan("getTableSchema", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.getTableSchema(session, tableHandle);
        }
    }

    @Override
    public TableMetadata getTableMetadata(Session session, TableHandle tableHandle)
    {
        Span span = startSpan("getTableMetadata", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.getTableMetadata(session, tableHandle);
        }
    }

    @Override
    public TableStatistics getTableStatistics(Session session, TableHandle tableHandle)
    {
        Span span = startSpan("getTableStatistics", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.getTableStatistics(session, tableHandle);
        }
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, QualifiedTablePrefix prefix)
    {
        Span span = startSpan("listTables", prefix);
        try (var _ = scopedSpan(span)) {
            return delegate.listTables(session, prefix);
        }
    }

    @Override
    public Map<SchemaTableName, RelationType> getRelationTypes(Session session, QualifiedTablePrefix prefix)
    {
        Span span = startSpan("getRelationTypes", prefix);
        try (var _ = scopedSpan(span)) {
            return delegate.getRelationTypes(session, prefix);
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(Session session, TableHandle tableHandle)
    {
        Span span = startSpan("getColumnHandles", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.getColumnHandles(session, tableHandle);
        }
    }

    @Override
    public ColumnMetadata getColumnMetadata(Session session, TableHandle tableHandle, ColumnHandle columnHandle)
    {
        Span span = startSpan("getColumnMetadata", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.getColumnMetadata(session, tableHandle, columnHandle);
        }
    }

    @Override
    public List<TableColumnsMetadata> listTableColumns(Session session, QualifiedTablePrefix prefix, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        Span span = startSpan("listTableColumns", prefix);
        try (var _ = scopedSpan(span)) {
            return delegate.listTableColumns(session, prefix, relationFilter);
        }
    }

    @Override
    public List<RelationCommentMetadata> listRelationComments(Session session, String catalogName, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        Span span = startSpan("listRelationComments", new QualifiedTablePrefix(catalogName, schemaName, Optional.empty()));
        try (var _ = scopedSpan(span)) {
            return delegate.listRelationComments(session, catalogName, schemaName, relationFilter);
        }
    }

    @Override
    public void createSchema(Session session, CatalogSchemaName schema, Map<String, Object> properties, TrinoPrincipal principal)
    {
        Span span = startSpan("createSchema", schema);
        try (var _ = scopedSpan(span)) {
            delegate.createSchema(session, schema, properties, principal);
        }
    }

    @Override
    public void dropSchema(Session session, CatalogSchemaName schema, boolean cascade)
    {
        Span span = startSpan("dropSchema", schema);
        try (var _ = scopedSpan(span)) {
            delegate.dropSchema(session, schema, cascade);
        }
    }

    @Override
    public void renameSchema(Session session, CatalogSchemaName source, String target)
    {
        Span span = startSpan("renameSchema", source);
        try (var _ = scopedSpan(span)) {
            delegate.renameSchema(session, source, target);
        }
    }

    @Override
    public void setSchemaAuthorization(Session session, CatalogSchemaName source, TrinoPrincipal principal)
    {
        Span span = startSpan("setSchemaAuthorization", source);
        try (var _ = scopedSpan(span)) {
            delegate.setSchemaAuthorization(session, source, principal);
        }
    }

    @Override
    public void createTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        Span span = startSpan("createTable", catalogName, tableMetadata);
        try (var _ = scopedSpan(span)) {
            delegate.createTable(session, catalogName, tableMetadata, saveMode);
        }
    }

    @Override
    public void renameTable(Session session, TableHandle tableHandle, CatalogSchemaTableName currentTableName, QualifiedObjectName newTableName)
    {
        Span span = startSpan("renameTable", currentTableName);
        try (var _ = scopedSpan(span)) {
            delegate.renameTable(session, tableHandle, currentTableName, newTableName);
        }
    }

    @Override
    public void setTableProperties(Session session, TableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        Span span = startSpan("setTableProperties", tableHandle);
        try (var _ = scopedSpan(span)) {
            delegate.setTableProperties(session, tableHandle, properties);
        }
    }

    @Override
    public void setTableComment(Session session, TableHandle tableHandle, Optional<String> comment)
    {
        Span span = startSpan("setTableComment", tableHandle);
        try (var _ = scopedSpan(span)) {
            delegate.setTableComment(session, tableHandle, comment);
        }
    }

    @Override
    public void setViewComment(Session session, QualifiedObjectName viewName, Optional<String> comment)
    {
        Span span = startSpan("setViewComment", viewName);
        try (var _ = scopedSpan(span)) {
            delegate.setViewComment(session, viewName, comment);
        }
    }

    @Override
    public void setViewColumnComment(Session session, QualifiedObjectName viewName, String columnName, Optional<String> comment)
    {
        Span span = startSpan("setViewColumnComment", viewName);
        try (var _ = scopedSpan(span)) {
            delegate.setViewColumnComment(session, viewName, columnName, comment);
        }
    }

    @Override
    public void setColumnComment(Session session, TableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        Span span = startSpan("setColumnComment", tableHandle);
        try (var _ = scopedSpan(span)) {
            delegate.setColumnComment(session, tableHandle, column, comment);
        }
    }

    @Override
    public void renameColumn(Session session, TableHandle tableHandle, CatalogSchemaTableName table, ColumnHandle source, String target)
    {
        Span span = startSpan("renameColumn", table);
        try (var _ = scopedSpan(span)) {
            delegate.renameColumn(session, tableHandle, table, source, target);
        }
    }

    @Override
    public void renameField(Session session, TableHandle tableHandle, List<String> fieldPath, String target)
    {
        Span span = startSpan("renameField", tableHandle);
        try (var _ = scopedSpan(span)) {
            delegate.renameField(session, tableHandle, fieldPath, target);
        }
    }

    @Override
    public void addColumn(Session session, TableHandle tableHandle, CatalogSchemaTableName table, ColumnMetadata column)
    {
        Span span = startSpan("addColumn", table);
        try (var _ = scopedSpan(span)) {
            delegate.addColumn(session, tableHandle, table, column);
        }
    }

    @Override
    public void addField(Session session, TableHandle tableHandle, List<String> parentPath, String fieldName, Type type, boolean ignoreExisting)
    {
        Span span = startSpan("addField", tableHandle);
        try (var _ = scopedSpan(span)) {
            delegate.addField(session, tableHandle, parentPath, fieldName, type, ignoreExisting);
        }
    }

    @Override
    public void setColumnType(Session session, TableHandle tableHandle, ColumnHandle column, Type type)
    {
        Span span = startSpan("setColumnType", tableHandle);
        try (var _ = scopedSpan(span)) {
            delegate.setColumnType(session, tableHandle, column, type);
        }
    }

    @Override
    public void setFieldType(Session session, TableHandle tableHandle, List<String> fieldPath, Type type)
    {
        Span span = startSpan("setFieldType", tableHandle);
        try (var _ = scopedSpan(span)) {
            delegate.setFieldType(session, tableHandle, fieldPath, type);
        }
    }

    @Override
    public void dropNotNullConstraint(Session session, TableHandle tableHandle, ColumnHandle column)
    {
        Span span = startSpan("dropNotNullConstraint", tableHandle);
        try (var _ = scopedSpan(span)) {
            delegate.dropNotNullConstraint(session, tableHandle, column);
        }
    }

    @Override
    public void setTableAuthorization(Session session, CatalogSchemaTableName table, TrinoPrincipal principal)
    {
        Span span = startSpan("setTableAuthorization", table);
        try (var _ = scopedSpan(span)) {
            delegate.setTableAuthorization(session, table, principal);
        }
    }

    @Override
    public void dropColumn(Session session, TableHandle tableHandle, CatalogSchemaTableName table, ColumnHandle column)
    {
        Span span = startSpan("dropColumn", table);
        try (var _ = scopedSpan(span)) {
            delegate.dropColumn(session, tableHandle, table, column);
        }
    }

    @Override
    public void dropField(Session session, TableHandle tableHandle, ColumnHandle column, List<String> fieldPath)
    {
        Span span = startSpan("dropField", tableHandle);
        try (var _ = scopedSpan(span)) {
            delegate.dropField(session, tableHandle, column, fieldPath);
        }
    }

    @Override
    public void dropTable(Session session, TableHandle tableHandle, CatalogSchemaTableName tableName)
    {
        Span span = startSpan("dropTable", tableName);
        try (var _ = scopedSpan(span)) {
            delegate.dropTable(session, tableHandle, tableName);
        }
    }

    @Override
    public void truncateTable(Session session, TableHandle tableHandle)
    {
        Span span = startSpan("truncateTable", tableHandle);
        try (var _ = scopedSpan(span)) {
            delegate.truncateTable(session, tableHandle);
        }
    }

    @Override
    public Optional<TableLayout> getNewTableLayout(Session session, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        Span span = startSpan("getNewTableLayout", catalogName, tableMetadata);
        try (var _ = scopedSpan(span)) {
            return delegate.getNewTableLayout(session, catalogName, tableMetadata);
        }
    }

    @Override
    public Optional<Type> getSupportedType(Session session, CatalogHandle catalogHandle, Map<String, Object> tableProperties, Type type)
    {
        Span span = startSpan("getSupportedType", catalogHandle.getCatalogName());
        try (var _ = scopedSpan(span)) {
            return delegate.getSupportedType(session, catalogHandle, tableProperties, type);
        }
    }

    @Override
    public OutputTableHandle beginCreateTable(Session session, String catalogName, ConnectorTableMetadata tableMetadata, Optional<TableLayout> layout, boolean replace)
    {
        Span span = startSpan("beginCreateTable", catalogName, tableMetadata);
        try (var _ = scopedSpan(span)) {
            return delegate.beginCreateTable(session, catalogName, tableMetadata, layout, replace);
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(Session session, OutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        Span span = startSpan("finishCreateTable", tableHandle.catalogHandle().getCatalogName());
        if (span.isRecording()) {
            span.setAttribute(TrinoAttributes.TABLE, tableHandle.connectorHandle().toString());
        }
        try (var _ = scopedSpan(span)) {
            return delegate.finishCreateTable(session, tableHandle, fragments, computedStatistics);
        }
    }

    @Override
    public Optional<TableLayout> getInsertLayout(Session session, TableHandle target)
    {
        Span span = startSpan("getInsertLayout", target);
        try (var _ = scopedSpan(span)) {
            return delegate.getInsertLayout(session, target);
        }
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(Session session, CatalogHandle catalogHandle, ConnectorTableMetadata tableMetadata)
    {
        Span span = startSpan("getStatisticsCollectionMetadataForWrite", catalogHandle.getCatalogName().toString(), tableMetadata);
        try (var _ = scopedSpan(span)) {
            return delegate.getStatisticsCollectionMetadataForWrite(session, catalogHandle, tableMetadata);
        }
    }

    @Override
    public AnalyzeMetadata getStatisticsCollectionMetadata(Session session, TableHandle tableHandle, Map<String, Object> analyzeProperties)
    {
        Span span = startSpan("getStatisticsCollectionMetadata", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.getStatisticsCollectionMetadata(session, tableHandle, analyzeProperties);
        }
    }

    @Override
    public AnalyzeTableHandle beginStatisticsCollection(Session session, TableHandle tableHandle)
    {
        Span span = startSpan("beginStatisticsCollection", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.beginStatisticsCollection(session, tableHandle);
        }
    }

    @Override
    public void finishStatisticsCollection(Session session, AnalyzeTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        Span span = startSpan("finishStatisticsCollection", tableHandle.catalogHandle().getCatalogName());
        if (span.isRecording()) {
            span.setAttribute(TrinoAttributes.TABLE, tableHandle.connectorHandle().toString());
        }
        try (var _ = scopedSpan(span)) {
            delegate.finishStatisticsCollection(session, tableHandle, computedStatistics);
        }
    }

    @Override
    public void beginQuery(Session session)
    {
        Span span = startSpan("beginQuery");
        try (var _ = scopedSpan(span)) {
            delegate.beginQuery(session);
        }
    }

    @Override
    public void cleanupQuery(Session session)
    {
        Span span = startSpan("cleanupQuery");
        try (var _ = scopedSpan(span)) {
            delegate.cleanupQuery(session);
        }
    }

    @Override
    public InsertTableHandle beginInsert(Session session, TableHandle tableHandle, List<ColumnHandle> columns)
    {
        Span span = startSpan("beginInsert", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.beginInsert(session, tableHandle, columns);
        }
    }

    @Override
    public boolean supportsMissingColumnsOnInsert(Session session, TableHandle tableHandle)
    {
        Span span = startSpan("supportsMissingColumnsOnInsert", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.supportsMissingColumnsOnInsert(session, tableHandle);
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(Session session, InsertTableHandle tableHandle, List<TableHandle> sourceTableHandles, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        Span span = startSpan("finishInsert", tableHandle.catalogHandle().getCatalogName());
        if (span.isRecording()) {
            span.setAttribute(TrinoAttributes.TABLE, tableHandle.connectorHandle().toString());
        }
        try (var _ = scopedSpan(span)) {
            return delegate.finishInsert(session, tableHandle, sourceTableHandles, fragments, computedStatistics);
        }
    }

    @Override
    public boolean delegateMaterializedViewRefreshToConnector(Session session, QualifiedObjectName viewName)
    {
        Span span = startSpan("delegateMaterializedViewRefreshToConnector", viewName);
        try (var _ = scopedSpan(span)) {
            return delegate.delegateMaterializedViewRefreshToConnector(session, viewName);
        }
    }

    @Override
    public ListenableFuture<Void> refreshMaterializedView(Session session, QualifiedObjectName viewName)
    {
        Span span = startSpan("refreshMaterializedView", viewName);
        try (var _ = scopedSpan(span)) {
            return delegate.refreshMaterializedView(session, viewName);
        }
    }

    @Override
    public InsertTableHandle beginRefreshMaterializedView(Session session, TableHandle tableHandle, List<TableHandle> sourceTableHandles, RefreshType refreshType)
    {
        Span span = startSpan("beginRefreshMaterializedView", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.beginRefreshMaterializedView(session, tableHandle, sourceTableHandles, refreshType);
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(
            Session session,
            TableHandle tableHandle,
            InsertTableHandle insertTableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics,
            List<TableHandle> sourceTableHandles,
            List<String> sourceTableFunctions)
    {
        Span span = startSpan("finishRefreshMaterializedView", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.finishRefreshMaterializedView(
                    session,
                    tableHandle,
                    insertTableHandle,
                    fragments,
                    computedStatistics,
                    sourceTableHandles,
                    sourceTableFunctions);
        }
    }

    @Override
    public Optional<TableHandle> applyUpdate(Session session, TableHandle tableHandle, Map<ColumnHandle, Constant> assignments)
    {
        Span span = startSpan("applyUpdate", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.applyUpdate(session, tableHandle, assignments);
        }
    }

    @Override
    public OptionalLong executeUpdate(Session session, TableHandle tableHandle)
    {
        Span span = startSpan("executeUpdate", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.executeUpdate(session, tableHandle);
        }
    }

    @Override
    public Optional<TableHandle> applyDelete(Session session, TableHandle tableHandle)
    {
        Span span = startSpan("applyDelete", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.applyDelete(session, tableHandle);
        }
    }

    @Override
    public OptionalLong executeDelete(Session session, TableHandle tableHandle)
    {
        Span span = startSpan("executeDelete", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.executeDelete(session, tableHandle);
        }
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(Session session, TableHandle tableHandle)
    {
        Span span = startSpan("getRowChangeParadigm", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.getRowChangeParadigm(session, tableHandle);
        }
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(Session session, TableHandle tableHandle)
    {
        Span span = startSpan("getMergeRowIdColumnHandle", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.getMergeRowIdColumnHandle(session, tableHandle);
        }
    }

    @Override
    public Optional<PartitioningHandle> getUpdateLayout(Session session, TableHandle tableHandle)
    {
        Span span = startSpan("getUpdateLayout", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.getUpdateLayout(session, tableHandle);
        }
    }

    @Override
    public MergeHandle beginMerge(Session session, TableHandle tableHandle, Multimap<Integer, ColumnHandle> updateCaseColumns)
    {
        Span span = startSpan("beginMerge", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.beginMerge(session, tableHandle, updateCaseColumns);
        }
    }

    @Override
    public void finishMerge(Session session, MergeHandle tableHandle, List<TableHandle> sourceTableHandles, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        Span span = startSpan("finishMerge", tableHandle.tableHandle().catalogHandle().getCatalogName());
        if (span.isRecording()) {
            span.setAttribute(TrinoAttributes.TABLE, tableHandle.tableHandle().connectorHandle().toString());
        }
        try (var _ = scopedSpan(span)) {
            delegate.finishMerge(session, tableHandle, sourceTableHandles, fragments, computedStatistics);
        }
    }

    @Override
    public Optional<CatalogHandle> getCatalogHandle(Session session, String catalogName)
    {
        Span span = startSpan("getCatalogHandle", catalogName);
        try (var _ = scopedSpan(span)) {
            return delegate.getCatalogHandle(session, catalogName);
        }
    }

    @Override
    public List<CatalogInfo> listCatalogs(Session session)
    {
        Span span = startSpan("listCatalogs");
        try (var _ = scopedSpan(span)) {
            return delegate.listCatalogs(session);
        }
    }

    @Override
    public List<QualifiedObjectName> listViews(Session session, QualifiedTablePrefix prefix)
    {
        Span span = startSpan("listViews", prefix);
        try (var _ = scopedSpan(span)) {
            return delegate.listViews(session, prefix);
        }
    }

    @Override
    public Map<QualifiedObjectName, ViewInfo> getViews(Session session, QualifiedTablePrefix prefix)
    {
        Span span = startSpan("getViews", prefix);
        try (var _ = scopedSpan(span)) {
            return delegate.getViews(session, prefix);
        }
    }

    @Override
    public boolean isView(Session session, QualifiedObjectName viewName)
    {
        Span span = startSpan("isView", viewName);
        try (var _ = scopedSpan(span)) {
            return delegate.isView(session, viewName);
        }
    }

    @Override
    public Optional<ViewDefinition> getView(Session session, QualifiedObjectName viewName)
    {
        Span span = startSpan("getView", viewName);
        try (var _ = scopedSpan(span)) {
            return delegate.getView(session, viewName);
        }
    }

    @Override
    public Map<String, Object> getViewProperties(Session session, QualifiedObjectName viewName)
    {
        Span span = startSpan("getViewProperties", viewName);
        try (var _ = scopedSpan(span)) {
            return delegate.getViewProperties(session, viewName);
        }
    }

    @Override
    public Map<String, Object> getSchemaProperties(Session session, CatalogSchemaName schemaName)
    {
        Span span = startSpan("getSchemaProperties", schemaName);
        try (var _ = scopedSpan(span)) {
            return delegate.getSchemaProperties(session, schemaName);
        }
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(Session session, CatalogSchemaName schemaName)
    {
        Span span = startSpan("getSchemaOwner", schemaName);
        try (var _ = scopedSpan(span)) {
            return delegate.getSchemaOwner(session, schemaName);
        }
    }

    @Override
    public void createView(Session session, QualifiedObjectName viewName, ViewDefinition definition, Map<String, Object> properties, boolean replace)
    {
        Span span = startSpan("createView", viewName);
        try (var _ = scopedSpan(span)) {
            delegate.createView(session, viewName, definition, properties, replace);
        }
    }

    @Override
    public void renameView(Session session, QualifiedObjectName existingViewName, QualifiedObjectName newViewName)
    {
        Span span = startSpan("renameView", existingViewName);
        try (var _ = scopedSpan(span)) {
            delegate.renameView(session, existingViewName, newViewName);
        }
    }

    @Override
    public void setViewAuthorization(Session session, CatalogSchemaTableName view, TrinoPrincipal principal)
    {
        Span span = startSpan("setViewAuthorization", view);
        try (var _ = scopedSpan(span)) {
            delegate.setViewAuthorization(session, view, principal);
        }
    }

    @Override
    public void dropView(Session session, QualifiedObjectName viewName)
    {
        Span span = startSpan("dropView", viewName);
        try (var _ = scopedSpan(span)) {
            delegate.dropView(session, viewName);
        }
    }

    @Override
    public Optional<ResolvedIndex> resolveIndex(Session session, TableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        Span span = startSpan("resolveIndex", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.resolveIndex(session, tableHandle, indexableColumns, outputColumns, tupleDomain);
        }
    }

    @Override
    public Optional<LimitApplicationResult<TableHandle>> applyLimit(Session session, TableHandle table, long limit)
    {
        Span span = startSpan("applyLimit", table);
        try (var _ = scopedSpan(span)) {
            return delegate.applyLimit(session, table, limit);
        }
    }

    @Override
    public Optional<ConstraintApplicationResult<TableHandle>> applyFilter(Session session, TableHandle table, Constraint constraint)
    {
        Span span = startSpan("applyFilter", table);
        try (var _ = scopedSpan(span)) {
            return delegate.applyFilter(session, table, constraint);
        }
    }

    @Override
    public Optional<ProjectionApplicationResult<TableHandle>> applyProjection(Session session, TableHandle table, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        Span span = startSpan("applyProjection", table);
        try (var _ = scopedSpan(span)) {
            return delegate.applyProjection(session, table, projections, assignments);
        }
    }

    @Override
    public Optional<SampleApplicationResult<TableHandle>> applySample(Session session, TableHandle table, SampleType sampleType, double sampleRatio)
    {
        Span span = startSpan("applySample", table);
        try (var _ = scopedSpan(span)) {
            return delegate.applySample(session, table, sampleType, sampleRatio);
        }
    }

    @Override
    public Optional<AggregationApplicationResult<TableHandle>> applyAggregation(Session session, TableHandle table, List<AggregateFunction> aggregations, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        Span span = startSpan("applyAggregation", table);
        try (var _ = scopedSpan(span)) {
            return delegate.applyAggregation(session, table, aggregations, assignments, groupingSets);
        }
    }

    @Override
    public Optional<JoinApplicationResult<TableHandle>> applyJoin(Session session, JoinType joinType, TableHandle left, TableHandle right, ConnectorExpression joinCondition, Map<String, ColumnHandle> leftAssignments, Map<String, ColumnHandle> rightAssignments, JoinStatistics statistics)
    {
        Span span = startSpan("applyJoin");
        if (span.isRecording() && left.catalogHandle().equals(right.catalogHandle())) {
            span.setAttribute(TrinoAttributes.CATALOG, left.catalogHandle().getCatalogName().toString());
        }
        try (var _ = scopedSpan(span)) {
            return delegate.applyJoin(session, joinType, left, right, joinCondition, leftAssignments, rightAssignments, statistics);
        }
    }

    @Override
    public Optional<TopNApplicationResult<TableHandle>> applyTopN(Session session, TableHandle handle, long topNCount, List<SortItem> sortItems, Map<String, ColumnHandle> assignments)
    {
        Span span = startSpan("applyTopN", handle);
        try (var _ = scopedSpan(span)) {
            return delegate.applyTopN(session, handle, topNCount, sortItems, assignments);
        }
    }

    @Override
    public Optional<TableFunctionApplicationResult<TableHandle>> applyTableFunction(Session session, TableFunctionHandle handle)
    {
        Span span = startSpan("applyTableFunction")
                .setAttribute(TrinoAttributes.CATALOG, handle.catalogHandle().getCatalogName().toString())
                .setAttribute(TrinoAttributes.HANDLE, handle.functionHandle().toString());
        try (var _ = scopedSpan(span)) {
            return delegate.applyTableFunction(session, handle);
        }
    }

    @Override
    public void validateScan(Session session, TableHandle table)
    {
        Span span = startSpan("validateScan", table);
        try (var _ = scopedSpan(span)) {
            delegate.validateScan(session, table);
        }
    }

    @Override
    public boolean isCatalogManagedSecurity(Session session, String catalog)
    {
        Span span = startSpan("isCatalogManagedSecurity", catalog);
        try (var _ = scopedSpan(span)) {
            return delegate.isCatalogManagedSecurity(session, catalog);
        }
    }

    @Override
    public boolean roleExists(Session session, String role, Optional<String> catalog)
    {
        Span span = startSpan("roleExists", catalog);
        try (var _ = scopedSpan(span)) {
            return delegate.roleExists(session, role, catalog);
        }
    }

    @Override
    public void createRole(Session session, String role, Optional<TrinoPrincipal> grantor, Optional<String> catalog)
    {
        Span span = startSpan("createRole", catalog);
        try (var _ = scopedSpan(span)) {
            delegate.createRole(session, role, grantor, catalog);
        }
    }

    @Override
    public void dropRole(Session session, String role, Optional<String> catalog)
    {
        Span span = startSpan("dropRole", catalog);
        try (var _ = scopedSpan(span)) {
            delegate.dropRole(session, role, catalog);
        }
    }

    @Override
    public Set<String> listRoles(Session session, Optional<String> catalog)
    {
        Span span = startSpan("listRoles", catalog);
        try (var _ = scopedSpan(span)) {
            return delegate.listRoles(session, catalog);
        }
    }

    @Override
    public Set<RoleGrant> listRoleGrants(Session session, Optional<String> catalog, TrinoPrincipal principal)
    {
        Span span = startSpan("listRoleGrants", catalog);
        try (var _ = scopedSpan(span)) {
            return delegate.listRoleGrants(session, catalog, principal);
        }
    }

    @Override
    public void grantRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, Optional<String> catalog)
    {
        Span span = startSpan("grantRoles", catalog);
        try (var _ = scopedSpan(span)) {
            delegate.grantRoles(session, roles, grantees, adminOption, grantor, catalog);
        }
    }

    @Override
    public void revokeRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, Optional<String> catalog)
    {
        Span span = startSpan("revokeRoles", catalog);
        try (var _ = scopedSpan(span)) {
            delegate.revokeRoles(session, roles, grantees, adminOption, grantor, catalog);
        }
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(Session session, TrinoPrincipal principal, Optional<String> catalog)
    {
        Span span = startSpan("listApplicableRoles", catalog);
        try (var _ = scopedSpan(span)) {
            return delegate.listApplicableRoles(session, principal, catalog);
        }
    }

    @Override
    public Set<String> listEnabledRoles(Identity identity)
    {
        Span span = startSpan("listEnabledRoles");
        try (var _ = scopedSpan(span)) {
            return delegate.listEnabledRoles(identity);
        }
    }

    @Override
    public Set<String> listEnabledRoles(Session session, String catalog)
    {
        Span span = startSpan("listEnabledRoles", catalog);
        try (var _ = scopedSpan(span)) {
            return delegate.listEnabledRoles(session, catalog);
        }
    }

    @Override
    public void grantSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        Span span = startSpan("grantSchemaPrivileges", schemaName);
        try (var _ = scopedSpan(span)) {
            delegate.grantSchemaPrivileges(session, schemaName, privileges, grantee, grantOption);
        }
    }

    @Override
    public void denySchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        Span span = startSpan("denySchemaPrivileges", schemaName);
        try (var _ = scopedSpan(span)) {
            delegate.denySchemaPrivileges(session, schemaName, privileges, grantee);
        }
    }

    @Override
    public void revokeSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        Span span = startSpan("revokeSchemaPrivileges", schemaName);
        try (var _ = scopedSpan(span)) {
            delegate.revokeSchemaPrivileges(session, schemaName, privileges, grantee, grantOption);
        }
    }

    @Override
    public void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        Span span = startSpan("grantTablePrivileges", tableName);
        try (var _ = scopedSpan(span)) {
            delegate.grantTablePrivileges(session, tableName, privileges, grantee, grantOption);
        }
    }

    @Override
    public void denyTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        Span span = startSpan("denyTablePrivileges", tableName);
        try (var _ = scopedSpan(span)) {
            delegate.denyTablePrivileges(session, tableName, privileges, grantee);
        }
    }

    @Override
    public void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        Span span = startSpan("revokeTablePrivileges", tableName);
        try (var _ = scopedSpan(span)) {
            delegate.revokeTablePrivileges(session, tableName, privileges, grantee, grantOption);
        }
    }

    @Override
    public List<GrantInfo> listTablePrivileges(Session session, QualifiedTablePrefix prefix)
    {
        Span span = startSpan("listTablePrivileges", prefix);
        try (var _ = scopedSpan(span)) {
            return delegate.listTablePrivileges(session, prefix);
        }
    }

    @Override
    public Set<EntityPrivilege> getAllEntityKindPrivileges(String entityKind)
    {
        return delegate.getAllEntityKindPrivileges(entityKind);
    }

    @Override
    public void grantEntityPrivileges(Session session, EntityKindAndName entity, Set<EntityPrivilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        Span span = startSpan("grantEntityPrivileges", entity, privileges, grantee, grantOption);
        try (var _ = scopedSpan(span)) {
            delegate.grantEntityPrivileges(session, entity, privileges, grantee, grantOption);
        }
    }

    @Override
    public void denyEntityPrivileges(Session session, EntityKindAndName entity, Set<EntityPrivilege> privileges, TrinoPrincipal grantee)
    {
        Span span = startSpan("denyEntityPrivileges", entity, privileges, grantee, false);
        try (var _ = scopedSpan(span)) {
            delegate.denyEntityPrivileges(session, entity, privileges, grantee);
        }
    }

    @Override
    public void revokeEntityPrivileges(Session session, EntityKindAndName entity, Set<EntityPrivilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        Span span = startSpan("revokeEntityPrivileges", entity, privileges, grantee, grantOption);
        try (var _ = scopedSpan(span)) {
            delegate.revokeEntityPrivileges(session, entity, privileges, grantee, grantOption);
        }
    }

    @Override
    public Collection<FunctionMetadata> listGlobalFunctions(Session session)
    {
        Span span = startSpan("listGlobalFunctions");
        try (var _ = scopedSpan(span)) {
            return delegate.listGlobalFunctions(session);
        }
    }

    @Override
    public Collection<FunctionMetadata> listFunctions(Session session, CatalogSchemaName schema)
    {
        Span span = startSpan("listFunctions", schema);
        try (var _ = scopedSpan(span)) {
            return delegate.listFunctions(session, schema);
        }
    }

    @Override
    public Collection<CatalogFunctionMetadata> getFunctions(Session session, CatalogSchemaFunctionName catalogSchemaFunctionName)
    {
        Span span = startSpan("getFunctions", catalogSchemaFunctionName);
        try (var _ = scopedSpan(span)) {
            return delegate.getFunctions(session, catalogSchemaFunctionName);
        }
    }

    @Override
    public ResolvedFunction resolveBuiltinFunction(String name, List<TypeSignatureProvider> parameterTypes)
    {
        Span span = startSpan("resolveBuiltinFunction")
                .setAttribute(TrinoAttributes.FUNCTION, name);
        try (var _ = scopedSpan(span)) {
            return delegate.resolveBuiltinFunction(name, parameterTypes);
        }
    }

    @Override
    public ResolvedFunction resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException
    {
        // no tracing since it doesn't call any connector
        return delegate.resolveOperator(operatorType, argumentTypes);
    }

    @Override
    public ResolvedFunction getCoercion(Type fromType, Type toType)
    {
        // no tracing since it doesn't call any connector
        return delegate.getCoercion(fromType, toType);
    }

    @Override
    public ResolvedFunction getCoercion(OperatorType operatorType, Type fromType, Type toType)
    {
        // no tracing since it doesn't call any connector
        return delegate.getCoercion(operatorType, fromType, toType);
    }

    @Override
    public ResolvedFunction getCoercion(CatalogSchemaFunctionName name, Type fromType, Type toType)
    {
        // no tracing since it doesn't call any connector
        return delegate.getCoercion(name, fromType, toType);
    }

    @Override
    public AggregationFunctionMetadata getAggregationFunctionMetadata(Session session, ResolvedFunction resolvedFunction)
    {
        Span span = startSpan("getAggregationFunctionMetadata")
                .setAttribute(TrinoAttributes.CATALOG, resolvedFunction.catalogHandle().getCatalogName().toString())
                .setAttribute(TrinoAttributes.FUNCTION, resolvedFunction.signature().getName().toString());
        try (var _ = scopedSpan(span)) {
            return delegate.getAggregationFunctionMetadata(session, resolvedFunction);
        }
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies(Session session, CatalogHandle catalogHandle, FunctionId functionId, BoundSignature boundSignature)
    {
        Span span = startSpan("getFunctionDependencies", catalogHandle.getCatalogName())
                .setAttribute(TrinoAttributes.FUNCTION, functionId.toString());
        try (var _ = scopedSpan(span)) {
            return delegate.getFunctionDependencies(session, catalogHandle, functionId, boundSignature);
        }
    }

    @Override
    public Collection<LanguageFunction> getLanguageFunctions(Session session, QualifiedObjectName name)
    {
        Span span = startSpan("getLanguageFunctions", name);
        try (var ignored = scopedSpan(span)) {
            return delegate.getLanguageFunctions(session, name);
        }
    }

    @Override
    public boolean languageFunctionExists(Session session, QualifiedObjectName name, String signatureToken)
    {
        Span span = startSpan("languageFunctionExists", name);
        try (var _ = scopedSpan(span)) {
            return delegate.languageFunctionExists(session, name, signatureToken);
        }
    }

    @Override
    public void createLanguageFunction(Session session, QualifiedObjectName name, LanguageFunction function, boolean replace)
    {
        Span span = startSpan("createLanguageFunction", name);
        try (var _ = scopedSpan(span)) {
            delegate.createLanguageFunction(session, name, function, replace);
        }
    }

    @Override
    public void dropLanguageFunction(Session session, QualifiedObjectName name, String signatureToken)
    {
        Span span = startSpan("dropLanguageFunction", name);
        try (var _ = scopedSpan(span)) {
            delegate.dropLanguageFunction(session, name, signatureToken);
        }
    }

    @Override
    public void createMaterializedView(
            Session session,
            QualifiedObjectName viewName,
            MaterializedViewDefinition definition,
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
    public void dropMaterializedView(Session session, QualifiedObjectName viewName)
    {
        Span span = startSpan("dropMaterializedView", viewName);
        try (var _ = scopedSpan(span)) {
            delegate.dropMaterializedView(session, viewName);
        }
    }

    @Override
    public List<QualifiedObjectName> listMaterializedViews(Session session, QualifiedTablePrefix prefix)
    {
        Span span = startSpan("listMaterializedViews", prefix);
        try (var _ = scopedSpan(span)) {
            return delegate.listMaterializedViews(session, prefix);
        }
    }

    @Override
    public Map<QualifiedObjectName, ViewInfo> getMaterializedViews(Session session, QualifiedTablePrefix prefix)
    {
        Span span = startSpan("getMaterializedViews", prefix);
        try (var _ = scopedSpan(span)) {
            return delegate.getMaterializedViews(session, prefix);
        }
    }

    @Override
    public boolean isMaterializedView(Session session, QualifiedObjectName viewName)
    {
        Span span = startSpan("isMaterializedView", viewName);
        try (var _ = scopedSpan(span)) {
            return delegate.isMaterializedView(session, viewName);
        }
    }

    @Override
    public Optional<MaterializedViewDefinition> getMaterializedView(Session session, QualifiedObjectName viewName)
    {
        Span span = startSpan("getMaterializedView", viewName);
        try (var _ = scopedSpan(span)) {
            return delegate.getMaterializedView(session, viewName);
        }
    }

    @Override
    public Map<String, Object> getMaterializedViewProperties(Session session, QualifiedObjectName objectName, MaterializedViewDefinition materializedViewDefinition)
    {
        Span span = startSpan("getMaterializedViewProperties", objectName);
        try (var _ = scopedSpan(span)) {
            return delegate.getMaterializedViewProperties(session, objectName, materializedViewDefinition);
        }
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(Session session, QualifiedObjectName name)
    {
        Span span = startSpan("getMaterializedViewFreshness", name);
        try (var _ = scopedSpan(span)) {
            return delegate.getMaterializedViewFreshness(session, name);
        }
    }

    @Override
    public void renameMaterializedView(Session session, QualifiedObjectName existingViewName, QualifiedObjectName newViewName)
    {
        Span span = startSpan("renameMaterializedView", existingViewName);
        try (var _ = scopedSpan(span)) {
            delegate.renameMaterializedView(session, existingViewName, newViewName);
        }
    }

    @Override
    public void setMaterializedViewProperties(Session session, QualifiedObjectName viewName, Map<String, Optional<Object>> properties)
    {
        Span span = startSpan("setMaterializedViewProperties", viewName);
        try (var _ = scopedSpan(span)) {
            delegate.setMaterializedViewProperties(session, viewName, properties);
        }
    }

    @Override
    public void setMaterializedViewColumnComment(Session session, QualifiedObjectName viewName, String columnName, Optional<String> comment)
    {
        Span span = startSpan("setMaterializedViewColumnComment", viewName);
        try (var _ = scopedSpan(span)) {
            delegate.setMaterializedViewColumnComment(session, viewName, columnName, comment);
        }
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> applyTableScanRedirect(Session session, TableHandle tableHandle)
    {
        Span span = startSpan("applyTableScanRedirect", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.applyTableScanRedirect(session, tableHandle);
        }
    }

    @Override
    public RedirectionAwareTableHandle getRedirectionAwareTableHandle(Session session, QualifiedObjectName tableName)
    {
        Span span = startSpan("getRedirectionAwareTableHandle", tableName);
        try (var _ = scopedSpan(span)) {
            return delegate.getRedirectionAwareTableHandle(session, tableName);
        }
    }

    @Override
    public RedirectionAwareTableHandle getRedirectionAwareTableHandle(Session session, QualifiedObjectName tableName, Optional<TableVersion> startVersion, Optional<TableVersion> endVersion)
    {
        Span span = startSpan("getRedirectionAwareTableHandle", tableName);
        try (var _ = scopedSpan(span)) {
            return delegate.getRedirectionAwareTableHandle(session, tableName, startVersion, endVersion);
        }
    }

    @Override
    public Optional<TableHandle> getTableHandle(Session session, QualifiedObjectName tableName, Optional<TableVersion> startVersion, Optional<TableVersion> endVersion)
    {
        Span span = startSpan("getTableHandle", tableName);
        try (var _ = scopedSpan(span)) {
            return delegate.getTableHandle(session, tableName, startVersion, endVersion);
        }
    }

    @Override
    public OptionalInt getMaxWriterTasks(Session session, String catalogName)
    {
        Span span = startSpan("getMaxWriterTasks", catalogName);
        try (var _ = scopedSpan(span)) {
            return delegate.getMaxWriterTasks(session, catalogName);
        }
    }

    @Override
    public boolean allowSplittingReadIntoMultipleSubQueries(Session session, TableHandle tableHandle)
    {
        Span span = startSpan("allowSplittingReadIntoMultipleSubQueries", tableHandle);
        try (var ignored = scopedSpan(span)) {
            return delegate.allowSplittingReadIntoMultipleSubQueries(session, tableHandle);
        }
    }

    @Override
    public WriterScalingOptions getNewTableWriterScalingOptions(Session session, QualifiedObjectName tableName, Map<String, Object> tableProperties)
    {
        Span span = startSpan("getNewTableWriterScalingOptions", tableName);
        try (var _ = scopedSpan(span)) {
            return delegate.getNewTableWriterScalingOptions(session, tableName, tableProperties);
        }
    }

    @Override
    public WriterScalingOptions getInsertWriterScalingOptions(Session session, TableHandle tableHandle)
    {
        Span span = startSpan("getInsertWriterScalingOptions", tableHandle);
        try (var _ = scopedSpan(span)) {
            return delegate.getInsertWriterScalingOptions(session, tableHandle);
        }
    }

    private Span startSpan(String methodName)
    {
        return tracer.spanBuilder("Metadata." + methodName)
                .startSpan();
    }

    private Span startSpan(String methodName, CatalogName catalogName)
    {
        return startSpan(methodName, catalogName.toString());
    }

    private Span startSpan(String methodName, String catalogName)
    {
        return startSpan(methodName)
                .setAttribute(TrinoAttributes.CATALOG, catalogName);
    }

    private Span startSpan(String methodName, Optional<String> catalog)
    {
        return startSpan(methodName, catalog.orElse(null));
    }

    private Span startSpan(String methodName, CatalogSchemaName schema)
    {
        return startSpan(methodName)
                .setAttribute(TrinoAttributes.CATALOG, schema.getCatalogName())
                .setAttribute(TrinoAttributes.SCHEMA, schema.getSchemaName());
    }

    private Span startSpan(String methodName, QualifiedObjectName table)
    {
        return startSpan(methodName)
                .setAttribute(TrinoAttributes.CATALOG, table.catalogName())
                .setAttribute(TrinoAttributes.SCHEMA, table.schemaName())
                .setAttribute(TrinoAttributes.TABLE, table.objectName());
    }

    private Span startSpan(String methodName, CatalogSchemaTableName table)
    {
        return startSpan(methodName)
                .setAttribute(TrinoAttributes.CATALOG, table.getCatalogName())
                .setAttribute(TrinoAttributes.SCHEMA, table.getSchemaTableName().getSchemaName())
                .setAttribute(TrinoAttributes.TABLE, table.getSchemaTableName().getTableName());
    }

    private Span startSpan(String methodName, QualifiedTablePrefix prefix)
    {
        return startSpan(methodName)
                .setAttribute(TrinoAttributes.CATALOG, prefix.getCatalogName())
                .setAttribute(TrinoAttributes.SCHEMA, prefix.getSchemaName().orElse(null))
                .setAttribute(TrinoAttributes.TABLE, prefix.getTableName().orElse(null));
    }

    private Span startSpan(String methodName, String catalogName, ConnectorTableMetadata tableMetadata)
    {
        return startSpan(methodName)
                .setAttribute(TrinoAttributes.CATALOG, catalogName)
                .setAttribute(TrinoAttributes.SCHEMA, tableMetadata.getTable().getSchemaName())
                .setAttribute(TrinoAttributes.TABLE, tableMetadata.getTable().getTableName());
    }

    private Span startSpan(String methodName, TableHandle handle)
    {
        Span span = startSpan(methodName);
        if (span.isRecording()) {
            span.setAttribute(TrinoAttributes.CATALOG, handle.catalogHandle().getCatalogName().toString());
            span.setAttribute(TrinoAttributes.HANDLE, handle.connectorHandle().toString());
        }
        return span;
    }

    private Span startSpan(String methodName, TableExecuteHandle handle)
    {
        Span span = startSpan(methodName);
        if (span.isRecording()) {
            span.setAttribute(TrinoAttributes.CATALOG, handle.catalogHandle().getCatalogName().toString());
            span.setAttribute(TrinoAttributes.HANDLE, handle.connectorHandle().toString());
        }
        return span;
    }

    private Span startSpan(String methodName, CatalogSchemaFunctionName table)
    {
        return startSpan(methodName)
                .setAttribute(TrinoAttributes.CATALOG, table.getCatalogName())
                .setAttribute(TrinoAttributes.SCHEMA, table.getSchemaName())
                .setAttribute(TrinoAttributes.FUNCTION, table.getFunctionName());
    }

    private Span startSpan(String methodName, EntityKindAndName entity, Set<EntityPrivilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        Span span = startSpan(methodName);
        if (span.isRecording()) {
            String grant = String.format("%s-%s-%s-%s-%s%s",
                    entity.entityKind(),
                    entity.name(),
                    grantee.getType(),
                    grantee.getName(),
                    privileges.stream().map(EntityPrivilege::name).collect(Collectors.joining("-")),
                    grantOption ? "-grantOption" : "");
            span.setAttribute(TrinoAttributes.PRIVILEGE_GRANT, grant);
        }
        return span;
    }

    private Span startSpan(String methodName, EntityKindAndName entity)
    {
        Span span = startSpan(methodName);
        if (span.isRecording()) {
            span.setAttribute(TrinoAttributes.ENTITY, String.format("%s-%s", entity.entityKind(), entity.name()));
        }
        return span;
    }
}
