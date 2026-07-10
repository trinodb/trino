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
package io.trino.spi.connector;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;

import jakarta.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CachedConnectorMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(CachedConnectorMetadata.class);
    private final Map<String, MetadataCache> cache = new HashMap<>();
    private final ConnectorMetadata delegate;
    private long timeToLive;
    private long maximumSize;

    public CachedConnectorMetadata(ConnectorMetadata delegate, long timeToLive, long maximumSize)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.timeToLive = timeToLive;
        this.maximumSize = maximumSize;
    }

    interface WrapDelegateMethod<T>
    {
        T delegateMethod();
    }

    <T> T logAndDelegate(String delegateInfo, WrapDelegateMethod<T> m)
    {
        log.debug("Cache invalid/expired for " + delegateInfo + ", delegating to parent");
        return m.delegateMethod();
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return delegate.schemaExists(session, schemaName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        Optional<MetadataCache> cacheOpt = getOrCreateCache(session);

        if (!cacheOpt.isPresent()) {
            return logAndDelegate("listSchemaNames", () -> delegate.listSchemaNames(session));
        }

        try {
            return cacheOpt.get().getSchemas().get("all-schemas", () -> {
                List<String> result = logAndDelegate("listSchemaNames", () -> delegate.listSchemaNames(session));

                if (result == null) {
                    throw new Exception();
                }

                return result;
            });
        }
        catch (Exception e) {
            return logAndDelegate("listSchemaNames", () -> delegate.listSchemaNames(session));
        }
    }

    @Override
    @Nullable
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        Optional<MetadataCache> cacheOpt = getOrCreateCache(session);

        if (!cacheOpt.isPresent()) {
            return logAndDelegate("getTableHandle", () -> delegate.getTableHandle(session, tableName, startVersion, endVersion));
        }

        try {
            return cacheOpt.get().getTableHandles().get(tableName.toString(), () -> {
                ConnectorTableHandle result = logAndDelegate("getTableHandle",
                        () -> delegate.getTableHandle(session, tableName, startVersion, endVersion));

                if (result == null) {
                    throw new Exception();
                }

                return result;
            });
        }
        catch (Exception e) {
            return logAndDelegate("getTableHandle", () -> delegate.getTableHandle(session, tableName, startVersion, endVersion));
        }
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return delegate.getSystemTable(session, tableName);
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getCommonPartitioningHandle(ConnectorSession session,
            ConnectorPartitioningHandle left,
            ConnectorPartitioningHandle right)
    {
        return delegate.getCommonPartitioningHandle(session, left, right);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        Optional<MetadataCache> cacheOpt = getOrCreateCache(session);

        if (!cacheOpt.isPresent() || table.getSchemaPrefixedTableName() == null) {
            return logAndDelegate("getTableMetadata", () -> delegate.getTableMetadata(session, table));
        }

        try {
            return cacheOpt.get().getTableMetadata().get(table.getSchemaPrefixedTableName(), () -> {
                ConnectorTableMetadata result = logAndDelegate("getTableMetadata",
                        () -> delegate.getTableMetadata(session, table));

                if (result == null) {
                    throw new RuntimeException();
                }

                return result;
            });
        }
        catch (Exception e) {
            return logAndDelegate("getTableMetadata", () -> delegate.getTableMetadata(session, table));
        }
    }

    @Override
    public Optional<Object> getInfo(ConnectorSession session, ConnectorTableHandle table)
    {
        return delegate.getInfo(session, table);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        Optional<MetadataCache> cacheOpt = getOrCreateCache(session);

        if (!cacheOpt.isPresent() || !schemaName.isPresent()) {
            return logAndDelegate("listTables", () -> delegate.listTables(session, schemaName));
        }

        try {
            return cacheOpt.get().getTables().get(schemaName.get(), () -> {
                List<SchemaTableName> result =
                        logAndDelegate("listTables", () -> delegate.listTables(session, schemaName));

                if (result == null) {
                    throw new Exception();
                }

                return result;
            });
        }
        catch (Exception e) {
            return logAndDelegate("listTables", () -> delegate.listTables(session, schemaName));
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        Optional<MetadataCache> cacheOpt = getOrCreateCache(session);

        if (!cacheOpt.isPresent() || tableHandle.getSchemaPrefixedTableName() == null) {
            return logAndDelegate("getColumnHandles", () -> delegate.getColumnHandles(session, tableHandle));
        }

        try {
            return cacheOpt.get().getColumnHandles().get(tableHandle.getSchemaPrefixedTableName(), () -> {
                Map<String, ColumnHandle> result =
                        logAndDelegate("getColumnHandles", () -> delegate.getColumnHandles(session, tableHandle));

                if (result == null) {
                    throw new Exception();
                }

                return result;
            });
        }
        catch (Exception e) {
            return logAndDelegate("getColumnHandles", () -> delegate.getColumnHandles(session, tableHandle));
        }
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        Optional<MetadataCache> cacheOpt = getOrCreateCache(session);

        if (!cacheOpt.isPresent() || tableHandle.getSchemaPrefixedTableName() == null) {
            return logAndDelegate("getColumnMetadata", () -> delegate.getColumnMetadata(session, tableHandle, columnHandle));
        }

        try {
            return cacheOpt.get().getColumnMetadata().get(tableHandle.getSchemaPrefixedTableName(), () -> {
                ColumnMetadata columnMetadata =
                        logAndDelegate("getColumnMetadata", () -> delegate.getColumnMetadata(session, tableHandle, columnHandle));

                if (columnMetadata == null) {
                    throw new Exception();
                }

                return columnMetadata;
            });
        }
        catch (Exception e) {
            return logAndDelegate("getColumnMetadata", () -> delegate.getColumnMetadata(session, tableHandle, columnHandle));
        }
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        Optional<MetadataCache> cacheOpt = getOrCreateCache(session);

        if (!cacheOpt.isPresent()) {
            return logAndDelegate("listTableColumns", () -> delegate.listTableColumns(session, prefix));
        }

        try {
            return cacheOpt.get().getColumns().get(prefix.toString(), () -> {
                Map<SchemaTableName, List<ColumnMetadata>> result =
                        logAndDelegate("listTableColumns", () -> delegate.listTableColumns(session, prefix));

                if (result == null) {
                    throw new Exception();
                }

                return result;
            });
        }
        catch (Exception e) {
            return logAndDelegate("listTableColumns", () -> delegate.listTableColumns(session, prefix));
        }
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle,
            Constraint constraint, boolean includeColumnStatistics)
    {
        Optional<MetadataCache> cacheOpt = getOrCreateCache(session);

        if (!cacheOpt.isPresent()) {
            return logAndDelegate("getTableStatistics", () -> delegate.getTableStatistics(session, tableHandle, constraint, includeColumnStatistics));
        }

        try {
            return cacheOpt.get().getTableStatistics().get(tableHandle.getSchemaPrefixedTableName(), () -> {
                TableStatistics tableStatistics =
                        logAndDelegate("getTableStatistics", () -> delegate.getTableStatistics(session, tableHandle, constraint, includeColumnStatistics));

                if (tableStatistics == null) {
                    throw new Exception();
                }

                return tableStatistics;
            });
        }
        catch (Exception e) {
            return logAndDelegate("getTableStatistics", () -> delegate.getTableStatistics(session, tableHandle, constraint, includeColumnStatistics));
        }
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        invalidateCaches(session);
        delegate.createSchema(session, schemaName, properties, owner);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        invalidateCaches(session);
        delegate.dropSchema(session, schemaName, cascade);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        invalidateCaches(session);
        delegate.renameSchema(session, source, target);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        invalidateCaches(session);
        delegate.createTable(session, tableMetadata, saveMode);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        invalidateCaches(session);
        delegate.dropTable(session, tableHandle);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        invalidateCaches(session);
        delegate.renameTable(session, tableHandle, newTableName);
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        delegate.setTableComment(session, tableHandle, comment);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column, ColumnPosition position)
    {
        invalidateCaches(session);
        delegate.addColumn(session, tableHandle, column, position);
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source,
            String target)
    {
        invalidateCaches(session);
        delegate.renameColumn(session, tableHandle, source, target);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        invalidateCaches(session);
        delegate.dropColumn(session, tableHandle, column);
    }

    @Override
    public Optional<ConnectorNewTableLayout> getOfNewTableLayout(ConnectorSession session,
            ConnectorTableMetadata tableMetadata)
    {
        return delegate.getOfNewTableLayout(session, tableMetadata);
    }

    @Override
    public Optional<ConnectorNewTableLayout> getNewInsertLayout(ConnectorSession session,
            ConnectorTableHandle tableHandle)
    {
        return delegate.getNewInsertLayout(session, tableHandle);
    }

    @Override
    public Optional<ConnectorNewTableLayout> getNewUpdateLayout(ConnectorSession session,
            ConnectorTableHandle tableHandle)
    {
        return delegate.getNewUpdateLayout(session, tableHandle);
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session,
            ConnectorTableMetadata tableMetadata, boolean tableReplace)
    {
        return delegate.getStatisticsCollectionMetadataForWrite(session, tableMetadata, tableReplace);
    }

    @Override
    public ConnectorAnalyzeMetadata getStatisticsCollectionMetadata(ConnectorSession session,
            ConnectorTableHandle tableHandle, Map<String, Object> analyzeProperties)
    {
        return delegate.getStatisticsCollectionMetadata(session, tableHandle, analyzeProperties);
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return delegate.beginStatisticsCollection(session, tableHandle);
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle,
            Collection<ComputedStatistics> computedStatistics)
    {
        delegate.finishStatisticsCollection(session, tableHandle, computedStatistics);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata,
            Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
    {
        return delegate.beginCreateTable(session, tableMetadata, layout, retryMode, replace);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session,
            ConnectorOutputTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        return delegate.finishCreateTable(session, tableHandle, fragments, computedStatistics);
    }

    @Override
    public void beginQuery(ConnectorSession session)
    {
        delegate.beginQuery(session);
    }

    @Override
    public void cleanupQuery(ConnectorSession session)
    {
        delegate.cleanupQuery(session);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns, RetryMode retryMode)
    {
        return delegate.beginInsert(session, tableHandle, columns, retryMode);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        return delegate.finishInsert(session, insertHandle, sourceTableHandles, fragments, computedStatistics);
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition,
            Map<String, Object> viewProperties, boolean replace)
    {
        invalidateCaches(session);
        delegate.createView(session, viewName, definition, viewProperties, replace);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        invalidateCaches(session);
        delegate.dropView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        Optional<MetadataCache> cacheOpt = getOrCreateCache(session);

        if (!cacheOpt.isPresent() || !schemaName.isPresent()) {
            return logAndDelegate("listViews", () -> delegate.listViews(session, schemaName));
        }

        try {
            return cacheOpt.get().getViewList().get(schemaName.get(), () -> {
                List<SchemaTableName> result =
                        logAndDelegate("listViews", () -> delegate.listViews(session, schemaName));

                if (result == null) {
                    throw new Exception();
                }

                return result;
            });
        }
        catch (Exception e) {
            return logAndDelegate("listViews", () -> delegate.listViews(session, schemaName));
        }
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session,
            Optional<String> schemaName)
    {
        Optional<MetadataCache> cacheOpt = getOrCreateCache(session);

        if (!cacheOpt.isPresent() || !schemaName.isPresent()) {
            return logAndDelegate("getViews", () -> delegate.getViews(session, schemaName));
        }

        try {
            return cacheOpt.get().getViews().get(schemaName.get(), () -> {
                Map<SchemaTableName, ConnectorViewDefinition> result =
                        logAndDelegate("getViews", () -> delegate.getViews(session, schemaName));

                if (result == null) {
                    throw new Exception();
                }

                return result;
            });
        }
        catch (Exception e) {
            return logAndDelegate("getViews", () -> delegate.getViews(session, schemaName));
        }
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        Optional<MetadataCache> cacheOpt = getOrCreateCache(session);

        if (!cacheOpt.isPresent()) {
            return logAndDelegate("getView", () -> delegate.getView(session, viewName));
        }

        try {
            return cacheOpt.get().getView().get(viewName.toString(), () -> {
                Optional<ConnectorViewDefinition> result =
                        logAndDelegate("getView", () -> delegate.getView(session, viewName));

                if (result == null) {
                    throw new Exception();
                }

                return result;
            });
        }
        catch (Exception e) {
            return logAndDelegate("getView", () -> delegate.getView(session, viewName));
        }
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return delegate.applyDelete(session, handle);
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return delegate.executeDelete(session, handle);
    }

    @Override
    public OptionalLong executeUpdate(ConnectorSession session, ConnectorTableHandle handle)
    {
        return delegate.executeUpdate(session, handle);
    }

    @Override
    public Optional<ConnectorResolvedIndex> resolveIndex(ConnectorSession session, ConnectorTableHandle tableHandle,
            Set<ColumnHandle> indexableColumns,
            Set<ColumnHandle> outputColumns,
            TupleDomain<ColumnHandle> tupleDomain)
    {
        return delegate.resolveIndex(session, tableHandle, indexableColumns, outputColumns, tupleDomain);
    }

    @Override
    public void createRole(ConnectorSession session, String role, Optional<TrinoPrincipal> grantor)
    {
        delegate.createRole(session, role, grantor);
    }

    @Override
    public void dropRole(ConnectorSession session, String role)
    {
        delegate.dropRole(session, role);
    }

    @Override
    public Set<String> listRoles(ConnectorSession session)
    {
        return delegate.listRoles(session);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(ConnectorSession session, TrinoPrincipal principal)
    {
        return delegate.listRoleGrants(session, principal);
    }

    @Override
    public void grantRoles(ConnectorSession connectorSession, Set<String> roles, Set<TrinoPrincipal> grantees,
            boolean withAdminOption, Optional<TrinoPrincipal> grantor)
    {
        delegate.grantRoles(connectorSession, roles, grantees, withAdminOption, grantor);
    }

    @Override
    public void revokeRoles(ConnectorSession connectorSession, Set<String> roles, Set<TrinoPrincipal> grantees,
            boolean adminOptionFor, Optional<TrinoPrincipal> grantor)
    {
        delegate.revokeRoles(connectorSession, roles, grantees, adminOptionFor, grantor);
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(ConnectorSession session, TrinoPrincipal principal)
    {
        return delegate.listApplicableRoles(session, principal);
    }

    @Override
    public Set<String> listEnabledRoles(ConnectorSession session)
    {
        return delegate.listEnabledRoles(session);
    }

    @Override
    public void grantTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges,
                                     TrinoPrincipal grantee, boolean grantOption)
    {
        delegate.grantTablePrivileges(session, tableName, privileges, grantee, grantOption);
    }

    @Override
    public void revokeTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges,
                                      TrinoPrincipal grantee, boolean grantOption)
    {
        delegate.revokeTablePrivileges(session, tableName, privileges, grantee, grantOption);
    }

    @Override
    public List<GrantInfo> listTablePrivileges(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return delegate.listTablePrivileges(session, prefix);
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return delegate.getTableProperties(session, table);
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session,
            ConnectorTableHandle handle, long limit)
    {
        return delegate.applyLimit(session, handle, limit);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session,
            ConnectorTableHandle handle,
            Constraint constraint)
    {
        return delegate.applyFilter(session, handle, constraint);
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        return delegate.applyProjection(session, handle, projections, assignments);
    }

    @Override
    public Optional<SampleApplicationResult<ConnectorTableHandle>> applySample(ConnectorSession session,
            ConnectorTableHandle handle,
            SampleType sampleType, double sampleRatio)
    {
        return delegate.applySample(session, handle, sampleType, sampleRatio);
    }

    private void invalidateCaches(ConnectorSession session)
    {
        Optional<MetadataCache> cacheOpt = getOrCreateCache(session);

        if (!cacheOpt.isPresent()) {
            return;
        }

        cacheOpt.get().invalidateAll();
    }

    private Optional<MetadataCache> getOrCreateCache(ConnectorSession session)
    {
        Optional<String> catalogOpt = session.getCatalog();

        if (!catalogOpt.isPresent()) {
            return Optional.empty();
        }

        if (cache.containsKey(catalogOpt.get())) {
            return Optional.of(cache.get(catalogOpt.get()));
        }

        long ttl = timeToLive;

        log.debug("creating cache with ttl=" + ttl + " max size=" + maximumSize);

        cache.put(catalogOpt.get(), new MetadataCache(ttl, maximumSize));
        return Optional.of(cache.get(catalogOpt.get()));
    }

    @Override
    public boolean isExecutionPlanCacheSupported(ConnectorSession session, ConnectorTableHandle handle)
    {
        return this.delegate.isExecutionPlanCacheSupported(session, handle);
    }

    @Override
    public long getTableModificationTime(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return this.delegate.getTableModificationTime(session, tableHandle);
    }

    @Override
    public boolean isPreAggregationSupported(ConnectorSession session)
    {
        return this.delegate.isPreAggregationSupported(session);
    }

    class MetadataCache
    {
        private long ttl;
        private long maximumSize;
        private final Cache<String, List<SchemaTableName>> tables; // schema -> table names
        private final Cache<String, ConnectorTableHandle> tableHandles; // table name -> table handle
        private final Cache<String, ConnectorTableMetadata> tableMetadata; // table qualified name -> table metadata
        private final Cache<String, Map<SchemaTableName, List<ColumnMetadata>>> columns; // table prefix -> column metadata
        private final Cache<String, Map<String, ColumnHandle>> columnHandles; // table qualified name -> column handles
        private final Cache<String, List<SchemaTableName>> viewList; // schemaName -> views
        private final Cache<String, Map<SchemaTableName, ConnectorViewDefinition>> views; // schemaName -> views
        private final Cache<String, Optional<ConnectorViewDefinition>> view; // viewName -> view
        private final Cache<String, List<String>> schemas; // catalog -> schemas
        private final Cache<String, TableStatistics> tableStatistics;  // table -> tableStatistics
        private final Cache<String, ColumnMetadata> columnMetadata;    // table -> columnMetadata

        public MetadataCache(long ttl, long maximumSize)
        {
            log.debug("New MetadataCache object created.");
            this.ttl = ttl;
            this.maximumSize = maximumSize;
            tables = newCacheBuilder().build();
            columns = newCacheBuilder().build();
            tableHandles = newCacheBuilder().build();
            tableMetadata = newCacheBuilder().build();
            columnHandles = newCacheBuilder().build();
            viewList = newCacheBuilder().build();
            views = newCacheBuilder().build();
            view = newCacheBuilder().build();
            schemas = newCacheBuilder().build();
            tableStatistics = newCacheBuilder().build();
            columnMetadata = newCacheBuilder().build();
        }

        private CacheBuilder<Object, Object> newCacheBuilder()
        {
            CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
            cacheBuilder = cacheBuilder.expireAfterWrite(ttl, MILLISECONDS);
            cacheBuilder = cacheBuilder.maximumSize(maximumSize);
            return cacheBuilder;
        }

        public Cache<String, List<SchemaTableName>> getTables()
        {
            return tables;
        }

        public Cache<String, Map<SchemaTableName, List<ColumnMetadata>>> getColumns()
        {
            return columns;
        }

        public Cache<String, ConnectorTableHandle> getTableHandles()
        {
            return tableHandles;
        }

        public Cache<String, Map<String, ColumnHandle>> getColumnHandles()
        {
            return columnHandles;
        }

        public Cache<String, ConnectorTableMetadata> getTableMetadata()
        {
            return tableMetadata;
        }

        public Cache<String, List<SchemaTableName>> getViewList()
        {
            return viewList;
        }

        public Cache<String, Map<SchemaTableName, ConnectorViewDefinition>> getViews()
        {
            return views;
        }

        public Cache<String, Optional<ConnectorViewDefinition>> getView()
        {
            return view;
        }

        public Cache<String, List<String>> getSchemas()
        {
            return schemas;
        }

        public Cache<String, TableStatistics> getTableStatistics()
        {
            return tableStatistics;
        }

        public Cache<String, ColumnMetadata> getColumnMetadata()
        {
            return columnMetadata;
        }

        public void invalidateAll()
        {
            tables.invalidateAll();
            tableHandles.invalidateAll();
            tableMetadata.invalidateAll();
            columns.invalidateAll();
            columnHandles.invalidateAll();
            viewList.invalidateAll();
            views.invalidateAll();
            view.invalidateAll();
            schemas.invalidateAll();
            tableStatistics.invalidateAll();
            columnMetadata.invalidateAll();
        }
    }
}
