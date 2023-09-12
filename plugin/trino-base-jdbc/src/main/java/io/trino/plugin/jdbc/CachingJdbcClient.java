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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;
import io.airlift.jmx.CacheStatsMBean;
import io.airlift.units.Duration;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.jdbc.IdentityCacheMapping.IdentityCacheKey;
import io.trino.plugin.jdbc.JdbcProcedureHandle.ProcedureQuery;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableScanRedirectApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.Type;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.cache.CacheUtils.invalidateAllIf;
import static io.trino.plugin.jdbc.BaseJdbcConfig.CACHING_DISABLED;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CachingJdbcClient
        implements JdbcClient
{
    private static final Object NULL_MARKER = new Object();

    private final JdbcClient delegate;
    private final List<PropertyMetadata<?>> sessionProperties;
    // specifies whether missing values should be cached
    private final boolean cacheMissing;
    private final IdentityCacheMapping identityMapping;

    private final Cache<IdentityCacheKey, Set<String>> schemaNamesCache;
    private final Cache<TableNamesCacheKey, List<SchemaTableName>> tableNamesCache;
    private final Cache<TableHandlesByNameCacheKey, Optional<JdbcTableHandle>> tableHandlesByNameCache;
    private final Cache<TableHandlesByQueryCacheKey, JdbcTableHandle> tableHandlesByQueryCache;
    private final Cache<ProcedureHandlesByQueryCacheKey, JdbcProcedureHandle> procedureHandlesByQueryCache;
    private final Cache<ColumnsCacheKey, List<JdbcColumnHandle>> columnsCache;
    private final Cache<JdbcTableHandle, TableStatistics> statisticsCache;

    @Inject
    public CachingJdbcClient(
            @StatsCollecting JdbcClient delegate,
            Set<SessionPropertiesProvider> sessionPropertiesProviders,
            IdentityCacheMapping identityMapping,
            BaseJdbcConfig config)
    {
        this(
                delegate,
                sessionPropertiesProviders,
                identityMapping,
                config.getMetadataCacheTtl(),
                config.getSchemaNamesCacheTtl(),
                config.getTableNamesCacheTtl(),
                config.isCacheMissing(),
                config.getCacheMaximumSize());
    }

    public CachingJdbcClient(
            JdbcClient delegate,
            Set<SessionPropertiesProvider> sessionPropertiesProviders,
            IdentityCacheMapping identityMapping,
            Duration metadataCachingTtl,
            boolean cacheMissing,
            long cacheMaximumSize)
    {
        this(delegate,
                sessionPropertiesProviders,
                identityMapping,
                metadataCachingTtl,
                metadataCachingTtl,
                metadataCachingTtl,
                cacheMissing,
                cacheMaximumSize);
    }

    public CachingJdbcClient(
            JdbcClient delegate,
            Set<SessionPropertiesProvider> sessionPropertiesProviders,
            IdentityCacheMapping identityMapping,
            Duration metadataCachingTtl,
            Duration schemaNamesCachingTtl,
            Duration tableNamesCachingTtl,
            boolean cacheMissing,
            long cacheMaximumSize)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.sessionProperties = sessionPropertiesProviders.stream()
                .flatMap(provider -> provider.getSessionProperties().stream())
                .collect(toImmutableList());
        this.cacheMissing = cacheMissing;
        this.identityMapping = requireNonNull(identityMapping, "identityMapping is null");

        long cacheSize = metadataCachingTtl.equals(CACHING_DISABLED)
                // Disables the cache entirely
                ? 0
                : cacheMaximumSize;

        schemaNamesCache = buildCache(cacheSize, schemaNamesCachingTtl);
        tableNamesCache = buildCache(cacheSize, tableNamesCachingTtl);
        tableHandlesByNameCache = buildCache(cacheSize, metadataCachingTtl);
        tableHandlesByQueryCache = buildCache(cacheSize, metadataCachingTtl);
        procedureHandlesByQueryCache = buildCache(cacheSize, metadataCachingTtl);
        columnsCache = buildCache(cacheSize, metadataCachingTtl);
        statisticsCache = buildCache(cacheSize, metadataCachingTtl);
    }

    private static <K, V> Cache<K, V> buildCache(long cacheSize, Duration cachingTtl)
    {
        return EvictableCacheBuilder.newBuilder()
                .maximumSize(cacheSize)
                .expireAfterWrite(cachingTtl.toMillis(), MILLISECONDS)
                .shareNothingWhenDisabled()
                .recordStats()
                .build();
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schema)
    {
        // this method cannot be delegated as that would bypass the cache
        return getSchemaNames(session).contains(schema);
    }

    @Override
    public Set<String> getSchemaNames(ConnectorSession session)
    {
        IdentityCacheKey key = getIdentityKey(session);
        return get(schemaNamesCache, key, () -> delegate.getSchemaNames(session));
    }

    @Override
    public List<SchemaTableName> getTableNames(ConnectorSession session, Optional<String> schema)
    {
        TableNamesCacheKey key = new TableNamesCacheKey(getIdentityKey(session), schema);
        return get(tableNamesCache, key, () -> delegate.getTableNames(session, schema));
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        if (tableHandle.getColumns().isPresent()) {
            return tableHandle.getColumns().get();
        }
        ColumnsCacheKey key = new ColumnsCacheKey(getIdentityKey(session), getSessionProperties(session), tableHandle.getRequiredNamedRelation().getSchemaTableName());
        return get(columnsCache, key, () -> delegate.getColumns(session, tableHandle));
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        return delegate.toColumnMapping(session, connection, typeHandle);
    }

    @Override
    public List<ColumnMapping> toColumnMappings(ConnectorSession session, List<JdbcTypeHandle> typeHandles)
    {
        return delegate.toColumnMappings(session, typeHandles);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        return delegate.toWriteMapping(session, type);
    }

    @Override
    public Optional<Type> getSupportedType(ConnectorSession session, Type type)
    {
        return delegate.getSupportedType(session, type);
    }

    @Override
    public boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        return delegate.supportsAggregationPushdown(session, table, aggregates, assignments, groupingSets);
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        return delegate.implementAggregation(session, aggregate, assignments);
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return delegate.convertPredicate(session, expression, assignments);
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return delegate.getSplits(session, tableHandle);
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorSession session, JdbcProcedureHandle procedureHandle)
    {
        return delegate.getSplits(session, procedureHandle);
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcSplit split, JdbcTableHandle tableHandle)
            throws SQLException
    {
        return delegate.getConnection(session, split, tableHandle);
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcSplit split, JdbcProcedureHandle procedureHandle)
            throws SQLException
    {
        return delegate.getConnection(session, split, procedureHandle);
    }

    @Override
    public void abortReadConnection(Connection connection, ResultSet resultSet)
            throws SQLException
    {
        delegate.abortReadConnection(connection, resultSet);
    }

    @Override
    public PreparedQuery prepareQuery(
            ConnectorSession session,
            JdbcTableHandle table,
            Optional<List<List<JdbcColumnHandle>>> groupingSets,
            List<JdbcColumnHandle> columns,
            Map<String, ParameterizedExpression> columnExpressions)
    {
        return delegate.prepareQuery(session, table, groupingSets, columns, columnExpressions);
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle table, List<JdbcColumnHandle> columns)
            throws SQLException
    {
        return delegate.buildSql(session, connection, split, table, columns);
    }

    @Override
    public CallableStatement buildProcedure(ConnectorSession session, Connection connection, JdbcSplit split, JdbcProcedureHandle procedureHandle)
            throws SQLException
    {
        return delegate.buildProcedure(session, connection, split, procedureHandle);
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
        return delegate.implementJoin(session, joinType, leftSource, rightSource, joinConditions, rightAssignments, leftAssignments, statistics);
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        return delegate.supportsTopN(session, handle, sortOrder);
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return delegate.isTopNGuaranteed(session);
    }

    @Override
    public boolean supportsLimit()
    {
        return delegate.supportsLimit();
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return delegate.isLimitGuaranteed(session);
    }

    @Override
    public Optional<JdbcTableHandle> getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        TableHandlesByNameCacheKey key = new TableHandlesByNameCacheKey(getIdentityKey(session), schemaTableName);
        Optional<JdbcTableHandle> cachedTableHandle = tableHandlesByNameCache.getIfPresent(key);
        //noinspection OptionalAssignedToNull
        if (cachedTableHandle != null) {
            if (cacheMissing || cachedTableHandle.isPresent()) {
                return cachedTableHandle;
            }
            tableHandlesByNameCache.invalidate(key);
        }
        return get(tableHandlesByNameCache, key, () -> delegate.getTableHandle(session, schemaTableName));
    }

    @Override
    public JdbcTableHandle getTableHandle(ConnectorSession session, PreparedQuery preparedQuery)
    {
        TableHandlesByQueryCacheKey key = new TableHandlesByQueryCacheKey(getIdentityKey(session), preparedQuery);
        return get(tableHandlesByQueryCache, key, () -> delegate.getTableHandle(session, preparedQuery));
    }

    @Override
    public JdbcProcedureHandle getProcedureHandle(ConnectorSession session, ProcedureQuery procedureQuery)
    {
        ProcedureHandlesByQueryCacheKey key = new ProcedureHandlesByQueryCacheKey(getIdentityKey(session), procedureQuery);
        return get(procedureHandlesByQueryCache, key, () -> delegate.getProcedureHandle(session, procedureQuery));
    }

    @Override
    public void commitCreateTable(ConnectorSession session, JdbcOutputTableHandle handle, Set<Long> pageSinkIds)
    {
        delegate.commitCreateTable(session, handle, pageSinkIds);
        invalidateTableCaches(new SchemaTableName(handle.getSchemaName(), handle.getTableName()));
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        return delegate.beginInsertTable(session, tableHandle, columns);
    }

    @Override
    public void finishInsertTable(ConnectorSession session, JdbcOutputTableHandle handle, Set<Long> pageSinkIds)
    {
        delegate.finishInsertTable(session, handle, pageSinkIds);
        onDataChanged(new SchemaTableName(handle.getSchemaName(), handle.getTableName()));
    }

    @Override
    public void dropTable(ConnectorSession session, JdbcTableHandle jdbcTableHandle)
    {
        delegate.dropTable(session, jdbcTableHandle);
        invalidateTableCaches(jdbcTableHandle.asPlainTable().getSchemaTableName());
    }

    @Override
    public void rollbackCreateTable(ConnectorSession session, JdbcOutputTableHandle handle)
    {
        delegate.rollbackCreateTable(session, handle);
    }

    @Override
    public boolean supportsRetries()
    {
        return delegate.supportsRetries();
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle, List<WriteFunction> columnWriters)
    {
        return delegate.buildInsertSql(handle, columnWriters);
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcOutputTableHandle handle)
            throws SQLException
    {
        return delegate.getConnection(session, handle);
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql, Optional<Integer> columnCount)
            throws SQLException
    {
        return delegate.getPreparedStatement(connection, sql, columnCount);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, TupleDomain<ColumnHandle> tupleDomain)
    {
        checkArgument(tupleDomain.isAll(), "Unexpected non-ALL constraint: %s", tupleDomain);
        return getTableStatistics(session, handle);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle)
    {
        // TODO depend on Identity when needed
        TableStatistics cachedStatistics = statisticsCache.getIfPresent(handle);
        if (cachedStatistics != null) {
            if (cacheMissing || !cachedStatistics.equals(TableStatistics.empty())) {
                return cachedStatistics;
            }
            statisticsCache.invalidate(handle);
        }
        return get(statisticsCache, handle, () -> delegate.getTableStatistics(session, handle));
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        delegate.createSchema(session, schemaName);
        invalidateSchemasCache();
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        delegate.dropSchema(session, schemaName, cascade);
        invalidateSchemasCache();
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        delegate.renameSchema(session, schemaName, newSchemaName);
        invalidateSchemasCache();
    }

    @Override
    public Optional<String> getTableComment(ResultSet resultSet)
            throws SQLException
    {
        return delegate.getTableComment(resultSet);
    }

    @Override
    public void setTableComment(ConnectorSession session, JdbcTableHandle handle, Optional<String> comment)
    {
        delegate.setTableComment(session, handle, comment);
        invalidateTableCaches(handle.asPlainTable().getSchemaTableName());
    }

    @Override
    public void setColumnComment(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Optional<String> comment)
    {
        delegate.setColumnComment(session, handle, column, comment);
        invalidateTableCaches(handle.asPlainTable().getSchemaTableName());
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        delegate.addColumn(session, handle, column);
        invalidateTableCaches(handle.asPlainTable().getSchemaTableName());
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        delegate.dropColumn(session, handle, column);
        invalidateTableCaches(handle.asPlainTable().getSchemaTableName());
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        delegate.renameColumn(session, handle, jdbcColumn, newColumnName);
        invalidateTableCaches(handle.asPlainTable().getSchemaTableName());
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        delegate.setColumnType(session, handle, column, type);
        invalidateTableCaches(handle.asPlainTable().getSchemaTableName());
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        delegate.renameTable(session, handle, newTableName);
        invalidateTableCaches(handle.asPlainTable().getSchemaTableName());
        invalidateTableCaches(newTableName);
    }

    @Override
    public void setTableProperties(ConnectorSession session, JdbcTableHandle handle, Map<String, Optional<Object>> properties)
    {
        delegate.setTableProperties(session, handle, properties);
        invalidateTableCaches(handle.asPlainTable().getSchemaTableName());
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        delegate.createTable(session, tableMetadata);
        invalidateTableCaches(tableMetadata.getTable());
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return delegate.beginCreateTable(session, tableMetadata);
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return delegate.getSystemTable(session, tableName);
    }

    @Override
    public String quoted(String name)
    {
        return delegate.quoted(name);
    }

    @Override
    public String quoted(RemoteTableName remoteTableName)
    {
        return delegate.quoted(remoteTableName);
    }

    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return delegate.getTableProperties(session, tableHandle);
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> getTableScanRedirection(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return delegate.getTableScanRedirection(session, tableHandle);
    }

    @Override
    public OptionalInt getMaxWriteParallelism(ConnectorSession session)
    {
        return delegate.getMaxWriteParallelism(session);
    }

    public void onDataChanged(SchemaTableName table)
    {
        invalidateAllIf(statisticsCache, key -> key.mayReference(table));
    }

    /**
     * @deprecated {@link JdbcTableHandle}  is not a good representation of the table. For example, we don't want
     * to distinguish between "a plan table" and "table with selected columns", or "a table with a constraint" here.
     * Use {@link #onDataChanged(SchemaTableName)}, which avoids these ambiguities.
     */
    @Deprecated
    public void onDataChanged(JdbcTableHandle handle)
    {
        statisticsCache.invalidate(handle);
    }

    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        OptionalLong deletedRowsCount = delegate.delete(session, handle);
        onDataChanged(handle.getRequiredNamedRelation().getSchemaTableName());
        return deletedRowsCount;
    }

    @Override
    public void truncateTable(ConnectorSession session, JdbcTableHandle handle)
    {
        delegate.truncateTable(session, handle);
        onDataChanged(handle.getRequiredNamedRelation().getSchemaTableName());
    }

    @Managed
    public void flushCache()
    {
        schemaNamesCache.invalidateAll();
        tableNamesCache.invalidateAll();
        tableHandlesByNameCache.invalidateAll();
        tableHandlesByQueryCache.invalidateAll();
        columnsCache.invalidateAll();
        statisticsCache.invalidateAll();
    }

    private IdentityCacheKey getIdentityKey(ConnectorSession session)
    {
        return identityMapping.getRemoteUserCacheKey(session);
    }

    private Map<String, Object> getSessionProperties(ConnectorSession session)
    {
        return sessionProperties.stream()
                .map(property -> Map.entry(property.getName(), getSessionProperty(session, property)))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Object getSessionProperty(ConnectorSession session, PropertyMetadata<?> property)
    {
        return firstNonNull(session.getProperty(property.getName(), property.getJavaType()), NULL_MARKER);
    }

    private void invalidateSchemasCache()
    {
        schemaNamesCache.invalidateAll();
    }

    private void invalidateTableCaches(SchemaTableName schemaTableName)
    {
        invalidateColumnsCache(schemaTableName);
        invalidateAllIf(tableHandlesByNameCache, key -> key.tableName.equals(schemaTableName));
        tableHandlesByQueryCache.invalidateAll();
        invalidateAllIf(tableNamesCache, key -> key.schemaName.equals(Optional.of(schemaTableName.getSchemaName())));
        invalidateAllIf(statisticsCache, key -> key.mayReference(schemaTableName));
    }

    private void invalidateColumnsCache(SchemaTableName table)
    {
        invalidateAllIf(columnsCache, key -> key.table.equals(table));
    }

    @VisibleForTesting
    CacheStats getSchemaNamesCacheStats()
    {
        return schemaNamesCache.stats();
    }

    @VisibleForTesting
    CacheStats getTableNamesCacheStats()
    {
        return tableNamesCache.stats();
    }

    @VisibleForTesting
    CacheStats getTableHandlesByNameCacheStats()
    {
        return tableHandlesByNameCache.stats();
    }

    @VisibleForTesting
    CacheStats getTableHandlesByQueryCacheStats()
    {
        return tableHandlesByQueryCache.stats();
    }

    @VisibleForTesting
    CacheStats getProcedureHandlesByQueryCacheStats()
    {
        return procedureHandlesByQueryCache.stats();
    }

    @VisibleForTesting
    CacheStats getColumnsCacheStats()
    {
        return columnsCache.stats();
    }

    @VisibleForTesting
    CacheStats getStatisticsCacheStats()
    {
        return statisticsCache.stats();
    }

    private record ColumnsCacheKey(IdentityCacheKey identity, Map<String, Object> sessionProperties, SchemaTableName table)
    {
        @SuppressWarnings("UnusedVariable") // TODO: Remove once https://github.com/google/error-prone/issues/2713 is fixed
        private ColumnsCacheKey
        {
            requireNonNull(identity, "identity is null");
            sessionProperties = ImmutableMap.copyOf(requireNonNull(sessionProperties, "sessionProperties is null"));
            requireNonNull(table, "table is null");
        }
    }

    private record TableHandlesByNameCacheKey(IdentityCacheKey identity, SchemaTableName tableName)
    {
        private TableHandlesByNameCacheKey
        {
            requireNonNull(identity, "identity is null");
            requireNonNull(tableName, "tableName is null");
        }
    }

    private record TableHandlesByQueryCacheKey(IdentityCacheKey identity, PreparedQuery preparedQuery)
    {
        private TableHandlesByQueryCacheKey
        {
            requireNonNull(identity, "identity is null");
            requireNonNull(preparedQuery, "preparedQuery is null");
        }
    }

    private record ProcedureHandlesByQueryCacheKey(IdentityCacheKey identity, ProcedureQuery procedureQuery)
    {
        private ProcedureHandlesByQueryCacheKey
        {
            requireNonNull(identity, "identity is null");
            requireNonNull(procedureQuery, "procedureQuery is null");
        }
    }

    private record TableNamesCacheKey(IdentityCacheKey identity, Optional<String> schemaName)
    {
        private TableNamesCacheKey
        {
            requireNonNull(identity, "identity is null");
            requireNonNull(schemaName, "schemaName is null");
        }
    }

    private static <K, V> V get(Cache<K, V> cache, K key, Callable<V> loader)
    {
        try {
            return cache.get(key, loader);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw e;
        }
        catch (ExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new UncheckedExecutionException(e);
        }
    }

    @Managed
    @Nested
    public CacheStatsMBean getSchemaNamesStats()
    {
        return new CacheStatsMBean(schemaNamesCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getTableNamesCache()
    {
        return new CacheStatsMBean(tableNamesCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getTableHandlesByNameCache()
    {
        return new CacheStatsMBean(tableHandlesByNameCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getTableHandlesByQueryCache()
    {
        return new CacheStatsMBean(tableHandlesByQueryCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getColumnsCache()
    {
        return new CacheStatsMBean(columnsCache);
    }

    @Managed
    @Nested
    public CacheStatsMBean getStatisticsCache()
    {
        return new CacheStatsMBean(statisticsCache);
    }
}
