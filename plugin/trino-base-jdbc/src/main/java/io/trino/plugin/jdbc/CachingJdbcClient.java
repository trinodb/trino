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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.jmx.CacheStatsMBean;
import io.airlift.units.Duration;
import io.trino.plugin.base.cache.EvictableCache;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.jdbc.IdentityCacheMapping.IdentityCacheKey;
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
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.Type;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.jdbc.BaseJdbcConfig.CACHING_DISABLED;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CachingJdbcClient
        implements JdbcClient
{
    private static final Object NULL_MARKER = new Object();

    private final JdbcClient delegate;
    private final List<PropertyMetadata<?>> sessionProperties;
    private final boolean cacheMissing;
    private final IdentityCacheMapping identityMapping;

    private final Cache<IdentityCacheKey, Set<String>> schemaNamesCache;
    private final Cache<TableNamesCacheKey, List<SchemaTableName>> tableNamesCache;
    private final Cache<TableHandleCacheKey, Optional<JdbcTableHandle>> tableHandleCache;
    private final Cache<ColumnsCacheKey, List<JdbcColumnHandle>> columnsCache;
    private final Cache<TableStatisticsCacheKey, TableStatistics> statisticsCache;

    @Inject
    public CachingJdbcClient(
            @StatsCollecting JdbcClient delegate,
            Set<SessionPropertiesProvider> sessionPropertiesProviders,
            IdentityCacheMapping identityMapping,
            BaseJdbcConfig config)
    {
        this(delegate, sessionPropertiesProviders, identityMapping, config.getMetadataCacheTtl(), config.isCacheMissing(), config.getCacheMaximumSize());
    }

    public CachingJdbcClient(
            JdbcClient delegate,
            Set<SessionPropertiesProvider> sessionPropertiesProviders,
            IdentityCacheMapping identityMapping,
            Duration metadataCachingTtl,
            boolean cacheMissing,
            long cacheMaximumSize)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.sessionProperties = requireNonNull(sessionPropertiesProviders, "sessionPropertiesProviders is null").stream()
                .flatMap(provider -> provider.getSessionProperties().stream())
                .collect(toImmutableList());
        this.cacheMissing = cacheMissing;
        this.identityMapping = requireNonNull(identityMapping, "identityMapping is null");

        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder()
                .expireAfterWrite(metadataCachingTtl.toMillis(), MILLISECONDS)
                .recordStats();

        if (metadataCachingTtl.equals(CACHING_DISABLED)) {
            // Disables the cache entirely
            cacheBuilder.maximumSize(0);
        }
        else {
            cacheBuilder.maximumSize(cacheMaximumSize);
        }

        schemaNamesCache = EvictableCache.buildWith(cacheBuilder);
        tableNamesCache = EvictableCache.buildWith(cacheBuilder);
        tableHandleCache = EvictableCache.buildWith(cacheBuilder);
        columnsCache = EvictableCache.buildWith(cacheBuilder);
        statisticsCache = EvictableCache.buildWith(cacheBuilder);
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
    public ConnectorSplitSource getSplits(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return delegate.getSplits(session, tableHandle);
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcSplit split)
            throws SQLException
    {
        return delegate.getConnection(session, split);
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
            Map<String, String> columnExpressions)
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
        TableHandleCacheKey key = new TableHandleCacheKey(getIdentityKey(session), schemaTableName);
        Optional<JdbcTableHandle> cachedTableHandle = tableHandleCache.getIfPresent(key);
        //noinspection OptionalAssignedToNull
        if (cachedTableHandle != null) {
            if (cacheMissing || cachedTableHandle.isPresent()) {
                return cachedTableHandle;
            }
            tableHandleCache.invalidate(key);
        }
        return get(tableHandleCache, key, () -> delegate.getTableHandle(session, schemaTableName));
    }

    @Override
    public void commitCreateTable(ConnectorSession session, JdbcOutputTableHandle handle)
    {
        delegate.commitCreateTable(session, handle);
        invalidateTableCaches(new SchemaTableName(handle.getSchemaName(), handle.getTableName()));
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        return delegate.beginInsertTable(session, tableHandle, columns);
    }

    @Override
    public void finishInsertTable(ConnectorSession session, JdbcOutputTableHandle handle)
    {
        delegate.finishInsertTable(session, handle);
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
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        return delegate.getPreparedStatement(connection, sql);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, TupleDomain<ColumnHandle> tupleDomain)
    {
        TableStatisticsCacheKey key = new TableStatisticsCacheKey(handle, tupleDomain);

        TableStatistics cachedStatistics = statisticsCache.getIfPresent(key);
        if (cachedStatistics != null) {
            if (cacheMissing || !cachedStatistics.equals(TableStatistics.empty())) {
                return cachedStatistics;
            }
            statisticsCache.invalidate(key);
        }
        return get(statisticsCache, key, () -> delegate.getTableStatistics(session, handle, tupleDomain));
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        delegate.createSchema(session, schemaName);
        invalidateSchemasCache();
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        delegate.dropSchema(session, schemaName);
        invalidateSchemasCache();
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        delegate.renameSchema(session, schemaName, newSchemaName);
        invalidateSchemasCache();
    }

    @Override
    public void setColumnComment(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Optional<String> comment)
    {
        delegate.setColumnComment(session, handle, column, comment);
        invalidateColumnsCache(handle.asPlainTable().getSchemaTableName());
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        delegate.addColumn(session, handle, column);
        invalidateColumnsCache(handle.asPlainTable().getSchemaTableName());
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        delegate.dropColumn(session, handle, column);
        invalidateColumnsCache(handle.asPlainTable().getSchemaTableName());
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        delegate.renameColumn(session, handle, jdbcColumn, newColumnName);
        invalidateColumnsCache(handle.asPlainTable().getSchemaTableName());
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        delegate.renameTable(session, handle, newTableName);
        invalidateTableCaches(handle.asPlainTable().getSchemaTableName());
        invalidateTableCaches(newTableName);
    }

    @Override
    public void setTableProperties(ConnectorSession session, JdbcTableHandle handle, Map<String, Object> properties)
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

    public void onDataChanged(SchemaTableName table)
    {
        invalidateCache(statisticsCache, key -> key.tableHandle.references(table));
    }

    /**
     * @deprecated {@link JdbcTableHandle}  is not a good representation of the table. For example, we don't want
     * to distinguish between "a plan table" and "table with selected columns", or "a table with a constraint" here.
     * Use {@link #onDataChanged(SchemaTableName)}, which avoids these ambiguities.
     */
    @Deprecated
    public void onDataChanged(JdbcTableHandle handle)
    {
        invalidateCache(statisticsCache, key -> key.tableHandle.equals(handle));
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
        tableHandleCache.invalidateAll();
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
        invalidateCache(tableHandleCache, key -> key.tableName.equals(schemaTableName));
        invalidateCache(tableNamesCache, key -> key.schemaName.equals(Optional.of(schemaTableName.getSchemaName())));
        invalidateCache(statisticsCache, key -> key.tableHandle.references(schemaTableName));
    }

    private void invalidateColumnsCache(SchemaTableName table)
    {
        invalidateCache(columnsCache, key -> key.table.equals(table));
    }

    @VisibleForTesting
    CacheStats getTableNamesCacheStats()
    {
        return tableNamesCache.stats();
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

    private static <T, V> void invalidateCache(Cache<T, V> cache, Predicate<T> filterFunction)
    {
        Set<T> cacheKeys = ((EvictableCache<T, V>) cache).keySet().stream()
                .filter(filterFunction)
                .collect(toImmutableSet());

        cache.invalidateAll(cacheKeys);
    }

    private static final class ColumnsCacheKey
    {
        private final IdentityCacheKey identity;
        private final SchemaTableName table;
        private final Map<String, Object> sessionProperties;

        private ColumnsCacheKey(IdentityCacheKey identity, Map<String, Object> sessionProperties, SchemaTableName table)
        {
            this.identity = requireNonNull(identity, "identity is null");
            this.sessionProperties = ImmutableMap.copyOf(requireNonNull(sessionProperties, "sessionProperties is null"));
            this.table = requireNonNull(table, "table is null");
        }

        public IdentityCacheKey getIdentity()
        {
            return identity;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ColumnsCacheKey that = (ColumnsCacheKey) o;
            return Objects.equals(identity, that.identity) &&
                    Objects.equals(sessionProperties, that.sessionProperties) &&
                    Objects.equals(table, that.table);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(identity, sessionProperties, table);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("identity", identity)
                    .add("sessionProperties", sessionProperties)
                    .add("table", table)
                    .toString();
        }
    }

    private static final class TableHandleCacheKey
    {
        private final IdentityCacheKey identity;
        private final SchemaTableName tableName;

        private TableHandleCacheKey(IdentityCacheKey identity, SchemaTableName tableName)
        {
            this.identity = requireNonNull(identity, "identity is null");
            this.tableName = requireNonNull(tableName, "tableName is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TableHandleCacheKey that = (TableHandleCacheKey) o;
            return Objects.equals(identity, that.identity) &&
                    Objects.equals(tableName, that.tableName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(identity, tableName);
        }
    }

    private static final class TableNamesCacheKey
    {
        private final IdentityCacheKey identity;
        private final Optional<String> schemaName;

        private TableNamesCacheKey(IdentityCacheKey identity, Optional<String> schemaName)
        {
            this.identity = requireNonNull(identity, "identity is null");
            this.schemaName = requireNonNull(schemaName, "schemaName is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TableNamesCacheKey that = (TableNamesCacheKey) o;
            return Objects.equals(identity, that.identity) &&
                    Objects.equals(schemaName, that.schemaName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(identity, schemaName);
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

    private static final class TableStatisticsCacheKey
    {
        // TODO depend on Identity when needed
        private final JdbcTableHandle tableHandle;
        private final TupleDomain<ColumnHandle> tupleDomain;

        private TableStatisticsCacheKey(JdbcTableHandle tableHandle, TupleDomain<ColumnHandle> tupleDomain)
        {
            this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
            this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TableStatisticsCacheKey that = (TableStatisticsCacheKey) o;
            return tableHandle.equals(that.tableHandle)
                    && tupleDomain.equals(that.tupleDomain);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableHandle, tupleDomain);
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
    public CacheStatsMBean getTableHandleCache()
    {
        return new CacheStatsMBean(tableHandleCache);
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
