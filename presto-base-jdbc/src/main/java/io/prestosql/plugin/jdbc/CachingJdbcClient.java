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
package io.prestosql.plugin.jdbc;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.units.Duration;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.AggregateFunction;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.Type;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.util.Objects.requireNonNull;

public final class CachingJdbcClient
        implements JdbcClient
{
    private final JdbcClient delegate;
    private final boolean cacheMissing;

    private final LoadingCache<JdbcIdentity, Set<String>> schemaNamesCache;
    private final LoadingCache<TableNamesCacheKey, List<SchemaTableName>> tableNamesCache;
    private final LoadingCache<TableHandleCacheKey, Optional<JdbcTableHandle>> tableHandleCache;
    private final Cache<ColumnsCacheKey, List<JdbcColumnHandle>> columnsCache;

    @Inject
    public CachingJdbcClient(@StatsCollecting JdbcClient delegate, BaseJdbcConfig config)
    {
        this(delegate, config.getMetadataCacheTtl(), config.isCacheMissing());
    }

    public CachingJdbcClient(JdbcClient delegate, Duration metadataCachingTtl, boolean cacheMissing)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.cacheMissing = cacheMissing;

        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder()
                .expireAfterWrite(metadataCachingTtl.toMillis(), TimeUnit.MILLISECONDS);
        schemaNamesCache = cacheBuilder.build(CacheLoader.from(delegate::getSchemaNames));
        tableNamesCache = cacheBuilder.build(CacheLoader.from(key -> delegate.getTableNames(key.identity, key.schemaName)));
        tableHandleCache = cacheBuilder.build(CacheLoader.from(key -> delegate.getTableHandle(key.identity, key.tableName)));

        // TODO use LoadingCache for columns (columns depend on session and session cannot be used in cache key)
        columnsCache = cacheBuilder.build();
    }

    @Override
    public boolean schemaExists(JdbcIdentity identity, String schema)
    {
        // this method cannot be delegated as that would bypass the cache
        return getSchemaNames(identity).contains(schema);
    }

    @Override
    public Set<String> getSchemaNames(JdbcIdentity identity)
    {
        return get(schemaNamesCache, identity);
    }

    @Override
    public List<SchemaTableName> getTableNames(JdbcIdentity identity, Optional<String> schema)
    {
        return get(tableNamesCache, new TableNamesCacheKey(identity, schema));
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        if (tableHandle.getColumns().isPresent()) {
            return tableHandle.getColumns().get();
        }

        ColumnsCacheKey key = new ColumnsCacheKey(JdbcIdentity.from(session), tableHandle.getSchemaTableName());
        List<JdbcColumnHandle> columns = columnsCache.getIfPresent(key);
        if (columns != null) {
            return columns;
        }
        columns = delegate.getColumns(session, tableHandle);
        columnsCache.put(key, columns);
        return columns;
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        return delegate.toPrestoType(session, connection, typeHandle);
    }

    @Override
    public List<ColumnMapping> getColumnMappings(ConnectorSession session, List<JdbcTypeHandle> typeHandles)
    {
        return delegate.getColumnMappings(session, typeHandles);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        return delegate.toWriteMapping(session, type);
    }

    @Override
    public boolean supportsGroupingSets()
    {
        return delegate.supportsGroupingSets();
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
    public Connection getConnection(JdbcIdentity identity, JdbcSplit split)
            throws SQLException
    {
        return delegate.getConnection(identity, split);
    }

    @Override
    public void abortReadConnection(Connection connection)
            throws SQLException
    {
        delegate.abortReadConnection(connection);
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle table, List<JdbcColumnHandle> columns)
            throws SQLException
    {
        return delegate.buildSql(session, connection, split, table, columns);
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
    public Optional<JdbcTableHandle> getTableHandle(JdbcIdentity identity, SchemaTableName schemaTableName)
    {
        TableHandleCacheKey key = new TableHandleCacheKey(identity, schemaTableName);
        Optional<JdbcTableHandle> cachedTableHandle = tableHandleCache.getIfPresent(key);
        //noinspection OptionalAssignedToNull
        if (cachedTableHandle != null) {
            return cachedTableHandle;
        }
        Optional<JdbcTableHandle> tableHandle = delegate.getTableHandle(identity, schemaTableName);
        if (tableHandle.isEmpty() || cacheMissing) {
            tableHandleCache.put(key, tableHandle);
        }
        return tableHandle;
    }

    @Override
    public void commitCreateTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        delegate.commitCreateTable(identity, handle);
        invalidateTablesCaches();
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        return delegate.beginInsertTable(session, tableHandle, columns);
    }

    @Override
    public void finishInsertTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        delegate.finishInsertTable(identity, handle);
        invalidateTablesCaches();
    }

    @Override
    public void dropTable(JdbcIdentity identity, JdbcTableHandle jdbcTableHandle)
    {
        delegate.dropTable(identity, jdbcTableHandle);
        invalidateTablesCaches();
    }

    @Override
    public void rollbackCreateTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        delegate.rollbackCreateTable(identity, handle);
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle)
    {
        return delegate.buildInsertSql(handle);
    }

    @Override
    public Connection getConnection(JdbcIdentity identity, JdbcOutputTableHandle handle)
            throws SQLException
    {
        return delegate.getConnection(identity, handle);
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
        return delegate.getTableStatistics(session, handle, tupleDomain);
    }

    @Override
    public void createSchema(JdbcIdentity identity, String schemaName)
    {
        delegate.createSchema(identity, schemaName);
        invalidateSchemasCache();
    }

    @Override
    public void dropSchema(JdbcIdentity identity, String schemaName)
    {
        delegate.dropSchema(identity, schemaName);
        invalidateSchemasCache();
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        delegate.addColumn(session, handle, column);
        invalidateColumnsCache(JdbcIdentity.from(session), handle.getSchemaTableName());
    }

    @Override
    public void dropColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        delegate.dropColumn(identity, handle, column);
        invalidateColumnsCache(identity, handle.getSchemaTableName());
    }

    @Override
    public void renameColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        delegate.renameColumn(identity, handle, jdbcColumn, newColumnName);
        invalidateColumnsCache(identity, handle.getSchemaTableName());
    }

    @Override
    public void renameTable(JdbcIdentity identity, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        delegate.renameTable(identity, handle, newTableName);
        invalidateTablesCaches();
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        delegate.createTable(session, tableMetadata);
        invalidateTablesCaches();
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
    public Map<String, Object> getTableProperties(JdbcIdentity identity, JdbcTableHandle tableHandle)
    {
        return delegate.getTableProperties(identity, tableHandle);
    }

    private void invalidateSchemasCache()
    {
        schemaNamesCache.invalidateAll();
    }

    private void invalidateTablesCaches()
    {
        columnsCache.invalidateAll();
        tableHandleCache.invalidateAll();
        tableNamesCache.invalidateAll();
    }

    private void invalidateColumnsCache(JdbcIdentity identity, SchemaTableName table)
    {
        columnsCache.invalidate(new ColumnsCacheKey(identity, table));
    }

    private static final class ColumnsCacheKey
    {
        private final JdbcIdentity identity;
        private final SchemaTableName table;

        private ColumnsCacheKey(JdbcIdentity identity, SchemaTableName table)
        {
            this.identity = requireNonNull(identity, "identity is null");
            this.table = requireNonNull(table, "table is null");
        }

        public JdbcIdentity getIdentity()
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
                    Objects.equals(table, that.table);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(identity, table);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("identity", identity)
                    .add("table", table)
                    .toString();
        }
    }

    private static final class TableHandleCacheKey
    {
        private final JdbcIdentity identity;
        private final SchemaTableName tableName;

        private TableHandleCacheKey(JdbcIdentity identity, SchemaTableName tableName)
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
        private final JdbcIdentity identity;
        private final Optional<String> schemaName;

        private TableNamesCacheKey(JdbcIdentity identity, Optional<String> schemaName)
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

    private static <K, V> V get(LoadingCache<K, V> cache, K key)
    {
        try {
            return cache.getUnchecked(key);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
    }
}
