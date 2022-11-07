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
package io.trino.plugin.jdbc.mapping;

import com.google.common.base.CharMatcher;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.collect.cache.NonKeyEvictableCache;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.mapping.IdentifierMappingModule.ForCachingIdentifierMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCacheWithWeakInvalidateAll;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class CachingIdentifierMapping
        implements IdentifierMapping
{
    private final NonKeyEvictableCache<ConnectorIdentity, Mapping> remoteSchemaNames;
    private final NonKeyEvictableCache<RemoteTableNameCacheKey, Mapping> remoteTableNames;
    private final IdentifierMapping identifierMapping;
    private final Provider<BaseJdbcClient> baseJdbcClient;

    @Inject
    public CachingIdentifierMapping(
            MappingConfig mappingConfig,
            @ForCachingIdentifierMapping IdentifierMapping identifierMapping,
            Provider<BaseJdbcClient> baseJdbcClient)
    {
        CacheBuilder<Object, Object> remoteNamesCacheBuilder = CacheBuilder.newBuilder()
                .expireAfterWrite(mappingConfig.getCaseInsensitiveNameMatchingCacheTtl().toMillis(), MILLISECONDS);
        this.remoteSchemaNames = buildNonEvictableCacheWithWeakInvalidateAll(remoteNamesCacheBuilder);
        this.remoteTableNames = buildNonEvictableCacheWithWeakInvalidateAll(remoteNamesCacheBuilder);

        this.identifierMapping = requireNonNull(identifierMapping, "identifierMapping is null");
        this.baseJdbcClient = requireNonNull(baseJdbcClient, "baseJdbcClient is null");
    }

    public void flushCache()
    {
        // Note: this may not invalidate ongoing loads (https://github.com/trinodb/trino/issues/10512, https://github.com/google/guava/issues/1881)
        // This is acceptable, since this operation is invoked manually, and not relied upon for correctness.
        remoteSchemaNames.invalidateAll();
        remoteTableNames.invalidateAll();
    }

    @Override
    public String fromRemoteSchemaName(String remoteSchemaName)
    {
        return identifierMapping.fromRemoteSchemaName(remoteSchemaName);
    }

    @Override
    public String fromRemoteTableName(String remoteSchemaName, String remoteTableName)
    {
        return identifierMapping.fromRemoteTableName(remoteSchemaName, remoteTableName);
    }

    @Override
    public String fromRemoteColumnName(String remoteColumnName)
    {
        return identifierMapping.fromRemoteColumnName(remoteColumnName);
    }

    @Override
    public String toRemoteSchemaName(ConnectorIdentity identity, Connection connection, String schemaName)
    {
        requireNonNull(schemaName, "schemaName is null");
        verify(CharMatcher.forPredicate(Character::isUpperCase).matchesNoneOf(schemaName), "Expected schema name from internal metadata to be lowercase: %s", schemaName);

        try {
            Mapping mapping = remoteSchemaNames.getIfPresent(identity);
            if (mapping != null && !mapping.hasRemoteObject(schemaName)) {
                // This might be a schema that has just been created. Force reload.
                mapping = null;
            }
            if (mapping == null) {
                mapping = createSchemaMapping(connection);
                remoteSchemaNames.put(identity, mapping);
            }
            String remoteSchema = mapping.get(schemaName);
            if (remoteSchema != null) {
                return remoteSchema;
            }
        }
        catch (RuntimeException e) {
            throw new TrinoException(JDBC_ERROR, "Failed to find remote schema name: " + firstNonNull(e.getMessage(), e), e);
        }

        return identifierMapping.toRemoteSchemaName(identity, connection, schemaName);
    }

    @Override
    public String toRemoteTableName(ConnectorIdentity identity, Connection connection, String remoteSchema, String tableName)
    {
        requireNonNull(remoteSchema, "remoteSchema is null");
        requireNonNull(tableName, "tableName is null");
        verify(CharMatcher.forPredicate(Character::isUpperCase).matchesNoneOf(tableName), "Expected table name from internal metadata to be lowercase: %s", tableName);

        try {
            RemoteTableNameCacheKey cacheKey = new RemoteTableNameCacheKey(identity, remoteSchema);
            Mapping mapping = remoteTableNames.getIfPresent(cacheKey);
            if (mapping != null && !mapping.hasRemoteObject(tableName)) {
                // This might be a table that has just been created. Force reload.
                mapping = null;
            }
            if (mapping == null) {
                mapping = createTableMapping(connection, remoteSchema);
                remoteTableNames.put(cacheKey, mapping);
            }
            String remoteTable = mapping.get(tableName);
            if (remoteTable != null) {
                return remoteTable;
            }
        }
        catch (RuntimeException e) {
            throw new TrinoException(JDBC_ERROR, "Failed to find remote table name: " + firstNonNull(e.getMessage(), e), e);
        }

        return identifierMapping.toRemoteTableName(identity, connection, remoteSchema, tableName);
    }

    @Override
    public String toRemoteColumnName(Connection connection, String columnName)
    {
        return identifierMapping.toRemoteColumnName(connection, columnName);
    }

    private Mapping createSchemaMapping(Connection connection)
    {
        return createMapping(baseJdbcClient.get().listSchemas(connection), identifierMapping::fromRemoteSchemaName);
    }

    private Mapping createTableMapping(Connection connection, String remoteSchema)
    {
        return createMapping(
                getTables(connection, remoteSchema),
                remoteTableName -> identifierMapping.fromRemoteTableName(remoteSchema, remoteTableName));
    }

    private static Mapping createMapping(Collection<String> remoteNames, Function<String, String> mapping)
    {
        Map<String, String> map = new HashMap<>();
        Set<String> duplicates = new HashSet<>();
        for (String remoteName : remoteNames) {
            String name = mapping.apply(remoteName);
            if (duplicates.contains(name)) {
                continue;
            }
            if (map.put(name, remoteName) != null) {
                duplicates.add(name);
                map.remove(name);
            }
        }
        return new Mapping(map, duplicates);
    }

    private List<String> getTables(Connection connection, String remoteSchema)
    {
        try (ResultSet resultSet = baseJdbcClient.get().getTables(connection, Optional.of(remoteSchema), Optional.empty())) {
            ImmutableList.Builder<String> tableNames = ImmutableList.builder();
            while (resultSet.next()) {
                tableNames.add(resultSet.getString("TABLE_NAME"));
            }
            return tableNames.build();
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private static final class Mapping
    {
        private final Map<String, String> mapping;
        private final Set<String> duplicates;

        public Mapping(Map<String, String> mapping, Set<String> duplicates)
        {
            this.mapping = ImmutableMap.copyOf(requireNonNull(mapping, "mapping is null"));
            this.duplicates = ImmutableSet.copyOf(requireNonNull(duplicates, "duplicates is null"));
        }

        public boolean hasRemoteObject(String remoteName)
        {
            return mapping.containsKey(remoteName) || duplicates.contains(remoteName);
        }

        @Nullable
        public String get(String remoteName)
        {
            checkArgument(!duplicates.contains(remoteName), "Ambiguous name: %s", remoteName);
            return mapping.get(remoteName);
        }
    }
}
