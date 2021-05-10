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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.JdbcIdentity;
import io.trino.plugin.jdbc.mapping.IdentifierMappingModule.ForCachingIdentifierMapping;
import io.trino.spi.TrinoException;

import javax.inject.Inject;
import javax.inject.Provider;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class CachingIdentifierMapping
        implements IdentifierMapping
{
    private final Cache<JdbcIdentity, Map<String, String>> remoteSchemaNames;
    private final Cache<RemoteTableNameCacheKey, Map<String, String>> remoteTableNames;
    private final IdentifierMapping identifierMapping;
    private final Provider<BaseJdbcClient> baseJdbcClient;

    @Inject
    public CachingIdentifierMapping(
            MappingConfig mappingConfig,
            @ForCachingIdentifierMapping IdentifierMapping identifierMapping,
            Provider<BaseJdbcClient> baseJdbcClient)
    {
        requireNonNull(mappingConfig, "mappingConfig is null");
        CacheBuilder<Object, Object> remoteNamesCacheBuilder = CacheBuilder.newBuilder()
                .expireAfterWrite(mappingConfig.getCaseInsensitiveNameMatchingCacheTtl().toMillis(), MILLISECONDS);
        this.remoteSchemaNames = remoteNamesCacheBuilder.build();
        this.remoteTableNames = remoteNamesCacheBuilder.build();

        this.identifierMapping = requireNonNull(identifierMapping, "identifierMapping is null");
        this.baseJdbcClient = requireNonNull(baseJdbcClient, "baseJdbcClient is null");
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
    public String toRemoteSchemaName(JdbcIdentity identity, Connection connection, String schemaName)
    {
        requireNonNull(schemaName, "schemaName is null");
        verify(CharMatcher.forPredicate(Character::isUpperCase).matchesNoneOf(schemaName), "Expected schema name from internal metadata to be lowercase: %s", schemaName);

        try {
            Map<String, String> mapping = remoteSchemaNames.getIfPresent(identity);
            if (mapping != null && !mapping.containsKey(schemaName)) {
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
    public String toRemoteTableName(JdbcIdentity identity, Connection connection, String remoteSchema, String tableName)
    {
        requireNonNull(remoteSchema, "remoteSchema is null");
        requireNonNull(tableName, "tableName is null");
        verify(CharMatcher.forPredicate(Character::isUpperCase).matchesNoneOf(tableName), "Expected table name from internal metadata to be lowercase: %s", tableName);

        try {
            RemoteTableNameCacheKey cacheKey = new RemoteTableNameCacheKey(identity, remoteSchema);
            Map<String, String> mapping = remoteTableNames.getIfPresent(cacheKey);
            if (mapping != null && !mapping.containsKey(tableName)) {
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

    private Map<String, String> createSchemaMapping(Connection connection)
    {
        return baseJdbcClient.get().listSchemas(connection).stream()
                // will throw on collisions to avoid ambiguity
                .collect(toImmutableMap(identifierMapping::fromRemoteSchemaName, schemaName -> schemaName));
    }

    private Map<String, String> createTableMapping(Connection connection, String remoteSchema)
    {
        try (ResultSet resultSet = baseJdbcClient.get().getTables(connection, Optional.of(remoteSchema), Optional.empty())) {
            ImmutableMap.Builder<String, String> map = ImmutableMap.builder();
            while (resultSet.next()) {
                String remoteTableName = resultSet.getString("TABLE_NAME");
                map.put(identifierMapping.fromRemoteTableName(remoteSchema, remoteTableName), remoteTableName);
            }
            // will throw on collisions to avoid ambiguity
            return map.build();
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }
}
