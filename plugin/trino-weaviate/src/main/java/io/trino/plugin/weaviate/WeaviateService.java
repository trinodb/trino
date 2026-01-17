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
package io.trino.plugin.weaviate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Floats;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.weaviate.client6.v1.api.Authentication;
import io.weaviate.client6.v1.api.Config;
import io.weaviate.client6.v1.api.WeaviateClient;
import io.weaviate.client6.v1.api.WeaviateException;
import io.weaviate.client6.v1.api.alias.Alias;
import io.weaviate.client6.v1.api.collections.CollectionConfig;
import io.weaviate.client6.v1.api.collections.CollectionHandle;
import io.weaviate.client6.v1.api.collections.WeaviateObject;
import io.weaviate.client6.v1.api.collections.pagination.Paginator;
import io.weaviate.client6.v1.api.collections.query.ConsistencyLevel;
import io.weaviate.client6.v1.api.collections.query.Metadata;
import io.weaviate.client6.v1.api.collections.tenants.Tenant;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.plugin.weaviate.WeaviateColumnHandle.CREATED_AT;
import static io.trino.plugin.weaviate.WeaviateColumnHandle.LAST_UPDATED_AT;
import static io.trino.plugin.weaviate.WeaviateColumnHandle.UUID;
import static io.trino.plugin.weaviate.WeaviateColumnHandle.VECTORS_COLUMN_NAME;
import static io.trino.plugin.weaviate.WeaviateErrorCode.WEAVIATE_SERVER_ERROR;
import static java.util.Objects.requireNonNull;

public class WeaviateService
{
    private final WeaviateClient w;
    private final ConsistencyLevel consistencyLevel;
    private final OptionalInt pageSize;

    @Inject
    public WeaviateService(WeaviateConfig config)
    {
        requireNonNull(config, "config is null");

        Config.Custom cfg = new Config.Custom();
        cfg.scheme(config.getScheme());
        cfg.httpHost(config.getHttpHost());
        cfg.httpPort(config.getHttpPort());
        cfg.grpcHost(config.getGrpcHost());
        cfg.grpcPort(config.getGrpcPort());

        if (config.getTimeout().isPresent()) {
            long timeout = config.getTimeout().orElseThrow().toJavaTime().toSeconds();
            cfg.timeout((int) timeout);
        }

        if (config.getApiKey().isPresent()) {
            cfg.authentication(Authentication.apiKey(config.getApiKey().orElseThrow()));
        }
        else if (config.getAccessToken().isPresent() && config.getRefreshToken().isPresent()) {
            cfg.authentication(Authentication.bearerToken(
                    config.getAccessToken().orElseThrow(),
                    config.getRefreshToken().orElseThrow(),
                    config.getAccessTokenLifetime()
                            .map(lifetime -> lifetime.toJavaTime().toSeconds())
                            .orElse(0L)));
        }
        else if (config.getUsername().isPresent() && config.getPassword().isPresent()) {
            cfg.authentication(Authentication.resourceOwnerPassword(
                    config.getUsername().orElseThrow(),
                    config.getPassword().orElseThrow(),
                    config.getScopes()));
        }
        else if (config.getClientSecret().isPresent()) {
            cfg.authentication(Authentication.clientCredentials(
                    config.getClientSecret().orElseThrow(),
                    config.getScopes()));
        }

        this.consistencyLevel = config.getConsistencyLevel();
        this.pageSize = config.getPageSize();
        this.w = new WeaviateClient(cfg.build());
    }

    List<String> listTenants()
    {
        ImmutableList.Builder<String> tenants = ImmutableList.<String>builder();
        try {
            List<CollectionConfig> collections = w.collections.list();

            for (var c : collections) {
                if (!c.multiTenancy().enabled()) {
                    continue;
                }
                w.collections.use(c.collectionName())
                        .tenants.list().stream()
                        .map(Tenant::name).forEach(tenants::add);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return tenants.build();
    }

    WeaviateTableHandle getTableHandle(String tableName)
    {
        requireNonNull(tableName, "tableName is null");

        try {
            return w.collections.getConfig(resolveTableName(tableName))
                    .map(WeaviateTableHandle::new)
                    .orElseThrow(() -> WeaviateErrorCode.tableNotFound(tableName));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    List<WeaviateTableHandle> listTableHandles()
    {
        try {
            return w.collections.list().stream()
                    .map(WeaviateTableHandle::new)
                    .toList();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    List<Alias> listAliases()
    {
        try {
            return w.alias.list();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    long countRows(WeaviateTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");

        try {
            return collectionHandle(tableHandle).size();
        }
        catch (WeaviateException e) {
            throw new TrinoException(WEAVIATE_SERVER_ERROR, e);
        }
    }

    Iterator<Map<String, Object>> getTableIterator(WeaviateTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");

        CollectionHandle<Map<String, Object>> collectionHandle = collectionHandle(tableHandle);
        Paginator<Map<String, Object>> paginator = collectionHandle.paginate(opt -> {
            if (pageSize.isPresent()) {
                opt.pageSize(pageSize.getAsInt());
            }
            return opt.returnMetadata(Metadata.ALL);
        });
        Stream<Map<String, Object>> stream = paginator.stream()
                .map(WeaviateService::collectColumns);

        if (tableHandle.limit().isPresent()) {
            stream = stream.limit(tableHandle.limit().getAsLong());
        }

        return stream.iterator();
    }

    private CollectionHandle<Map<String, Object>> collectionHandle(WeaviateTableHandle tableHandle)
    {
        return w.collections.use(
                resolveTableName(tableHandle.tableName()), h -> h
                        .consistencyLevel(consistencyLevel)
                        .tenant(tableHandle.tenantIgnoreDefault()));
    }

    private static Map<String, Object> collectColumns(WeaviateObject<Map<String, Object>> object)
    {
        ImmutableMap.Builder<String, Object> columns = ImmutableMap.builder();
        columns.putAll(collectMetadata(object));
        if (object.properties() != null) {
            columns.putAll(object.properties());
        }
        if (object.vectors() != null) {
            Map<String, Object> vectors = object.vectors().asMap().entrySet().stream()
                    .collect(Collectors.toUnmodifiableMap(
                            Map.Entry::getKey,
                            entry -> switch (entry.getValue()) {
                                case float[] single -> Floats.asList(single);
                                case float[][] multi -> Arrays.stream(multi)
                                        .map(Floats::asList)
                                        .toList();
                                case Object other -> throw WeaviateErrorCode.vectorTypeNotSupported(other);
                            }));
            columns.put(VECTORS_COLUMN_NAME, vectors);
        }
        return columns.buildOrThrow();
    }

    private static Map<String, Object> collectMetadata(WeaviateObject<?> object)
    {
        ImmutableMap.Builder<String, Object> metadata = ImmutableMap.builder();
        if (object.uuid() != null) {
            metadata.put(UUID.name(), object.uuid());
        }
        if (object.createdAt() != null) {
            metadata.put(CREATED_AT.name(), object.createdAt());
        }
        if (object.lastUpdatedAt() != null) {
            metadata.put(LAST_UPDATED_AT.name(), object.lastUpdatedAt());
        }
        return metadata.buildOrThrow();
    }

    private String resolveTableName(String tableNameNeedle)
    {
        return listTableHandles().stream()
                .map(WeaviateTableHandle::tableName)
                .filter(tableName -> tableName.equalsIgnoreCase(tableNameNeedle))
                .findFirst()
                .orElseThrow(() -> WeaviateErrorCode.tableNotFound(tableNameNeedle));
    }
}
