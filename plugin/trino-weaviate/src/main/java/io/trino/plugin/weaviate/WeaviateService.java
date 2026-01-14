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
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.weaviate.client6.v1.api.Config;
import io.weaviate.client6.v1.api.WeaviateClient;
import io.weaviate.client6.v1.api.WeaviateException;
import io.weaviate.client6.v1.api.alias.Alias;
import io.weaviate.client6.v1.api.collections.CollectionConfig;
import io.weaviate.client6.v1.api.collections.CollectionHandle;
import io.weaviate.client6.v1.api.collections.Vectors;
import io.weaviate.client6.v1.api.collections.WeaviateObject;
import io.weaviate.client6.v1.api.collections.query.ConsistencyLevel;
import io.weaviate.client6.v1.api.collections.query.Metadata;
import io.weaviate.client6.v1.api.collections.query.QueryResponse;
import io.weaviate.client6.v1.api.collections.tenants.Tenant;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.trino.plugin.weaviate.WeaviateColumnHandle.CREATED_AT;
import static io.trino.plugin.weaviate.WeaviateColumnHandle.LAST_UPDATED_AT;
import static io.trino.plugin.weaviate.WeaviateColumnHandle.UUID;
import static io.trino.plugin.weaviate.WeaviateColumnHandle.makeName;
import static io.trino.plugin.weaviate.WeaviateColumnHandle.makeVectorName;
import static io.trino.plugin.weaviate.WeaviateErrorCode.WEAVIATE_SERVER_ERROR;

public class WeaviateService
{
    private final WeaviateClient w;
    private final ConsistencyLevel consistencyLevel;

    @Inject
    public WeaviateService(WeaviateConfig config)
    {
        Config.Custom cfg = new Config.Custom();
        cfg.scheme(config.getScheme());
        cfg.httpHost(config.getHttpHost());
        cfg.httpPort(config.getHttpPort());
        cfg.grpcHost(config.getGrpcHost());
        cfg.grpcPort(config.getGrpcPort());

        if (config.getTimeout() != null) {
            long timeout = config.getTimeout().toJavaTime().toSeconds();
            cfg.timeout((int) timeout);
        }

        this.w = new WeaviateClient(cfg.build());
        this.consistencyLevel = config.getConsistencyLevel();
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
                w.collections.use(c.collectionName()).tenants.list().stream().map(Tenant::name).forEach(tenants::add);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return tenants.build();
    }

    WeaviateTableHandle getTableHandle(String tableName)
    {
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

    WeaviateTable getTable(WeaviateTableHandle table)
    {
        CollectionHandle<Map<String, Object>> handle = w.collections.use(
                resolveTableName(table.tableName()), h -> h
                        .consistencyLevel(consistencyLevel)
                        .tenant(table.tenantIgnoreDefault()));

        QueryResponse<Map<String, Object>> response;
        try {
            response = handle.query.fetchObjects(
                    opt -> opt.returnMetadata(Metadata.ALL));
        }
        catch (WeaviateException e) {
            throw new TrinoException(WEAVIATE_SERVER_ERROR, e);
        }

        List<Map<String, Object>> values = response.objects().stream()
                .map(object -> {
                    ImmutableMap.Builder<String, Object> columns = ImmutableMap.builder();
                    columns.putAll(collectMetadata(object));
                    if (object.properties() != null) {
                        columns.putAll(object.properties());
                    }
                    if (object.vectors() != null) {
                        columns.putAll(collectVectors(object.vectors()));
                    }
                    return (Map<String, Object>) columns.buildOrThrow();
                })
                .toList();
        return new WeaviateTable(table.columns(), values);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> collectProperties(Map<String, Object> properties)
    {
        ImmutableMap.Builder<String, Object> flat = ImmutableMap.builder();

        properties.forEach((thisName, thisValue) -> {
            if (thisValue == null) {
                return;
            }
            if (thisValue instanceof Map<?, ?> object) {
                collectProperties((Map<String, Object>) object)
                        .forEach((nestedName, nestedValue) ->
                                flat.put(makeName(thisName, nestedName), nestedValue));
            }
            else {
                flat.put(thisName, thisValue);
            }
        });

        return flat.buildOrThrow();
    }

    private static Map<String, Object> collectVectors(Vectors vectors)
    {
        return vectors.asMap().entrySet().stream().collect(Collectors.toUnmodifiableMap(
                entry -> makeVectorName(entry.getKey()),
                Map.Entry::getValue));
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
