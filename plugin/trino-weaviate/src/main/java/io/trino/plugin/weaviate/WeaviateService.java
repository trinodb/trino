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
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.plugin.weaviate.WeaviateErrorCode.WEAVIATE_SERVER_ERROR;
import static io.trino.plugin.weaviate.WeaviateMetadata.DEFAULT_SCHEMA;
import static io.trino.plugin.weaviate.WeaviateUtil.CREATED_AT;
import static io.trino.plugin.weaviate.WeaviateUtil.LAST_UPDATED_AT;
import static io.trino.plugin.weaviate.WeaviateUtil.METADATA_COLUMNS;
import static io.trino.plugin.weaviate.WeaviateUtil.UUID;
import static io.trino.plugin.weaviate.WeaviateUtil.VECTORS_COLUMN_NAME;
import static java.util.Objects.requireNonNull;

public class WeaviateService
{
    private final WeaviateClient weaviateClient;
    private final ConsistencyLevel consistencyLevel;
    private final OptionalInt pageSize;

    @Inject
    public WeaviateService(WeaviateConfig weaviateConfig)
    {
        requireNonNull(weaviateConfig, "weaviateConfig is null");

        Config.Custom config = new Config.Custom();
        config.scheme(weaviateConfig.getScheme());
        config.httpHost(weaviateConfig.getHttpHost());
        config.httpPort(weaviateConfig.getHttpPort());
        config.grpcHost(weaviateConfig.getGrpcHost());
        config.grpcPort(weaviateConfig.getGrpcPort());

        if (weaviateConfig.getTimeout().isPresent()) {
            long timeout = weaviateConfig.getTimeout().orElseThrow().toJavaTime().toSeconds();
            config.timeout((int) timeout);
        }

        if (weaviateConfig.getApiKey().isPresent()) {
            config.authentication(Authentication.apiKey(weaviateConfig.getApiKey().orElseThrow()));
        }
        else if (weaviateConfig.getAccessToken().isPresent() && weaviateConfig.getRefreshToken().isPresent()) {
            config.authentication(Authentication.bearerToken(
                    weaviateConfig.getAccessToken().orElseThrow(),
                    weaviateConfig.getRefreshToken().orElseThrow(),
                    weaviateConfig.getAccessTokenLifetime()
                            .map(lifetime -> lifetime.toJavaTime().toSeconds())
                            .orElse(0L)));
        }
        else if (weaviateConfig.getUsername().isPresent() && weaviateConfig.getPassword().isPresent()) {
            config.authentication(Authentication.resourceOwnerPassword(
                    weaviateConfig.getUsername().orElseThrow(),
                    weaviateConfig.getPassword().orElseThrow(),
                    weaviateConfig.getScopes()));
        }
        else if (weaviateConfig.getClientSecret().isPresent()) {
            config.authentication(Authentication.clientCredentials(
                    weaviateConfig.getClientSecret().orElseThrow(),
                    weaviateConfig.getScopes()));
        }

        this.consistencyLevel = weaviateConfig.getConsistencyLevel();
        this.pageSize = weaviateConfig.getPageSize();
        this.weaviateClient = new WeaviateClient(config.build());
    }

    List<String> listTenants()
    {
        try {
            return weaviateClient.collections.list().stream()
                    .filter(collection -> collection.multiTenancy().enabled())
                    .map(CollectionConfig::collectionName)
                    .flatMap(collectionName -> weaviateClient.collections.use(collectionName).tenants.list().stream())
                    .map(Tenant::name)
                    .toList();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    WeaviateTableHandle getTableHandle(String tableName)
    {
        requireNonNull(tableName, "tableName is null");

        try {
            return weaviateClient.collections.getConfig(resolveTableName(tableName))
                    .map(WeaviateService::newTableHandle)
                    .orElseThrow(() -> WeaviateErrorCode.tableNotFound(tableName));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    List<WeaviateTableHandle> listTableHandles()
    {
        try {
            return weaviateClient.collections.list().stream()
                    .map(WeaviateService::newTableHandle)
                    .toList();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    List<Alias> listAliases()
    {
        try {
            return weaviateClient.alias.list();
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
        return weaviateClient.collections.use(
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
            OffsetDateTime createdAt = Instant.ofEpochMilli(object.createdAt()).atOffset(ZoneOffset.UTC);
            metadata.put(CREATED_AT.name(), createdAt);
        }
        if (object.lastUpdatedAt() != null) {
            OffsetDateTime lastUpdatedAt = Instant.ofEpochMilli(object.lastUpdatedAt()).atOffset(ZoneOffset.UTC);
            metadata.put(LAST_UPDATED_AT.name(), lastUpdatedAt);
        }
        return metadata.buildOrThrow();
    }

    private static WeaviateTableHandle newTableHandle(CollectionConfig collection)
    {
        requireNonNull(collection, "collection is null");

        ImmutableList.Builder<WeaviateColumnHandle> columns = ImmutableList.builder();
        columns.addAll(METADATA_COLUMNS);
        if (!collection.vectors().isEmpty()) {
            columns.add(new WeaviateColumnHandle(collection.vectors()));
        }
        collection.properties().stream().map(WeaviateColumnHandle::new).forEach(columns::add);
        List<WeaviateColumnHandle> columnHandles = columns.build();

        return new WeaviateTableHandle(
                Optional.of(DEFAULT_SCHEMA),
                collection.collectionName(),
                collection.description(),
                OptionalLong.empty(),
                columnHandles);
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
